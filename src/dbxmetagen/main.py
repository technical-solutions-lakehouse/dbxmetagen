"""Entry point to dbxmetagen generate metadata."""

import os
import mlflow
from pyspark.sql import SparkSession
from src.dbxmetagen.error_handling import validate_csv
from src.dbxmetagen.processing import (
    setup_ddl,
    create_tables,
    setup_queue,
    upsert_table_names_to_control_table,
    generate_and_persist_metadata,
    get_control_table,
)
from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.deterministic_pi import ensure_spacy_model
from src.dbxmetagen.benchmarking import log_token_usage


def get_dbr_version():
    dbr_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", None)
    if dbr_version:
        print(f"Databricks Runtime Version: {dbr_version}")
    else:
        print("DATABRICKS_RUNTIME_VERSION environment variable not found.")
    return dbr_version


def main(kwargs):
    """Main function to generate metadata."""

    spark = SparkSession.builder.getOrCreate()
    mlflow.openai.autolog()

    notebook_path = kwargs.get("notebook_path")
    experiment_name = (
        notebook_path
        if notebook_path
        else (
            mlflow.get_experiment(mlflow.active_run().info.experiment_id).name
            if mlflow.active_run()
            else None
        )
    )
    # TODO: this is returning the SPARK version, not the DBR version. Need to replace this with the DBR version.
    # Get Spark version in a serverless-compatible way
    dbr_version = None
    # try:
    #     # Try traditional approach first (works on regular clusters)
    #     spark_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    #     print(f"Using traditional spark version: {spark_version}")
    # except Exception as e:
    #     # Fallback for serverless compute
    #     print(
    #         f"Traditional spark version lookup failed (likely serverless): {str(e)[:100]}..."
    #     )

    #     # On serverless, Excel is not supported due to networking restrictions
    #     # Use version string without "ml" to trigger Excel blocking in compatibility check
    #     spark_version = spark.version
    #     print(f"Using serverless fallback: {spark_version} (Excel disabled)")
    dbr_version = get_dbr_version()
    if not validate_csv("./metadata_overrides.csv"):
        raise Exception(
            """Invalid metadata_overrides.csv file. Please check the format of 
            your metadata_overrides configuration file..."""
        )

    config = MetadataConfig(**kwargs)
    if config.include_deterministic_pi and config.mode == "pi":
        ensure_spacy_model(config.spacy_model_names)

    if "client" in dbr_version and "excel" in (
        config.ddl_output_format,
        config.review_output_file_type,
    ):
        raise ValueError(
            "Serverless runtime is supported, but Excel writes are not supported."
        )

    if "ml" not in dbr_version and "excel" in (
        config.ddl_output_format,
        config.review_output_file_type,
    ):
        raise ValueError(
            "Excel writes in dbxmetagen are not supported on standard runtimes. Please change your output file type to tsv or sql if appropriate."
        )

    print("DATABRICKS_HOST", os.environ.get("DATABRICKS_HOST"))
    print("DEBUG 1")
    if not os.environ.get("DATABRICKS_HOST"):
        os.environ["DATABRICKS_HOST"] = config.base_url
    print("DEBUG 2")
    # os.environ["DATABRICKS_HOST"] = config.base_url
    setup_ddl(config)
    print("DEBUG 3")
    create_tables(config)
    print("DEBUG 4")
    config.table_names = setup_queue(config)
    print("DEBUG 5")
    if config.control_table:
        upsert_table_names_to_control_table(config.table_names, config)
    print("DEBUG 6")
    print("Running generate on...", config.table_names)
    generate_and_persist_metadata(config)
    print("DEBUG 7")
    # Get the unique temp table name for this specific job run
    temp_table = config.get_temp_metadata_log_table_name()
    print("DEBUG 8")
    control_table = get_control_table(config)
    control_table_full = f"{config.catalog_name}.{config.schema_name}.{control_table}"
    print("DEBUG 9")
    # Clean up this job's unique temp table using DROP (safe since each job has its own table)
    try:
        spark.sql(f"""DROP TABLE IF EXISTS {temp_table}""")
        print(f"Cleaned up temp table: {temp_table}")
    except Exception as e:
        print(f"Temp table cleanup failed: {e}")

    # For control table, use DELETE FROM since multiple jobs might share it (depending on config)
    try:
        if config.cleanup_control_table == "true" or config.cleanup_control_table:
            if config.job_id is not None:
                spark.sql(
                    f"""DELETE FROM {control_table_full} WHERE job_id = {config.job_id}"""
                )
            else:
                spark.sql(f"""DELETE FROM {control_table_full}""")
            print(f"Cleaned up control table: {control_table_full}")
    except Exception as e:
        # If table doesn't exist, that's fine - it means cleanup already happened
        print(f"Control table cleanup skipped (table may not exist): {e}")

    # Log token usage if benchmarking is enabled
    if experiment_name:
        log_token_usage(config, experiment_name)
