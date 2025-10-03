"""Databricks environment setup utilities."""

import os
import json
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession


def setup_databricks_environment(dbutils_instance=None):
    """Set up Databricks environment variables and return current user."""
    current_user = None
    try:
        w = WorkspaceClient()
        current_user = w.current_user.me().user_name

        if w.config.host:
            os.environ["DATABRICKS_HOST"] = w.config.host.rstrip("/")

        print(f"✓ Successfully authenticated as: {current_user}")

    except Exception:
        try:
            spark = SparkSession.getActiveSession()
            if spark:
                current_user = spark.sql("SELECT current_user()").collect()[0][0]
                workspace_url = spark.conf.get("spark.databricks.workspaceUrl", None)
                if workspace_url:
                    if not workspace_url.startswith("https://"):
                        workspace_url = f"https://{workspace_url}"
                    os.environ["DATABRICKS_HOST"] = workspace_url
        except Exception:
            print("Warning: Could not get user info from Spark")

    try:
        if dbutils_instance:
            api_token = (
                dbutils_instance.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .apiToken()
                .get()
            )
            os.environ["DATABRICKS_TOKEN"] = api_token
        else:
            print("Warning: dbutils not provided - DATABRICKS_TOKEN not set")
    except Exception as e:
        print(f"Warning: Could not set DATABRICKS_TOKEN: {e}")

    return current_user


def get_job_context(dbutils_instance=None):
    """Get job context information if running in a job."""
    try:
        spark = SparkSession.getActiveSession()
        if spark:
            job_id = spark.conf.get("spark.databricks.clusterUsageTags.jobId", None)
            if job_id:
                return job_id

        if dbutils_instance:
            context_json = (
                dbutils_instance.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .toJson()
            )
            context = json.loads(context_json)
            return context.get("tags", {}).get("jobId", None)

        return None
    except Exception:
        return None


def setup_widgets(dbutils):
    """Setup widgets for the notebook."""
    dbutils.widgets.dropdown("cleanup_control_table", "false", ["true", "false"])
    dbutils.widgets.dropdown("mode", "comment", ["comment", "pi"])
    dbutils.widgets.text("env", "")
    dbutils.widgets.text("catalog_name", "")
    dbutils.widgets.text("schema_name", "")
    dbutils.widgets.text("host_name", "")
    dbutils.widgets.text("table_names", "")
    dbutils.widgets.text("current_user", "")
    dbutils.widgets.text("apply_ddl", "")
    dbutils.widgets.text("columns_per_call", "")
    dbutils.widgets.text("sample_size", "")


def get_widgets(dbutils):
    """Get widgets for the notebook."""
    cleanup_control_table = dbutils.widgets.get("cleanup_control_table")
    mode = dbutils.widgets.get("mode")
    env = dbutils.widgets.get("env")
    catalog_name = dbutils.widgets.get("catalog_name")
    schema_name = dbutils.widgets.get("schema_name")
    host_name = dbutils.widgets.get("host_name")
    table_names = dbutils.widgets.get("table_names")
    current_user = dbutils.widgets.get("current_user")
    apply_ddl = dbutils.widgets.get("apply_ddl")
    columns_per_call = dbutils.widgets.get("columns_per_call")
    sample_size = dbutils.widgets.get("sample_size")
    notebook_variables = {
        "cleanup_control_table": cleanup_control_table,
        "mode": mode,
        "env": env,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "host_name": host_name,
        "table_names": table_names,
        "current_user": current_user,
        "apply_ddl": apply_ddl,
        "columns_per_call": columns_per_call,
        "sample_size": sample_size,
    }
    return {k: v for k, v in notebook_variables.items() if v is not None and v != ""}


def get_host_name(host_name=None):
    """Get host name from environment or parameter."""
    if not host_name:
        host_name = os.environ.get("DATABRICKS_HOST")
    print("host_name", host_name)
    print("DATABRICKS_HOST", os.environ.get("DATABRICKS_HOST"))
    return host_name


def get_current_user(dbutils_instance=None, current_user_param=None):
    """Get current user from parameter or detected user."""
    # Set up Databricks environment variables and get current user
    detected_user = setup_databricks_environment(dbutils_instance)
    if current_user_param and current_user_param.strip():
        current_user = current_user_param.strip()
        print(f"Using current_user parameter: {current_user}")
    else:
        current_user = detected_user
        print(f"Using detected current_user: {current_user}")
    return current_user


def setup_notebook_variables(dbutils):
    """Setup notebook variables."""
    notebook_variables = get_widgets(dbutils)
    print("notebook_variables", notebook_variables)
    job_id = get_job_context(dbutils)
    host_name = get_host_name()
    current_user = get_current_user(dbutils_instance=dbutils)
    notebook_variables["job_id"] = job_id
    notebook_variables["host_name"] = host_name
    notebook_variables["current_user"] = current_user
    return notebook_variables


# notebook_variables = {
#     "catalog_name": catalog_name,
#     "schema_name": schema_name,
#     "host_name": host_name,
#     "table_names": table_names,
#     "mode": mode,
#     "env": env,
#     "current_user": current_user,
#     "cleanup_control_table": cleanup_control_table,
#     "job_id": job_id,
# }
