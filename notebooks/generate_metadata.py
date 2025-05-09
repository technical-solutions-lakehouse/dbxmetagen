# Databricks notebook source
# MAGIC %md
# MAGIC # GenAI-Assisted Metadata Utility (a.k.a `dbxmetagen`)

# COMMAND ----------

# MAGIC %md
# MAGIC #`dbxmetagen` Overview
# MAGIC ### This is a utility to help generate high quality descriptions for tables and columns to enhance enterprise search and data governance, identify and classify PI, improve Databricks Genie performance for Text-2-SQL, and generally help curate a high quality metadata layer and data dictionary for enterprise data.
# MAGIC
# MAGIC While Databricks does offer [AI Generated Documentation](https://docs.databricks.com/en/comments/ai-comments.html), this is not sustainable at scale as a human must manually select and approve AI generated metadata. This utility, `dbxmetagen`, helps generate table and column descriptions at scale. Eventually Databricks utilities will undoubtedly be more flexible, but this solution accelerator can allow customers to close the gap in a customizable fashion until then.
# MAGIC
# MAGIC Please review the readme for full details and documentation.
# MAGIC
# MAGIC ###Disclaimer
# MAGIC AI generated comments are not always accurate and comment DDLs should be reviewed prior to modifying your tables. Databricks strongly recommends human review of AI-generated comments to check for inaccuracies. While the model has been guided to avoids generating harmful or inappropriate descriptions, you can mitigate this risk by setting up [AI Guardrails](https://docs.databricks.com/en/ai-gateway/index.html#ai-guardrails) in the AI Gateway where you connect your LLM.
# MAGIC
# MAGIC ###Solution Overview:
# MAGIC There are a few key sections in this notebook:
# MAGIC - Library installs and setup using the config referenced in `src/dbxmetagen/config.py`
# MAGIC - Function definitions for:
# MAGIC   - Retrieving table and column information from the list of tables provided in `table_names.csv`
# MAGIC   - Sampling data from those tables, with exponential backoff, to help generate more accurate metadata, especially for columns with categorical data, that will also indicate the structure of the data. This is particularly helpful for [Genie](https://www.databricks.com/product/ai-bi/genie). This sampling also checks for nulls.
# MAGIC   - Use of `Pydantic` to ensure that LLM metadata generation conforms to a particular format. 
# MAGIC   - Creation of a log table keeping track of tables read/modified
# MAGIC   - Creation of DDL scripts, one for each table, that have the DDL commands to `ALTER TABLE` to add comments to table and columns. This is to help integrate with your CI/CD processes, in case you do not have access in a production environment
# MAGIC - Application of the functions above to generate metadata and DDL for the list of tables provided in `notebooks/table_names.csv`

# COMMAND ----------

# MAGIC %md
# MAGIC # Library installs

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Library imports

# COMMAND ----------

import sys
sys.path.append('../')

# COMMAND ----------

import os
from src.dbxmetagen.prompts import Prompt, PIPrompt, CommentPrompt, PromptFactory
from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.metadata_generator import (PIResponse, CommentResponse, Response, MetadataGenerator, CommentGenerator, PIIdentifier, MetadataGeneratorFactory)
from src.dbxmetagen.processing import split_table_names, sanitize_email
from src.dbxmetagen.error_handling import exponential_backoff
from src.dbxmetagen.main import main

# COMMAND ----------

# MAGIC %md
# MAGIC # Set up widgets

# COMMAND ----------

dbutils.widgets.dropdown("mode", "pi", ["comment", "pi"])
dbutils.widgets.text("env", "")
dbutils.widgets.text("table_names", "")

# COMMAND ----------

table_names = dbutils.widgets.get("table_names")
mode = dbutils.widgets.get("mode")
env = dbutils.widgets.get("env")
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
notebook_variables = {
    "table_names": table_names,
    "mode": mode,
    "env": env,
    "current_user": current_user,
}
api_key=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
os.environ["DATABRICKS_TOKEN"]=api_key

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now run the code - this may take some time depending on how many tables/columns you are running and what your tokens/sec throughput on the LLM is.

# COMMAND ----------

main(notebook_variables)
