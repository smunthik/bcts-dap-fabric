# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c35a1880-4d21-42b2-b055-983b7ac0563b",
# META       "default_lakehouse_name": "bcts_lakehouse",
# META       "default_lakehouse_workspace_id": "7a798b59-e76b-4af6-9ffb-fa0a2959519c",
# META       "known_lakehouses": [
# META         {
# META           "id": "c35a1880-4d21-42b2-b055-983b7ac0563b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS bcts_metadata")
spark.sql("CREATE SCHEMA IF NOT EXISTS bcts_staging")
spark.sql("CREATE SCHEMA IF NOT EXISTS bcts_reporting")

spark.sql("""
CREATE TABLE IF NOT EXISTS bcts_metadata.cdc_master_table_list
(
    business            STRING,
    application_name    STRING,
    custodian           STRING,
    source_schema_name  STRING,
    source_table_name   STRING,
    target_schema_name  STRING,
    target_table_name   STRING,
    truncate_flag       STRING,
    cdc_flag            STRING,
    full_inc_flag       STRING,
    cdc_column          STRING,
    active_ind          STRING,
    replication_order   INT,
    where_clause        STRING,
    customsql_ind       STRING,
    customsql_query     STRING,
    replication_source  STRING
)
USING DELTA
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""DROP TABLE IF EXISTS bcts_metadata.transformation_config""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bcts_metadata.transformation_config (
  report_name        STRING,
  sql_path           STRING,   -- e.g. sql/bidder_details.sql (relative to Files/)
  enabled_ind        STRING,   -- 'Y'/'N'
  target_table       STRING,
  target_schema      STRING,
  branch_name        STRING,
  execution_order    INT,      -- optional ordering hint
  depends_on         STRING,    -- optional: comma-separated report_names (e.g. 'bronze_done,bidder_silver')
  date_updated TIMESTAMP 
)
USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS bcts_metadata.run_log (
    run_id             STRING,
    pipeline_name      STRING,
    source_schema      STRING,
    source_table       STRING,
    report_name        STRING,
    status             STRING,     -- STARTED / SUCCESS / FAILED
    start_time         TIMESTAMP,
    end_time           TIMESTAMP,
    error_message      STRING
)
USING DELTA
"""
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
DROP TABLE IF EXISTS bcts_metadata.run_log 
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
