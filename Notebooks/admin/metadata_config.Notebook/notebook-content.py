# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2fc9f370-5018-46eb-a774-9ed60d52a11b",
# META       "default_lakehouse_name": "bcts_lakehouse",
# META       "default_lakehouse_workspace_id": "7a798b59-e76b-4af6-9ffb-fa0a2959519c",
# META       "known_lakehouses": [
# META         {
# META           "id": "2fc9f370-5018-46eb-a774-9ed60d52a11b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS bcts_metada")

spark.sql("""
CREATE TABLE IF NOT EXISTS bcts_metada.cdc_master_table_list
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
