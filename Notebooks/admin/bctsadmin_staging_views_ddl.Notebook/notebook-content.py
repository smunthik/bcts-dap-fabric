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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "33e8d584-f574-40c6-93a6-5b74b02fe30a",
# META       "known_warehouses": [
# META         {
# META           "id": "33e8d584-f574-40c6-93a6-5b74b02fe30a",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Creates bctsadmin views in staging area
spark.sql("CREATE SCHEMA IF NOT EXISTS bcts_staging")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


metadata_df = spark.sql("""
SELECT
    target_schema_name,
    target_table_name
FROM bcts_metada.cdc_master_table_list
WHERE application_name = 'bctsadmin'
  AND target_schema_name = 'bctsadmin_replication'
  AND active_ind = 'Y'
ORDER BY replication_order, target_table_name
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for row in metadata_df.collect():
    
    source_schema = row['target_schema_name'].strip()
    table_name = row['target_table_name'].strip()
    
    source_table = f"{source_schema}.{table_name}"
    target_view = f"bcts_staging.{table_name}"
    
    print(f"Creating view: {target_view}")
    
    spark.sql(f"""
        CREATE OR REPLACE VIEW {target_view} AS
        SELECT *
        FROM {source_table}
    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
