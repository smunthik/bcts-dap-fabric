# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2fc9f370-5018-46eb-a774-9ed60d52a11b",
# META       "default_lakehouse_name": "BCTS_Lakehouse",
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

'''
Example Spark/SQL Notebook for data transformation
'''


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import col
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run the query

result_df = spark.sql("""
SELECT 
    CURRENT_TIMESTAMP() AS SnapshotTimestamp,
    COUNT(*) AS RowCount
FROM FTA_REPLICATION.FOREST_FILE_CLIENT;
""")

result_df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Create staging schema and table to store data
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS FTA_STAGING;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS FTA_STAGING.FOREST_FILE_CLIENT_RowCounts
# MAGIC (
# MAGIC     SnapshotTimestamp TIMESTAMP    NOT NULL,
# MAGIC     RowCount          BIGINT       NOT NULL
# MAGIC );
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write results to table in Lakehouse
result_df_cast = ( result_df .select( F.col("SnapshotTimestamp").cast("timestamp"), F.col("RowCount").cast("long") ) )
result_df.write.mode("append").saveAsTable("FTA_STAGING.FOREST_FILE_CLIENT_RowCounts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Display updated table from Lakehouse
# MAGIC select * from FTA_STAGING.FOREST_FILE_CLIENT_RowCounts

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


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
