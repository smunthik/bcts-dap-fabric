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

import json
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)
from pyspark.sql.functions import current_timestamp

# parameters are ingested at runtime

logArray = json.loads(logArray)

rows = []
for item in logArray:
    rows.append((
        run_id,
        pipeline_name,
        item.get("report_name"),
        item.get("status"),
        None,  # start_time
        None,  # end_time
        item.get("error_message"),
        item.get("source_schema"),
        item.get("source_table")
    ))

schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("pipeline_name", StringType(), True),
    StructField("report_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("error_message", StringType(), True),
    StructField("source_schema", StringType(), True),
    StructField("source_table", StringType(), True)
])

df = spark.createDataFrame(rows, schema=schema)

df = (
    df.withColumn("start_time", current_timestamp())
      .withColumn("end_time", current_timestamp())
)

df.write.mode("append").saveAsTable("bcts_metadata.run_log")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
