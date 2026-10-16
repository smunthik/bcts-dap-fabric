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

from pyspark.sql.types import *

# Path to CSV in Lakehouse Files
csv_path = "Files/data/licence_issued_ra_apportionment.csv"

schema = StructType([
    StructField("fiscal_year", StringType(), True),
    StructField("licence_issued", DecimalType(18, 2), True),
    StructField("ra", DecimalType(18, 2), True),
    StructField("full_apportionment", DecimalType(18, 2), True),
    StructField("actual_fte_count", DecimalType(18, 2), True),
    StructField("annual_developed_volume", DecimalType(18, 2), True),
    StructField("volume_harvested", DecimalType(18, 2), True)
])

# Read CSV
df = (
    spark.read
         .format("csv")
         .option("header", "true")
         .option("multiLine", "true")
         .schema(schema)
         .load(csv_path)
)

# Preview
display(df)

# Create/overwrite Fabric Delta table
(
    df.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("bcts_reporting.licence_issued_ra_apportionment")
)

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
