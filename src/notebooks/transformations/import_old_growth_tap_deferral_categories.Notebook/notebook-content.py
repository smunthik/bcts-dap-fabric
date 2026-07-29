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
csv_path = 'Files/data/DIP_ADV_TAP_Overlap_Type_2025_06_01_version1_short.csv'


schema = StructType([
    StructField("ubi", StringType(), False),
    StructField("ancient", StringType(), False),
    StructField("remnant", StringType(), False),
    StructField("big_treed", StringType(), False)
])


# Read CSV
df = (
    spark.read
         .format("csv")
         .option("header", "true")        # set false if no header
         .option("inferSchema", "true")   # or define schema explicitly (recommended for prod)
         .option("multiLine", "true") 
         .schema(schema)    # if needed
         .load(csv_path)
)

# Preview
display(df)


(df.write
    .format("delta")   # Fabric lakehouse tables use Delta
    .mode("overwrite")
    .saveAsTable("bcts_staging.old_growth_tap_deferral_categories")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql = \
"""
CREATE TABLE IF NOT EXISTS bcts_reporting.old_growth_tap_deferral_categories AS

SELECT * FROM bcts_staging.old_growth_tap_deferral_categories;
"""

spark.sql(sql)

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
