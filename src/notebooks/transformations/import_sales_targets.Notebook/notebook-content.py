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
from pyspark.sql.functions import current_timestamp

# Path to CSV in Lakehouse Files
csv_path = "Files/data/FY27_sales_targets.csv"

# Schema matching columns present in the CSV
schema = StructType([
    StructField("business_area_region_category", StringType(), True),
    StructField("business_area_region", StringType(), True),
    StructField("business_area", StringType(), True),

    StructField("q1_ytd_sales_target_volume", DecimalType(38, 18), True),
    StructField("q2_ytd_sales_target_volume", DecimalType(38, 18), True),
    StructField("q3_ytd_sales_target_volume", DecimalType(38, 18), True),
    StructField("total_fiscal_year_sales_target_volume", DecimalType(38, 18), True),

    StructField("rationalized_apportionment", DecimalType(38, 18), True),
    StructField("full_apportionment", DecimalType(38, 18), True),

    StructField("q1_ytd_sales_target_volume_cat_4", DecimalType(38, 18), True),
    StructField("q2_ytd_sales_target_volume_cat_4", DecimalType(38, 18), True),
    StructField("q3_ytd_sales_target_volume_cat_4", DecimalType(38, 18), True),
    StructField("total_fiscal_year_sales_target_volume_cat_4", DecimalType(38, 18), True),

    StructField("fiscal_year", IntegerType(), True)
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

# Add current timestamp
df = df.withColumn("date_updated", current_timestamp())

# Verify schema
df.printSchema()

# Preview
display(df)

# Overwrite data in existing Fabric Delta table
(
    df.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("bcts_staging.bcts_sales_targets_hist")
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
