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

# spark.sql("""DROP TABLE IF EXISTS bcts_metadata.transformation_config""")

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
  has_start_date     STRING, 
  has_end_date       STRING,
  frequency          STRING,
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

# spark.sql("""
# DROP TABLE IF EXISTS bcts_metadata.run_log 
# """)

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

spark.sql(
"""
CREATE TABLE IF NOT EXISTS bcts_staging.bcbids_tsl_weekly_report
(
    `Status` STRING,
    `Opportunity ID` STRING,
    `Opportunity Description` STRING,
    `Commodities` STRING,
    `Type` STRING,
    `Issue Date
and Time
(Pacific Time)` STRING,
    `Closing Date
and Time
(Pacific Time)` STRING,
    `Ends in` STRING,
    `# of Amendments` STRING,
    `Last Updated` STRING,
    `Organization (Issued by)` STRING,
    `Organization (Issued for)` STRING,
    `Interested
Vendor
List
Available` STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);
"""
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE bcts_staging.dim_business_area_management_unit AS

WITH src AS (
    SELECT
        business_area_region_category,
        business_area_region,

        CASE
            WHEN business_area IN ('Prince George (TPG)', 'Stuart-Nechako (TSN)')
                THEN 'Omineca (TPG-TSN)'
            WHEN business_area = 'Seaward-tlasta (TST)'
                THEN 'Seaward-Tlasta (TST)'
            ELSE business_area
        END AS business_area,

        CASE
            WHEN business_area = 'Seaward-tlasta (TST)'
                THEN 'Seaward-Tlasta (TST)'
            ELSE business_area
        END AS business_area_reporting_unit,

        CONCAT(
            CASE
                WHEN business_area = 'Seaward-tlasta (TST)'
                    THEN 'Seaward-Tlasta (TST)'
                ELSE business_area
            END,
            '-',
            TRIM(manu_id)
        ) AS management_unit,

        CASE
            WHEN business_area_region = 'North Interior' THEN 1
            WHEN business_area_region = 'South Interior' THEN 2
            WHEN business_area_region = 'Coast' THEN 3
        END AS business_area_region_sort_order,

        CASE
            WHEN business_area_region_category = 'Interior' THEN 1
            ELSE 2
        END AS business_area_region_cat_sort_order

    FROM bcts_staging.annual_developed_volume_hist
)

SELECT
    business_area_region_category,
    business_area_region,
    business_area,
    business_area_reporting_unit,
    management_unit,
    business_area_region_sort_order,
    business_area_region_cat_sort_order

FROM (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY management_unit
            ORDER BY management_unit
        ) AS rn
    FROM src
) d

WHERE rn = 1
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Generate one row per date from 1900-01-01 through 2100-12-31
date_df = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.to_date(F.lit("1900-01-01")),
                F.to_date(F.lit("2100-12-31")),
                F.expr("INTERVAL 1 DAY")
            )
        ).alias("full_date")
    )
)

# Reusable date components
day_number = F.dayofmonth("full_date")
month_number = F.month("full_date")
year_number = F.year("full_date")

# Convert Spark weekday numbering from Sunday=1 to ISO Monday=1
iso_day_of_week = F.pmod(F.dayofweek("full_date") + F.lit(5), F.lit(7)) + F.lit(1)

first_day_of_month = F.trunc("full_date", "month")
first_day_iso_weekday = (
    F.pmod(F.dayofweek(first_day_of_month) + F.lit(5), F.lit(7)) + F.lit(1)
)

# ISO year is the calendar year containing the Thursday of the ISO week
iso_week_thursday = F.date_add(
    F.col("full_date"),
    F.lit(4) - iso_day_of_week
)

dim_date_df = (
    date_df

    # Date key
    .withColumn(
        "date_key",
        F.date_format("full_date", "yyyyMMdd").cast("int")
    )

    # Day level
    .withColumn("day", day_number.cast("int"))
    .withColumn(
        "day_suffix",
        F.when(
            day_number.isin(11, 12, 13),
            F.concat(day_number.cast("string"), F.lit("th"))
        )
        .when(
            (day_number % 10) == 1,
            F.concat(day_number.cast("string"), F.lit("st"))
        )
        .when(
            (day_number % 10) == 2,
            F.concat(day_number.cast("string"), F.lit("nd"))
        )
        .when(
            (day_number % 10) == 3,
            F.concat(day_number.cast("string"), F.lit("rd"))
        )
        .otherwise(
            F.concat(day_number.cast("string"), F.lit("th"))
        )
    )
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("day_of_week", iso_day_of_week.cast("int"))
    .withColumn("day_of_month", day_number.cast("int"))
    .withColumn("day_of_year", F.dayofyear("full_date").cast("int"))
    .withColumn(
        "is_weekend",
        iso_day_of_week.isin(6, 7)
    )

    # Week level
    .withColumn("week_of_year", F.weekofyear("full_date").cast("int"))
    .withColumn(
        "week_of_month",
        (
            F.floor(
                (
                    day_number +
                    first_day_iso_weekday -
                    F.lit(2)
                ) / F.lit(7)
            ) + F.lit(1)
        ).cast("int")
    )
    .withColumn("iso_week", F.weekofyear("full_date").cast("int"))
    .withColumn("iso_year", F.year(iso_week_thursday).cast("int"))

    # Month level
    .withColumn("month", month_number.cast("int"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("month_abbrev", F.date_format("full_date", "MMM"))
    .withColumn("month_year", F.date_format("full_date", "yyyy-MM"))
    .withColumn(
        "first_day_of_month",
        first_day_of_month.cast("date")
    )
    .withColumn(
        "last_day_of_month",
        F.last_day("full_date").cast("date")
    )

    # Quarter level
    .withColumn("quarter", F.quarter("full_date").cast("int"))
    .withColumn(
        "quarter_name",
        F.concat(F.lit("Q"), F.quarter("full_date").cast("string"))
    )
    .withColumn(
        "year_quarter",
        F.concat(
            year_number.cast("string"),
            F.lit("-Q"),
            F.quarter("full_date").cast("string")
        )
    )
    .withColumn(
        "first_day_of_quarter",
        F.make_date(
            year_number,
            ((F.quarter("full_date") - 1) * 3 + 1).cast("int"),
            F.lit(1)
        )
    )
    .withColumn(
        "last_day_of_quarter",
        F.date_sub(
            F.add_months(F.col("first_day_of_quarter"), 3),
            1
        )
    )

    # Year level
    .withColumn("year", year_number.cast("int"))
    .withColumn(
        "is_leap_year",
        (
            ((year_number % 4 == 0) & (year_number % 100 != 0)) |
            (year_number % 400 == 0)
        )
    )
    .withColumn(
        "first_day_of_year",
        F.make_date(year_number, F.lit(1), F.lit(1))
    )
    .withColumn(
        "last_day_of_year",
        F.make_date(year_number, F.lit(12), F.lit(31))
    )

    # Fiscal year: April through March
    .withColumn(
        "fiscal_year",
        F.when(
            month_number >= 4,
            year_number
        ).otherwise(
            year_number - 1
        ).cast("int")
    )
    .withColumn(
        "fiscal_quarter",
        F.when(month_number.between(4, 6), 1)
        .when(month_number.between(7, 9), 2)
        .when(month_number.between(10, 12), 3)
        .otherwise(4)
        .cast("int")
    )
    .withColumn(
        "fiscal_month",
        F.when(
            month_number >= 4,
            month_number - 3
        ).otherwise(
            month_number + 9
        ).cast("int")
    )

    # Holiday attributes
    .withColumn("is_holiday", F.lit(False).cast("boolean"))
    .withColumn("holiday_name", F.lit(None).cast("string"))

    # Audit
    .withColumn(
        "created_at",
        F.from_utc_timestamp(
            F.current_timestamp(),
            "America/Vancouver"
        )
    )

    # Final column order
    .select(
        "date_key",
        "full_date",
        "day",
        "day_suffix",
        "day_name",
        "day_of_week",
        "day_of_month",
        "day_of_year",
        "is_weekend",
        "week_of_year",
        "week_of_month",
        "iso_week",
        "iso_year",
        "month",
        "month_name",
        "month_abbrev",
        "month_year",
        "first_day_of_month",
        "last_day_of_month",
        "quarter",
        "quarter_name",
        "year_quarter",
        "first_day_of_quarter",
        "last_day_of_quarter",
        "year",
        "is_leap_year",
        "first_day_of_year",
        "last_day_of_year",
        "fiscal_year",
        "fiscal_quarter",
        "fiscal_month",
        "is_holiday",
        "holiday_name",
        "created_at"
    )
)

# Create or replace the Delta table
(
    dim_date_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bcts_reporting.dim_date")
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
