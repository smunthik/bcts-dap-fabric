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

# MARKDOWN ********************

# ### Import raw data to Lakehouse

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import current_date

# Path to CSV in Lakehouse Files
csv_path = "Files/data/cfs_data.csv"

schema = StructType([
    StructField("client", StringType(), True),
    StructField("responsibility", StringType(), True),
    StructField("service_line", StringType(), True),
    StructField("stob", StringType(), True),
    StructField("project", StringType(), True),
    StructField("ap_batch_name", StringType(), True),
    StructField("batch_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("creation_date", StringType(), True),
    StructField("description", StringType(), True),
    StructField("description_or_supplier_name", StringType(), True),
    StructField("distribution_supplier_name", StringType(), True),
    StructField("distribution_supplier_number", StringType(), True),
    StructField("document_number", StringType(), True),
    StructField("effective_date", StringType(), True),
    StructField("encumbrance_type", StringType(), True),
    StructField("gl_posted_date", StringType(), True),
    StructField("je_batch_name", StringType(), True),
    StructField("po_number", StringType(), True),
    StructField("requisition_number", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("supplier_location", StringType(), True),
    StructField("supplier_name", StringType(), True),
    StructField("supplier_number", StringType(), True),
    StructField("supplier_num_and_location", StringType(), True),
    StructField("gl_account", StringType(), True),
    StructField("period_name", StringType(), True),
    StructField("po_number_2", StringType(), True),
    StructField("row_id", StringType(), True),
    StructField("actual_amount", DecimalType(18, 2), True),
    StructField("encumbrance_amount", DecimalType(18, 2), True),
    StructField("je_header_id", LongType(), True),
    StructField("je_line_number", IntegerType(), True),
    StructField("balances", StringType(), True),
    StructField("je_line_status", StringType(), True)
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

# Add load/update date
df = df.withColumn("date_updated", current_date())

# Preview
display(df)

# Write to Fabric Lakehouse Delta table
(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable("bcts_staging.financial_transactions_raw")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Clean the data and replace the fiscal year rows in the _hist table

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

raw_table = "bcts_staging.financial_transactions_raw"
hist_table = "bcts_staging.financial_transactions_hist"


# ============================================================
# 1. Read raw transactions
# ============================================================

raw_df = spark.table(raw_table)


# ============================================================
# 2. Verify the required raw columns
# ============================================================

required_columns = [
    "client",
    "period_name",
    "project",
    "responsibility",
    "service_line",
    "stob",
    "actual_amount",
    "effective_date",
    "creation_date",
    "encumbrance_amount",
    "encumbrance_type",
    "batch_name"
]

missing_columns = [
    column_name
    for column_name in required_columns
    if column_name not in raw_df.columns
]

if missing_columns:
    raise ValueError(
        "Missing required raw columns: "
        + ", ".join(missing_columns)
    )


# ============================================================
# 3. Select the same columns selected by Access
# ============================================================

transformed_df = raw_df.select(
    F.col("client"),
    F.col("period_name").alias("period_name_raw"),
    F.col("project"),
    F.col("responsibility"),
    F.col("service_line"),
    F.col("stob"),
    F.col("actual_amount"),
    F.col("effective_date"),
    F.col("creation_date"),
    F.col("encumbrance_amount"),
    F.col("encumbrance_type"),
    F.col("batch_name")
)


# ============================================================
# 4. Convert blank strings to null
#
# Equivalent to:
# IIf(arFieldValue(i) = "", Null, arFieldValue(i))
# ============================================================

string_columns = [
    field.name
    for field in transformed_df.schema.fields
    if field.dataType.simpleString() == "string"
]

for column_name in string_columns:
    transformed_df = transformed_df.withColumn(
        column_name,
        F.when(
            F.trim(F.col(column_name)) == "",
            F.lit(None)
        ).otherwise(F.trim(F.col(column_name)))
    )


# ============================================================
# 5. Apply Access Project eligibility rule
#
# Original Access logic:
# Left(Right(Project, 5), 1) = "A" or "B"
# ============================================================

transformed_df = (
    transformed_df
    .withColumn(
        "project_last_5",
        F.substring(
            F.trim(F.col("project")),
            -5,
            5
        )
    )
    .withColumn(
        "project_type",
        F.upper(
            F.substring(
                F.col("project_last_5"),
                1,
                1
            )
        )
    )
    .filter(
        F.col("project_type").isin("A", "B")
    )
    .drop(
        "project_last_5",
        "project_type"
    )
)


# ============================================================
# 6. Convert period from YY-Mon to Mon-YY
#
# 27-Aug -> Aug-27
# ============================================================

transformed_df = (
    transformed_df
    .withColumn(
        "period_name_raw",
        F.trim(F.col("period_name_raw"))
    )
    .withColumn(
        "fiscal_year",
        F.split(F.col("period_name_raw"), "-").getItem(1)
    )
    .withColumn(
        "period_month",
        F.initcap(
            F.split(F.col("period_name_raw"), "-").getItem(0)
        )
    )
    .withColumn(
        "period_name",
        F.concat_ws(
            "-",
            F.col("period_month"),
            F.col("fiscal_year")
        )
    )
    .drop("period_name_raw", "period_month")
)


# ============================================================
# 7. Standardize values and add load date
# ============================================================

transformed_df = (
    transformed_df
    .withColumn(
        "actual_amount",
        F.col("actual_amount").cast(DecimalType(18, 2))
    )
    .withColumn(
        "encumbrance_amount",
        F.col("encumbrance_amount").cast(DecimalType(18, 2))
    )
    .withColumn("date_updated", F.current_date())
)


# ============================================================
# 8. Arrange final history columns
# ============================================================

transformed_df = transformed_df.select(
    "client",
    "period_name",
    "project",
    "responsibility",
    "service_line",
    "stob",
    "actual_amount",
    "effective_date",
    "creation_date",
    "encumbrance_amount",
    "encumbrance_type",
    "batch_name",
    "fiscal_year",
    "date_updated"
)


# ============================================================
# 9. Validate fiscal year before modifying history
# ============================================================

invalid_period_count = (
    transformed_df
    .filter(
        ~F.col("fiscal_year").rlike(r"^\d{2}$")
        | F.col("period_name").isNull()
    )
    .count()
)

if invalid_period_count > 0:
    raise ValueError(
        f"Transformation stopped: {invalid_period_count} row(s) "
        "have an invalid period. Expected YY-Mon, such as 27-Aug."
    )

incoming_fiscal_years = [
    row["fiscal_year"]
    for row in (
        transformed_df
        .select("fiscal_year")
        .distinct()
        .orderBy("fiscal_year")
        .collect()
    )
]

if not incoming_fiscal_years:
    raise ValueError(
        "No eligible transaction rows remain after applying "
        "the Access Project filter."
    )

print(
    "Incoming fiscal year(s): "
    + ", ".join(incoming_fiscal_years)
)

print(
    "Eligible records to load: "
    + str(transformed_df.count())
)


# ============================================================
# 10. Create or refresh history
# ============================================================

if not spark.catalog.tableExists(hist_table):

    (
        transformed_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(hist_table)
    )

    print(f"Created {hist_table}")

else:

    hist_delta = DeltaTable.forName(spark, hist_table)

    hist_delta.delete(
        F.col("fiscal_year").isin(incoming_fiscal_years)
    )

    (
        transformed_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(hist_table)
    )

    print(
        f"Replaced fiscal-year data in {hist_table}: "
        + ", ".join(incoming_fiscal_years)
    )


# ============================================================
# 11. Validate loaded history
# ============================================================

display(
    spark.table(hist_table)
    .groupBy("fiscal_year", "period_name")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("actual_amount").alias("total_actual_amount"),
        F.sum("encumbrance_amount").alias(
            "total_encumbrance_amount"
        ),
        F.max("date_updated").alias("date_updated")
    )
    .orderBy("fiscal_year", "period_name")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Clean the _hist data and generate transaction summary

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

source_table = "bcts_staging.financial_transactions_hist"
target_table = "bcts_staging.financial_transactions_summary"


# ============================================================
# 1. Read historical financial transactions
# ============================================================

cfs_df = spark.table(source_table)


# ============================================================
# 2. Standardize fields used by the Access query
# ============================================================

cfs_df = (
    cfs_df
    .withColumn("project", F.trim(F.col("project")))
    .withColumn("service_line", F.trim(F.col("service_line")))
    .withColumn("stob", F.trim(F.col("stob")))
    .withColumn(
        "actual_amount",
        F.col("actual_amount").cast(DecimalType(18, 2))
    )
    .withColumn(
        "effective_date",
        F.coalesce(
            F.to_date(F.col("effective_date"), "dd-MMM-yyyy"),
            F.to_date(F.col("effective_date"), "yyyy-MM-dd"),
            F.to_date(F.col("effective_date"), "MM/dd/yyyy"),
            F.to_date(F.col("effective_date"), "M/d/yyyy")
        )
    )
)


# ============================================================
# 3. Reproduce: Right(Project, 5) Like 'B*'
#
# Take the last five characters and retain records where the
# resulting value starts with B.
# ============================================================

cfs_df = (
    cfs_df
    .withColumn(
        "project_last_5",
        F.upper(
            F.substring(
                F.trim(F.col("project")),
                -5,
                5
            )
        )
    )
    .filter(
        F.col("project_last_5").startswith("B")
    )
    .filter(
        F.col("actual_amount") != 0
    )
)

# ============================================================
# 4. Derive CFS_UBI from Project
#
# Replicates the Access getUBI_by_Project() function:
#   1. Take characters 3 and 4 from Project
#   2. Append Project from character 5 onward
#   3. Replace "I" with "1" from character 5 onward
#   4. Replace "O" with "0" in the resulting CFS_UBI
# ============================================================

cfs_df = (
    cfs_df
    .withColumn(
        "cfs_ubi",
        F.regexp_replace(
            F.concat(
                # Mid(stProject, 3, 2)
                F.substring(
                    F.col("project"),
                    3,
                    2
                ),

                # Replace(stProject, "I", "1", 5)
                F.regexp_replace(
                    F.substring(
                        F.col("project"),
                        5,
                        1000
                    ),
                    "I",
                    "1"
                )
            ),

            # Replace(..., "O", "0")
            "O",
            "0"
        )
    )
)

# ============================================================
# 5. Define service-line groups
# ============================================================

layout_service_lines = [
    "42467",
    "42469",
    "42472"
]

planning_service_lines = [
    "42420",
    "42421",
    "42424",
    "42425"
]

silviculture_service_lines = [
    "42550",
    "42551",
    "42552",
    "42553",
    "42554",
    "42556",
    "42558",
    "42559",
    "42579",
    "42580"
]

individual_service_lines = [
    "42420",
    "42421",
    "42430",
    "42467",
    "42468",
    "42469",
    "42470",
    "42471",
    "42472",
    "42501",
    "42503",
    "42504",
    "42506",
    "42507",
    "42520",
    "42521",
    "42524",
    "42550",
    "42552",
    "42553",
    "42554",
    "42556",
    "42558",
    "42559",
    "42579"
]


# ============================================================
# 6. Calculate first and last layout dates by CFS UBI
#
# Access conditions:
#   Service Line in 42467, 42469, 42472
#   Project's last five characters start with B
#   Actual Amount is not zero
# ============================================================

layout_dates_df = (
    cfs_df
    .filter(F.col("service_line").isin(layout_service_lines))
    .groupBy("cfs_ubi")
    .agg(
        F.min("effective_date").alias("first_layout_date"),
        F.max("effective_date").alias("last_layout_date")
    )
)


# ============================================================
# 7. Join layout dates back to all qualifying transactions
# ============================================================

joined_df = (
    cfs_df.alias("c")
    .join(
        layout_dates_df.alias("ld"),
        on="cfs_ubi",
        how="left"
    )
)


# ============================================================
# 8. Build aggregate expressions
# ============================================================

zero_amount = F.lit(0).cast(DecimalType(18, 2))

aggregate_expressions = [
    F.min("effective_date").alias("first_date"),

    F.max("effective_date").alias("last_date"),

    F.sum("actual_amount").alias("total_amount"),

    F.first(
        "first_layout_date",
        ignorenulls=True
    ).alias("first_layout_date"),

    F.sum(
        F.when(
            F.col("service_line").isin(layout_service_lines)
            & (F.col("effective_date") == F.col("first_layout_date")),
            F.col("actual_amount")
        ).otherwise(zero_amount)
    ).alias("first_layout_amount"),

    F.first(
        "last_layout_date",
        ignorenulls=True
    ).alias("last_layout_date"),

    F.sum(
        F.when(
            F.col("service_line").isin(layout_service_lines)
            & (F.col("effective_date") == F.col("last_layout_date")),
            F.col("actual_amount")
        ).otherwise(zero_amount)
    ).alias("last_layout_amount"),

    F.sum(
        F.when(
            F.col("service_line").isin(layout_service_lines)
            & ~F.col("stob").isin("7017", "7018"),
            F.col("actual_amount")
        ).otherwise(zero_amount)
    ).alias("layout_amount_no_7017_7018"),

    F.sum(
        F.when(
            F.col("service_line").isin(layout_service_lines)
            & (F.col("stob") == "7017"),
            F.col("actual_amount")
        ).otherwise(zero_amount)
    ).alias("layout_amount_7017"),

    F.sum(
        F.when(
            F.col("service_line").isin(layout_service_lines)
            & (F.col("stob") == "7018"),
            F.col("actual_amount")
        ).otherwise(zero_amount)
    ).alias("layout_amount_7018"),

    F.sum(
        F.when(
            F.col("service_line").isin(layout_service_lines),
            F.col("actual_amount")
        ).otherwise(zero_amount)
    ).alias("layout_amount"),

    F.sum(
        F.when(
            F.col("service_line").isin(planning_service_lines),
            F.col("actual_amount")
        ).otherwise(zero_amount)
    ).alias("planning_amount"),

    F.sum(
        F.when(
            F.col("service_line").isin(silviculture_service_lines),
            F.col("actual_amount")
        ).otherwise(zero_amount)
    ).alias("silviculture_amount")
]


# Add one aggregate for each individual service line
for service_line in individual_service_lines:
    aggregate_expressions.append(
        F.sum(
            F.when(
                F.col("service_line") == service_line,
                F.col("actual_amount")
            ).otherwise(zero_amount)
        ).alias(f"service_line_{service_line}")
    )


# ============================================================
# 9. Create one summarized row per CFS UBI
# ============================================================

summary_df = (
    joined_df
    .groupBy("cfs_ubi")
    .agg(*aggregate_expressions)
    .withColumn("date_updated", F.current_date())
    .orderBy("cfs_ubi")
)


# ============================================================
# 10. Preview results
# ============================================================

display(summary_df)


# ============================================================
# 11. Write the transformed summary table
#
# This is a derived table, so it can be fully rebuilt after the
# monthly history refresh.
# ============================================================

(
    summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Successfully refreshed {target_table}")

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
