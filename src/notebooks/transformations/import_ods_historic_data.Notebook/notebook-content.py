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

from pyspark.sql.functions import col

# ============================================================
# Generic Historical CSV Loader -> Existing Fabric Delta Table
# ============================================================

# -----------------------------
# Configuration
# -----------------------------

config = {
    "bcts_staging.annual_developed_volume_hist" : "Files/data/annual_developed_volume_hist.csv",
    "bcts_staging.annual_development_ready_hist" : "Files/data/annual_development_ready_hist.csv",
    "bcts_staging.licence_issued_advertised_main_hist" : "Files/data/licence_issued_advertised_main_hist.csv",
    "bcts_staging.currently_in_market_hist" : "Files/data/currently_in_market_hist.csv",
    "bcts_staging.licence_issued_with_unbilled_volume_hist" : "Files/data/licence_issued_with_unbilled_volume_hist.csv",
    "bcts_staging.licence_transfer_hist" : "Files/data/licence_transfer_hist.csv",
    "bcts_staging.roads_constructed_hist" : "Files/data/roads_constructed_hist.csv",
    "bcts_staging.roads_deactivated_hist" : "Files/data/roads_deactivated_hist.csv",
    "bcts_staging.roads_transferred_in_hist" : "Files/data/roads_transferred_in_hist.csv",
    "bcts_staging.roads_transferred_out_hist" : "Files/data/roads_transferred_out_hist.csv",
    "bcts_staging.timber_inventory_development_in_progress_hist" : "Files/data/timber_inventory_development_in_progress_hist.csv",
    "bcts_staging.timber_inventory_ready_to_develop_hist" : "Files/data/timber_inventory_ready_to_develop_hist.csv",
    "bcts_staging.timber_inventory_ready_to_sell_hist" : "Files/data/timber_inventory_ready_to_sell_hist.csv",
    "bcts_staging.tsl_summary_main_hist" : "Files/data/tsl_summary_main_hist.csv",
    "bcts_staging.volume_advertised_main_hist" : "Files/data/volume_advertised_main_hist.csv"
    # "bcts_staging.weighted_sale_term_hist" : "Files/data/weighted_sale_term_hist.csv",

}

for target_table, csv_path in config.items():

    # -----------------------------
    # Validate target table exists
    # -----------------------------
    if not spark.catalog.tableExists(target_table):
        raise Exception(f"Target table does not exist: {target_table}")


    # -----------------------------
    # Read CSV as STRING
    # Do NOT infer schema
    # -----------------------------
    df = (
        spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("multiLine", "true")
            .option("escape", '"')
            .option("quote", '"')
            .load(csv_path)
    )

    row_count = df.count()

    print(f"CSV loaded from: {csv_path}")
    print(f"Rows to load: {row_count}")

    # display(df)


    # -----------------------------
    # Get target table schema
    # -----------------------------
    target_df = spark.table(target_table)
    target_schema = target_df.schema

    print(f"\nTarget table: {target_table}")
    # print("Target schema:")
    # target_df.printSchema()


    # -----------------------------
    # Validate CSV columns
    # -----------------------------
    csv_columns = set(df.columns)

    # Only columns actually present in CSV need to be loaded.
    # This allows target tables to contain additional columns
    # such as report_start_date, report_end_date,
    # report_run_date and report_run_timestamp.

    load_fields = [
        field
        for field in target_schema
        if field.name in csv_columns
    ]

    if not load_fields:
        raise Exception(
            "No matching columns were found between the CSV "
            f"and target table {target_table}"
        )


    # -----------------------------
    # Cast CSV columns using
    # EXISTING target table datatypes
    # -----------------------------
    df_casted = df.select(
        *[
            col(field.name)
                .cast(field.dataType)
                .alias(field.name)
            for field in load_fields
        ]
    )


    # -----------------------------
    # Show resulting schema
    # -----------------------------
    # print("\nSchema after casting to target datatypes:")
    # df_casted.printSchema()

    # display(df_casted)


    # -----------------------------
    # Append historical data
    # -----------------------------
    (
        df_casted.write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table)
    )

    print(
        f"\nSUCCESS: {row_count} historical rows appended to "
        f"{target_table}"
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
