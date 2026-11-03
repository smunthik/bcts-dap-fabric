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

tables = [
    "annual_developed_volume_hist",
    "annual_development_ready_hist",
    "currently_in_market_hist",
    "licence_issued_advertised_main_hist",
    "licence_issued_with_unbilled_volume_hist",
    "licence_transfer_hist",
    "roads_constructed_hist",
    "roads_deactivated_hist",
    "roads_transferred_in_hist",
    "roads_transferred_out_hist",
    "timber_inventory_development_in_progress_hist",
    "timber_inventory_ready_to_develop_hist",
    "timber_inventory_ready_to_sell_hist",
    "tsl_summary_main_hist",
    "volume_advertised_main_hist",
    "weighted_sale_term_hist"
]

for table in tables:
    table_name = f"bcts_staging.{table}"

    print(f"Deleting data from {table_name}...")

    spark.sql(f"""
        DELETE FROM {table_name}
    """)

    print(f"✓ Deleted all rows from {table_name}")

print("✓ All BCTS staging history tables cleared successfully.")

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
