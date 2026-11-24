# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d6ba1d09-b75c-44bb-b0d8-cbaa9a71dd56",
# META       "default_lakehouse_name": "bcts_lakehouse",
# META       "default_lakehouse_workspace_id": "fa20a958-6604-405e-b2b8-3c53bd795387",
# META       "known_lakehouses": [
# META         {
# META           "id": "d6ba1d09-b75c-44bb-b0d8-cbaa9a71dd56"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC ALTER TABLE bcts_staging.timber_inventory_development_in_progress_hist
# MAGIC ADD COLUMNS (
# MAGIC     cfs_ubi STRING,
# MAGIC     first_date DATE,
# MAGIC     last_date DATE,
# MAGIC     total_amount DECIMAL(28,2),
# MAGIC     first_layout_date DATE,
# MAGIC     first_layout_amount DECIMAL(28,2),
# MAGIC     last_layout_date DATE,
# MAGIC     last_layout_amount DECIMAL(28,2),
# MAGIC     layout_amount_no_7017_7018 DECIMAL(28,2),
# MAGIC     layout_amount_7017 DECIMAL(28,2),
# MAGIC     layout_amount_7018 DECIMAL(28,2),
# MAGIC     layout_amount DECIMAL(28,2),
# MAGIC     planning_amount DECIMAL(28,2),
# MAGIC     silviculture_amount DECIMAL(28,2),
# MAGIC     service_line_42420 DECIMAL(28,2),
# MAGIC     service_line_42421 DECIMAL(28,2),
# MAGIC     service_line_42430 DECIMAL(28,2),
# MAGIC     service_line_42467 DECIMAL(28,2),
# MAGIC     service_line_42468 DECIMAL(28,2),
# MAGIC     service_line_42469 DECIMAL(28,2),
# MAGIC     service_line_42470 DECIMAL(28,2),
# MAGIC     service_line_42471 DECIMAL(28,2),
# MAGIC     service_line_42472 DECIMAL(28,2),
# MAGIC     service_line_42501 DECIMAL(28,2),
# MAGIC     service_line_42503 DECIMAL(28,2),
# MAGIC     service_line_42504 DECIMAL(28,2),
# MAGIC     service_line_42506 DECIMAL(28,2),
# MAGIC     service_line_42507 DECIMAL(28,2),
# MAGIC     service_line_42520 DECIMAL(28,2),
# MAGIC     service_line_42521 DECIMAL(28,2),
# MAGIC     service_line_42524 DECIMAL(28,2),
# MAGIC     service_line_42550 DECIMAL(28,2),
# MAGIC     service_line_42552 DECIMAL(28,2),
# MAGIC     service_line_42553 DECIMAL(28,2),
# MAGIC     service_line_42554 DECIMAL(28,2),
# MAGIC     service_line_42556 DECIMAL(28,2),
# MAGIC     service_line_42558 DECIMAL(28,2),
# MAGIC     service_line_42559 DECIMAL(28,2),
# MAGIC     service_line_42579 DECIMAL(28,2)
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
