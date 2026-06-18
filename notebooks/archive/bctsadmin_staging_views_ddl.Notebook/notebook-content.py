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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Creates bctsadmin tables in staging area
spark.sql("CREATE SCHEMA IF NOT EXISTS bcts_staging")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


metadata_df = spark.sql("""
SELECT
    target_schema_name,
    target_table_name
FROM bcts_metada.cdc_master_table_list
WHERE application_name = 'bctsadmin'
  AND target_schema_name = 'bctsadmin_replication'
  AND active_ind = 'Y'
ORDER BY replication_order, target_table_name
""")

metadata_df.collect()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_table = "bctsadmin_replication.BCTS_TENURE_BIDDER"
target_table = "bcts_staging.THE_BCTS_TENURE_BIDDER".lower()

df = spark.table(source_table)
columns = df.columns

# Build formatted column list (one column per line)
column_lines = []

for col in columns:
    column_lines.append(f"    {col.lower()},")   # comma at end for easy editing

# Remove comma from last column
column_lines[-1] = column_lines[-1].rstrip(',')

column_block = "\n".join(column_lines)

# Final SQL
sql = f"""
CREATE OR REPLACE TABLE {target_table} AS
SELECT
{column_block}
FROM {source_table}
"""

# Print (important for visibility / Git versioning)
print(sql)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add data cleaning logic here
sql = \
"""

CREATE OR REPLACE TABLE bcts_staging.the_bcts_tenure_bidder AS
SELECT
    client_number,
    forest_file_id,
    auction_date,
    bonus_bid,
    bonus_offer,
    deposit_amount,
    bid_deposit_type_code,
    ineligible_ind,
    ineligibility_comments,
    sale_awarded_ind,
    deposit_returned_ind,
    nmbr_months_of_registration,
    completed_forest_file_id,
    completed_file_status_date,
    completed_file_type_code,
    issued_forest_file_id,
    issued_file_status_date,
    issued_file_type_code,
    deposit_level_code,
    primary_rqmt_met_ind,
    financial_rqmt_met_ind,
    primary_rqmt_met_comment,
    financial_rqmt_met_comment,
    deposit_level_comment,
    electronic_bid_ind,
    update_timestamp,
    update_userid
FROM bctsadmin_replication.BCTS_TENURE_BIDDER
"""
# Execute
spark.sql(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_table = "bctsadmin_replication.BCTS_TIMBER_SALE"
target_table = "bcts_staging.THE_BCTS_TIMBER_SALE".lower()

df = spark.table(source_table)
columns = df.columns

# Build formatted column list (one column per line)
column_lines = []

for col in columns:
    column_lines.append(f"    {col.lower()},")   # comma at end for easy editing

# Remove comma from last column
column_lines[-1] = column_lines[-1].rstrip(',')

column_block = "\n".join(column_lines)

# Final SQL
sql = f"""
CREATE OR REPLACE TABLE {target_table} AS
SELECT
{column_block}
FROM {source_table}
"""

# Print (important for visibility / Git versioning)
print(sql)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add data cleaning logic here
sql = \
"""
CREATE OR REPLACE TABLE bcts_staging.the_bcts_timber_sale AS
SELECT
    forest_file_id,
    auction_date,
    bcts_category_code,
    bcts_rate_code,
    bcts_silv_obligation_code,
    lump_sum_category_code,
    no_sale_rationale_code,
    sale_location,
    sale_volume,
    upset_rate,
    total_upset_value,
    deposit_amount,
    sale_time,
    decked_timber_ind,
    lump_sum_ind,
    timber_sale_comment,
    approved_by,
    date_approved,
    legal_effective_date,
    update_userid,
    update_timestamp,
    third_party_deposit_name,
    third_party_deposit_ind
FROM bctsadmin_replication.BCTS_TIMBER_SALE

"""
# Execute
spark.sql(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_table = "bctsadmin_replication.NO_SALE_RATIONALE_CODE"
target_table = "bcts_staging.THE_NO_SALE_RATIONALE_CODE".lower()

df = spark.table(source_table)
columns = df.columns

# Build formatted column list (one column per line)
column_lines = []

for col in columns:
    column_lines.append(f"    {col.lower()},")   # comma at end for easy editing

# Remove comma from last column
column_lines[-1] = column_lines[-1].rstrip(',')

column_block = "\n".join(column_lines)

# Final SQL
sql = f"""
CREATE OR REPLACE TABLE {target_table} AS
SELECT
{column_block}
FROM {source_table}
"""

# Print (important for visibility / Git versioning)
print(sql)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add data cleaning logic here
sql = \
"""

CREATE OR REPLACE TABLE bcts_staging.the_no_sale_rationale_code AS
SELECT
    no_sale_rationale_code,
    description,
    effective_date,
    expiry_date,
    update_timestamp
FROM bctsadmin_replication.NO_SALE_RATIONALE_CODE
"""
# Execute
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
