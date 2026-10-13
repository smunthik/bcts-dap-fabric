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

spark.conf.set(
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead",
    "LEGACY"
)

spark.conf.set(
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite",
    "LEGACY"
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE bcts_staging.fta_sale_method_code AS
SELECT * FROM fta_replication.SALE_METHOD_CODE
""")

spark.sql("""
CREATE OR REPLACE TABLE bcts_staging.fta_tfl_number_code AS
SELECT * FROM fta_replication.TFL_NUMBER_CODE
""")

spark.sql("""
CREATE OR REPLACE TABLE bcts_staging.fta_tsa_number_code AS
SELECT * FROM fta_replication.TSA_NUMBER_CODE
""")

spark.sql("""
CREATE OR REPLACE TABLE bcts_staging.fta_tenure_term AS
SELECT * FROM fta_replication.TENURE_TERM
""")

spark.sql("""
CREATE OR REPLACE TABLE bcts_staging.fta_timber_mark AS
SELECT * FROM fta_replication.TIMBER_MARK
""")

spark.sql("""
CREATE OR REPLACE TABLE bcts_staging.fta_harvest_sale AS
SELECT * FROM fta_replication.HARVEST_SALE
""")

spark.sql("""
CREATE OR REPLACE TABLE bcts_staging.fta_sb_category_code AS
SELECT * FROM fta_replication.SB_CATEGORY_CODE
""")

spark.sql("""
CREATE OR REPLACE TABLE bcts_staging.fta_tenure_file_status_code AS
SELECT * FROM fta_replication.TENURE_FILE_STATUS_CODE
""")

spark.sql("""
CREATE OR REPLACE TABLE bcts_staging.fta_org_unit AS
SELECT * FROM fta_replication.ORG_UNIT
""")

spark.sql("""
CREATE OR REPLACE TABLE bcts_staging.fta_forest_file_client AS
SELECT * FROM fta_replication.FOREST_FILE_CLIENT
""")

spark.sql("""
CREATE OR REPLACE TABLE bcts_staging.fta_mgmt_unit_type_code AS
SELECT * FROM fta_replication.MGMT_UNIT_TYPE_CODE
""")

spark.sql("""
CREATE OR REPLACE TABLE bcts_staging.fta_prov_forest_use AS
SELECT * FROM fta_replication.PROV_FOREST_USE
""")

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
