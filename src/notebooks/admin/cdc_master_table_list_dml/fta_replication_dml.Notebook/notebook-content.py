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

# ### FTA

# CELL ********************

sql = \
"""
delete from bcts_metadata.cdc_master_table_list
where application_name = 'FTA'
"""

spark.sql(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Auto-generated metadata merges for Fabric
# Run this file in a Fabric notebook

metadata_rows = [
    {
        "source_schema_name": "dbp01_fta_sharing",
        "source_table_name": "sv_sale_method_code",
        "target_table_name": "SALE_METHOD_CODE",
        "customsql_query": """SELECT
    "SALE_METHOD_CODE",
    "DESCRIPTION",
    "EFFECTIVE_DATE",
    "EXPIRY_DATE",
    "UPDATE_TIMESTAMP"
FROM dbp01_fta_sharing.sv_sale_method_code"""
    },
    {
        "source_schema_name": "dbp01_corp_sharing",
        "source_table_name": "sv_tfl_number_code",
        "target_table_name": "TFL_NUMBER_CODE",
        "customsql_query": """SELECT
    "TFL_NUMBER",
    "DESCRIPTION",
    "EFFECTIVE_DATE",
    "EXPIRY_DATE",
    "UPDATE_TIMESTAMP"
FROM dbp01_corp_sharing.sv_tfl_number_code"""
    },
    {
        "source_schema_name": "dbp01_fta_sharing",
        "source_table_name": "sv_tsa_number_code",
        "target_table_name": "TSA_NUMBER_CODE",
        "customsql_query": """SELECT
    "TSA_NUMBER",
    "DESCRIPTION",
    "EFFECTIVE_DATE",
    "EXPIRY_DATE",
    "UPDATE_TIMESTAMP"
FROM dbp01_fta_sharing.sv_tsa_number_code"""
    },
    {
        "source_schema_name": "dbp01_fta_sharing",
        "source_table_name": "sv_tenure_term",
        "target_table_name": "TENURE_TERM",
        "customsql_query": """SELECT
    "FOREST_FILE_ID",
    "TENURE_TERM",
    "LEGAL_EFFECTIVE_DT",
    "INITIAL_EXPIRY_DT",
    "CURRENT_EXPIRY_DT",
    "TENURE_EXTEND_CNT",
    "TENR_EXTEND_RSN_ST",
    "ENTRY_USERID",
    "ENTRY_TIMESTAMP",
    "UPDATE_USERID",
    "UPDATE_TIMESTAMP",
    "REVISION_COUNT"
FROM dbp01_fta_sharing.sv_tenure_term"""
    },
    {
        "source_schema_name": "dbp01_fta_sharing",
        "source_table_name": "sv_timber_mark",
        "target_table_name": "TIMBER_MARK",
        "customsql_query": """SELECT
    "TIMBER_MARK",
    "FOREST_FILE_ID",
    "CUTTING_PERMIT_ID",
    "FOREST_DISTRICT",
    "GEOGRAPHIC_DISTRCT",
    "CASCADE_SPLIT_CODE",
    "QUOTA_TYPE_CODE",
    "DECIDUOUS_IND",
    "CATASTROPHIC_IND",
    "CROWN_GRANTED_IND",
    "CRUISE_BASED_IND",
    "CERTIFICATE",
    "HDBS_TIMBER_MARK",
    "VM_TIMBER_MARK",
    "TENURE_TERM",
    "BCAA_FOLIO_NUMBER",
    "ACTIVATED_USERID",
    "AMENDED_USERID",
    "DISTRICT_ADMN_ZONE",
    "GRANTED_ACQRD_DATE",
    "LANDS_REGION",
    "CROWN_GRANTED_ACQ_DESC",
    "MARK_STATUS_ST",
    "MARK_STATUS_DATE",
    "MARK_AMEND_DATE",
    "MARK_APPL_DATE",
    "MARK_CANCEL_DATE",
    "MARK_EXTEND_DATE",
    "MARK_EXTEND_RSN_CD",
    "MARK_EXTEND_COUNT",
    "MARK_ISSUE_DATE",
    "MARK_EXPIRY_DATE",
    "MARKNG_INSTRMNT_CD",
    "MARKING_METHOD_CD",
    "ENTRY_USERID",
    "ENTRY_TIMESTAMP",
    "UPDATE_USERID",
    "UPDATE_TIMESTAMP",
    "REVISION_COUNT",
    "SMALL_PATCH_SALVAGE_IND",
    "SALVAGE_TYPE_CODE"
FROM dbp01_fta_sharing.sv_timber_mark"""
    },
    {
        "source_schema_name": "dbp01_fta_sharing",
        "source_table_name": "sv_harvest_sale",
        "target_table_name": "HARVEST_SALE",
        "customsql_query": """SELECT
    "FOREST_FILE_ID",
    "SB_FUND_IND",
    "SALE_METHOD_CODE",
    "SALE_TYPE_CD",
    "PLANNED_SALE_DATE",
    "TENDER_OPENING_DT",
    "PLND_SB_CAT_CODE",
    "SOLD_SB_CAT_CODE",
    "TOTAL_BIDDERS",
    "LUMPSUM_BONUS_AMT",
    "CASH_SALE_EST_VOL",
    "CASH_SALE_TOT_DOL",
    "PAYMENT_METHOD_CD",
    "SALVAGE_IND",
    "SALE_VOLUME",
    "ADMIN_AREA_IND",
    "MINOR_FACILITY_IND",
    "BCTS_ORG_UNIT",
    "FTA_BONUS_BID",
    "FTA_BONUS_OFFER",
    "REVISION_COUNT",
    "ENTRY_USERID",
    "ENTRY_TIMESTAMP",
    "UPDATE_USERID",
    "UPDATE_TIMESTAMP"
FROM dbp01_fta_sharing.sv_harvest_sale"""
    },
    {
        "source_schema_name": "dbp01_fta_sharing",
        "source_table_name": "sv_sb_category_code",
        "target_table_name": "SB_CATEGORY_CODE",
        "customsql_query": """SELECT
    "SB_CATEGORY_CODE",
    "DESCRIPTION",
    "EFFECTIVE_DATE",
    "EXPIRY_DATE",
    "UPDATE_TIMESTAMP"
FROM dbp01_fta_sharing.sv_sb_category_code"""
    },
    {
        "source_schema_name": "dbp01_fta_sharing",
        "source_table_name": "sv_tenure_file_status_code",
        "target_table_name": "TENURE_FILE_STATUS_CODE",
        "customsql_query": """SELECT
    "TENURE_FILE_STATUS_CODE",
    "DESCRIPTION",
    "EFFECTIVE_DATE",
    "EXPIRY_DATE",
    "UPDATE_TIMESTAMP"
FROM dbp01_fta_sharing.sv_tenure_file_status_code"""
    },
    {
        "source_schema_name": "dbp01_corp_sharing",
        "source_table_name": "sv_org_unit",
        "target_table_name": "ORG_UNIT",
        "customsql_query": """SELECT
    "ORG_UNIT_NO",
    "ORG_UNIT_CODE",
    "ORG_UNIT_NAME",
    "LOCATION_CODE",
    "AREA_CODE",
    "TELEPHONE_NO",
    "ORG_LEVEL_CODE",
    "OFFICE_NAME_CODE",
    "ROLLUP_REGION_NO",
    "ROLLUP_REGION_CODE",
    "ROLLUP_DIST_NO",
    "ROLLUP_DIST_CODE",
    "EFFECTIVE_DATE",
    "EXPIRY_DATE",
    "UPDATE_TIMESTAMP"
FROM dbp01_corp_sharing.sv_org_unit"""
    },
    {
        "source_schema_name": "dbp01_fta_sharing",
        "source_table_name": "sv_forest_file_client",
        "target_table_name": "FOREST_FILE_CLIENT",
        "customsql_query": """SELECT
    "FOREST_FILE_CLIENT_SKEY",
    "FOREST_FILE_ID",
    "CLIENT_NUMBER",
    "CLIENT_LOCN_CODE",
    "FOREST_FILE_CLIENT_TYPE_CODE",
    "LICENSEE_START_DATE",
    "LICENSEE_END_DATE",
    "REVISION_COUNT",
    "ENTRY_USERID",
    "ENTRY_TIMESTAMP",
    "UPDATE_USERID",
    "UPDATE_TIMESTAMP"
FROM dbp01_fta_sharing.sv_forest_file_client"""
    },
    {
        "source_schema_name": "dbp01_fta_sharing",
        "source_table_name": "sv_mgmt_unit_type_code",
        "target_table_name": "MGMT_UNIT_TYPE_CODE",
        "customsql_query": """SELECT
    "MGMT_UNIT_TYPE_CODE",
    "DESCRIPTION",
    "EFFECTIVE_DATE",
    "EXPIRY_DATE",
    "UPDATE_TIMESTAMP"
FROM dbp01_fta_sharing.sv_mgmt_unit_type_code"""
    },
    {
        "source_schema_name": "dbp01_fta_sharing",
        "source_table_name": "sv_prov_forest_use",
        "target_table_name": "PROV_FOREST_USE",
        "customsql_query": """SELECT
    "FOREST_FILE_ID",
    "FILE_STATUS_ST",
    "FILE_STATUS_DATE",
    "FILE_TYPE_CODE",
    "FOREST_REGION",
    "BCTS_ORG_UNIT",
    "SB_FUNDED_IND",
    "DISTRICT_ADMIN_ZONE",
    "MGMT_UNIT_TYPE",
    "MGMT_UNIT_ID",
    "REVISION_COUNT",
    "ENTRY_USERID",
    "ENTRY_TIMESTAMP",
    "UPDATE_USERID",
    "UPDATE_TIMESTAMP",
    "FOREST_TENURE_GUID"
FROM dbp01_fta_sharing.sv_prov_forest_use"""
    }
]


for row in metadata_rows:
    # Escape any apostrophes before embedding the query in Spark SQL.
    customsql_query = row["customsql_query"].replace("'", "''")

    spark.sql(f"""
        MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
        USING (
            SELECT
                'FTA' AS business,
                'FTA' AS application_name,
                CAST(NULL AS STRING) AS custodian,
                '{row["source_schema_name"]}' AS source_schema_name,
                '{row["source_table_name"]}' AS source_table_name,
                'fta_replication' AS target_schema_name,
                '{row["target_table_name"]}' AS target_table_name,
                'Y' AS truncate_flag,
                CAST(NULL AS STRING) AS cdc_flag,
                CAST(NULL AS STRING) AS full_inc_flag,
                CAST(NULL AS STRING) AS cdc_column,
                'Y' AS active_ind,
                1 AS replication_order,
                CAST(NULL AS STRING) AS where_clause,
                'Y' AS customsql_ind,
                '{customsql_query}' AS customsql_query,
                'Oracle' AS replication_source
        ) AS src
        ON tgt.source_schema_name = src.source_schema_name
        AND tgt.source_table_name = src.source_table_name

        WHEN MATCHED THEN UPDATE SET
            tgt.business = src.business,
            tgt.application_name = src.application_name,
            tgt.custodian = src.custodian,
            tgt.target_schema_name = src.target_schema_name,
            tgt.target_table_name = src.target_table_name,
            tgt.truncate_flag = src.truncate_flag,
            tgt.cdc_flag = src.cdc_flag,
            tgt.full_inc_flag = src.full_inc_flag,
            tgt.cdc_column = src.cdc_column,
            tgt.active_ind = src.active_ind,
            tgt.replication_order = src.replication_order,
            tgt.where_clause = src.where_clause,
            tgt.customsql_ind = src.customsql_ind,
            tgt.customsql_query = src.customsql_query,
            tgt.replication_source = src.replication_source

        WHEN NOT MATCHED THEN INSERT (
            business,
            application_name,
            custodian,
            source_schema_name,
            source_table_name,
            target_schema_name,
            target_table_name,
            truncate_flag,
            cdc_flag,
            full_inc_flag,
            cdc_column,
            active_ind,
            replication_order,
            where_clause,
            customsql_ind,
            customsql_query,
            replication_source
        )
        VALUES (
            src.business,
            src.application_name,
            src.custodian,
            src.source_schema_name,
            src.source_table_name,
            src.target_schema_name,
            src.target_table_name,
            src.truncate_flag,
            src.cdc_flag,
            src.full_inc_flag,
            src.cdc_column,
            src.active_ind,
            src.replication_order,
            src.where_clause,
            src.customsql_ind,
            src.customsql_query,
            src.replication_source
        )
    """)

print(f"Completed {len(metadata_rows)} metadata MERGE operations.")

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
