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

# ### BCTSADMIN


# CELL ********************

# Auto-generated metadata merges for Fabric
# Run this file in a Fabric notebook

spark.sql("""
    MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
    USING (
        SELECT
            'bctsadmin' AS business,
            'bctsadmin' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'the' AS source_schema_name,
            'BCTS_TENURE_BIDDER' AS source_table_name,
            'bctsadmin_replication' AS target_schema_name,
            'BCTS_TENURE_BIDDER' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CLIENT_NUMBER",
    "FOREST_FILE_ID",
    "AUCTION_DATE",
    "BONUS_BID",
    "BONUS_OFFER",
    "DEPOSIT_AMOUNT",
    "BID_DEPOSIT_TYPE_CODE",
    "INELIGIBLE_IND",
    "INELIGIBILITY_COMMENTS",
    "SALE_AWARDED_IND",
    "DEPOSIT_RETURNED_IND",
    "NMBR_MONTHS_OF_REGISTRATION",
    "COMPLETED_FOREST_FILE_ID",
    "COMPLETED_FILE_STATUS_DATE",
    "COMPLETED_FILE_TYPE_CODE",
    "ISSUED_FOREST_FILE_ID",
    "ISSUED_FILE_STATUS_DATE",
    "ISSUED_FILE_TYPE_CODE",
    "DEPOSIT_LEVEL_CODE",
    "PRIMARY_RQMT_MET_IND",
    "FINANCIAL_RQMT_MET_IND",
    "PRIMARY_RQMT_MET_COMMENT",
    "FINANCIAL_RQMT_MET_COMMENT",
    "DEPOSIT_LEVEL_COMMENT",
    "ELECTRONIC_BID_IND",
    "UPDATE_TIMESTAMP",
    "UPDATE_USERID"
FROM the.BCTS_TENURE_BIDDER' AS customsql_query,
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

spark.sql("""
    MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
    USING (
        SELECT
            'bctsadmin' AS business,
            'bctsadmin' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'the' AS source_schema_name,
            'BCTS_REGISTRANT' AS source_table_name,
            'bctsadmin_replication' AS target_schema_name,
            'BCTS_REGISTRANT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CLIENT_NUMBER",
    "CLIENT_LOCN_CODE",
    "ORG_UNIT_NO",
    "ORIGINAL_REGISTRATION_DATE",
    "REGISTRANT_EXPIRY_DATE",
    "REGISTRANT_DEACTIVATION_DATE",
    "BCTS_CATEGORY_CODE",
    "BCTS_CATEGORY_CHANGE_DATE",
    "BCTS_MILL_TYPE_CODE",
    "BCTS_PREVIOUS_CATEGORY_CODE",
    "DEBARKER_CHIPPER_IND",
    "DEBARKER_CHIPPER_EXEMPTTO_DATE",
    "DEBARKER_CHIPPER_EXEMPTION_IND",
    "REGISTRANT_COMMENTS",
    "DISQUALIFICATION_START_DATE",
    "DISQUALIFICATION_END_DATE",
    "DISQUALIFICATION_COMMENTS",
    "UPDATE_USERID",
    "UPDATE_TIMESTAMP"
FROM the.BCTS_REGISTRANT' AS customsql_query,
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

spark.sql("""
    MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
    USING (
        SELECT
            'bctsadmin' AS business,
            'bctsadmin' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'the' AS source_schema_name,
            'NO_SALE_RATIONALE_CODE' AS source_table_name,
            'bctsadmin_replication' AS target_schema_name,
            'NO_SALE_RATIONALE_CODE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "NO_SALE_RATIONALE_CODE",
    "DESCRIPTION",
    "EFFECTIVE_DATE",
    "EXPIRY_DATE",
    "UPDATE_TIMESTAMP"
FROM the.NO_SALE_RATIONALE_CODE' AS customsql_query,
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

spark.sql("""
    MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
    USING (
        SELECT
            'bctsadmin' AS business,
            'bctsadmin' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'the' AS source_schema_name,
            'BCTS_TIMBER_SALE' AS source_table_name,
            'bctsadmin_replication' AS target_schema_name,
            'BCTS_TIMBER_SALE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "FOREST_FILE_ID",
    "AUCTION_DATE",
    "BCTS_CATEGORY_CODE",
    "BCTS_RATE_CODE",
    "BCTS_SILV_OBLIGATION_CODE",
    "LUMP_SUM_CATEGORY_CODE",
    "NO_SALE_RATIONALE_CODE",
    "SALE_LOCATION",
    "SALE_VOLUME",
    "UPSET_RATE",
    "TOTAL_UPSET_VALUE",
    "DEPOSIT_AMOUNT",
    "SALE_TIME",
    "DECKED_TIMBER_IND",
    "LUMP_SUM_IND",
    "TIMBER_SALE_COMMENT",
    "APPROVED_BY",
    "DATE_APPROVED",
    "LEGAL_EFFECTIVE_DATE",
    "UPDATE_USERID",
    "UPDATE_TIMESTAMP",
    "THIRD_PARTY_DEPOSIT_NAME",
    "THIRD_PARTY_DEPOSIT_IND"
FROM the.BCTS_TIMBER_SALE' AS customsql_query,
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

spark.sql("""
    MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
    USING (
        SELECT
            'bctsadmin' AS business,
            'bctsadmin' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'the' AS source_schema_name,
            'BCTS_CATEGORY_CODE' AS source_table_name,
            'bctsadmin_replication' AS target_schema_name,
            'BCTS_CATEGORY_CODE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BCTS_CATEGORY_CODE",
    "DESCRIPTION",
    "EFFECTIVE_DATE",
    "EXPIRY_DATE",
    "UPDATE_TIMESTAMP"
FROM the.BCTS_CATEGORY_CODE' AS customsql_query,
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### MOF_CLIENT

# CELL ********************

# Auto-generated metadata merges for Fabric
# Run this file in a Fabric notebook

spark.sql("""
    MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
    USING (
        SELECT
            'mof-client' AS business,
            'mof-client' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'the' AS source_schema_name,
            'FILE_CLIENT_TYPE_CODE' AS source_table_name,
            'mofclient_replication' AS target_schema_name,
            'FILE_CLIENT_TYPE_CODE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "FILE_CLIENT_TYPE_CODE",
    "DESCRIPTION",
    "EFFECTIVE_DATE",
    "EXPIRY_DATE",
    "UPDATE_TIMESTAMP"
FROM the.FILE_CLIENT_TYPE_CODE' AS customsql_query,
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

spark.sql("""
    MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
    USING (
        SELECT
            'mof-client' AS business,
            'mof-client' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'the' AS source_schema_name,
            'CLIENT_LOCATION' AS source_table_name,
            'mofclient_replication' AS target_schema_name,
            'CLIENT_LOCATION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CLIENT_NUMBER",
    "CLIENT_LOCN_CODE",
    "CLIENT_LOCN_NAME",
    "HDBS_COMPANY_CODE",
    "ADDRESS_1",
    "ADDRESS_2",
    "ADDRESS_3",
    "CITY",
    "PROVINCE",
    "POSTAL_CODE",
    "COUNTRY",
    "BUSINESS_PHONE",
    "HOME_PHONE",
    "CELL_PHONE",
    "FAX_NUMBER",
    "EMAIL_ADDRESS",
    "LOCN_EXPIRED_IND",
    "RETURNED_MAIL_DATE",
    "TRUST_LOCATION_IND",
    "CLI_LOCN_COMMENT",
    "UPDATE_TIMESTAMP",
    "UPDATE_USERID",
    "UPDATE_ORG_UNIT",
    "ADD_TIMESTAMP",
    "ADD_USERID",
    "ADD_ORG_UNIT",
    "REVISION_COUNT"
FROM the.CLIENT_LOCATION' AS customsql_query,
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

spark.sql("""
    MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
    USING (
        SELECT
            'mof-client' AS business,
            'mof-client' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'the' AS source_schema_name,
            'V_CLIENT_PUBLIC' AS source_table_name,
            'mofclient_replication' AS target_schema_name,
            'V_CLIENT_PUBLIC' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CLIENT_NUMBER",
    "CLIENT_NAME",
    "LEGAL_FIRST_NAME",
    "LEGAL_MIDDLE_NAME",
    "CLIENT_STATUS_CODE",
    "CLIENT_TYPE_CODE"
FROM the.V_CLIENT_PUBLIC' AS customsql_query,
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

spark.sql("""
    MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
    USING (
        SELECT
            'mof-client' AS business,
            'mof-client' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'the' AS source_schema_name,
            'ORG_UNIT' AS source_table_name,
            'mofclient_replication' AS target_schema_name,
            'ORG_UNIT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
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
FROM the.ORG_UNIT' AS customsql_query,
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
