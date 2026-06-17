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

spark.sql(\
"""
delete from bcts_metadata.cdc_master_table_list;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Forest Views

# CELL ********************

# Auto-generated metadata merges for Fabric
# Run this file in a Fabric notebook

spark.sql("""
    MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
    USING (
        SELECT
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SCALING_HISTORY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SCALING_HISTORY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TIMBER_MARK",
    "SCALE_SITE",
    "SCALING_PERIOD",
    "SCALE_SPECIES_CODE",
    "SCALE_PRODUCT_CODE",
    "SCALE_GRADE_CODE",
    "BILLING_TYPE_CODE",
    "VOLUME_SCALED",
    "TOTAL_AMOUNT",
    "ROYALTY_AMOUNT",
    "RESERVE_STMPG_AMT",
    "BONUS_STUMPAGE_AMT",
    "SILV_LEVY_AMOUNT",
    "DEV_LEVY_AMOUNT",
    "ENTRY_TIMESTAMP",
    "ENTRY_USERID",
    "UPDATE_TIMESTAMP",
    "UPDATE_USERID"
FROM forestview.V_SCALING_HISTORY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_CP' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_CP' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "TENURE",
    "LICENCE_ID",
    "PERMIT_ID",
    "PERM_APPLICATION_DESCRIPTION",
    "DIVI_DIV_NBR",
    "LICN_SEQ_NBR",
    "PERM_SEQ_NBR"
FROM forestview.V_CP' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'SV_SALES_SCHEDULE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'SV_SALES_SCHEDULE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "DIVI_SHORT_CODE",
    "OZON_OPERATING_ZONE_ID",
    "FIELD_TEAM",
    "MANU_ID",
    "OPAR_OPERATING_AREA_NAME",
    "LICENCE_ID",
    "LICN_CATEGORY_ID",
    "LINC_CERT_LEVEL_ID",
    CAST("TERM" AS NUMBER(38,10)) AS "TERM",
    "TENURE_TYPE",
    "LICN_LICENCE_STATE",
    "REGISTRANT",
    "PERM_PERMIT_ID",
    "GEOGRAPHIC_LOCATION",
    "MARK_MARK_ID",
    "BLOCK_ID",
    "BLOCK_FULL_NAME",
    "CUTB_SYSTEM_ID",
    "CUTB_BLOCK_STATE",
    "CUTB_LONGITUDE",
    "CUTB_LATITUDE",
    "STTP_STAND_TYPE",
    "CUTB_VOL_DATA_SOURCE",
    "BLAL_MERCH_HA_AREA",
    "BLAL_HARVESTED_HA_AREA",
    "BLAL_CRUISE_M3_VOL",
    "RW_VOL",
    CAST("BLOCK_VOLUME" AS NUMBER(38,10)) AS "BLOCK_VOLUME",
    "BLAL_HARVESTED_M3_VOL",
    "AUCTION_STATUS",
    "AUCTION_DATE",
    CAST("AUCTION_FISCAL" AS NUMBER(38,10)) AS "AUCTION_FISCAL",
    "AUCTION_QUARTER",
    "AUC_QUART_MONTH",
    "CURRENT_PERIOD_FLAG",
    "CURRENT_PERIOD_START",
    "CURRENT_PERIOD_END",
    "SILVICULTURE_SYSTEM",
    "YARDING_SYSTEM",
    "HARVEST_CONSIDERATIONS",
    CAST("PIECE_SIZE" AS NUMBER(38,10)) AS "PIECE_SIZE",
    "SPECIES_COMPOSITION",
    "HI_STATUS",
    "HI_DATE",
    "EXPIRE_STATUS",
    "EXPIRE_DATE",
    "RC_STATUS",
    "RC_DATE",
    "DR_STATUS",
    "DR_DATE",
    "DVS_STATUS",
    "DVS_DATE",
    "LAYOUT_STATUS",
    "LAYOUT_DATE",
    "DVC_STATUS",
    "DVC_DATE",
    "WRITE_OFF_STATUS",
    "WRITE_OFF_DATE",
    "LICN_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "OBJECTID",
    SDE.ST_AsText("SHAPE") AS "SHAPE"
FROM forestview.SV_SALES_SCHEDULE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_LICENCE_ACTIVITY_ALL' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_LICENCE_ACTIVITY_ALL' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "FIELD_TEAM_DESC",
    "NAV_NAME",
    "TENURE",
    "LICENCE_ID",
    "LICN_LICENCE_STATE",
    "ACTIVITY_CLASS",
    "ACTIVITY_TYPE",
    "ACTT_KEY_IND",
    "ACCL_OBJECT_TYPE",
    "ACTI_RESPONSIBILITY",
    "ACTI_STATUS_IND",
    "ACTI_TARGET_DATE",
    "ACTI_TARGET_COST",
    "ACTIVITY_DATE",
    "ACTI_STATUS_DATE",
    "ACTI_COST",
    "ACTI_COMMENTS",
    "LICENSEE",
    "ACTI_SEQ_NBR",
    "LICN_SEQ_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "CREATEDBY",
    "CREATEDON"
FROM forestview.V_LICENCE_ACTIVITY_ALL' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ACTIVITY_COST' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ACTIVITY_COST' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "ACTC_SEQ_NBR",
    "ACTI_SEQ_NBR",
    "ACTC_ITEM_COST",
    "CSTI_COST_ITEM_ID",
    "DIVI_DIV_NBR",
    "CSTI_COST_ITEM_DESCRIPTION",
    "CSTI_COST_ITEM_ACCOUNT_CODE",
    "ACTC_SERIES"
FROM forestview.V_ACTIVITY_COST' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_APPORTIONMENT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_APPORTIONMENT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "MANAGEMENT_TYPE",
    "APPO_DATE",
    "APPORTIONMENT_TENURE",
    "APPO_M3_VOL",
    "AUTHORIZED_BY",
    "ENTERED_BY",
    "APPORTIONMENT_PARTITION",
    "APPP_PARTITION_M3_VOL",
    CAST("COMMITMENT_M3_VOL" AS NUMBER(38,10)) AS "COMMITMENT_M3_VOL",
    CAST("ACTUAL_M3_VOLUME" AS NUMBER(38,10)) AS "ACTUAL_M3_VOLUME",
    CAST("VARIANCE" AS NUMBER(38,10)) AS "VARIANCE",
    "MANU_SEQ_NBR",
    "DIVI_DIV_NBR",
    "APPO_SEQ_NBR",
    CAST("FISCAL_YEAR" AS NUMBER(38,10)) AS "FISCAL_YEAR"
FROM forestview.V_APPORTIONMENT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_APPR_BRIDGE_TAB_COST' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_APPR_BRIDGE_TAB_COST' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "BTAB_ROAD_NAME",
    "BTAB_CRIB_HEIGHT_NBR",
    "BTAB_SPAN_LENGTH_NBR",
    "OPTN_LEAST_COST_OPTION_CODE",
    "BTAB_IDENTIFIER_DESC",
    "BTAB_APPLICABLE_VOL",
    CAST("BTAB_STATION_NBR" AS NUMBER(38,10)) AS "BTAB_STATION_NBR",
    "BTAB_CROWN_PORTION_PCT",
    "BTAB_APPRAISAL_YEAR",
    "BTAB_COMMENT",
    "BTAB_SECTION_BUILT_IND",
    "BTAB_START_METRE_NBR",
    "BTAB_END_METRE_NBR",
    "BTAB_BRIDGE_TYPE",
    "BTAB_INSPECTION_DATE",
    "BTAB_BRIDGE_STATUS",
    "PROJ_SEQ_NBR",
    CAST("PRJV_VERSION_NBR" AS NUMBER(38,10)) AS "PRJV_VERSION_NBR",
    "PVER_VERSION_NBR",
    "CUTB_SEQ_NBR",
    "BTAB_SEQ_NBR",
    "BTAB_SEQ_NBR_LNG"
FROM forestview.V_APPR_BRIDGE_TAB_COST' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_APPR_CULVERT_TAB_COST' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_APPR_CULVERT_TAB_COST' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "CTAB_ROAD_NAME",
    "CTAB_STATION_NBR",
    "CTAB_END_STATION_NBR",
    "RCTP_CONSTRUCTION_TYPE_ID",
    "CTAB_IDENTIFIER_DESC",
    "OPTN_LEAST_COST_OPTION_CODE",
    "CTAB_WOODEN_IND",
    "CTAB_CULVERT_DIAMETER_NBR",
    "CTAB_CULVERT_CNT",
    "CTAB_LENGTH_NBR",
    "CTAB_APPLICABLE_VOL",
    "CTAB_CROWN_PORTION_PCT",
    "CTAB_APPRAISAL_YEAR",
    "CTAB_COMMENT",
    "CULS_STATUS_ID",
    "PROJ_SEQ_NBR",
    CAST("PRJV_VERSION_NBR" AS NUMBER(38,10)) AS "PRJV_VERSION_NBR",
    "PVER_VERSION_NBR",
    "CUTB_SEQ_NBR",
    "CTAB_SEQ_NBR",
    CAST("ROAD_SEQ_NBR" AS NUMBER(38,10)) AS "ROAD_SEQ_NBR",
    "CTAB_SEQ_NBR_LNG"
FROM forestview.V_APPR_CULVERT_TAB_COST' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_APPR_INTERNAL_RATE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_APPR_INTERNAL_RATE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "INTR_START_DATE",
    "INTR_END_DATE",
    "INTR_LUMBER_SP",
    "INTR_CHIP_SP",
    "INTR_STUD_LOG_PCT",
    "INTR_SELLING_PRICE",
    "INTR_LOGGING_COST",
    "INTR_MANU_COST",
    "INTR_SILV_COST",
    "INTR_OPERATING_COST",
    "INTR_BASE_RATE",
    "INTR_VALUE_INDEX",
    "INTR_MEAN_VALUE_INDEX",
    "INTR_INDICATED_RATE",
    "INTR_STUMPAGE_RATE",
    "MAND_EFF_DATE",
    "ADJD_EFF_DATE",
    "PERM_SEQ_NBR"
FROM forestview.V_APPR_INTERNAL_RATE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_APPR_MINISTRY_RATE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_APPR_MINISTRY_RATE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "MINR_START_DATE",
    "MINR_END_DATE",
    "MINR_LUMBER_SP",
    "MINR_CHIP_SP",
    "MINR_STUD_LOG_PCT",
    "MINR_SELLING_PRICE",
    "MINR_LOGGING_COST",
    "MINR_MANU_COST",
    "MINR_SILV_COST",
    "MINR_OPERATING_COST",
    "MINR_BASE_RATE",
    "MINR_VALUE_INDEX",
    "MINR_MEAN_VALUE_INDEX",
    "MINR_INDICATED_RATE",
    "MINR_STUMPAGE_RATE",
    "MAND_EFF_DATE",
    "ADJD_EFF_DATE",
    "PERM_SEQ_NBR"
FROM forestview.V_APPR_MINISTRY_RATE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_APPR_ROAD_TAB_COST' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_APPR_ROAD_TAB_COST' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "ROAD_ROAD_NAME",
    "RTAB_ROAD_NAME",
    "RTAB_ROAD_TYPE",
    "RTAB_START_METRE_NBR",
    "RTAB_END_METRE_NBR",
    "RTAB_LENGTH_NBR",
    "RTAB_SOIL_MOISTURE_TYPE",
    "RTAB_ROCK_PCT",
    "RTAB_BIOGEO_ZONE",
    "RTAB_SIDE_SLOPE_PCT",
    "RTAB_HAUL_DISTANCE_NBR",
    "RTAB_STABILIZING_MATERIAL_TYPE",
    "PVER_VERSION_NBR",
    "CUTB_SEQ_NBR",
    "RTAB_SEQ_NBR",
    CAST("ROAD_SEQ_NBR" AS NUMBER(38,10)) AS "ROAD_SEQ_NBR",
    "RTAB_SEQ_NBR_LNG"
FROM forestview.V_APPR_ROAD_TAB_COST' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ATTACHMENT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ATTACHMENT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BSTO_TABLE_NAME",
    "SEQ_NBR_TYPE",
    CAST("SEQ_NBR_VALUE" AS NUMBER(38,10)) AS "SEQ_NBR_VALUE",
    "BSTO_FILE_NAME",
    "BSTO_FILE_DESCRIPTION",
    "BSTO_SEQ_NBR"
FROM forestview.V_ATTACHMENT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_BLOCK_AREA' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_BLOCK_AREA' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    CAST("TOTAL_AREA" AS NUMBER(38,10)) AS "TOTAL_AREA",
    CAST("GROSS_AREA" AS NUMBER(38,10)) AS "GROSS_AREA",
    CAST("INTRES_AREA" AS NUMBER(38,10)) AS "INTRES_AREA",
    CAST("EXTRES_AREA" AS NUMBER(38,10)) AS "EXTRES_AREA",
    CAST("RESRV_AREA" AS NUMBER(38,10)) AS "RESRV_AREA",
    CAST("NPNAT_AREA" AS NUMBER(38,10)) AS "NPNAT_AREA",
    CAST("NPUNN_AREA" AS NUMBER(38,10)) AS "NPUNN_AREA",
    CAST("NAR_AREA" AS NUMBER(38,10)) AS "NAR_AREA"
FROM forestview.V_BLOCK_AREA' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_BLOCK_CRUISE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_BLOCK_CRUISE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BLKC_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "PERM_SEQ_NBR",
    "BLKC_SYSTEM_NBR",
    "BLKC_PLOT_NBR",
    "CRUISE_SOURCE",
    "SPECIES_ID",
    "SPECIES",
    "GROSS_MERCH_M3",
    "NET_MERCH_M3",
    "NET_MERCH_PER_HA",
    "GROSS_VOL_PER_TREE",
    "NET_VOL_PER_TREE",
    "DECAY",
    "WASTE",
    "BREAKAGE",
    "GROSS_DWB",
    "STEMS",
    "AVG_DBH",
    "SNAGS",
    "DISTRIBUTION",
    "DEAD_POTENTIAL",
    "AVG_WEIGHT_MERCH_HT",
    "BASAL_AREA",
    "SAMPLING_ERROR",
    "OLD_GROWTH",
    "SECOND_GROWTH",
    "BLOWDOWN",
    "BLKC_AREA_HA",
    CAST("BLKC_INSECT_DAMAGE_GREEN_PCT" AS NUMBER(38,10)) AS "BLKC_INSECT_DAMAGE_GREEN_PCT",
    CAST("BLKC_INSECT_DAMAGE_RED_PCT" AS NUMBER(38,10)) AS "BLKC_INSECT_DAMAGE_RED_PCT",
    CAST("BLKC_INSECT_DAMAGE_GREY_PCT" AS NUMBER(38,10)) AS "BLKC_INSECT_DAMAGE_GREY_PCT",
    "BLKC_STUD_PCT",
    "BLKC_SMALL_LOG_PCT",
    "BLKC_LARGE_LOG_PCT",
    "BLKC_LRF_BDFT_PER_M3",
    "BLKC_M3_PER_LINEAL_M",
    "BLKC_GROSS_M3_PER_HA",
    "BLKC_PULP_M3_PER_HA",
    "BLKC_SAWLOG_M3_PER_HA",
    "BLKC_SAWLOG_M3_PER_TREE",
    "BLKC_PULP_NET_M3_PER_TREE",
    CAST("BLKC_BURN_DAMAGE_PCT" AS NUMBER(38,10)) AS "BLKC_BURN_DAMAGE_PCT",
    CAST("SPECIE_PCT" AS NUMBER(38,10)) AS "SPECIE_PCT",
    "FINZ_FOREST_INVENTORY_ZONE_ID",
    "DIVI_DIV_NBR"
FROM forestview.V_BLOCK_CRUISE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_BLOCK_DEPLETION_STAGE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_BLOCK_DEPLETION_STAGE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DPST_SEQ_NBR",
    "ACTI_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "DPST_START_DATE",
    "DPST_COMP_DATE",
    "DPST_AREA",
    "DPST_DEPLETION_VOL",
    "DPST_DIGI_IND",
    "DPST_PHASE_ID",
    "DPST_DEPLETION_VOL_DECID",
    "DPST_DEPLETION_VOL_CONIF"
FROM forestview.V_BLOCK_DEPLETION_STAGE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_BLOCK_HARVEST_METHOD' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_BLOCK_HARVEST_METHOD' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CBHM_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "DIVI_DIV_NBR",
    "HAME_METHOD_ID",
    "CBHM_AREA",
    "CBHM_HARVEST_VOL",
    "CBHM_DIGI_IND"
FROM forestview.V_BLOCK_HARVEST_METHOD' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_BLOCK_HARVEST_STRATEGY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_BLOCK_HARVEST_STRATEGY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CBST_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "DIVI_DIV_NBR",
    "STGY_STRATEGY_ID",
    "CBST_AREA",
    "CBST_HARVEST_VOL",
    "CBST_DIGI_IND"
FROM forestview.V_BLOCK_HARVEST_STRATEGY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_BLOCK_NMAR' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_BLOCK_NMAR' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "NMAR_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "NMCL_CLASS_ID",
    "NMTP_TYPE_ID",
    "NMAR_LABEL",
    "DIVI_DIV_NBR"
FROM forestview.V_BLOCK_NMAR' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_BLOCK_OLD' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_BLOCK_OLD' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "TENURE",
    "LICENCE_ID",
    "PERMIT_ID",
    "MARK_ID",
    "BLOCK_ID",
    "BLOCK_NBR",
    "UBI",
    "OPENING",
    "OP_AREA",
    "SUPPLY_BLOCK",
    "PHOTO",
    "LATITUDE",
    "LONGITUDE",
    "PROV_FRST_CONFLICT",
    "MAPSHEET_ID",
    "LANDSCAPE_UNIT",
    "SP_EXEMPT",
    "STAND_TYPE",
    "AGE_CLASS",
    "HGT_CLASS",
    "STK_CLASS",
    "SITE_INDEX",
    "SOURCE",
    "FDP_STATUS",
    "FUNDING_CODE",
    "CUTB_BLOCK_MEMO",
    "GROSS_AREA",
    "EST_AREA",
    "MERCH_AREA",
    "HARVEST_AREA",
    CAST("REMAINING_AREA" AS NUMBER(38,10)) AS "REMAINING_AREA",
    "CRUISE_VOL",
    "HARVEST_VOL",
    CAST("REMAINING_VOL" AS NUMBER(38,10)) AS "REMAINING_VOL",
    "CUTB_FORMA_PRINT_DATE",
    "CUTB_FORMA_PRINTED",
    "CUTB_BLOCK_STATE",
    "PMOD_MODIFIER_ID",
    "HARVEST_STARTED",
    "HARVEST_COMPLETED",
    "CUTB_LOCATION",
    "DIVI_DIV_NBR",
    "LICN_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MARK_SEQ_NBR",
    "CUTB_SEQ_NBR"
FROM forestview.V_BLOCK_OLD' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_BLOCK_TASKS_ISSUE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_BLOCK_TASKS_ISSUE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "NAV_NAME",
    "LICENCE_ID",
    "PERMIT_ID",
    "MARK_ID",
    "BLOCK_ID",
    "ISSUE",
    "TASK_DESCRIPTION",
    "RESPONSIBILITY",
    "TARGET_DATE",
    "COMPLETION_DATE",
    "PROVINCIAL_ENV_COMMENTS",
    "GOVERNMENT_PUBLIC_COMMENTS",
    "LICENCEE_COMMENTS",
    "FORESTER_COMMENTS",
    "RECOMMENDED_REQUIREMENTS",
    "DIVI_DIV_NBR",
    "TSO_NAME",
    "ISST_ISSUE_ID",
    "ACTP_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "FIELD_TEAM_DESC",
    "UBI",
    "LICN_LICENCE_STATE"
FROM forestview.V_BLOCK_TASKS_ISSUE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_BLOCK_TIMBER' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_BLOCK_TIMBER' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    "TIMBER_TYPE",
    "TIMBER_ORDER"
FROM forestview.V_BLOCK_TIMBER' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_COASTAL_COST_SURVEY_BH' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_COASTAL_COST_SURVEY_BH' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BHCT_ID",
    "CSUD_SOFT_MED_ROCK_COST_PER_M",
    "CSUD_HARD_ROCK_COST_PER_M",
    "CSUD_SOFT_MED_ROCK_CONST_M",
    "CSUD_HARD_ROCK_CONST_M",
    "CSUD_BALLAST_HAUL_KM",
    "CSUR_SEQ_NBR"
FROM forestview.V_COASTAL_COST_SURVEY_BH' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_COASTAL_COST_SURVEY_ROADS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_COASTAL_COST_SURVEY_ROADS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "CONSTRUCTION_FISCAL",
    "MANU_ID",
    "MANU_TYPE_ID",
    "MANU_NAME",
    "ROAD_NAME",
    "URI",
    "RCOM_START_METRE_NBR",
    "RCOM_END_METRE_NBR",
    "RCOM_PLANNED_DATE",
    "RCOM_COMPLETION_DATE",
    "CSUR_START_METRE_NBR",
    "CSUR_END_METRE_NBR",
    "CSUR_REPORT_DATE",
    "CSUR_PROJECT_ID",
    "CSUR_BID_TYPE",
    "PORG_POINT_OF_ORIGIN_ID",
    "CSUR_OPERATION_SPECIFICS",
    "PREPARED_BY",
    "CSUR_COMMENT",
    "CSUR_SEQ_NBR",
    "ROAD_SEQ_NBR",
    "CSUR_SEQ_NBR_LNG"
FROM forestview.V_COASTAL_COST_SURVEY_ROADS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_COMMITMENTS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_COMMITMENTS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "LICENCE_ID",
    "PERMIT_ID",
    "MARK_ID",
    "APPORTIONMENT_TYPE",
    "PARTITION_TYPE",
    "COPA_PERCENT",
    CAST("COMMITMENT_PARTITION_AREA" AS NUMBER(38,10)) AS "COMMITMENT_PARTITION_AREA",
    CAST("PRED_COMMITTED_VOLUME" AS NUMBER(38,10)) AS "PRED_COMMITTED_VOLUME",
    CAST("ACTUAL_PARTITION_VOLUME" AS NUMBER(38,10)) AS "ACTUAL_PARTITION_VOLUME",
    "LICENCE_ISSUED_DATE",
    CAST("FISCAL_YEAR" AS NUMBER(38,10)) AS "FISCAL_YEAR",
    "DIVI_DIV_NBR",
    "MANU_SEQ_NBR",
    "MARK_SEQ_NBR",
    "PERM_SEQ_NBR",
    "LICN_SEQ_NBR",
    "COPA_SEQ_NBR"
FROM forestview.V_COMMITMENTS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_COMMIT_TRACKING_SALE_INFO' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_COMMIT_TRACKING_SALE_INFO' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "LICENCE_ID",
    "MARK_ID",
    "CTOR_NAME",
    "LICN_LICENCE_DESC",
    "APPORTIONMENT_TYPE",
    "PARTITION_TYPE",
    CAST("FISCAL_YEAR" AS NUMBER(38,10)) AS "FISCAL_YEAR",
    "LICENCE_ISSUED_DATE",
    "LICENCE_STATE",
    CAST("APPRAISAL_VOL" AS NUMBER(38,10)) AS "APPRAISAL_VOL",
    CAST("BILLED_VOLUME" AS NUMBER(38,10)) AS "BILLED_VOLUME",
    CAST("OUTSTANDING_VOLUME" AS NUMBER(38,10)) AS "OUTSTANDING_VOLUME",
    CAST("CUT_CRUISE_RATIO" AS NUMBER(38,10)) AS "CUT_CRUISE_RATIO",
    "LICN_SEQ_NBR",
    "MANU_SEQ_NBR",
    "DIVI_DIV_NBR",
    CAST("ACTUAL_COMMIT_VOL" AS NUMBER(38,10)) AS "ACTUAL_COMMIT_VOL"
FROM forestview.V_COMMIT_TRACKING_SALE_INFO' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_COST_SURVEY_BRIDGES' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_COST_SURVEY_BRIDGES' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BRIDGE_STATION",
    "BRIDGE_NAME",
    "FOOTINGS",
    "CONSTCATEGORY",
    "CRIBHEIGHT",
    "DECKLENGTH",
    "DECKMATERIAL",
    "DECKWIDTH",
    "FOOTINGSMATERIAL",
    "LOADCAPACITY",
    "LOADRATING",
    "MAXDPTPILEDRN",
    "SPANMATERIAL",
    "CSUS_COMMENT",
    "CSUS_BRIDGE_COST",
    "CSUS_BRIDGE_APPROACH_WORK_COST",
    "CSUS_BRIDGE_BARGING_COST",
    CAST("TOTAL_COST" AS NUMBER(38,10)) AS "TOTAL_COST",
    "CSUR_LAST_UPDATE_MODE",
    "CSUR_SEQ_NBR",
    "CSUR_SEQ_NBR_LNG"
FROM forestview.V_COST_SURVEY_BRIDGES' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_DETAILED_SITE_ASSESSMENT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_DETAILED_SITE_ASSESSMENT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    "SILA_SEQ_NBR",
    "DSAS_CHEMICAL",
    "DSAS_ACTIVE",
    "DSAS_APPLY_RATE",
    "PMPL_PMP_NBR",
    "DSAS_PFZ_WIDTH",
    "DSAS_STRATUM_NAME",
    "POTR_PURPOSE_OF_TREATMENT",
    "DSAS_BUFFER_SIZE",
    "DSAS_APPROVAL_IND",
    "DSAS_REFERAL_IND",
    "DSAS_WATER_BODIES",
    "DSAS_FISH_HABITAT",
    "DSAS_COMMUNITY_WATERSHED",
    "DSAS_WILDLIFE_HABITAT",
    "DSA_STOCK_SPECIES",
    "DSA_SITE_SERIES"
FROM forestview.V_DETAILED_SITE_ASSESSMENT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ECOLOGY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ECOLOGY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    "ECOU_SEQ_NBR",
    "ECO_UNIT",
    "GROSS_AREA",
    "SITE_SERIES_COMPLEX",
    "BIOZ_ZONE_ID",
    "BIOS_SUBZONE_ID",
    "BIOV_VARIANT_ID",
    "SITE_SERIES_PERCENT",
    CAST("ECO_PROD_AREA" AS NUMBER(38,10)) AS "ECO_PROD_AREA",
    CAST("RESERVE_AREA" AS NUMBER(38,10)) AS "RESERVE_AREA",
    CAST("NPNAT" AS NUMBER(38,10)) AS "NPNAT",
    CAST("NPUNN" AS NUMBER(38,10)) AS "NPUNN",
    CAST("NCBR" AS NUMBER(38,10)) AS "NCBR",
    "DIVI_DIV_NBR",
    "ECOU_ELEVATION_MIN",
    "ECOU_ELEVATION_MAX",
    "ECOU_ELEVATION_AVG",
    "ECOU_ASPECT",
    "ECOU_SLOPE_MIN",
    "ECOU_SLOPE_MAX",
    "ECOU_SLOPE_AVG",
    "ECOU_SLOPE_POSITION"
FROM forestview.V_ECOLOGY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ECOLOGY_SITE_SERIES' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ECOLOGY_SITE_SERIES' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "ECOU_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "ECOU_NAME",
    "BIOZ_ZONE_ID",
    "BIOS_SUBZONE_ID",
    "BIOV_VARIANT_ID",
    "BGRG_REGION_CODE",
    "BISS_SITE_SERIES_ID",
    "EUSS_PERCENT_COVER"
FROM forestview.V_ECOLOGY_SITE_SERIES' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_EMS_ACTION_PLAN' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_EMS_ACTION_PLAN' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "FIELD_TEAM",
    "LICENCE_ID",
    "ENTERED_DATE",
    "COMPLETION_DATE",
    "TARGET_COMPLETION_DATE",
    "ACTION_PLAN_ID",
    "ACTION_STATUS",
    "ASSIGNED_TO",
    "INSPECTION_TYPE",
    "INSPECTION_SUB_TYPE",
    "PROJECT_ID",
    "PROJECT_NAME",
    "EMSA_SEQ_NBR",
    "EMSI_SEQ_NBR",
    "EINS_SEQ_NBR",
    CAST("EMSP_SEQ_NBR" AS NUMBER(38,10)) AS "EMSP_SEQ_NBR",
    "LICN_SEQ_NBR",
    CAST("LEMS_SEQ_NBR" AS NUMBER(38,10)) AS "LEMS_SEQ_NBR"
FROM forestview.V_EMS_ACTION_PLAN' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_EMS_ACTION_PLAN_DESCR' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_EMS_ACTION_PLAN_DESCR' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "LICENCE_ID",
    "ITEM_NBR",
    "RESPONSIBILITY",
    "CAUSE_DESCRIPTION",
    "ACTION_STATUS",
    "DESCRIPTION",
    "ACTION",
    "TARGET_DATE",
    "COMPLETION_DATE",
    "EMSA_SEQ_NBR",
    "LICN_SEQ_NBR"
FROM forestview.V_EMS_ACTION_PLAN_DESCR' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_EMS_CONTRACT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_EMS_CONTRACT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "CONTRACT_ID",
    "CTOR_NAME",
    "CONTRACT_COORDINATOR",
    "CONTRACT_LOCATION",
    "START_DATE",
    "START_STATUS",
    "END_DATE",
    "END_STATUS",
    "VIEWING_DATE",
    "INTERNAL_IND",
    "EMSC_SEQ_NBR"
FROM forestview.V_EMS_CONTRACT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_EMS_INSPECTION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_EMS_INSPECTION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "FIELD_TEAM",
    "CLASS",
    "TYPE",
    "SUB_TYPE",
    "INSPECTION_DATE",
    "PROJECTID_LICENCE",
    "PROJECT_NAME",
    "ROAD_TENURE",
    "LOG_PERMIT_ID",
    "AGENT",
    "CONTRACTOR",
    "INSPECTION_STATUS",
    "SUPERVISOR",
    "GEO_LOCATION",
    "DATA_SOURCE",
    "FUNCTIONAL_AREA",
    "LEMS_EPEA_STATUS",
    "CONTRACT_LOCATION",
    CAST("EMSP_SEQ_NBR" AS NUMBER(38,10)) AS "EMSP_SEQ_NBR",
    CAST("LICN_SEQ_NBR" AS NUMBER(38,10)) AS "LICN_SEQ_NBR",
    "EINS_SEQ_NBR",
    CAST("EPEA_SEQ_NBR" AS NUMBER(38,10)) AS "EPEA_SEQ_NBR",
    CAST("LEMS_SEQ_NBR" AS NUMBER(38,10)) AS "LEMS_SEQ_NBR",
    "CTOR_SEQ_NBR"
FROM forestview.V_EMS_INSPECTION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_EMS_INSPECTION_FREQUENCY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_EMS_INSPECTION_FREQUENCY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "FIELD_TEAM",
    "PROJECTID_LICENCE",
    "ROAD_PERMIT",
    "LOG_DUMP_PERMIT",
    "INSPECTION_DATE",
    "STATUS",
    "INSPECTOR",
    "EIFR_TYPE",
    "EINS_NUMBER",
    "FREQUENCY",
    "DATA_SOURCE",
    "INSPECTION_TYPE",
    "FUNCTIONAL_AREA",
    "PROJECT_NAME",
    "CONTRACT_LOCATION",
    "CONTRACTOR",
    "LICN_SEQ_NBR",
    "EMSC_SEQ_NBR",
    "LEMS_SEQ_NBR",
    "EPEA_SEQ_NBR",
    "EMSP_SEQ_NBR",
    "EINS_SEQ_NBR"
FROM forestview.V_EMS_INSPECTION_FREQUENCY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_EMS_INSPECTION_TEST_DRILL' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_EMS_INSPECTION_TEST_DRILL' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    CAST("DIVI_DIV_NBR" AS NUMBER(38,10)) AS "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "FIELD_TEAM",
    "PROJECTID_LICENCE",
    "ROAD_PERMIT",
    "LOG_DUMP_PERMIT",
    "INSPECTION_DATE",
    "STATUS",
    "TEST_DRILL",
    "EINS_TYPE",
    "DUE_DATE",
    "COMPLETION_DATE",
    "DATA_SOURCE",
    "INSPECTION_TYPE",
    "FUNCTIONAL_AREA",
    "PROJECT_NAME",
    "GEO_LOCATION",
    "CONTRACTOR",
    "LICN_SEQ_NBR",
    "EMSC_SEQ_NBR",
    "LEMS_SEQ_NBR",
    "EPEA_SEQ_NBR",
    "EMSP_SEQ_NBR",
    "EINS_SEQ_NBR"
FROM forestview.V_EMS_INSPECTION_TEST_DRILL' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_EMS_ISSUE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_EMS_ISSUE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO",
    "BUSINESS_AREA",
    "FIELD_TEAM",
    "ISSUE_PROJECT_NAME",
    "ISSUE_ID",
    "ENTERED_BY",
    "ENTERED_DATE",
    "ISSUE_SOURCE",
    "ISSUE_TYPE",
    "SUB_TYPE",
    "ISSUE_EVENT",
    "ISSUE_SUPERVISOR",
    "ISSUE_INVESTIGATOR",
    "ISSUE_CLIENT",
    "RDPM_PERMIT_ID",
    "ISSUE_STATUS",
    "OCCURRENCE_DATE",
    "REPORTED_DATE",
    "LICN_LICENCE_ID",
    "FUNCT_AREA",
    "FUNCT_AREA_ACTIVITY",
    "ISSUE_DESCRIPTION",
    "GENERAL_LOCATION",
    "DETAILED_DESCRIPTION",
    "ROOT_CAUSE",
    "ENVIRONMENTAL_IMPACT",
    "LICN_SEQ_NBR",
    "SDOM_SEQ_NBR",
    "EMSI_SEQ_NBR"
FROM forestview.V_EMS_ISSUE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_EMS_ISSUE_GOVERNMENT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_EMS_ISSUE_GOVERNMENT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "GOVT_AGENCY",
    "GOVT_DATE",
    "GOVT_DETAILS",
    "GOVT_CONTACT",
    "GOVT_DETERMINIATION",
    "EMSI_SEQ_NBR"
FROM forestview.V_EMS_ISSUE_GOVERNMENT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_EMS_ISSUE_INVESTIGATION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_EMS_ISSUE_INVESTIGATION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "INVESTIGATION_DATE",
    "INVESTIGATION_DETAILS",
    "EMSI_SEQ_NBR"
FROM forestview.V_EMS_ISSUE_INVESTIGATION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_EMS_PLAN_VS_COMPLETE_INSP' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_EMS_PLAN_VS_COMPLETE_INSP' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    CAST("DIVI_DIV_NBR" AS NUMBER(38,10)) AS "DIVI_DIV_NBR",
    "TSO",
    "DIVISION",
    "PROJECT_LICENCE_ID",
    "LICENCE_STATE",
    "EMS_CLASS",
    "EMS_TYPE",
    "EMS_SUBTYPE",
    "CONTRACTOR",
    "PROJECT_RISK_RANKING",
    "EMS_RISK_RATING",
    "INSPECTOR",
    "INSPECTION_TYPE",
    "INSPECTION_NUMBER",
    "FREQUENCY",
    CAST("CALCEMSINSP" AS NUMBER(38,10)) AS "CALCEMSINSP",
    CAST("COMPEMSINSP" AS NUMBER(38,10)) AS "COMPEMSINSP",
    CAST("PERCEMSINSPCOMP" AS NUMBER(38,10)) AS "PERCEMSINSPCOMP",
    "DATA_SOURCE",
    "LICN_INSPECTION_TYPE",
    "INSPECTION_DATE",
    "INSPECTION_STATUS",
    "FUNCTIONAL_AREA",
    "FIELD_TEAM_ID",
    "EMSP_PROJECT_NAME",
    "EMSC_CTOR_LOCATION",
    "EIFR_SEQ_NBR",
    "EINS_SEQ_NBR",
    CAST("LICN_SEQ_NBR" AS NUMBER(38,10)) AS "LICN_SEQ_NBR",
    CAST("EMSC_SEQ_NBR" AS NUMBER(38,10)) AS "EMSC_SEQ_NBR",
    CAST("MANU_SEQ_NBR" AS NUMBER(38,10)) AS "MANU_SEQ_NBR",
    CAST("LEMS_SEQ_NBR" AS NUMBER(38,10)) AS "LEMS_SEQ_NBR",
    CAST("EPEA_SEQ_NBR" AS NUMBER(38,10)) AS "EPEA_SEQ_NBR",
    "CTOR_SEQ_NBR",
    CAST("EMSP_SEQ_NBR" AS NUMBER(38,10)) AS "EMSP_SEQ_NBR"
FROM forestview.V_EMS_PLAN_VS_COMPLETE_INSP' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_EMS_PLAN_VS_COMPL_PREWORK' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_EMS_PLAN_VS_COMPL_PREWORK' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    CAST("DIVI_DIV_NBR" AS NUMBER(38,10)) AS "DIVI_DIV_NBR",
    "TSO",
    "BUSINESS_AREA",
    "FIELD_TEAM",
    "PROJECT_LICENCE_ID",
    "LICENCE_STATE",
    "LICENCE_ACTIVITY",
    "PLANNED_DATE",
    "PLANNED_STATUS",
    "DONE_DATE",
    "DONE_STATUS",
    "PREWORK_PRESENT",
    "EMS_CLASS",
    "EMS_TYPE",
    "EMS_SUBTYPE",
    "PREWORK_DATE",
    "PREWORK_STATUS",
    "DATA_SOURCE",
    "INSPECTION_TYPE",
    "INSPECTION_DATE",
    "INSPECTION_STATUS",
    "FUNCTIONAL_AREA",
    "PROJECT_NAME",
    "PROJECT_LOCATION",
    "CONTRACTOR",
    CAST("LICN_SEQ_NBR" AS NUMBER(38,10)) AS "LICN_SEQ_NBR",
    CAST("EMSC_SEQ_NBR" AS NUMBER(38,10)) AS "EMSC_SEQ_NBR",
    CAST("MANU_SEQ_NBR" AS NUMBER(38,10)) AS "MANU_SEQ_NBR",
    CAST("LEMS_SEQ_NBR" AS NUMBER(38,10)) AS "LEMS_SEQ_NBR",
    CAST("EPEA_SEQ_NBR" AS NUMBER(38,10)) AS "EPEA_SEQ_NBR",
    "CTOR_SEQ_NBR",
    CAST("EMSP_SEQ_NBR" AS NUMBER(38,10)) AS "EMSP_SEQ_NBR"
FROM forestview.V_EMS_PLAN_VS_COMPL_PREWORK' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_EMS_REQUIREMENT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_EMS_REQUIREMENT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    CAST("DIVI_DIV_NBR" AS NUMBER(38,10)) AS "DIVI_DIV_NBR",
    "TSO",
    "BUSINESS_AREA",
    "FIELD_TEAM",
    "LICENCE_PROJECT_ID",
    "ROAD_TENURE",
    "LOG_DUMP_PERMIT",
    "LICENCE_INSPECTION_DATE",
    "LICENCE_INSPECTION_STATUS",
    "REQUIREMENT_TYPE",
    "REQUIREMENT_DESC",
    "REQUIREMENT_STATUS",
    "DATA_SOURCE",
    "PROJECT_INSPECTION_TYPE",
    "PROJECT_INSPECTION_DATE",
    "PROJECT_INSPECTION_STATUS",
    "FUNCTIONAL_AREA",
    "PROJECT_NAME",
    "PROJECT_LOCATION",
    "CONTRACTOR",
    "EINS_SEQ_NBR",
    "LICN_SEQ_NBR",
    "EMSC_SEQ_NBR",
    "LEMS_SEQ_NBR",
    "EPEA_SEQ_NBR",
    "EMSP_SEQ_NBR"
FROM forestview.V_EMS_REQUIREMENT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_FDU' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_FDU' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "FSFD_NAME",
    "FSPM_SEQ_NBR",
    "FSFD_SEQ_NBR"
FROM forestview.V_FDU' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_FOREST_COMMENT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_FOREST_COMMENT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SEQ_NBR_TYPE",
    "SEQ_NBR_VALUE",
    "COMM_COMMENT_DATE",
    "COMM_COMMENT",
    "COMM_AGENCY",
    "COMM_AGENT",
    "COMM_RESPONSE_DATE",
    "COMM_RESPONDANT",
    "COMM_RESPONSE",
    "COMM_ACTION_REQUIRED",
    "COMM_ACTION_ITEM",
    "COMM_COMMENT_TYPE",
    "COMM_FDP_ID"
FROM forestview.V_FOREST_COMMENT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_FRPA_RESULTS_STRATEGIES' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_FRPA_RESULTS_STRATEGIES' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "PARENT_PLAN_NAME",
    "PARENT_EFFECTIVE_DATE",
    "PARENT_EXPIRY_DATE",
    "PARENT_PLAN_STATUS",
    "PLAN_TYPE",
    "PLAN_NAME",
    "PLAN_DESCRIPTION",
    "PLAN_STATUS",
    "PLAN_EFFECTIVE_DATE",
    "PLAN_EXPIRY_DATE",
    "PLAN_APPROVAL_DATE",
    "PLAN_COMMENT",
    "FSPM_SUBMISSION_DATE",
    "FSPM_TS_NUM",
    "FDU_NAME",
    "FDU_AREA",
    "FDU_SELECTED",
    "FDU_COMMENTS",
    "FDU_BLOCK_AREA",
    "FDU_ROAD_LENGTH",
    "DOCUMENTKEY",
    CAST("SDE_STATE_ID" AS NUMBER(38,10)) AS "SDE_STATE_ID",
    "LICENCE_ID",
    "PERMIT",
    "BLOCK_ID",
    "UBI",
    "BLOCK_AREA",
    "BLOCK_PLAN_STATUS",
    "LANDSCAPE_UNIT",
    "MAPSHEET",
    "FSOB_ID",
    "FSRS_ID",
    "APPLIES",
    "FSRS_DESCRIPTION",
    "FSRS_COMMENT",
    CAST("ACTT_SEQ_NBR" AS NUMBER(38,10)) AS "ACTT_SEQ_NBR",
    "ASSES_DESC",
    "FSRL_COMMENT",
    "FSRC_COMMENT_RS",
    "CUTB_SEQ_NBR",
    "FDU_SEQ_NBR",
    "PLAN_SEQ_NBR"
FROM forestview.V_FRPA_RESULTS_STRATEGIES' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_GIS_ACTD' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_GIS_ACTD' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "MANU_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MARK_SEQ_NBR",
    "LICN_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "BLAL_MERCH_HA_AREA",
    "BLAL_HARVESTED_HA_AREA",
    "BLAL_CRUISE_M3_VOL",
    "BLAL_RW_VOL",
    "BLAL_HARVESTED_M3_VOL",
    "AUCTION_DATE_COMPARE",
    "AUCTION_DATE"
FROM forestview.V_GIS_ACTD' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_GIS_ACTD_DATES' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_GIS_ACTD_DATES' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "MANU_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MARK_SEQ_NBR",
    "LICN_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "BLAL_MERCH_HA_AREA",
    "BLAL_HARVESTED_HA_AREA",
    "BLAL_CRUISE_M3_VOL",
    "BLAL_RW_VOL",
    "BLAL_HARVESTED_M3_VOL",
    "ACTT_KEY_IND",
    "STATUS_DATE",
    "AUCTION_DATE_COMPARE"
FROM forestview.V_GIS_ACTD_DATES' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_GIS_ACTS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_GIS_ACTS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "LICN_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "AUCTION_STATUS"
FROM forestview.V_GIS_ACTS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_GIS_ACTS_STATUS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_GIS_ACTS_STATUS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "LICN_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "ACTT_KEY_IND",
    "STATUS_IND"
FROM forestview.V_GIS_ACTS_STATUS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_HARVESTED_BLOCK_STATUS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_HARVESTED_BLOCK_STATUS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "NAV_NAME",
    "LICENCE_ID",
    "PERMIT_ID",
    "MARK_ID",
    "BLOCK_ID",
    "BLOCK_NBR",
    "OP_AREA",
    "GROSS_AREA",
    "ACTI_STATUS_IND",
    "ACTI_STATUS_DATE",
    "BLOCK_RG_STATUS",
    "BLOCK_FG_STATUS",
    "BLOCK_GU_STATUS",
    "BLOCK_STATUS",
    "DIVI_DIV_NBR",
    "LICN_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MARK_SEQ_NBR",
    "CUTB_SEQ_NBR"
FROM forestview.V_HARVESTED_BLOCK_STATUS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_HARVEST_HISTORY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_HARVEST_HISTORY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "LICN_LICENCE_ID",
    "MARK_MARK_ID",
    "BCHH_BILLING_PERIOD",
    "BCHH_HDBS_TREE_SPECIES",
    "BCHH_FOREST_PRODUCT_CODE",
    "BCHH_LOG_GRADE",
    "BCHH_BILLING_TYPE_CODE",
    "BCHH_VOLUME_BILLED",
    "BCHH_PIECES_BILLED",
    "BCHH_ROYALTY_AMOUNT",
    "BCHH_RESERVE_STMPG_AMT",
    "BCHH_BONUS_STUMPAGE_AMT",
    "BCHH_SILV_LEVY_AMOUNT",
    "BCHH_DEV_LEVY_AMOUNT",
    "BCHH_UPDATE_TIMESTAMP",
    "LICN_SEQ_NBR",
    "MARK_SEQ_NBR"
FROM forestview.V_HARVEST_HISTORY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_HARVEST_UNIT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_HARVEST_UNIT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CBIN_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "HARVEST_UNIT_ID",
    "SEASON",
    "METHOD",
    "STRATEGY",
    "TOTAL_AREA",
    "TOTAL_VOL",
    CAST("DEPLETED_AREA" AS NUMBER(38,10)) AS "DEPLETED_AREA",
    CAST("DEPLETED_VOL" AS NUMBER(38,10)) AS "DEPLETED_VOL",
    "DIGITIZED",
    "CBIN_COMMENTS",
    "SEASON_ID",
    "METHOD_ID",
    "STRATEGY_ID",
    "SILV_STRATEGY_ID"
FROM forestview.V_HARVEST_UNIT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_INTERIOR_COST_SURVEY_CULV' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_INTERIOR_COST_SURVEY_CULV' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CULVERT_STATION",
    "DIAMETER",
    "LENGTH",
    "CTYPE",
    "XRISE",
    "CSUS_COMMENT",
    "CSUS_CULVERT_MATERIAL_COST",
    "CSUS_CULVERT_INSTALL_COST",
    CAST("TOTAL_COST" AS NUMBER(38,10)) AS "TOTAL_COST",
    "CSUR_SEQ_NBR",
    "CSUR_SEQ_NBR_LNG"
FROM forestview.V_INTERIOR_COST_SURVEY_CULV' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_INTERIOR_COST_SURVEY_ROADS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_INTERIOR_COST_SURVEY_ROADS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "CONSTRUCTION_FISCAL",
    "MANU_ID",
    "MANU_TYPE_ID",
    "MANU_NAME",
    "ROAD_NAME",
    "URI",
    "RCOM_START_METRE_NBR",
    "RCOM_END_METRE_NBR",
    "RCOM_PLANNED_DATE",
    "RCOM_COMPLETION_DATE",
    "CSUR_START_METRE_NBR",
    "CSUR_END_METRE_NBR",
    "CSUR_REPORT_DATE",
    "CSUR_PROJECT_ID",
    "CSUR_ROAD_TYPE",
    "CSUR_MOISTURE_CODE",
    "BIOZ_ZONE_ID",
    "CSUR_SIDE_SLOPE_PERC",
    "CSUR_BOULDER_AREA_PERC",
    "PREPARED_BY",
    "CSUR_COMMENT",
    "CSUR_MT_HARD_ROCK_PERC",
    "CSUR_MT_RIPPABLE_ROCK_PERC",
    "CSUR_MT_COARSE_PERC",
    "CSUR_MT_FINE_PERC",
    "CSUR_MT_ORGANIC_PERC",
    CAST("TOTAL_PCT" AS NUMBER(38,10)) AS "TOTAL_PCT",
    "CSUR_AS_CODE",
    "CSUR_AS_LENGTH_KM",
    "CSUR_AS_SURFACE_WIDTH_M",
    "CSUR_AS_TYPE",
    "CSUR_AS_DEPTH_M",
    "CSUR_AS_DISTANCE_TO_SOURCE_KM",
    "CSUR_AS_ACTUAL_COST",
    "CSUR_AS_TTT_TRANSFER_COST",
    "CSUR_AS_OTHER_TRANSFER_COST",
    CAST("ADDL_STAB_COST" AS NUMBER(38,10)) AS "ADDL_STAB_COST",
    CAST("ADDL_STAB_COST_PER_KM" AS NUMBER(38,10)) AS "ADDL_STAB_COST_PER_KM",
    "CSUR_SG_LENGTH_KM",
    "CSUR_SG_SURFACE_WIDTH_M",
    "CSUR_SG_ACTUAL_COST",
    "CSUR_SG_TTT_TRANSFER_COST",
    "CSUR_SG_OTHER_TRANSFER_COST",
    CAST("SUB_GRADE_COST" AS NUMBER(38,10)) AS "SUB_GRADE_COST",
    "CSUR_SG_LANDING_COST",
    "CSUR_SG_END_HAUL_COST",
    "CSUR_SG_OVERLAND_COST",
    "CSUR_SG_OTHER_ENG_COST",
    "CSUR_END_HAUL_DISTANCE_M",
    "CSUR_END_HAUL_VOLUME_M3",
    "CSUR_OVERLAND_DISTANCE_M",
    "CSUR_OVERLAND_VOLUME_M3",
    "CSUR_SEQ_NBR",
    "ROAD_SEQ_NBR",
    "CSUR_SEQ_NBR_LNG"
FROM forestview.V_INTERIOR_COST_SURVEY_ROADS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_MARK' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_MARK' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "LICN_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MARK_SEQ_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "TENURE",
    "LICENCE_ID",
    "PERMIT_ID",
    "MARK_ID",
    "DIVI_DIV_NBR",
    CAST("MARK_CRUISE_VOLUME_M3" AS NUMBER(38,10)) AS "MARK_CRUISE_VOLUME_M3",
    CAST("HBS_VOLUME_BILLED" AS NUMBER(38,10)) AS "HBS_VOLUME_BILLED",
    "MARK_STATE",
    "MARK_DESCRIPTION",
    "HBS_UPDATE_TIMESTAMP",
    CAST("MARK_RW_VOLUME_M3" AS NUMBER(38,10)) AS "MARK_RW_VOLUME_M3",
    CAST("APPRAISAL_VOLUME" AS NUMBER(38,10)) AS "APPRAISAL_VOLUME",
    "FIELD_TEAM_DESC",
    "LICN_LICENCE_STATE"
FROM forestview.V_MARK' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_MARK_ACTIVITY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_MARK_ACTIVITY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "MARK_SEQ_NBR",
    "ACTI_SEQ_NBR",
    "ACTIVITY_CLASS",
    "ACTIVITY_TYPE",
    "ACTIVITY_DATE",
    "STATUS_IND"
FROM forestview.V_MARK_ACTIVITY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_NON_FRPA_RESULTS_STRATEGIES' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_NON_FRPA_RESULTS_STRATEGIES' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CMCM_SEQ_NBR",
    "DIVI_DIV_NBR",
    CAST("SILP_SEQ_NBR" AS NUMBER(38,10)) AS "SILP_SEQ_NBR",
    "CMTG_CONTENT",
    "CMCM_TEXT"
FROM forestview.V_NON_FRPA_RESULTS_STRATEGIES' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_STEWARD' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_STEWARD' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "RDST_START_METRE_NBR",
    "RDST_END_METRE_NBR",
    CAST("RDST_LENGTH" AS NUMBER(38,10)) AS "RDST_LENGTH",
    "RDST_STEWARD_NAME",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RDST_SEQ_NBR_LNG"
FROM forestview.V_ROAD_STEWARD' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_STRUCTURE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_STRUCTURE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVISION_NUMBER",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "RSTR_OBJECT_TYPE",
    "RSTR_PLANNED_DATE",
    "RSTR_INSTALL_DATE",
    "RSTR_REPLACE_DATE",
    "RSTR_DEACTIVATE_DATE",
    "RSTR_CLASS_TYPE",
    "RSTR_STATUS_CODE",
    "RSTR_STATUS_DATE",
    "RSTR_AT_METRE_NBR",
    "RSTR_LOCATION_DESC",
    "RSTR_OBJECT_COMMENTS",
    "RSTR_FILE_ID",
    "RSTR_LATITUDE",
    "RSTR_LONGITUDE",
    "RSTR_LAT_LONG_DATATYPE",
    "RSTR_DRAWINGS_ON_FILE",
    "RSTR_OBJECT_SUBTYPE",
    "RSTR_STRUCTURE_ID",
    "RSTR_BUDGETED_COST",
    "RSTR_ACTUAL_COST",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RSTR_SEQ_NBR",
    "RSTR_SEQ_NBR_LNG"
FROM forestview.V_ROAD_STRUCTURE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_STRUCTURE_ATTR' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_STRUCTURE_ATTR' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "ROAD_ROAD_NAME",
    "URI",
    "RSTR_OBJECT_TYPE",
    "RSTR_AT_METRE_NBR",
    "RSTR_START_METRE_NBR",
    "SDEF_ATTRIBUTE_TYPE",
    "SATR_DATA_DESC",
    "SATR_UNIT_CODE",
    "SDEF_SORT_NBR",
    "SDEF_OPTION_IND",
    "SDEF_ABR_CODE_ID",
    "SDEF_COST_SURVEY_IND",
    "ROAD_SEQ_NBR",
    "RSTR_SEQ_NBR",
    "SATR_SEQ_NBR",
    "SATR_SEQ_NBR_LNG",
    "SDEF_SEQ_NBR"
FROM forestview.V_ROAD_STRUCTURE_ATTR' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_STRUCTURE_CULVERT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_STRUCTURE_CULVERT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "RSTR_OBJECT_TYPE",
    "RSTR_OBJECT_SUBTYPE",
    "RSTR_AT_METRE_NBR",
    "RSTR_STRUCTURE_ID",
    "RSTR_STATUS_CODE",
    "RSTR_CLASS_TYPE",
    "DIAM_DIAMETER_WIDTH",
    "DIAM_DIAMETER_UNIT_CODE",
    "DIAM_LENGTH_LGTH",
    "DIAM_LENGTH_UNIT_CODE",
    "DIAM_FLOW_RATE",
    CAST("DIAM_SEQ_NBR" AS NUMBER(38,10)) AS "DIAM_SEQ_NBR",
    "ROAD_SEQ_NBR",
    "RSTR_SEQ_NBR",
    "RSTR_SEQ_NBR_LNG"
FROM forestview.V_ROAD_STRUCTURE_CULVERT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_STRUCTURE_INSPECTION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_STRUCTURE_INSPECTION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "RSTR_SEQ_NBR",
    "INSP_SEQ_NBR",
    "METH_METHOD_TYPE",
    "INSP_PLANNED_DATE",
    "INSP_INSPECTION_DATE",
    "INSP_INSPECTION_FILE_ID",
    "INSP_INSPECTOR_TYPE",
    "PERFORMED_BY",
    "INSP_BUDGETED_COST",
    "INSP_ACTUAL_COST",
    CAST("VARIANCE" AS NUMBER(38,10)) AS "VARIANCE",
    "RESPONSIBILITY",
    "INSP_CONDITION_DESC",
    "INSP_INSPECTION_MEMO",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "INSP_SEQ_NBR_LNG"
FROM forestview.V_ROAD_STRUCTURE_INSPECTION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_STRUCTURE_MAINTENANCE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_STRUCTURE_MAINTENANCE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "RSTR_SEQ_NBR",
    "MAIN_SEQ_NBR",
    "INSP_SEQ_NBR",
    "MAIN_PLANNED_DATE",
    "MAIN_COMPLETED_DATE",
    "METH_METHOD_TYPE",
    "MAIN_BUDGETED_COST",
    "MAIN_ACTUAL_COST",
    CAST("VARIANCE" AS NUMBER(38,10)) AS "VARIANCE",
    "CTOR_NAME",
    "RESPONSIBILITY",
    "MAIN_PRIORITY",
    "MAIN_ACTIVITY_MEMO",
    "MAIN_ACTIVITY_TYPE",
    "MAIN_METHOD_TYPE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "MAIN_SEQ_NBR_LNG"
FROM forestview.V_ROAD_STRUCTURE_MAINTENANCE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_USE_PERMIT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_USE_PERMIT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_NAME",
    "ROAD_URI",
    "RUSE_START_METRE_NBR",
    "RUSE_END_METRE_NBR",
    CAST("RUSE_LENGTH" AS NUMBER(38,10)) AS "RUSE_LENGTH",
    "RDPM_START_DATE",
    "RDPM_EXPIRY_DATE",
    "RDPM_PERMIT_TYPE",
    "RDPM_PERMIT_ID",
    "RUSE_PERMIT_ID",
    "RUSE_PRIMARY_NAME",
    "RUSE_SECONDARY_NAME",
    "RUSE_TENURE_TYPE",
    "RUSE_AMENDMENT_NBR",
    "RUSE_SECTION_ID",
    "RUSE_POC",
    "RUSE_MEMO",
    "RUSE_GROSS_HA_AREA",
    "RUSE_RIGHT_OF_WAY_WIDTH",
    "RUSE_CRUISE_M3_VOL",
    "RUSE_HARV_COMPLETED_STATUS",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RDPM_SEQ_NBR",
    "RUSE_SEQ_NBR",
    "RUSE_SEQ_NBR_LNG"
FROM forestview.V_ROAD_USE_PERMIT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SALE_FORECAST_TKA_TEMP' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SALE_FORECAST_TKA_TEMP' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "FIELD_TEAM",
    "LICN_LICENCE_ID",
    "CUTB_BLOCK_ID",
    "OPAR_OPERATING_AREA_NAME",
    "BLAL_MERCH_HA_AREA",
    CAST("CRUISE_M3" AS NUMBER(38,10)) AS "CRUISE_M3",
    CAST("CRUISE_M3_PER_TREE" AS NUMBER(38,10)) AS "CRUISE_M3_PER_TREE",
    "CRUISE_LABEL",
    CAST("EST_M3" AS NUMBER(38,10)) AS "EST_M3",
    CAST("EST_M3_PER_TREE" AS NUMBER(38,10)) AS "EST_M3_PER_TREE",
    "EST_LABEL",
    CAST("FC_M3" AS NUMBER(38,10)) AS "FC_M3",
    CAST("FC_M3_PER_TREE" AS NUMBER(38,10)) AS "FC_M3_PER_TREE",
    "FC_LABEL",
    CAST("OCULAR_M3" AS NUMBER(38,10)) AS "OCULAR_M3",
    CAST("OCULAR_M3_PER_TREE" AS NUMBER(38,10)) AS "OCULAR_M3_PER_TREE",
    "OCULAR_LABEL",
    CAST("COMP_M3" AS NUMBER(38,10)) AS "COMP_M3",
    CAST("COMP_M3_PER_TREE" AS NUMBER(38,10)) AS "COMP_M3_PER_TREE",
    "COMP_LABEL",
    "SISY_SILVICULTURAL_SYSTEM_ID",
    CAST("NUM_SYSTEM" AS NUMBER(38,10)) AS "NUM_SYSTEM",
    "STATUS"
FROM forestview.V_SALE_FORECAST_TKA_TEMP' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SILVICULTURAL_SYSTEM' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SILVICULTURAL_SYSTEM' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    "SILP_SEQ_NBR",
    "DIVI_DIV_NBR",
    "SISY_SILVICULTURAL_SYSTEM_ID",
    "SIVA_VARIANT_ID",
    "SIPH_CUT_PHASE_ID",
    "SRTY_RESERVE_TYPE",
    "SPSS_AREA",
    "SPSS_STAND_STRUCTURE_SITE_COND",
    "SPSS_LEAVE_TREE_SPECIES_COMM",
    "SPSS_COMPOSITION_GOALS",
    "SPSS_RESIDUAL_BASAL_AREA",
    "SPSS_RESIDUAL_BASAL_DENSITY",
    "SPSS_OPENING_SIZE_HA_MAX",
    "SPSS_OPENING_SIZE_HA_MIN",
    "SPSS_OPENING_SIZE_HA_AVG"
FROM forestview.V_SILVICULTURAL_SYSTEM' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SILVICULTURE_AMENDMENT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SILVICULTURE_AMENDMENT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SAMM_AMENDMENT_NBR",
    "SAMM_AMENDMENT_DATE",
    "SAMM_LICENSEE_APPR_DATE",
    "SAMM_DIST_APPR_DATE",
    "SAMM_MAP_REQUIRED",
    "SAMM_COMMENT",
    "SAMM_AMENDMENT_TYPE",
    "SAMM_STATUS",
    "AMENDMENT_TYPE_CODE",
    "AMND_DESCRIPTION",
    "RESPONSIBILITY",
    "SILP_SEQ_NBR"
FROM forestview.V_SILVICULTURE_AMENDMENT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_GAP_ANALYSIS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_GAP_ANALYSIS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "DIVI_DIVISION_NAME",
    "ROAD_SEQ_NBR",
    "URI",
    "ROAD_ROAD_NAME",
    "ROAD_ROAD_DESC",
    "FIELD_TEAM_DESC",
    "ROAD_END_METRE_NBR",
    "POC",
    CAST("POT" AS NUMBER(38,10)) AS "POT",
    CAST("PRIME_ROAD_SEQ_NBR" AS NUMBER(38,10)) AS "PRIME_ROAD_SEQ_NBR",
    "RDST_SEQ_NBR",
    "RDST_STEWARD_NAME",
    "RCLS_SEQ_NBR",
    "RCLS_CLASS_TYPE",
    "RCLS_ACCOUNTING_TYPE",
    "RSTA_SEQ_NBR",
    "RSTA_STATUS_CODE",
    "RSTA_ROAD_STATE",
    "RUSE_SEQ_NBR",
    "RDPM_PERMIT_ID",
    "RUSE_SECTION_ID",
    "RDPM_PERMIT_TYPE",
    "RDPM_START_DATE",
    "RDPM_EXPIRY_DATE",
    "RDPM_AMENDMENT_NBR",
    "RCOM_SEQ_NBR",
    "CONST_METHOD_TYPE",
    "RCOM_PLANNED_DATE",
    "RCOM_COMPLETION_DATE",
    "RCOM_BUDGETED_COST",
    "RCOM_METHOD",
    "DEAC_SEQ_NBR",
    "DEAC_PLANNED_DATE",
    "DEAC_END_DATE",
    "DEAC_METHOD_TYPE",
    "DEAC_LEVEL_TYPE",
    "DEAC_BUDGETED_COST"
FROM forestview.V_ROAD_GAP_ANALYSIS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_BLOCK' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_BLOCK' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "FIELD_TEAM_DESC",
    "NAV_NAME",
    "TENURE",
    "LICENCE_ID",
    "PERMIT_ID",
    "MARK_ID",
    "BLOCK_ID",
    "BLOCK_NBR",
    "UBI",
    "OPENING",
    "OP_AREA",
    "OPAR_OPERATING_AREA_NAME",
    "OZON_OPERATING_ZONE_ID",
    "SUPPLY_BLOCK",
    "EBM_INDICATOR",
    "PHOTO",
    "LATITUDE",
    "LONGITUDE",
    "PROV_FRST_CONFLICT",
    "MAPSHEET_ID",
    "LANDSCAPE_UNIT",
    "SP_EXEMPT",
    "STAND_TYPE",
    "AGE_CLASS",
    "HGT_CLASS",
    "STK_CLASS",
    CAST("SITE_INDEX" AS NUMBER(38,10)) AS "SITE_INDEX",
    "SOURCE",
    "FDP_STATUS",
    "FUNDING_CODE",
    "CUTB_BLOCK_MEMO",
    "GROSS_AREA",
    "EST_AREA",
    "MERCH_AREA",
    "HARVEST_AREA",
    CAST("REMAINING_AREA" AS NUMBER(38,10)) AS "REMAINING_AREA",
    "CRUISE_VOL",
    "DATA_SOURCE",
    "HARVEST_VOL",
    CAST("REMAINING_VOL" AS NUMBER(38,10)) AS "REMAINING_VOL",
    "BLAL_USR_CRUISE_M3_VOL",
    "RW_AREA",
    "BLAL_RW_VOL",
    "CUTB_FORMA_PRINT_DATE",
    "CUTB_FORMA_PRINTED",
    "CUTB_BLOCK_STATE",
    "PMOD_MODIFIER_ID",
    "CUTB_LOCATION",
    "SUOP_SUBOP_AREA_ID",
    "SUOP_SUBOP_AREA_NAME",
    "CUTB_OPENING_ID",
    "LICN_LICENCE_STATE",
    "SEED_ZONE",
    "CUTB_FILE_ID",
    CAST("MIN_ELEVATION" AS NUMBER(38,10)) AS "MIN_ELEVATION",
    CAST("MAX_ELEVATION" AS NUMBER(38,10)) AS "MAX_ELEVATION",
    "BCAT_CATEGORY_CODE",
    "CUTB_ACCESS_RESTRICTION",
    "REGIME_CREATED_BY",
    "TREG_REGIME_ID",
    "TREG_REGIME_NAME",
    "TREG_CREATE_DATE",
    "TREG_ACTIVE_IND",
    "TREG_DEF_IND",
    "NAV_ID",
    "FIZ",
    "HVS_STATUS",
    "HVS_TARGET_DATE",
    "HVS_STATUS_DATE",
    "HVC_STATUS",
    "HVC_TARGET_DATE",
    "HVC_STATUS_DATE",
    "MANU_SEQ_NBR",
    "LICN_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MARK_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "CUTB_CPRP_PROTECTION_IND",
    "CUTB_RC_RISK_RATING",
    "CUTB_RC_RISK_SOURCE",
    "CUTB_RC_RISK_COMMENTS"
FROM forestview.V_BLOCK' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_BLOCK_ACTIVITY_ALL' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_BLOCK_ACTIVITY_ALL' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "TENURE",
    "LICENCE_ID",
    "PERMIT_ID",
    "MARK_ID",
    "BLOCK_ID",
    "BLOCK_NBR",
    "CUTB_SEQ_NBR",
    "ACTI_SEQ_NBR",
    "LICN_SEQ_NBR",
    "ACTT_SEQ_NBR",
    "ACTIVITY_CLASS",
    "ACTIVITY_TYPE",
    "ACTT_KEY_IND",
    "ACTIVITY_DATE",
    "ACTI_STATUS_IND",
    "ACCL_OBJECT_TYPE",
    "ACTI_RESPONSIBILITY",
    "CTOR_NAME",
    "ACTI_COST",
    "ACTI_TARGET_DATE",
    "ACTI_TARGET_COST",
    "ACTI_COMMENTS",
    "DIVI_DIV_NBR",
    "FIELD_TEAM_DESC",
    "UBI",
    "LICN_LICENCE_STATE"
FROM forestview.V_BLOCK_ACTIVITY_ALL' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SILVICULTURE_OVERLAY_XREF' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SILVICULTURE_OVERLAY_XREF' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SIOX_SEQ_NBR",
    "SIOX_HA_AREA",
    "SIOX_DIGITISED_IND",
    "STUN_SEQ_NBR",
    "SILA_SEQ_NBR",
    "PLUN_SEQ_NBR",
    "SISH_SEQ_NBR",
    "CBHM_SEQ_NBR",
    "SIOX_COMMENTS"
FROM forestview.V_SILVICULTURE_OVERLAY_XREF' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SILVICULTURE_PRESCRIPTION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SILVICULTURE_PRESCRIPTION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "PRESCRIPTION_PREPARED_BY",
    "ADDITIONAL_SP_COMMENTS",
    "RFP_CERTIFICATION_COMMENTS",
    "SILP_ADMIN_ASSESSMENT_COMMENTS",
    "AMENDMENTS_COMMENTS",
    "SILP_SP_REPORT_FORMAT",
    "SILP_PERM_ACCESS_MAX_AREA",
    "SILP_PERM_ACCESS_MAX_PCT",
    "MAX_ROADSIDE_DISTURBANCE",
    "SOIL_CONSERVATION_COMMENTS",
    "SILP_TEMP_ACCESS_MAX_REHAB",
    "SILP_TEMP_ACCESS_COMMENTS",
    "SILP_SLOPE_INSTABILITY",
    "SILP_SLOPE_REQUIRED",
    "SILP_SLOPE_ASSESSMENT",
    "FOREST_HEALTH_COMMENTS",
    "PEST_SPECIFIC_COMMENTS",
    "SILP_PEST_REQUIRED",
    "SILP_PEST_ASSESSMENT",
    "SILP_VEGETATION_MANAGEMENT",
    "SILP_VEGETATION_REQUIRED",
    "SILP_LIVESTOCK_USED",
    "SILP_RIPARIAN_COMMENTS",
    "SILP_RIPARIAN_REQUIRED",
    "SILP_RIPARIAN_ASSESSMENT",
    "SILP_GULLY_MANAGE_STRATEGY",
    "SILP_GULLY_REQUIRED",
    "SILP_GULLY_ASSESSMENT",
    "SILP_ARCHAEOLOGICAL_SITES",
    "SILP_ARCHEOSITE_REQUIRED",
    "SILP_ARCHAEOLOGY_ASSESSMENT",
    "SILP_LTERM_MAN_OBJ",
    "SILP_WILDLIFE",
    "SILP_LICHEN_REQUIRED",
    "SILP_LICHEN_ASSESSMENT",
    "SILP_RANGE",
    "SILP_RANGE_TENURE_HOLDER",
    "SILP_FISHERIES",
    "SILP_COARSE_WOODY_DEBRIS",
    "PERFORMANCE_STD_PCT",
    "BLOCK_TARGET_PCT",
    "SILP_COARSE_WOODY_DEBRIS_M3_HA",
    "SILP_WATERSHEDS",
    "SILP_COMMUNITY_WATERSHED_IND",
    "SILP_SENSITIVE_AREAS",
    "SILP_RECREATION",
    "SILP_NA_CONDITIONS",
    "SILP_VISUAL_LANDSCAPE",
    "SILP_VISUAL_REQUIRED",
    "SILP_VISUAL_ASSESSMENT",
    "SILP_CULTURAL_HERITAGE",
    "SILP_BIO_DIVERSITY",
    "SILP_BIODIV_PERF_STD_START_PCT",
    "SILP_BIODIV_PERF_STD_END_PCT",
    "SILP_BIO_DIVERS_BLOCK_TGT_PCT",
    "SILP_WILDLIFE_TREE_CREDIT_HA",
    "SILP_BIO_DIVERSITY_CALC_IND",
    "SILP_OTHER_RESOURCES",
    "SILP_TRAPPER_REGISTRATION",
    "SILP_GUIDE_REGISTRATION",
    "CUTB_SEQ_NBR",
    "SILP_SEQ_NBR",
    "DIVI_DIV_NBR"
FROM forestview.V_SILVICULTURE_PRESCRIPTION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SILVICULTURE_STRATUM_HISTORY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SILVICULTURE_STRATUM_HISTORY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "SISH_SEQ_NBR",
    "SILA_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "SISH_STRATUM_NAME",
    "SISH_DIGITISED_IND",
    "SISH_HA_AREA",
    "SISH_GREEN_UP_DATE",
    "SISH_GREEN_UP_IND",
    "SISH_FREE_GROW_DATE",
    "SISH_FREE_GROW_IND",
    "SISH_REGEN_DELAY_MET_DATE",
    "SISH_REGEN_DELAY_MET_IND",
    "SILA_COMMENTS",
    "STST_STOCKING_STATUS_ID",
    "STTP_STOCKING_TYPE_ID",
    "SISH_MORTALITY_PCT",
    "SISH_POST_HARVEST_DATE",
    "SISH_POST_HARVEST_IND",
    "SISH_PLOTS",
    "SISH_ESTABLISHMENT_YEAR",
    "SISH_CROP_STEMS_PER_HA",
    "SISH_TOTAL_STEMS_PER_HA",
    "SISH_SITE_INDEX",
    "SISH_PCT_DISTRIBUTION",
    "SISH_PCT_PRODUCTIVE",
    "SISH_PREFIX",
    "SISH_CLASS",
    "SICA_ACTIVITY_NAME",
    "SILA_START_DATE",
    "SILA_COMPLETION_DATE",
    "SILA_STATUS",
    "SILA_GROSS_HA_AREA",
    "STUN_ID",
    "SIPR_PROJECT_ID",
    "STUN_SEQ_NBR",
    "SUHA_HA_AREA",
    "SUHA_DIGITISED_IND"
FROM forestview.V_SILVICULTURE_STRATUM_HISTORY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SILV_PROJECT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SILV_PROJECT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SIPR_SEQ_NBR",
    "CTOR_SEQ_NBR",
    "CLOC_SEQ_NBR",
    "PERS_SEQ_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "PROJECT_ID",
    "CONTRACTOR",
    "CONTRACT_COORD",
    "START_DATE",
    "START_STATUS",
    "END_DATE",
    "END_STATUS",
    "VIEWING_DATE",
    "UNIT_BID_CODE",
    "TOTAL_BID_PRICE",
    "TOTAL_OVERHEAD_COST",
    CAST("TOTAL_ACTUAL_COST" AS NUMBER(38,10)) AS "TOTAL_ACTUAL_COST",
    "RESULT_TYPE",
    "SIPR_CONTR_RES_COMMENT",
    "SIPR_GENERAL_COMMENT",
    "DIVI_DIV_NBR",
    CAST("DONE" AS NUMBER(38,10)) AS "DONE",
    CAST("PLANNED" AS NUMBER(38,10)) AS "PLANNED",
    "SIPR_PROJECT_TYPE",
    "SIPR_CREW_HIRE_CODE",
    "SIPR_RESULT_TYPE"
FROM forestview.V_SILV_PROJECT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SILV_PROJECT_BLOCK' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SILV_PROJECT_BLOCK' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    "SILA_SEQ_NBR",
    "SIPR_SEQ_NBR",
    "SIPR_PROJECT_ID",
    "DIVI_DIV_NBR"
FROM forestview.V_SILV_PROJECT_BLOCK' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SPECIES_COMPOSITION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SPECIES_COMPOSITION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SIST_SEQ_NBR",
    "SLAY_LAYER",
    "SISP_SPECIES_ID",
    "SPCO_PERCENT",
    "LEADING_SPECIES_IND",
    "SPCO_AGE",
    "SPCO_HEIGHT",
    "SPCO_STEMS_PER_HA",
    "SPCO_INCREMENT",
    "SPCO_WELL_SPACED",
    "SPCO_FREE_GROWING"
FROM forestview.V_SPECIES_COMPOSITION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SPECIES_COMPOSITION_HIST' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SPECIES_COMPOSITION_HIST' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SISH_SEQ_NBR",
    "SLAY_LAYER",
    "SISP_SPECIES_ID",
    "SPCH_PERCENT",
    "SPCH_AGE",
    "SPCH_HEIGHT",
    "SPCH_STEMS_PER_HA",
    "SPCH_INCREMENT",
    "SPCH_WELL_SPACED",
    "SPCH_FREE_GROWING"
FROM forestview.V_SPECIES_COMPOSITION_HIST' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_STOCKING_STANDARD' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_STOCKING_STANDARD' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "STUN_SEQ_NBR",
    "LAYT_LAYER_CODE",
    "STSD_POST_SPACING_MIN",
    "STSD_POST_SPACING_MAX",
    "STSD_MAX_CONIFEROUS",
    "STSD_WELL_SPACED_TARGET",
    "STSD_WELL_SPACED_MIN",
    "STSD_WELL_SPACED_PREF_MIN",
    "STSD_WELL_SPACED_MIN_HORIZ",
    "STSD_MIN_PRUNE_HGT_M",
    "STSD_HEIGHT_REL_TO_COMP_PCT",
    "STSD_HEIGHT_REL_TO_COMP_CM",
    "SILP_STOCKING_REQ_COMMENTS"
FROM forestview.V_STOCKING_STANDARD' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_STRATUM_STATUS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_STRATUM_STATUS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    "SIST_SEQ_NBR",
    "STRATUM_NAME",
    "AREA",
    "SICA_ACTIVITY_NAME",
    "BASE",
    "TECHNIQUE",
    "METHOD",
    "START_DATE",
    "COMPLETE_DATE",
    "CURRENT_STATUS",
    "DIVI_DIV_NBR",
    "SSTATUS",
    "SIPR_SEQ_NBR",
    "STUN_SEQ_NBR",
    "STUN_ID",
    "STYPE",
    "SIST_GREEN_UP_DATE",
    "SIST_GREEN_UP_IND",
    "SIST_REGEN_DELAY_MET_DATE",
    "SIST_REGEN_DELAY_MET_IND",
    "SIST_POST_HARVEST_DATE",
    "SIST_POST_HARVEST_IND",
    "SIST_FREE_GROW_DATE",
    "SIST_FREE_GROW_IND"
FROM forestview.V_STRATUM_STATUS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SU' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SU' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    "STUN_SEQ_NBR",
    "STUN_ID",
    "SU_REGEN_OBLIGATION",
    "GROSS_AREA",
    "NCBR_AREA",
    CAST("NAR_NUM" AS NUMBER(38,10)) AS "NAR_NUM",
    "NAR",
    CAST("NPNAT_AREA" AS NUMBER(38,10)) AS "NPNAT_AREA",
    CAST("NPUNN_AREA" AS NUMBER(38,10)) AS "NPUNN_AREA",
    "DIVI_DIV_NBR",
    "STUN_MAX_DISTURBANCE",
    "REGEN_DELAY",
    "STUN_REGEN_DATE_EARLY",
    "EARLY_FTG",
    "STUN_FREEGROW_DATE_EARLY",
    "LATE_FTG",
    "STUN_FREEGROW_DATE_LATE",
    "SILV_SYSTEM_ID",
    "SIVA_VARIANT_ID",
    "SIPH_CUT_PHASE_ID",
    "FREE_GROW_DATE",
    "FREE_GROW_IND",
    "POST_HARV_DATE",
    "POST_HARV_IND",
    "REGEN_DATE",
    "REGEN_IND",
    "SHSD_SUBMISSION_DATE",
    "SHSD_SUBMISSION_IND",
    "STUN_FORMC_POSTHARV_PRN_DATE",
    "STUN_FORMC_REGEN_PRN_DATE",
    "STUN_FORMC_FREEGROW_PRN_DATE",
    "SUTY_TYPE_ID",
    "STUN_DESCRIPTION",
    "STDS_LEGAL_ID",
    CAST("SU_DISTURB_AREA" AS NUMBER(38,10)) AS "SU_DISTURB_AREA",
    "STUN_OBJECTIVE_TYPE",
    "STTY_ORIGINAL_STANDARD_TYPE",
    "STUN_ORIGINAL_STD_DECLARE_DATE",
    "STUN_DECLARATION_AREA",
    "STUN_DECLARATION_AREA_LOCK_IND",
    "STUN_DESIGNATION_CODE",
    "SPSS_LEAVE_TREE_SPECIES_COMM",
    "STUN_SOIL_COMPACTION",
    "STUN_SOIL_EROSION",
    "STUN_SOIL_DISPLACEMENT",
    "STUN_TYPE_UNFAVOURABLE",
    "STUN_SEDIMENT_RISK",
    "STUN_DEPTH_UNFAVOURABLE_MAX",
    "STUN_DEPTH_UNFAVOURABLE_MIN",
    "SILP_SP_REPORT_FORMAT"
FROM forestview.V_SU' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SU_ECO_ALLOCATION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SU_ECO_ALLOCATION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "ECOU_SEQ_NBR",
    "STUN_SEQ_NBR",
    "SUEA_HA_AREA",
    "SUEA_DIGITISED_IND",
    "CUTB_SEQ_NBR"
FROM forestview.V_SU_ECO_ALLOCATION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SU_LAYER' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SU_LAYER' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "STUN_SEQ_NBR",
    "STUN_ID",
    "LAYT_LAYER_CODE",
    "STSD_POST_SPACING_MIN",
    "STSD_POST_SPACING_MAX",
    "STSD_MAX_CONIFEROUS",
    "STSD_RESIDUAL_AREA",
    "STSD_WELL_SPACED_TARGET",
    "STSD_WELL_SPACED_MIN",
    "STSD_WELL_SPACED_PREF_MIN",
    "STSD_WELL_SPACED_MIN_HORIZ",
    "STSD_HEIGHT_REL_TO_COMP_PCT",
    "STSD_HEIGHT_REL_TO_COMP_CM",
    "STSD_MIN_PRUNE_HGT_M",
    "PREFERRED_SPECIES",
    "ACCEPTED_SPECIES",
    "CUTB_SEQ_NBR"
FROM forestview.V_SU_LAYER' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SU_STRATUM_ALLOCATION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SU_STRATUM_ALLOCATION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SIST_SEQ_NBR",
    "STUN_SEQ_NBR",
    "SUSA_HA_AREA",
    "SUSA_DIGITISED_IND"
FROM forestview.V_SU_STRATUM_ALLOCATION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_THIRD_PARTY_INTEREST' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_THIRD_PARTY_INTEREST' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    "TRDP_SEQ_NBR",
    "PATY_PARTY_TYPE_ID",
    "TRDP_IDENTIFIER_CODE",
    "DIVI_DIV_NBR"
FROM forestview.V_THIRD_PARTY_INTEREST' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_TIMBER_INVENTORY_DIP' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_TIMBER_INVENTORY_DIP' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BUSINESS_AREA_REGION_CATEGORY",
    "BUSINESS_AREA_REGION",
    "BUSINESS_AREA",
    "BUSINESS_AREA_CODE",
    "FIELDTEAM",
    "MANU_ID",
    "LICENCE_ID",
    "TENURE_TYPE",
    "PERM_PERMIT_ID",
    "MARK_MARK_ID",
    "BLOCK_ID",
    "UBI",
    "BLOCK_NBR",
    "SUB_OPERATING_AREA",
    "LICN_LICENCE_STATE",
    "CUTB_BLOCK_STATE",
    "DEFERRED_AT_REPORT_DATE",
    "INVENTORY_CATEGORY",
    "MERCH_AREA",
    "CRUISE_VOLUME",
    "RW_VOLUME",
    "RC_STATUS",
    "RC_DATE",
    CAST("RC_FISCAL" AS NUMBER(38,10)) AS "RC_FISCAL",
    "DR_STATUS",
    "DR_DATE",
    CAST("DR_FISCAL" AS NUMBER(38,10)) AS "DR_FISCAL",
    "DVS_STATUS",
    "DVS_DATE",
    CAST("DVS_FISCAL" AS NUMBER(38,10)) AS "DVS_FISCAL",
    "DSC_STATUS",
    "DSC_DATE",
    "DVC_STATUS",
    "DVC_DATE",
    CAST("DVC_FISCAL" AS NUMBER(38,10)) AS "DVC_FISCAL",
    CAST("DAYS_IN_DIP" AS NUMBER(38,10)) AS "DAYS_IN_DIP",
    "WOFF_STATUS",
    "WOFF_DATE",
    CAST("WOFF_FISCAL" AS NUMBER(38,10)) AS "WOFF_FISCAL",
    "AUC_STATUS",
    "AUC_DATE",
    "HI_STATUS",
    "HI_DATE",
    "HVS_STATUS",
    "HVS_DATE",
    "HVC_STATUS",
    "HVC_DATE",
    "FG_MET_STATUS",
    "FG_DATE",
    "DEF_CHANGE_OF_OP_PLAN_STATUS",
    "DEF_CHANGE_OF_OP_PLAN",
    "DEF_FIRST_NATIONS_STATUS",
    "DEF_FIRST_NATIONS",
    "DEF_LOSS_OF_ACCESS_STATUS",
    "DEF_LOSS_OF_ACCESS",
    "DEF_OTHER_STATUS",
    "DEF_OTHER",
    "DEF_PLANNING_CONSTRAINT_STATUS",
    "DEF_PLANNING_CONSTRAINT",
    "DEF_RETURNED_TO_BCTS_STATUS",
    "DEF_RETURNED_TO_BCTS",
    "DEF_STALE_DATED_FIELDWORK_STATUS",
    "DEF_STALE_DATED_FIELDWORK",
    "DEF_STAKEHOLDER_ISSUE_STATUS",
    "DEF_STAKEHOLDER_ISSUE",
    "DEF_ENVIRONMENTAL_STEWARDSHIP_INITIATIVE_STATUS",
    "DEF_ENVIRONMENTAL_STEWARDSHIP_INITIATIVE",
    "DEF_REACTIVATED_STATUS",
    "DEF_REACTIVATED",
    "OLD_GROWTH_STRATEGY_STATUS",
    "OLD_GROWTH_STRATEGY",
    "OGS_REACTIVATED_FOREST_HEALTH_STATUS",
    "OGS_REACTIVATED_FOREST_HEALTH",
    "OGS_REACTIVATED_FN_PROCEED_STATUS",
    "OGS_REACTIVATED_FN_PROCEED",
    "OGS_REACTIVATED_FIELD_VERIFIED_STATUS",
    "OGS_REACTIVATED_FIELD_VERIFIED",
    "OGS_REACTIVATED_MINOR_STATUS",
    "OGS_REACTIVATED_MINOR",
    "OGS_REACTIVATED_ROAD_STATUS",
    "OGS_REACTIVATED_ROAD",
    "OGS_REACTIVATED_RE_ENGINEERED_STATUS",
    "OGS_REACTIVATED_RE_ENGINEERED",
    "XXX_ZZZ_FLAG",
    "SPATIAL_FLAG",
    "RC_FLAG",
    "DR_FLAG",
    "DVS_FLAG",
    "DSC_FLAG",
    "DVC_FLAG",
    CAST("COUNT_OF_BLOCKS" AS NUMBER(38,10)) AS "COUNT_OF_BLOCKS",
    "LICN_SEQ_NBR",
    "MARK_SEQ_NBR",
    "CUTB_SEQ_NBR",
    CAST("OBJECTID" AS NUMBER(38,10)) AS "OBJECTID"
FROM forestview.V_TIMBER_INVENTORY_DIP' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SILV_STOCKSTAT_HISTORY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SILV_STOCKSTAT_HISTORY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            '
SELECT
    "SISH_SEQ_NBR",
    "SLAY_LAYER",
    "SRNK_RANK",
    "STST_STOCKING_STATUS_ID",
    "STTP_STOCKING_TYPE_ID",
    "SSSH_SURVEY_SOURCE",
    "SSSH_SURVEY_DATE",
    "SSSH_STOCK_AGE",
    "SSSH_STOCK_AGE_PLANT",
    "SSSH_STOCK_HEIGHT",
    "SSSH_CROWN_CLOSURE",
    "SSSH_REFERENCE_YEAR",
    "SICL_SITE_CLASS",

    CAST("SSSH_SITE_INDEX" AS NUMBER(38,10)) AS "SSSH_SITE_INDEX",
    CAST("SSSH_DENSITY" AS NUMBER(38,10)) AS "SSSH_DENSITY",
    CAST("SSSH_WELL_SPACED_DENSITY" AS NUMBER(38,10)) AS "SSSH_WELL_SPACED_DENSITY",
    CAST("SSSH_FREE_GROWING_DENSITY" AS NUMBER(38,10)) AS "SSSH_FREE_GROWING_DENSITY",

    "SRTY_RESERVE_TYPE",

    CAST("SSSH_BASAL_AREA" AS NUMBER(38,10)) AS "SSSH_BASAL_AREA",
    CAST("SSSH_CM_TOP_HEIGHT" AS NUMBER(38,10)) AS "SSSH_CM_TOP_HEIGHT",
    CAST("SSSH_CM_LEADER" AS NUMBER(38,10)) AS "SSSH_CM_LEADER",

    CAST("SSSH_VIGOUR_PCT_GOOD" AS NUMBER(38,10)) AS "SSSH_VIGOUR_PCT_GOOD",
    CAST("SSSH_VIGOUR_PCT_MEDIUM" AS NUMBER(38,10)) AS "SSSH_VIGOUR_PCT_MEDIUM",
    CAST("SSSH_VIGOUR_PCT_POOR" AS NUMBER(38,10)) AS "SSSH_VIGOUR_PCT_POOR",

    "SSSH_COMMENTS",

    CAST("SSSH_TOTAL_CONIFEROUS" AS NUMBER(38,10)) AS "SSSH_TOTAL_CONIFEROUS",
    CAST("SSSH_PLANTABLE_SPOTS" AS NUMBER(38,10)) AS "SSSH_PLANTABLE_SPOTS",
    CAST("SSSH_PREPABLE_SLASH_SPOTS" AS NUMBER(38,10)) AS "SSSH_PREPABLE_SLASH_SPOTS",
    CAST("SSSH_PREPABLE_BRUSH_SPOTS" AS NUMBER(38,10)) AS "SSSH_PREPABLE_BRUSH_SPOTS",
    CAST("SSSH_PREPABLE_DUFF_SPOTS" AS NUMBER(38,10)) AS "SSSH_PREPABLE_DUFF_SPOTS",
    CAST("SSSH_NON_PRODUCTIVE_SPOTS" AS NUMBER(38,10)) AS "SSSH_NON_PRODUCTIVE_SPOTS",
    CAST("SSSH_ROOT_COLLAR_DIAMETER" AS NUMBER(38,10)) AS "SSSH_ROOT_COLLAR_DIAMETER",

    "SSSC_SOURCE_CODE",

    CAST("SSSH_FREE_GROW_DENSITY_PREF" AS NUMBER(38,10)) AS "SSSH_FREE_GROW_DENSITY_PREF",
    CAST("SSSH_WELL_SPACED_DENSITY_PREF" AS NUMBER(38,10)) AS "SSSH_WELL_SPACED_DENSITY_PREF",
    CAST("SSSH_FREE_GROW_LCL_DENSITY" AS NUMBER(38,10)) AS "SSSH_FREE_GROW_LCL_DENSITY",
    CAST("SSSH_WELL_SPACED_LCL_DENSITY" AS NUMBER(38,10)) AS "SSSH_WELL_SPACED_LCL_DENSITY",
    CAST("SSSH_COUNTABLE_CONIFERS" AS NUMBER(38,10)) AS "SSSH_COUNTABLE_CONIFERS",

    "SSSH_REENTRY_YEAR",
    "SSSH_CVR_PATTERN",
    "SSSH_RES_OBJECTIVE",

    CAST("SSSH_TOTAL_WELL_SPACED" AS NUMBER(38,10)) AS "SSSH_TOTAL_WELL_SPACED",
    CAST("SSSH_GERMINANT_PER_HA" AS NUMBER(38,10)) AS "SSSH_GERMINANT_PER_HA",
    CAST("SSSH_GREENED_UP_TREE_PER_HA" AS NUMBER(38,10)) AS "SSSH_GREENED_UP_TREE_PER_HA"

FROM forestview.V_SILV_STOCKSTAT_HISTORY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SILVICULTURE_STOCKING_STATUS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SILVICULTURE_STOCKING_STATUS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SIST_SEQ_NBR",
    "SLAY_LAYER",
    "SRNK_RANK",
    "STTP_STOCKING_TYPE_ID",
    "SSTS_SURVEY_SOURCE",
    "SSTS_SURVEY_DATE",
    CAST("SSTS_STOCK_AGE" AS NUMBER(38,10)) AS "SSTS_STOCK_AGE",
    CAST("SSTS_STOCK_AGE_PLANT" AS NUMBER(38,10)) AS "SSTS_STOCK_AGE_PLANT",
    "SSTS_STOCK_HEIGHT",
    "SSTS_CROWN_CLOSURE",
    "SSTS_REFERENCE_YEAR",
    "SICL_SITE_CLASS",
    "SSTS_SITE_INDEX",
    "SSTS_DENSITY",
    "SSTS_WELL_SPACED_DENSITY",
    "SSTS_FREE_GROWING_DENSITY",
    "SRTY_RESERVE_TYPE",
    "SSTS_BASAL_AREA",
    "SSTS_CM_TOP_HEIGHT",
    "SSTS_CM_LEADER",
    "SSTS_VIGOUR_PCT_GOOD",
    "SSTS_VIGOUR_PCT_MEDIUM",
    "SSTS_VIGOUR_PCT_POOR",
    "SSTS_COMMENTS",
    "SSTS_TOTAL_CONIFEROUS",
    "SSTS_PLANTABLE_SPOTS",
    "SSTS_PREPABLE_SLASH_SPOTS",
    "SSTS_PREPABLE_BRUSH_SPOTS",
    "SSTS_PREPABLE_DUFF_SPOTS",
    "SSTS_NON_PRODUCTIVE_SPOTS",
    "SSTS_FORMC_PRINTED_IND",
    "SSTS_FORMC_DATE",
    "STST_STOCKING_STATUS_ID",
    "SSTS_ROOT_COLLAR_DIAMETER",
    "SSSC_SOURCE_CODE",
    "SSTS_FREE_GROW_DENSITY_PREF",
    "SSTS_WELL_SPACED_DENSITY_PREF",
    "SSTS_FREE_GROW_LCL_DENSITY",
    "SSTS_WELL_SPACED_LCL_DENSITY",
    "SSTS_COUNTABLE_CONIFERS",
    "SSTS_REENTRY_YEAR",
    "SSTS_CVR_PATTERN",
    "SSTS_RES_OBJECTIVE",
    "SSTS_TOTAL_WELL_SPACED",
    "SPECIES"
FROM forestview.V_SILVICULTURE_STOCKING_STATUS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_PLANTING' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_PLANTING' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    "SILA_SEQ_NBR",
    "PLUN_SEQ_NBR",
    "LICENCE_ID",
    "PERMIT_ID",
    "BLOCK_ID",
    "PLANT_ID",
    "BASE",
    "TECHNIQUE",
    "METHOD",
    "ACT_NAME",
    "FUND_FUNDING_CODE",
    "SILA_GROSS_HA_AREA",
    "START_DATE",
    "COMPLETE_DATE",
    "STATUS",
    "PLUN_HA_AREA",
    "CONTRACTOR",
    "DENSITY",
    "PLANT_SPECIES",
    "PLANT_SPECIES_AND_PCT",
    "COMMENTS",
    "DIVI_DIV_NBR",
    "SIPR_SEQ_NBR",
    "SIPR_PROJECT_ID",
    "PLUN_QUALITY_PERCENT",
    CAST("TREES" AS NUMBER(38,10)) AS "TREES",
    "PLUN_CONTRACT_MAX_SPACING",
    "PLUN_CONTRACT_MIN_SPACING",
    "PLUN_CONTRACT_TARGET_SPACING"
FROM forestview.V_PLANTING' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SILVICULTURE_ACTIVITY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SILVICULTURE_ACTIVITY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "FIELD_TEAM",
    "MANU_ID",
    "OPERATING_ZONE",
    "TENURE",
    "LICENCE",
    "PERMIT",
    "MARK",
    "BLOCK_ID",
    "BLOCK_NBR",
    "UBI",
    "BLOCK_STATE",
    "CUTB_OPENING",
    "SP_EXEMPT",
    "CRUISE_VOLUME",
    CAST("NAR_AREA" AS NUMBER(38,10)) AS "NAR_AREA",
    "HVS_STATUS",
    "HVS_DATE",
    CAST("HVS_FISCAL" AS NUMBER(38,10)) AS "HVS_FISCAL",
    "HVC_STATUS",
    "HVC_DATE",
    CAST("HVC_FISCAL" AS NUMBER(38,10)) AS "HVC_FISCAL",
    "TREATMENT_UNIT",
    "BLOCK_FUNDING",
    "ACTIVITY_FUNDING",
    "BASE",
    "TECHNIQUE",
    "METHOD",
    "ACTIVITY",
    "STATUS",
    "START_DATE",
    CAST("START_FISCAL" AS NUMBER(38,10)) AS "START_FISCAL",
    "COMPLETE_DATE",
    CAST("COMPLETE_FISCAL" AS NUMBER(38,10)) AS "COMPLETE_FISCAL",
    "SILA_SEASON",
    CAST("TREATMENT_AREA" AS NUMBER(38,10)) AS "TREATMENT_AREA",
    "SILA_GROSS_AREA_CHAR",
    CAST("COST_PER_HA_PLAN" AS NUMBER(38,10)) AS "COST_PER_HA_PLAN",
    CAST("COST_PER_HA" AS NUMBER(38,10)) AS "COST_PER_HA",
    CAST("PLANNED_TOTAL_COST" AS NUMBER(38,10)) AS "PLANNED_TOTAL_COST",
    CAST("ACTUAL_TOTAL_COST" AS NUMBER(38,10)) AS "ACTUAL_TOTAL_COST",
    "PLANNED_SERVICE_LINE",
    "PLANNED_SL_DESCRIPTION",
    CAST("ITEMS_PLANNED_TOTAL_COST" AS NUMBER(38,10)) AS "ITEMS_PLANNED_TOTAL_COST",
    "ACTUAL_SERVICE_LINE",
    "ACTUAL_SL_DESCRIPTION",
    CAST("ITEMS_ACTUAL_TOTAL_COST" AS NUMBER(38,10)) AS "ITEMS_ACTUAL_TOTAL_COST",
    "PLANTING_UNIT",
    "PLUN_DIGITISED_IND",
    "PLUN_PLANNED_TOTAL_COST",
    CAST("PLUN_ITEM_PLANNED_TOTAL_COST" AS NUMBER(38,10)) AS "PLUN_ITEM_PLANNED_TOTAL_COST",
    "PLUN_ACTUAL_TOTAL_COST",
    CAST("PLUN_ITEM_ACTUAL_TOTAL_COST" AS NUMBER(38,10)) AS "PLUN_ITEM_ACTUAL_TOTAL_COST",
    "PLUN_HA_AREA",
    "PLUN_NET_AREA",
    "PLUN_DENSITY",
    "TOTAL_TREES",
    "SICA_KEY_IND",
    "OBJECTIVE1",
    "OBJECTIVE2",
    "OBJECTIVE3",
    "RESPONSIBILITY",
    "TARGET_SPECIES",
    "APPLY_RATE",
    "PMP_NBR",
    "COMMENTS",
    "SILA_FORMB_DATE",
    "SILA_FORMB_PRINTED",
    "PROJECT",
    "PROJECT_START_DATE",
    "PROJECT_END_DATE",
    "CONTRACT",
    "CONTRACTOR",
    "CONTRACT_START_DATE",
    "CONTRACT_END_DATE",
    "PLUN_MODIFIEDBY",
    "PLUN_MODIFIEDON",
    "PLUN_MODIFIEDUSING",
    "MODIFIEDBY",
    "MODIFIEDON",
    "PRIMARY_RECORD",
    "EMSP_SEQ_NBR",
    "EMSC_SEQ_NBR",
    CAST("PLUN_SEQ_NBR" AS NUMBER(38,10)) AS "PLUN_SEQ_NBR",
    "SILA_SEQ_NBR",
    "SICA_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "LICN_SEQ_NBR"
FROM forestview.V_SILVICULTURE_ACTIVITY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_PEST_INFESTATION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_PEST_INFESTATION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SLAY_LAYER",
    "SIST_SEQ_NBR",
    "SPST_PEST_DESC",
    "SPST_PEST_CODE",
    "PEIN_PERCENT"
FROM forestview.V_PEST_INFESTATION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_PLANTING_SPECIES' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_PLANTING_SPECIES' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "PLUN_SEQ_NBR",
    "SISP_SPECIES_ID",
    "PLSP_NUMBER_TREES",
    "SISL_SEED_LOT_NUMBER",
    "SIRK_REQUEST_KEY",
    "PLSP_SEED_WEIGHT",
    "PLSP_PRICE_PER_TREE",
    "CUTB_SEQ_NBR",
    "SIRK_STOCK_TYPE",
    CAST("CC_PERCENTAGE" AS NUMBER(38,10)) AS "CC_PERCENTAGE",
    "PLSP_TREES_OUTSIDE_ZONE",
    "PLSP_COMMENT",
    CAST("SIRK_YEARS_ORIGINAL_CONTAINER" AS NUMBER(38,10)) AS "SIRK_YEARS_ORIGINAL_CONTAINER",
    CAST("SIRK_YEARS_TRANSPLANTED" AS NUMBER(38,10)) AS "SIRK_YEARS_TRANSPLANTED",
    "SISL_SEED_ZONE",
    "SISL_GENETIC_CLASS",
    "SISL_GENETIC_WORTH",
    "SISL_ELEVATION_MIN",
    "SISL_ELEVATION_MAX",
    "SISL_ELEVATION_MEAN",
    CAST("SISL_LATITUDE_MIN" AS NUMBER(38,10)) AS "SISL_LATITUDE_MIN",
    CAST("SISL_LATITUDE_MAX" AS NUMBER(38,10)) AS "SISL_LATITUDE_MAX",
    CAST("SISL_LATITUDE_MEAN" AS NUMBER(38,10)) AS "SISL_LATITUDE_MEAN",
    CAST("SISL_LONGITUDE" AS NUMBER(38,10)) AS "SISL_LONGITUDE",
    "SISL_NUMBER_TREES",
    "SISL_KILOGRAMS",
    "SISL_OWNER_NAME",
    "SISL_COMMENTS",
    "SISL_VIABILITY_PCT",
    "SSUC_SEED_USE_CODE",
    "SISL_ACTIVE_IND",
    "BIOZ_ZONE_ID"
FROM forestview.V_PLANTING_SPECIES' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_QB_VALUATION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_QB_VALUATION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "NAV_NAME",
    "TENURE",
    "LICENCE_ID",
    "PERMIT_ID",
    "MARK_ID",
    "BLOCK_ID",
    "BLOCK_NBR",
    "SPPR_EFFECTIVE_DATE",
    "HAULING_SOURCE",
    CAST("HAULING_COST" AS NUMBER(38,10)) AS "HAULING_COST",
    "SILVICULTURE_SOURCE",
    CAST("SILVICULTURE_COST" AS NUMBER(38,10)) AS "SILVICULTURE_COST",
    "STUMPAGE_SOURCE",
    CAST("STUMPAGE_COST" AS NUMBER(38,10)) AS "STUMPAGE_COST",
    "INDIRECT_SOURCE",
    CAST("INDIRECT_COST" AS NUMBER(38,10)) AS "INDIRECT_COST",
    "ROADS_SOURCE",
    CAST("ROADS_COST" AS NUMBER(38,10)) AS "ROADS_COST",
    "TREE_TO_TRUCK_SOURCE",
    CAST("TREE_TO_TRUCK_COST" AS NUMBER(38,10)) AS "TREE_TO_TRUCK_COST",
    "ITEM1_SOURCE",
    CAST("ITEM1" AS NUMBER(38,10)) AS "ITEM1",
    "ITEM2_SOURCE",
    CAST("ITEM2" AS NUMBER(38,10)) AS "ITEM2",
    "ITEM3_SOURCE",
    CAST("ITEM3" AS NUMBER(38,10)) AS "ITEM3",
    "ITEM4_SOURCE",
    CAST("ITEM4" AS NUMBER(38,10)) AS "ITEM4",
    "ITEM5_SOURCE",
    CAST("ITEM5" AS NUMBER(38,10)) AS "ITEM5",
    "ITEM6_SOURCE",
    CAST("ITEM6" AS NUMBER(38,10)) AS "ITEM6",
    "ITEM7_SOURCE",
    CAST("ITEM7" AS NUMBER(38,10)) AS "ITEM7",
    "ITEM8_SOURCE",
    CAST("ITEM8" AS NUMBER(38,10)) AS "ITEM8",
    "ITEM9_SOURCE",
    CAST("ITEM9" AS NUMBER(38,10)) AS "ITEM9",
    "ITEM10_SOURCE",
    CAST("ITEM10" AS NUMBER(38,10)) AS "ITEM10",
    "MILL_MILL_ID",
    "MILL_ID",
    "USER_ID",
    "ESTIM_DATE",
    "LCST_SEQ_NBR",
    "LCST_ITEM6_SOURCE",
    CAST("BLOCK_AVG_COST" AS NUMBER(38,10)) AS "BLOCK_AVG_COST",
    CAST("COASTAL_APPR_RATE" AS NUMBER(38,10)) AS "COASTAL_APPR_RATE",
    CAST("PROFIT_RISK_PCT" AS NUMBER(38,10)) AS "PROFIT_RISK_PCT",
    CAST("BLOCK_CRUISE_VOL" AS NUMBER(38,10)) AS "BLOCK_CRUISE_VOL",
    CAST("APPR_ROAD_BRIDGE_COST" AS NUMBER(38,10)) AS "APPR_ROAD_BRIDGE_COST",
    CAST("APPR_ROAD_CULVERT_COST" AS NUMBER(38,10)) AS "APPR_ROAD_CULVERT_COST",
    CAST("APPR_ROAD_ROAD_COST" AS NUMBER(38,10)) AS "APPR_ROAD_ROAD_COST",
    CAST("APPR_ROAD_LAND_USE_COST" AS NUMBER(38,10)) AS "APPR_ROAD_LAND_USE_COST",
    CAST("APPR_ROAD_SP_OP_COST" AS NUMBER(38,10)) AS "APPR_ROAD_SP_OP_COST",
    CAST("APPR_ROAD_ENG_PROJ_COST" AS NUMBER(38,10)) AS "APPR_ROAD_ENG_PROJ_COST",
    CAST("APPR_ROAD_COST" AS NUMBER(38,10)) AS "APPR_ROAD_COST",
    CAST("EST_BONUS_BID_COEFF" AS NUMBER(38,10)) AS "EST_BONUS_BID_COEFF",
    CAST("ROAD_COST_ACTI_CONSTRUCTION" AS NUMBER(38,10)) AS "ROAD_COST_ACTI_CONSTRUCTION",
    CAST("ROAD_COST_ACTI_INSPECTION" AS NUMBER(38,10)) AS "ROAD_COST_ACTI_INSPECTION",
    CAST("ROAD_COST_ACTI_MAINTENANCE" AS NUMBER(38,10)) AS "ROAD_COST_ACTI_MAINTENANCE",
    CAST("ROAD_COST_ACTI_DEACTIVATION" AS NUMBER(38,10)) AS "ROAD_COST_ACTI_DEACTIVATION",
    CAST("ROAD_COST_ACTI_ACCESS_CONTROL" AS NUMBER(38,10)) AS "ROAD_COST_ACTI_ACCESS_CONTROL",
    CAST("ROAD_COST_ACTI_ASSESSMENT" AS NUMBER(38,10)) AS "ROAD_COST_ACTI_ASSESSMENT",
    CAST("ROAD_COST_STRU_GENERAL" AS NUMBER(38,10)) AS "ROAD_COST_STRU_GENERAL",
    CAST("ROAD_COST_STRU_INSPECTION" AS NUMBER(38,10)) AS "ROAD_COST_STRU_INSPECTION",
    CAST("ROAD_COST_STRU_MAINTENANCE" AS NUMBER(38,10)) AS "ROAD_COST_STRU_MAINTENANCE",
    CAST("ROAD_COST_STRU_REPLACEMENT" AS NUMBER(38,10)) AS "ROAD_COST_STRU_REPLACEMENT",
    CAST("ROAD_COST_TOTAL" AS NUMBER(38,10)) AS "ROAD_COST_TOTAL",
    CAST("SILVICULTURE_COST_PL_TOTAL" AS NUMBER(38,10)) AS "SILVICULTURE_COST_PL_TOTAL",
    CAST("SILVICULTURE_COST_SU_TOTAL" AS NUMBER(38,10)) AS "SILVICULTURE_COST_SU_TOTAL",
    CAST("SILVICULTURE_COST_BR_TOTAL" AS NUMBER(38,10)) AS "SILVICULTURE_COST_BR_TOTAL",
    CAST("SILVICULTURE_COST_SP_TOTAL" AS NUMBER(38,10)) AS "SILVICULTURE_COST_SP_TOTAL",
    CAST("SILVICULTURE_COST_GEN_TOTAL" AS NUMBER(38,10)) AS "SILVICULTURE_COST_GEN_TOTAL",
    CAST("SILVICULTURE_COST_TOTAL" AS NUMBER(38,10)) AS "SILVICULTURE_COST_TOTAL",
    CAST("INDIRECT_COST_TOTAL" AS NUMBER(38,10)) AS "INDIRECT_COST_TOTAL",
    CAST("HAULING_COMMENT_CNT" AS NUMBER(38,10)) AS "HAULING_COMMENT_CNT",
    CAST("STUMPAGE_COMMENT_CNT" AS NUMBER(38,10)) AS "STUMPAGE_COMMENT_CNT",
    CAST("INDIRECT_COMMENT_CNT" AS NUMBER(38,10)) AS "INDIRECT_COMMENT_CNT",
    CAST("TTT_COMMENT_CNT" AS NUMBER(38,10)) AS "TTT_COMMENT_CNT",
    CAST("SILVICULTURE_COMMENT_CNT" AS NUMBER(38,10)) AS "SILVICULTURE_COMMENT_CNT",
    CAST("ROADS_COMMENT_CNT" AS NUMBER(38,10)) AS "ROADS_COMMENT_CNT",
    CAST("ITEM1_COMMENT_CNT" AS NUMBER(38,10)) AS "ITEM1_COMMENT_CNT",
    CAST("ITEM2_COMMENT_CNT" AS NUMBER(38,10)) AS "ITEM2_COMMENT_CNT",
    CAST("ITEM3_COMMENT_CNT" AS NUMBER(38,10)) AS "ITEM3_COMMENT_CNT",
    CAST("ITEM4_COMMENT_CNT" AS NUMBER(38,10)) AS "ITEM4_COMMENT_CNT",
    CAST("ITEM5_COMMENT_CNT" AS NUMBER(38,10)) AS "ITEM5_COMMENT_CNT",
    CAST("ITEM6_COMMENT_CNT" AS NUMBER(38,10)) AS "ITEM6_COMMENT_CNT",
    CAST("ITEM7_COMMENT_CNT" AS NUMBER(38,10)) AS "ITEM7_COMMENT_CNT",
    CAST("ITEM8_COMMENT_CNT" AS NUMBER(38,10)) AS "ITEM8_COMMENT_CNT",
    CAST("ITEM9_COMMENT_CNT" AS NUMBER(38,10)) AS "ITEM9_COMMENT_CNT",
    CAST("ITEM10_COMMENT_CNT" AS NUMBER(38,10)) AS "ITEM10_COMMENT_CNT",
    "LICN_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MARK_SEQ_NBR",
    "CUTB_SEQ_NBR"
FROM forestview.V_QB_VALUATION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_RIPARIAN_ZONE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_RIPARIAN_ZONE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    "SILP_SEQ_NBR",
    "DIVI_DIV_NBR",
    "RIPZ_LAKE_ID",
    "RIPZ_CLASS",
    "RIMZ_ZONE_ID",
    "RIZD_BUFFER_WIDTH",
    "RIZD_BASAL_AREA_MIN",
    "RIZD_BASAL_AREA_MAX",
    "RIZD_DENSITY",
    "RIZD_HARVESTING_IND",
    "RIZD_SU_CROSS_REFERENCE",
    "RIZD_COMMENTS",
    "RIZD_AREA",
    "RIZD_DIGITISED_IND",
    "RIZD_SEQ_NBR"
FROM forestview.V_RIPARIAN_ZONE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "ROAD_SEQ_NBR",
    "DIVI_DIV_NBR",
    "DIVISION",
    "DIVISION_NAME",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_NAME",
    "ROAD_TYPE",
    "ROAD_DESC",
    "URI",
    "RDST_STEWARD_NAME",
    "STATE",
    "START_STATION",
    "END_STATION",
    CAST("LENGTH" AS NUMBER(38,10)) AS "LENGTH",
    "MODIFIED_DATE",
    "MODIFIEDBY",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "STATION_SOURCE",
    "ADJ_VALID",
    "SPATIAL_TITLE",
    "OWNER",
    "VALID_MARK",
    "WITHIN_ALR",
    "ROAD_SEQ_NBR_LNG",
    "FIELD_TEAM",
    "RCLS_ACCOUNTING_TYPE",
    "MODIFIEDON"
FROM forestview.V_ROAD' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_ACTIVITY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_ACTIVITY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "ROAD_ROAD_DESC",
    "URI",
    "RCOM_START_METRE_NBR",
    "RCOM_END_METRE_NBR",
    "RCOM_PLANNED_DATE",
    "RCOM_COMPLETION_DATE",
    "RCOM_CONSTR_TYPE",
    "RINS_START_METRE_NBR",
    "RINS_END_METRE_NBR",
    "RINS_PLANNED_DATE",
    "RINS_INSPECTION_DATE",
    "RINS_CONDITION_DESC",
    "RINS_INSP_TYPE",
    "MTCE_START_METRE_NBR",
    "MTCE_END_METRE_NBR",
    "MTCE_PLANNED_DATE",
    "MTCE_COMPLETION_DATE",
    "MTCE_METHOD",
    "DEAC_START_METRE_NBR",
    "DEAC_END_METRE_NBR",
    "DEAC_PLANNED_DATE",
    "DEAC_START_DATE",
    "DEAC_END_DATE",
    "DEAC_LEVEL_TYPE",
    "DEAC_ACCESS_TYPE",
    "DEAC_METHOD",
    "ACON_START_METRE_NBR",
    "ACON_END_METRE_NBR",
    "ACON_PLANNED_DATE",
    "ACON_START_DATE",
    "ACON_END_DATE",
    "ACON_ACCESS_TYPE",
    "ACON_METHOD",
    "RASS_START_METRE_NBR",
    "RASS_END_METRE_NBR",
    "RASS_PLANNED_DATE",
    "RASS_COMPLETION_DATE",
    "RASS_TYPE",
    "ROAD_SEQ_NBR"
FROM forestview.V_ROAD_ACTIVITY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_ACTIVITY_COST_ROW' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_ACTIVITY_COST_ROW' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "RACM_ACTIVITY_TYPE",
    "CSTI_COST_ITEM_ID",
    "CSTI_COST_ITEM_DESCRIPTION",
    "RACO_ITEM_COST",
    "RACO_COST_TYPE",
    "RACO_COST_DATE",
    "RACO_COMMENT",
    "RACO_SEQ_NBR",
    "RACM_SEQ_NBR",
    "RACT_SEQ_ID",
    CAST("RACT_SEQ_NBR" AS NUMBER(38,10)) AS "RACT_SEQ_NBR"
FROM forestview.V_ROAD_ACTIVITY_COST_ROW' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_ACTIVITY_ROW' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_ACTIVITY_ROW' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "METH_ACTIVITY_TYPE",
    "METH_METHOD_TYPE",
    "PERS_DISPLAY_NAME",
    "CTOR_NAME",
    "START_STATION",
    "END_STATION",
    "PLANNED_DATE",
    "START_DATE",
    "COMPLETION_DATE",
    "BUDGETED_COST",
    "ACTUAL_COST",
    "MEMO",
    CAST("RSTR_AT_METRE_NBR" AS NUMBER(38,10)) AS "RSTR_AT_METRE_NBR",
    "STRUCTURE_ID",
    "STRUCTURE_FILE_ID",
    CAST("INSPECT_FREQUENCY" AS NUMBER(38,10)) AS "INSPECT_FREQUENCY",
    "RACT_SEQ_ID",
    "RACT_SEQ_NBR",
    "ROAD_SEQ_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forestview.V_ROAD_ACTIVITY_ROW' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_AGRI_LAND_RESERVE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_AGRI_LAND_RESERVE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "FIELD_TEAM_DESC",
    "MANU_ID",
    "ROAD_ROAD_NAME",
    "URI",
    "RALR_START_METRE_NBR",
    "RALR_END_METRE_NBR",
    CAST("RALR_LENGTH" AS NUMBER(38,10)) AS "RALR_LENGTH",
    "RALR_WITHIN_ALR_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RALR_SEQ_NBR",
    "RALR_SEQ_NBR_LNG"
FROM forestview.V_ROAD_AGRI_LAND_RESERVE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_ASSESSMENT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_ASSESSMENT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "METH_ACTIVITY_TYPE",
    "RASS_START_METRE_NBR",
    "RASS_END_METRE_NBR",
    "RASS_PLANNED_DATE",
    "RASS_COMPLETION_DATE",
    "METH_METHOD_TYPE",
    "RASS_STATUS",
    "RASS_RESPONSIBILITY",
    "RASS_CONTRACTOR",
    "RASS_ACTIVITY_MEMO",
    "RASS_BUDGETED_COST",
    "RASS_ACTUAL_COST",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RASS_SEQ_NBR_LNG"
FROM forestview.V_ROAD_ASSESSMENT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_CLASS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_CLASS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "RCLS_START_METRE_NBR",
    "RCLS_END_METRE_NBR",
    CAST("RCLS_LENGTH" AS NUMBER(38,10)) AS "RCLS_LENGTH",
    "RCLS_CLASS_TYPE",
    "RCLS_ROAD_TYPE",
    "RCLS_ACCOUNTING_TYPE",
    "RCLS_COMMENTS",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RCLS_SEQ_NBR",
    "RCLS_SEQ_NBR_LNG"
FROM forestview.V_ROAD_CLASS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_COMPLETION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_COMPLETION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "RCOM_START_METRE_NBR",
    "RCOM_END_METRE_NBR",
    CAST("RCOM_LENGTH" AS NUMBER(38,10)) AS "RCOM_LENGTH",
    "RCOM_PLANNED_DATE",
    "RCOM_COMPLETION_DATE",
    "RCOM_STAGE_TYPE",
    "RCOM_SURFACE_TYPE",
    "RCOM_ACTUAL_COST",
    "RCOM_BUDGETED_COST",
    "RCOM_METHOD",
    "RCOM_OPTION_TYPE",
    "RCOM_FDP_CRITICAL",
    "RCOM_JOINT_APPROVAL",
    "RCOM_CONSTRUCTION_TYPE",
    "RCOM_CONTRACTOR",
    "RCOM_ACCOUNTING_TYPE",
    "METH_ACTIVITY_TYPE",
    "METH_METHOD_TYPE",
    "RCOM_RESPONSIBILITY",
    "RCOM_ABR_REPORTED_IND",
    "RCOM_ACTUAL_START_DATE",
    "RCOM_ACTUAL_COMPLETION_DATE",
    "RCOM_GRADE_PERCENT",
    "CTOR_NAME",
    "PERS_DISPLAY_NAME",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RCOM_SEQ_NBR",
    "RCOM_SEQ_NBR_LNG"
FROM forestview.V_ROAD_COMPLETION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_CUT_BLOCK' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_CUT_BLOCK' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "RCUT_START_METRE_NBR",
    "RCUT_END_METRE_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RCUT_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "RCUT_SEQ_NBR_LNG"
FROM forestview.V_ROAD_CUT_BLOCK' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_DEACTIVATION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_DEACTIVATION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "DEAC_START_METRE_NBR",
    "DEAC_END_METRE_NBR",
    CAST("DEAC_LENGTH" AS NUMBER(38,10)) AS "DEAC_LENGTH",
    "DEAC_PLANNED_DATE",
    "DEAC_START_DATE",
    "DEAC_END_DATE",
    "METH_ACTIVITY_TYPE",
    "METH_METHOD_TYPE",
    "DEAC_LEVEL_TYPE",
    "DEAC_ACCESS_TYPE",
    "DEAC_ACTIVITY_MEMO",
    "DEAC_METHOD_OPTION_TYPE",
    "DEAC_METHOD_ATTRIBUTE_DESC",
    "DEAC_FDP_CRITICAL",
    "DEAC_CONTRACTOR",
    "DEAC_RESPONSIBILITY",
    "CTOR_SEQ_NBR",
    "PERS_SEQ_NBR",
    "CLOC_SEQ_NBR",
    "DEAC_BUDGETED_COST",
    "DEAC_ACTUAL_COST",
    "DEAC_ABR_REPORTED_IND",
    "DEAC_ACTUAL_START_DATE",
    "DEAC_ACTUAL_COMPLETION_DATE",
    "DEAC_COMPLETION_DATE",
    "CTOR_NAME",
    "PERS_DISPLAY_NAME",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "DEAC_SEQ_NBR",
    "DEAC_SEQ_NBR_LNG"
FROM forestview.V_ROAD_DEACTIVATION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_EVENT_MAPPING' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_EVENT_MAPPING' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "DIVI_DIVISION_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_SEQ_NBR",
    "ROAD_ROAD_NAME",
    "URI",
    "POC",
    CAST("POT" AS NUMBER(38,10)) AS "POT",
    CAST("PRIME_ROAD_SEQ_NBR" AS NUMBER(38,10)) AS "PRIME_ROAD_SEQ_NBR",
    "RDPM_PERMIT_TYPE",
    "RDPM_PERMIT_ID",
    "RUSE_SECTION_ID",
    "RDST_STEWARD_NAME",
    "RSTA_STATUS_CODE",
    "RSTA_ROAD_STATE",
    "RCLS_CLASS_TYPE",
    "RCLS_ROAD_TYPE"
FROM forestview.V_ROAD_EVENT_MAPPING' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_INSPECTION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_INSPECTION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "RINS_START_METRE_NBR",
    "RINS_END_METRE_NBR",
    CAST("RINS_LENGTH" AS NUMBER(38,10)) AS "RINS_LENGTH",
    "RINS_PLANNED_DATE",
    "RINS_INSPECTION_DATE",
    "RINS_CONDITION_DESC",
    "RINS_PERFORMED_BY_NAME",
    "RINS_ACTIVITY_MEMO",
    "RINS_INSPECTION_TYPE",
    "RINS_OPTION_TYPE",
    "RINS_INSPECTION_FILE_ID",
    "METH_ACTIVITY_TYPE",
    "METH_METHOD_TYPE",
    "RINS_RESPONSIBILITY",
    "INSPECTED_BY_PERSON",
    "RINS_INSPECTOR_TYPE",
    "RINS_BUDGETED_COST",
    "RINS_ACTUAL_COST",
    "RINS_ACTUAL_START_DATE",
    "RINS_ACTUAL_COMPLETION_DATE",
    "RINS_INTEGRITY_OK",
    "RINS_DRAINAGE_OK",
    "RINS_INDUSTRIAL_USE_OK",
    "RINS_REVEGETATED",
    "CTOR_NAME",
    "PERS_DISPLAY_NAME",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RINS_SEQ_NBR",
    "RINS_SEQ_NBR_LNG"
FROM forestview.V_ROAD_INSPECTION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_LINEAR_STRUCT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_LINEAR_STRUCT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "RLST_START_METRE_NBR",
    "RLST_END_METRE_NBR",
    "ABRE_STRUCT_CLASS",
    "ABRF_STRUCT_TYPE_FD",
    "ABRW_STRUCT_TYPE_WALL",
    "RLST_ENG_IND",
    "RLST_OFFSET_NBR",
    "RLST_ROAD_SIDE",
    "RLST_REPORTED_IND",
    "ROAD_SEQ_NBR",
    "RLST_SEQ_NBR",
    "RLST_SEQ_NBR_LNG"
FROM forestview.V_ROAD_LINEAR_STRUCT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_MAINTENANCE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_MAINTENANCE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "METH_ACTIVITY_TYPE",
    "METH_METHOD_TYPE",
    "MTCE_START_METRE_NBR",
    "MTCE_END_METRE_NBR",
    CAST("MTCE_LENGTH" AS NUMBER(38,10)) AS "MTCE_LENGTH",
    "MTCE_PLANNED_DATE",
    "MTCE_COMPLETION_DATE",
    "MTCE_ACTIVITY_MEMO",
    "MTCE_METHOD_OPTION_TYPE",
    "MTCE_METHOD_ATTRIBUTE_DESC",
    "MTCE_CONTRACTOR",
    "RINS_SEQ_NBR",
    "MTCE_RESPONSIBILITY",
    "CTOR_NAME",
    "PERS_DISPLAY_NAME",
    "CLOC_SEQ_NBR",
    "MTCE_BUDGETED_COST",
    "MTCE_ACTUAL_COST",
    "MTCE_ACTUAL_START_DATE",
    "MTCE_ACTUAL_COMPLETION_DATE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "MTCE_SEQ_NBR",
    "MTCE_SEQ_NBR_LNG"
FROM forestview.V_ROAD_MAINTENANCE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_MANAGEMENT_UNIT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_MANAGEMENT_UNIT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "ROAD_NAME",
    "URI",
    "FIELD_TEAM_DESC",
    "MANU_ID",
    "MANU_NAME",
    "RMANU_START_METRE_NBR",
    "RMANU_END_METRE_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "MANU_SEQ_NBR",
    "RMANU_SEQ_NBR"
FROM forestview.V_ROAD_MANAGEMENT_UNIT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_MAPSHEET' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_MAPSHEET' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "RMAP_START_METRE_NBR",
    "RMAP_END_METRE_NBR",
    "MAPS_MAPSHEET_ID",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RMAP_SEQ_NBR_LNG"
FROM forestview.V_ROAD_MAPSHEET' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_ON_BLOCK' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_ON_BLOCK' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "FIELD_TEAM_DESC",
    "MANU_ID",
    "ROAD_ROAD_NAME",
    "URI",
    "RONB_START_METRE_NBR",
    "RONB_END_METRE_NBR",
    CAST("RONB_LENGTH" AS NUMBER(38,10)) AS "RONB_LENGTH",
    "LICN_LICENCE_ID",
    "MARK_MARK_ID",
    "CUTB_BLOCK_ID",
    "RONB_SECTION_ID",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "RONB_SEQ_NBR",
    "ROAD_SEQ_NBR",
    "LICN_SEQ_NBR",
    "MARK_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "RONB_SEQ_NBR_LNG"
FROM forestview.V_ROAD_ON_BLOCK' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_OP_AREA' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_OP_AREA' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "ROPA_START_METRE_NBR",
    "ROPA_END_METRE_NBR",
    CAST("ROPA_LENGTH" AS NUMBER(38,10)) AS "ROPA_LENGTH",
    "ROPA_OPERATING_AREA",
    "ROPA_WORKING_CIRCLE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "ROPA_SEQ_NBR",
    "ROPA_SEQ_NBR_LNG"
FROM forestview.V_ROAD_OP_AREA' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_ORGANIC_MAT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_ORGANIC_MAT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "ROAD_SEQ_NBR",
    "DIVI_DIV_NBR",
    "ROAD_ROAD_NAME",
    "URI",
    "ROMT_START_METRE_NBR",
    "ROMT_END_METRE_NBR",
    "ABRM_FEATURE_TYPE",
    "ROMT_REPORTED_IND",
    "ROMT_SEQ_NBR",
    "ROMT_SEQ_NBR_LNG"
FROM forestview.V_ROAD_ORGANIC_MAT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_PERMIT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_PERMIT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "RDPM_SEQ_NBR",
    "TSO_CODE",
    "DSCT_DISTRICT_NAME",
    "LSEE_LICENSEE_ID",
    "RDPM_PERMIT_ID",
    "RDPM_PERMIT_TYPE",
    "RDPM_EXPIRY_DATE",
    "RDPM_START_DATE",
    "RDPM_PRIMARY_USER",
    "RDPM_SECONDARY_USER",
    "RDPM_COMMENTS",
    "RDPM_AMENDMENT_NBR",
    "RDPM_TIMBER_MARK_ID",
    "RDPM_PARENT_PERM_ID",
    "ROAD_INITIAL_ROAD_LENGTH",
    "ROAD_CURRENT_ROAD_LENGTH",
    "ROAD_AMENDMENT_REASON",
    "RDPM_CASCADE_SPLIT_CODE",
    "RDPM_MGMT_UNIT_TYPE",
    "RDPM_MGMT_UNIT_ID",
    "RDPM_AMENDMENT_TYPE",
    "RDPM_APPLICATION_DESC",
    "RDPM_DEEMED_OWNER_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forestview.V_ROAD_PERMIT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_ACTIVITY_COST' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_ACTIVITY_COST' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "RACM_ACTIVITY_TYPE",
    "CSTI_COST_ITEM_ID",
    "CSTI_COST_ITEM_DESCRIPTION",
    "RACO_ITEM_COST",
    "RACO_COST_TYPE",
    "RACO_COST_DATE",
    "RACO_COMMENT",
    "RCOM_SEQ_NBR",
    "RINS_SEQ_NBR",
    "MTCE_SEQ_NBR",
    "DEAC_SEQ_NBR",
    "ACON_SEQ_NBR",
    "RASS_SEQ_NBR",
    "INSP_SEQ_NBR",
    "MAIN_SEQ_NBR",
    "REPL_SEQ_NBR",
    "RSTR_SEQ_NBR"
FROM forestview.V_ROAD_ACTIVITY_COST' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_PROV_FOREST' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_PROV_FOREST' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "ROAD_SEQ_NBR",
    "DIVI_DIV_NBR",
    "ROAD_ROAD_NAME",
    "URI",
    "RPRF_START_METRE_NBR",
    "RPRF_END_METRE_NBR",
    CAST("RPRF_LENGTH" AS NUMBER(38,10)) AS "RPRF_LENGTH",
    "RPRF_CONFLICT_CODE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "RPRF_SEQ_NBR",
    "RPRF_SEQ_NBR_LNG"
FROM forestview.V_ROAD_PROV_FOREST' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_RADIO' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_RADIO' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "RRAD_START_METRE_NBR",
    "RRAD_END_METRE_NBR",
    "RRAD_CHANNEL_NAME",
    "RRAD_TRANSMIT_NBR",
    "RRAD_RECEIVE_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RRAD_SEQ_NBR",
    "RRAD_SEQ_NBR_LNG"
FROM forestview.V_ROAD_RADIO' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_RISK' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_RISK' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "RRRA_RISK_RATING",
    "RRIS_START_METRE_NBR",
    "RRIS_END_METRE_NBR",
    "RRIS_INSP_FREQ",
    "RRIS_ADJUSTED_INSP_FREQ",
    "RRIS_MEMO",
    "RRIS_DATA_SOURCE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RRIS_SEQ_NBR",
    "RRIS_SEQ_NBR_LNG"
FROM forestview.V_ROAD_RISK' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_SECTION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_SECTION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    CAST("ROAD_SEQ_NBR" AS NUMBER(38,10)) AS "ROAD_SEQ_NBR",
    "POC",
    CAST("POT" AS NUMBER(38,10)) AS "POT"
FROM forestview.V_ROAD_SECTION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_SPATIAL_META' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_SPATIAL_META' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "ROAD_SEQ_NBR",
    "DIVI_DIV_NBR",
    "ROAD_ROAD_NAME",
    "URI",
    "RSPM_START_METRE_NBR",
    "RSPM_END_METRE_NBR",
    CAST("RSPM_LENGTH" AS NUMBER(38,10)) AS "RSPM_LENGTH",
    "ABRU_DATA_SOURCE",
    "RSPM_SOURCE_DATE",
    "GMLC_CAPTURE_METHOD",
    "RSPM_OBSERVATION_DATE",
    "ABRH_HORIZONTAL_DATUM",
    "ABRZ_VERTICAL_DATUM",
    "ABRS_CCSM_CODE",
    "ABRO_COORDINATE_SYSTEM",
    "RSPM_DATA_ACCURACY",
    "RSPM_HORIZ_ACCURACY",
    "RSPM_VERT_ACCURACY",
    "RSPM_REPORTED_IND",
    "RSPM_DESCRIPTION",
    "RSPM_SEQ_NBR",
    "RSPM_SEQ_NBR_LNG"
FROM forestview.V_ROAD_SPATIAL_META' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_ROAD_STATUS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_ROAD_STATUS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "FIELD_TEAM_DESC",
    "ROAD_ROAD_NAME",
    "URI",
    "RSTA_START_METRE_NBR",
    "RSTA_END_METRE_NBR",
    CAST("RSTA_LENGTH" AS NUMBER(38,10)) AS "RSTA_LENGTH",
    "RSTA_STATUS_CODE",
    "RSTA_STATUS_CHANGED_DATE",
    "RSTA_ROAD_STATE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "ROAD_SEQ_NBR",
    "RSTA_SEQ_NBR_LNG",
    "RSTA_CPRP_PROTECTION_IND"
FROM forestview.V_ROAD_STATUS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_LICENCE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_LICENCE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "LICN_SEQ_NBR",
    "TENT_SEQ_NBR",
    "CTOR_SEQ_NBR",
    "CLOC_SEQ_NBR",
    "TSO_CODE",
    "TSO_NAME",
    "NAV_NAME",
    "LICENCE_ID",
    "CATEGORY",
    "TENURE",
    "LICENSEE",
    "REGISTRANT",
    "REGISTRANT_CITY",
    "FIELD_TEAM",
    "DISTRICT_NAME",
    "DIVI_DIV_NBR",
    "LICN_CATEGORY_ID",
    "BLAZ_ADMIN_ZONE_ID",
    "BLAZ_ADMIN_ZONE_DESC",
    "LICN_LICENCE_STATE",
    "LICN_LICENCE_DESC",
    "LICN_LICENCE_TO_CUT_CODE",
    "LINC_CERT_LEVEL_ID",
    "LICN_DIGI_IND",
    "LICN_SALVAGE_IND",
    "LICN_APPORTION_TENURE_TYPE",
    "APPORTION_TYPE",
    "PARTITION_TYPE",
    "COMMIT_LICENCE_TYPE",
    CAST("COMMIT_VOLUME" AS NUMBER(38,10)) AS "COMMIT_VOLUME",
    CAST("REMAIN_COMMIT_VOLUME" AS NUMBER(38,10)) AS "REMAIN_COMMIT_VOLUME",
    "BCHH_BILLING_YEAR",
    "MANU_SEQ_NBR"
FROM forestview.V_LICENCE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_BLOCK_SPATIAL' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_BLOCK_SPATIAL' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TSO_CODE",
    "NAV_NAME",
    "LICENCE_ID",
    "PERMIT_ID",
    "MARK_ID",
    "BLOCK_ID",
    "CUTB_SEQ_NBR",
    "SPATIAL_FLAG"
FROM forestview.V_BLOCK_SPATIAL' AS customsql_query,
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

# ### FOREST Tables and Views

# CELL ********************

# Auto-generated metadata merges for Fabric
# Run this file in a Fabric notebook

spark.sql("""
    MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
    USING (
        SELECT
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'BIOGEOCLIMATIC_VARIANT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'BIOGEOCLIMATIC_VARIANT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BIOZ_ZONE_ID",
    "BIOS_SUBZONE_ID",
    "BIOV_VARIANT_ID",
    "BIOV_VARIANT_NAME",
    "BIOV_ACTIVE_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.BIOGEOCLIMATIC_VARIANT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'DETAILED_SITE_ASSESSMENT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'DETAILED_SITE_ASSESSMENT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SILA_SEQ_NBR",
    "DSAS_STRATUM_NAME",
    "POTR_PURPOSE_OF_TREATMENT",
    "DSAS_WATER_BODIES",
    "DSAS_FISH_HABITAT",
    "DSAS_WILDLIFE_HABITAT",
    "DSAS_COMMUNITY_WATERSHED",
    "DSAS_BUFFER_SIZE",
    "DSAS_CHEMICAL",
    "DSAS_ACTIVE",
    "DSAS_APPLY_RATE",
    "DSAS_PMP_PUP_NBR",
    "DSAS_REFERAL_IND",
    "DSAS_REFERAL",
    "DSAS_APPROVAL_IND",
    "DSAS_PFZ_WIDTH",
    "PMPL_PMP_NBR",
    "DSAS_ACTUAL_BRUSH_STEMS",
    "DSAS_PERFORMANCE_QUALITY_PCT",
    "DSAS_PRODUCT_ID",
    "DSAS_CHEMICAL_SECONDARY",
    "DSAS_PRODUCT_ID_SECONDARY",
    "DSAS_SOLUTION_VOLUME_TOTAL",
    "DSAS_SOLUTION_VOLUME",
    "DSAS_MIX_LOAD_LOCATION",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.DETAILED_SITE_ASSESSMENT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'PLANTING_SPECIES' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'PLANTING_SPECIES' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "PLSP_SEQ_NBR",
    "PLUN_SEQ_NBR",
    "SISP_SPECIES_ID",
    "PLSP_NUMBER_TREES",
    "SISL_SEED_LOT_NUMBER",
    "SIRK_REQUEST_KEY",
    "PLSP_SEED_WEIGHT",
    "PLSP_PRICE_PER_TREE",
    "PLSP_COMMENT",
    "SSFM_SEQ_NBR",
    "SINU_CODE",
    "PLSP_STOCK_TYPE",
    "PLSP_LIFT_DATE",
    "PLSP_TREES_OUTSIDE_ZONE",
    "PLSP_YRS_ORIG_CNTNR",
    "PLSP_YRS_TRANSPLANTED",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "V_OUTSIDE_TRANSFER_LIMIT",
    "CBST_COMMENTS"
FROM forest.PLANTING_SPECIES' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'SILVICULTURE_COMPANY_ACTIVITY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'SILVICULTURE_COMPANY_ACTIVITY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SICA_SEQ_NBR",
    "DIVI_DIV_NBR",
    "SIAB_BASE_CODE",
    "SIAT_TECHNIQUE_CODE",
    "SIAM_METHOD_CODE",
    "SICA_ACTIVITY_NAME",
    "SIAO_OBJECTIVE_CODE",
    "SIAO_OBJECTIVE_CODE2",
    "SIAO_OBJECTIVE_CODE3",
    "SICA_DEFAULT_IND",
    "SICA_SORT_ORDER",
    "SICA_KEY_IND",
    "SICA_STATUS_IND",
    "SICA_DATE_RELATIVE",
    "SICA_DATE_REL_START_END",
    "SICA_COST_PER_HA",
    "SICA_DEFAULT_AREA_PCT",
    "SICA_CURRENT_STATUS_IND",
    "SICA_LIABILITY_PCT",
    "SICA_ACTIVE_IND",
    "SICA_PRE_HARVEST_IND",
    "SICA_SILV_REGIME_COMMENTS_IND",
    "SICA_SYSTEM_CODE",
    "SICA_LAYER_CLASS",
    "SICA_WINDOW",
    "SICA_ROTATION_IND",
    "SICA_INV_TYPE_ID",
    "SICA_ESTABL_METHOD_IND",
    "SICA_GROUP",
    "SICA_COMMENT",
    "SICA_COST_TYPE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "UPDATE_PLANNED_ACT_FLAG",
    "SICA_SPATIAL_IND"
FROM forest.SILVICULTURE_COMPANY_ACTIVITY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'SILVICULTURE_STOCKING_STATUS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'SILVICULTURE_STOCKING_STATUS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SIST_SEQ_NBR",
    "SLAY_LAYER",
    "SRNK_RANK",
    "STTP_STOCKING_TYPE_ID",
    "SSTS_SURVEY_SOURCE",
    "SSTS_SURVEY_DATE",
    CAST("SSTS_STOCK_AGE" AS NUMBER(38,10)) AS "SSTS_STOCK_AGE",
    CAST("SSTS_STOCK_AGE_PLANT" AS NUMBER(38,10)) AS "SSTS_STOCK_AGE_PLANT",
    "SSTS_STOCK_HEIGHT",
    "SSTS_CROWN_CLOSURE",
    "SSTS_REFERENCE_YEAR",
    "SICL_SITE_CLASS",
    "SSTS_SITE_INDEX",
    "SSTS_DENSITY",
    "SSTS_WELL_SPACED_DENSITY",
    "SSTS_FREE_GROWING_DENSITY",
    "SRTY_RESERVE_TYPE",
    "SSTS_BASAL_AREA",
    "SSTS_CM_TOP_HEIGHT",
    "SSTS_CM_LEADER",
    "SSTS_VIGOUR_PCT_GOOD",
    "SSTS_VIGOUR_PCT_MEDIUM",
    "SSTS_VIGOUR_PCT_POOR",
    "SSTS_COMMENTS",
    "SSTS_TOTAL_CONIFEROUS",
    "SSTS_PLANTABLE_SPOTS",
    "SSTS_PREPABLE_SLASH_SPOTS",
    "SSTS_PREPABLE_BRUSH_SPOTS",
    "SSTS_PREPABLE_DUFF_SPOTS",
    "SSTS_NON_PRODUCTIVE_SPOTS",
    "SSTS_FORMC_PRINTED_IND",
    "SSTS_FORMC_DATE",
    "STST_STOCKING_STATUS_ID",
    "SSTS_ROOT_COLLAR_DIAMETER",
    "SSSC_SOURCE_CODE",
    "SSTS_FREE_GROW_DENSITY_PREF",
    "SSTS_WELL_SPACED_DENSITY_PREF",
    "SSTS_FREE_GROW_LCL_DENSITY",
    "SSTS_WELL_SPACED_LCL_DENSITY",
    "SSTS_COUNTABLE_CONIFERS",
    "SSTS_REENTRY_YEAR",
    "SSTS_CVR_PATTERN",
    "SSTS_RES_OBJECTIVE",
    "SSTS_TOTAL_WELL_SPACED",
    "SSTS_GERMINANT_PER_HA",
    "SSTS_GREENED_UP_TREE_PER_HA",
    "SSTS_TOTAL_TREES_PA_FG",
    "SSTS_TOTAL_TREES_P_FG",
    "SSTS_TOTAL_TREES_WPA_FG",
    "SSTS_TOTAL_TREES_WP_FG",
    "SSTS_DECID_P_FG",
    "SSTS_TPI_PA_FG",
    "SSTS_PPI_PA_FG",
    "SSTS_PPI_LOW_CONF_LIMIT_FG",
    "SSTS_PPI_SAMPLING_ERROR_FG",
    "SSTS_TOTAL_TREES_PA_REGEN",
    "SSTS_TOTAL_TREES_P_REGEN",
    "SSTS_TOTAL_TREES_WPA_REGEN",
    "SSTS_TOTAL_TREES_WP_REGEN",
    "SSTS_DECID_P_REGEN",
    "SSTS_TPI_PA_REGEN",
    "SSTS_PPI_PA_REGEN",
    "SSTS_PPI_LOW_CONF_LIMIT_REGEN",
    "SSTS_PPI_SAMPLING_ERROR_REGEN",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.SILVICULTURE_STOCKING_STATUS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'SILV_ACTIVITY_COST' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'SILV_ACTIVITY_COST' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SACC_SEQ_NBR",
    "SILA_SEQ_NBR",
    "PLUN_SEQ_NBR",
    "SCAC_SEQ_NBR",
    "SACC_ITEM_COST",
    "SACC_COMMENT",
    "SACC_SERIES",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.SILV_ACTIVITY_COST' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'LICENCE_SHAPE_EVW' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'LICENCE_SHAPE_EVW' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "OBJECTID",
    "TRANSACTION_ID",
    "LICN_SEQ_NBR",
    "FEATURE_LEN",
    "FEATURE_AREA",
    "SHAPE_LEN",
    "SHAPE_AREA",
    SDE.ST_AsText("SHAPE") AS "SHAPE",
    "MANU_SEQ_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    CAST("SDE_STATE_ID" AS NUMBER(38,10)) AS "SDE_STATE_ID"
FROM forest.LICENCE_SHAPE_EVW' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'TENURE_TYPE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'TENURE_TYPE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TENT_SEQ_NBR",
    "DIVI_DIV_NBR",
    "TENT_TENURE_ID",
    "TENT_TENURE_NAME",
    "TETY_TENURE_TYPE",
    "TENT_ACTIVE_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.TENURE_TYPE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'MARK' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'MARK' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "MARK_SEQ_NBR",
    "MARK_MARK_ID",
    "DIVI_DIV_NBR",
    "MARK_MARK_DESC",
    "MARK_MARK_STATE",
    "MARK_CROWN_GRANTED_IND",
    "MARK_AAC_PARTITION",
    "MARK_APPORTIONMENT",
    "MARK_ENDEMIC_PERCENT",
    "MARK_SPECIES_TYPE",
    "MARK_ALL_LOG_GRADES_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.MARK' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'CUT_BLOCK' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'CUT_BLOCK' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    "CUTB_BLOCK_ID",
    "CUTB_BLOCK_NUMBER",
    "DIVI_DIV_NBR",
    "CUTB_DEVT_PLAN_STATUS_CODE",
    "CUTB_DEVT_PLAN_APPRVL_DATE",
    "CUTB_SILVI_PLAN_STATUS_CODE",
    "CUTB_SILVI_PLAN_APPRVL_DATE",
    "CUTB_BLOCK_MEMO",
    "CUTB_WOODS_RUN_KM_NBR",
    "CUTB_HAULING_KM_NBR",
    "CUTB_HLCPTR_DROP_CRUISE_IND",
    "CUTB_LOGGING_PLAN_STATUS_CODE",
    "CUTB_LOGGING_PLAN_APPRVL_DATE",
    "OPAR_OPERATING_AREA_ID",
    "BIOZ_ZONE_ID",
    "CUTB_GROSS_HA_AREA",
    "CUTB_BCGS",
    "CUTB_OPENING",
    "CUTB_NTS",
    "CUTB_EXT_HA_AREA",
    "CUTB_PHOTOS",
    "CUTB_FIELD_WORK_BY",
    "CUTB_FIELD_WORK_DATE",
    "CUTB_SITE_CLASS",
    "FDPS_STATUS_ID",
    "CUTB_SITE_INDEX",
    "CUTB_CPI_SLOPE_PCT",
    "CUTB_CPI_ROAD_ACCESS_KM",
    "CUTB_CPI_ROAD_BLOCK_KM",
    "CUTB_CPI_ROAD_NUM_LANDINGS",
    "CUTB_CPI_ROAD_HAUL_KM",
    "SBLK_SUPPLY_BLOCK_ID",
    "CUTB_FORMC_HARVEST_PRINT_DATE",
    "CUTB_FORMC_REGEN_PRINT_DATE",
    "CUTB_FORMC_FREEGROW_PRINT_DATE",
    "FUND_FUNDING_CODE",
    "CUTB_HIGHWAY_IND",
    "CUTB_WINTER_ROAD_IND",
    "FINZ_FOREST_INVENTORY_ZONE_ID",
    "CUTB_USER_MAPSHEET_ID",
    "CUTB_CELL_NUMBER",
    "FJAP_FDP_JOINT_APPROVAL",
    "PMOD_MODIFIER_ID",
    "CUTB_TRAVERSE_METHOD_CODE",
    "CUTB_CRITICAL_DATE_IND",
    "CUTB_GREENUP_DATE",
    "GRNS_GREENUP_SOURCE",
    "PMPO_OPERATING_ZONE",
    "LSUN_LANDSCAPE_UNIT",
    "PLAN_SEQ_NBR",
    "CUTB_FORMA_PRINT_DATE",
    "CUTB_BLOCK_STATE",
    "CUTB_DIGI_IND",
    "STTP_STAND_TYPE",
    "TTAC_TIMBERTYPE_AGE_CLASS",
    "TTSC_TIMBERTYPE_STOCK_CLASS",
    "TTHC_TIMBERTYPE_HEIGHT_CLASS",
    "CUTB_SITE_PLAN_EXEMPT_IND",
    "SSSC_SOURCE_CODE",
    "CUTB_VOL_DATA_SOURCE",
    "CUTB_VOL_DATA_SOURCE_TYPE",
    "CUTB_HELI_FLIGHT_DIST",
    "CUTB_HARVEST_SEQUENCE",
    "CUTB_PROV_FOREST_CONFLICT",
    "TREG_SEQ_NBR",
    "BCAT_CATEGORY_CODE",
    "CUTB_TRAVERSE_START_POINT",
    "CUTB_TRAVERSE_END_POINT",
    "CUTB_FORMA_PRINTED",
    "CUTB_SYSTEM_ID",
    "CUTB_BLOCK_STATUS",
    "CUTB_LATITUDE_BAK",
    "CUTB_LONGITUDE_BAK",
    "CUTB_DAMAGE_TYPE",
    "CUTB_OPENING_ID",
    "SILP_USE_BLOCK_NUM_IND",
    "CUTB_LOCATION",
    "CUTB_SILV_ACT_HARV_LINK",
    "CUTB_FILE_ID",
    "SUOP_SUBOP_AREA_ID",
    "CUTB_SELLING_PRICE_PERIOD",
    "CUTB_SYNCH_STATUS",
    "CUTB_BLOCK_GROUPING_ID",
    "PERS_SEQ_NBR",
    "CTOR_SEQ_NBR",
    "CUTB_VARIANT_ID",
    "CUTB_SEASON_ID",
    "CUTB_VOLUME",
    "CLOC_SEQ_NBR",
    "CUTB_PARENT_SEQ",
    "CUTB_PREV_FORMA_PRINT_DATE",
    "SILA_SEQ_NBR",
    "CUTB_CRUISE_LOCK_IND",
    "CUTB_REF_DEC",
    "CUTB_REG_CODE",
    "CUTB_APR_EFF_DATE",
    "SIPH_SEQ_NBR",
    CAST("CDAT_SEQ_NBR" AS NUMBER(38,10)) AS "CDAT_SEQ_NBR",
    "CUTB_ACCESS_RESTRICTION",
    "CUTB_PLANNED_VOL",
    "CUTB_ROW_IND",
    "CUTB_TIM_DEV_DATE",
    "CUTB_EBM_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "DOCUMENTKEY",
    "LOG_START_CASCADE_DATE_IND",
    "PRICING_DATE",
    "CUTB_LATITUDE_DD",
    "CUTB_LONGITUDE_DD",
    "CUTB_LATITUDE",
    "CUTB_LONGITUDE",
    "MARKED_FOR_DEL_IND",
    "MARKED_FOR_DEL_BY",
    "MARKED_FOR_DEL_ON",
    "CUTB_ARCHIVE_REASON",
    "CUTB_ARCHIVE_DATE",
    "CUTB_CPRP_PROTECTION_IND",
    "CUTB_RC_RISK_RATING",
    "CUTB_RC_RISK_SOURCE",
    "CUTB_RC_RISK_COMMENTS",
    "OPERATIONAL_SITE_FACTOR",
    "SAFETY_CONCERN"
FROM forest.CUT_BLOCK' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'ACTIVITY_CLASS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'ACTIVITY_CLASS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "ACCL_SEQ_NBR",
    "ACCL_DESCRIPTION",
    "ACCL_OBJECT_TYPE",
    "ACCL_DISPLAY_ORDER",
    "DIVI_DIV_NBR",
    "ACCL_KEY_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.ACTIVITY_CLASS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'DIVISION_CODE_LOOKUP' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'DIVISION_CODE_LOOKUP' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "COLU_LOOKUP_TYPE",
    "COLU_LOOKUP_ID",
    "DIVI_DIV_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.DIVISION_CODE_LOOKUP' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'DIVISION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'DIVISION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "DIVI_DIV_NBR",
    "DIVI_COUNTRY_CODE",
    "DIVI_PROV_STATE_CODE",
    "DIVI_DIVISION_NAME",
    "DIVI_STARTUP_DATE",
    "DIVI_END_DATE",
    "DIVI_SHORT_CODE",
    "DIVI_LINE1_ADDR",
    "DIVI_LINE2_ADDR",
    "DIVI_LINE3_ADDR",
    "DIVI_CITY_NAME",
    "DIVI_POSTAL_CODE",
    "DIVI_INTL_ROUTING_CODE",
    "DIVI_PHONE_NBR",
    "DIVI_FAX_NBR",
    "DIVI_STMPG_ACCTCD",
    "DIVI_STMPG_OFFSET_ACCTCD",
    "DIVI_WALKER_ENTITY_ACCTCD",
    "DIVI_GST_PAY_ACCTCD",
    "DIVI_GST_RECOVERY_ACCTD",
    "DIVI_OPERATION_LOCATION_IND",
    "DIVI_CLIENT_LOCATION_CODE",
    "DIVI_ABBREVIATION_CODE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.DIVISION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'ACTIVITY_TYPE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'ACTIVITY_TYPE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "ACTT_SEQ_NBR",
    "ACCL_SEQ_NBR",
    "ACTT_DESCRIPTION",
    "DIVI_DIV_NBR",
    "ACTT_DEFAULT_IND",
    "ACTT_RESPONSIBILITY",
    "ACTT_DISPLAY_ORDER",
    "ACTT_KEY_IND",
    "ACTT_STATUS_IND",
    "ACTT_DATE_RELATIVE",
    "CTOR_CONTRACTOR_ID",
    "ACTT_VIEW_LEVEL",
    "ACTT_SYSTEM_IND",
    "CTOR_SEQ_NBR",
    "ACTT_ACTIVE_IND",
    "ACTT_KEY_PAIR",
    "ACTT_HARVS_IND",
    "ACTT_TRVOL_IND",
    "ACTT_INDIRECT_COST_IND",
    "ACTT_DEFAULT_COST",
    "ACTT_COST_UOM",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.ACTIVITY_TYPE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'SILV_TREATMENT_REGIME' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'SILV_TREATMENT_REGIME' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "TREG_SEQ_NBR",
    "PERS_SEQ_NBR",
    "DIVI_DIV_NBR",
    "TREG_REGIME_ID",
    "TREG_REGIME_NAME",
    "TREG_CREATE_DATE",
    "TREG_ACTIVE_IND",
    "TREG_DEF_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.SILV_TREATMENT_REGIME' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'BCTS_HARVEST_HISTORY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'BCTS_HARVEST_HISTORY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BCHH_SEQ_NBR",
    "MARK_SEQ_NBR",
    "BCHH_BILLING_YEAR",
    "BCHH_BILLING_MONTH",
    "BCHH_BILLING_PERIOD",
    "BCHH_HDBS_TREE_SPECIES",
    "BCHH_FOREST_PRODUCT_CODE",
    "BCHH_LOG_GRADE",
    "BCHH_BILLING_TYPE_CODE",
    "BCHH_VOLUME_BILLED",
    "BCHH_PIECES_BILLED",
    "BCHH_ROYALTY_AMOUNT",
    "BCHH_RESERVE_STMPG_AMT",
    "BCHH_BONUS_STUMPAGE_AMT",
    "BCHH_SILV_LEVY_AMOUNT",
    "BCHH_DEV_LEVY_AMOUNT",
    "BCHH_UPDATE_TIMESTAMP",
    "BCHH_UPDATE_USERID",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.BCTS_HARVEST_HISTORY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'COMMITMENTS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'COMMITMENTS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "LICN_SEQ_NBR",
    "COMMIT_SEQ_NBR",
    "COPA_PARTITION",
    "COPA_COMMIT_APPO",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "MANU_SEQ_NBR",
    "COPA_COMMIT_LIC_TYPE"
FROM forest.COMMITMENTS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'ECOLOGY_UNIT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'ECOLOGY_UNIT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "ECOU_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "ECOU_NAME",
    "BIOZ_ZONE_ID",
    "BIOS_SUBZONE_ID",
    "BIOV_VARIANT_ID",
    "ECOU_SITE_TYPE_ID",
    "ECOU_ADDITIONAL_INFO",
    "ECOU_ECOSITE_FIT_CODE",
    "ECOU_HA_AREA",
    "ECOU_DIGITISED_IND",
    "MREG_EDATOPIC_MOISTURE_ID",
    "NREG_EDATOPIC_NUTRIENT_ID",
    "ECOU_ELEVATION_MIN",
    "ECOU_ELEVATION_MAX",
    "ECOU_ELEVATION_AVG",
    "ECOU_ASPECT",
    "ECOU_SLOPE_MIN",
    "ECOU_SLOPE_MAX",
    "ECOU_SLOPE_AVG",
    "ECOU_SLOPE_POSITION",
    "ECOU_TERRAIN",
    "ECOU_PARENT_MATERIAL",
    "ECOU_DRAINAGE",
    "ECOU_SUBSTRATE_DECAYING_WOOD",
    "ECOU_SUBSTRATE_BEDROCK",
    "ECOU_SUBSTRATE_COBBLES",
    "ECOU_SUBSTRATE_MINERAL_SOIL",
    "ECOU_SUBSTRATE_ORGANIC",
    "ECOU_SUBSTRATE_WATER",
    "ECOU_ORGANIC_LAYER",
    "ECOU_HUMUS_L_DEPTH_MIN",
    "ECOU_HUMUS_L_DEPTH_MAX",
    "ECOU_HUMUS_F_DEPTH_MIN",
    "ECOU_HUMUS_F_DEPTH_MAX",
    "ECOU_HUMUS_H_DEPTH_MIN",
    "ECOU_HUMUS_H_DEPTH_MAX",
    "ECOU_HUMUS_FORM",
    "ECOU_SOIL_ORDER",
    "ECOU_SOIL_TYPE",
    "ECOU_SOIL_DEPTH_WATER",
    "ECOU_SOIL_DEPTH_WATER_MAX",
    "ECOU_SOIL_DEPTH_MOTTLES",
    "ECOU_SOIL_DEPTH_MOTTLES_MAX",
    "ECOU_SOIL_DEPTH_GLEYING",
    "ECOU_SOIL_DEPTH_GLEYING_MAX",
    "ECOU_SOIL_DEPTH_CALCAR",
    "ECOU_SOIL_DEPTH_CALCAR_MAX",
    "ECOU_SOIL_DEPTH_BEDROCK",
    "ECOU_SOIL_DEPTH_BEDROCK_MAX",
    "ECOU_SOIL_DEPTH_COMPACT",
    "ECOU_SOIL_DEPTH_COMPACT_MAX",
    "ECOU_SOIL_ROOTING_DEPTH",
    "ECOU_SOIL_SEEPAGE_IND",
    "ECOU_SOIL_SEEPAGE_DEPTH",
    "ECOU_SOIL_PIT_DEPTH",
    "ECOU_SOIL_ACID_TEST",
    "ECOU_SOIL_ACID_TEST_DEPTH",
    "ECOU_SOIL_ACID_PH_TEST",
    "ECOU_SOIL_COMMENTS",
    "ECOU_FIELD_GUIDE",
    "ECOU_FIELD_GUIDE_YEAR",
    "ECOU_SURVEYED_BY",
    "ECOU_SURVEY_DATE",
    "ECOU_COMMENTS",
    "ECOU_PERCHED_IND",
    "ECOU_SOIL_ACID_PH",
    "BGRG_REGION_CODE",
    "ECOU_SOIL_OP",
    "ECOU_COARSE_FRAGMENT",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "DOCUMENTKEY"
FROM forest.ECOLOGY_UNIT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'SUB_OPERATING_AREA' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'SUB_OPERATING_AREA' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SUOP_SUBOP_AREA_ID",
    "SUOP_SUBOP_AREA_NAME",
    "DIVI_DIV_NBR",
    "OPAR_OPERATING_AREA_ID",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.SUB_OPERATING_AREA' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'LICENCE_ALLOCATION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'LICENCE_ALLOCATION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "LICN_SEQ_NBR",
    "DIVI_DIV_NBR",
    "MANU_SEQ_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.LICENCE_ALLOCATION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'LICENSEE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'LICENSEE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "LSEE_LICENSEE_ID",
    "LSEE_LICENSEE_NAME",
    "LSEE_CLIENT_CODE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.LICENSEE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'CODE_LOOKUP' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'CODE_LOOKUP' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "COLU_LOOKUP_TYPE",
    "COLU_LOOKUP_ID",
    "COLU_LOOKUP_DESC",
    "COLU_USER_DEFINED_IND",
    "COLU_DISPLAY_IND",
    "COLU_DISPLAY_ORDER",
    "COLU_COMMENT",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "HQ_DISPLAY_IND"
FROM forest.CODE_LOOKUP' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'CUT_PERMIT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'CUT_PERMIT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "PERM_SEQ_NBR",
    "REGN_REGION_ID",
    "PERM_PERMIT_ID",
    "PERM_REVISION_DATE",
    "PAPR_POINT_OF_APPRSL_ID",
    "LSEE_LICENSEE_ID",
    "ADMIN_DSCT_DISTRICT_NAME",
    "GEO_DSCT_DISTRICT_NAME",
    "PERM_MGMT_UNIT_CODE",
    "PERM_TSB_NAME",
    "PERM_EXPIRY_DATE",
    "PERM_REMOTE_OPER_IND",
    "PERM_BASIC_SILVI_IND",
    "PERM_ROAD_MTCE_IND",
    "PERM_ROAD_LAND_USE_CHRG",
    "PERM_COST_EFF_DATE",
    "PERM_RATE_EFF_DATE",
    "PERM_WATER_TRANSPORT_IND",
    "PERM_PRIMARY_BLOCK_SEQ_NBR",
    "PERM_PRIMARY_MARK_ID",
    "PERM_HAUL_IND",
    "TSAR_TSA_NBR",
    "SBLK_SUPPLY_BLOCK_ID",
    "SPTC_SUPPORT_CENTRE_NAME",
    "DIVI_DIV_NBR",
    "PERM_DAMAGE_STATUS",
    "PERM_SUPPORT_CENTRE_DIST",
    "OPTY_OPENING_TYPE_ID",
    "PERS_SEQ_NBR",
    "PERM_PERMIT_STATE",
    "PERM_LOGPROD_EXTERNAL_IND",
    "PERM_LOGPROD_CUSTOMER_ID",
    "PERM_CRUISE_RES_SOURCE",
    "PERM_JURISDICTION",
    "PERM_APPLICATION_DESCRIPTION",
    "PERM_PRIMARY_SPECIES",
    "MKME_MARKING_METHOD_CODE",
    "MKIN_MARKING_INSTRUMENT_CODE",
    "PERM_PROV_FOREST_CONFLICT",
    "PERM_CRUISE_BASED_IND",
    "PERM_LSEE_REPRESENTATIVE",
    "PERM_TRAVERSE_START_POINT",
    "PERM_TRAVERSE_END_POINT",
    "PERM_DIGI_IND",
    "PERM_BDT_CONVERSION",
    "CPCL_PERMIT_CLASS",
    "PERM_PARENT_PERMIT",
    "PERM_SPUC",
    "PERM_SALVAGE_IND",
    "PERM_LOCATION",
    "PERM_STATUS",
    "CTOR_SEQ_NBR",
    "CLOC_SEQ_NBR",
    "PERM_FST_SEQ_NBR",
    "PERM_BID_AMOUNT",
    "PERM_TOTAL_COST",
    "PERM_LEGAL_DESCRIPTION",
    "PERM_GROSS_AREA",
    "PERM_HIGH_LVL_PLN",
    "PERM_LCN_HVST_AUTH",
    "PERM_LCN_REG",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.CUT_PERMIT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'ACTIVITY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'ACTIVITY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "ACTI_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "PERM_SEQ_NBR",
    "ACTT_SEQ_NBR",
    "CTOR_SEQ_NBR",
    "ACTI_STATUS_IND",
    "ACTI_STATUS_DATE",
    "ACTI_COMMENTS",
    "ACTI_COST",
    "ACTI_HARV_SEAS_ID",
    "ACTI_RESPONSIBILITY",
    "ACTI_AREA",
    "ACTI_HARVEST_VOL",
    "ACTI_INT_REASON",
    "ACTI_TARGET_DATE",
    "ACTI_FDP_REASON",
    "ACTI_TARGET_COST",
    "PLAN_SEQ_NBR",
    "LICN_SEQ_NBR",
    "MARK_SEQ_NBR",
    "CLOC_SEQ_NBR",
    "ACTI_DIGITIZED_IND",
    "ACTI_COST_UOM",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "DOCUMENTKEY",
    "ACCL_DESCRIPTION",
    "ACTI_VARIABLE_COST_UPSET",
    "ACTI_TOTAL_COST_UPSET",
    "ACTI_MPS70"
FROM forest.ACTIVITY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'OPERATING_AREA' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'OPERATING_AREA' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "OPAR_OPERATING_AREA_ID",
    "DIVI_DIV_NBR",
    "OPAR_OPERATING_AREA_NAME",
    "OZON_OPERATING_ZONE_ID",
    "OPAR_SEQ_NBR",
    "OPAR_M3_HA_FACTOR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.OPERATING_AREA' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'PERMIT_ALLOCATION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'PERMIT_ALLOCATION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "PEAL_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MANU_SEQ_NBR",
    "DIVI_DIV_NBR",
    "LICN_SEQ_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.PERMIT_ALLOCATION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'BIOGEOCLIMATIC_SUBZONE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'BIOGEOCLIMATIC_SUBZONE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BIOZ_ZONE_ID",
    "BIOS_SUBZONE_ID",
    "BIOS_SUBZONE_NAME",
    "BIOS_ACTIVE_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.BIOGEOCLIMATIC_SUBZONE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'BIOGEOCLIMATIC_ZONE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'BIOGEOCLIMATIC_ZONE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BIOZ_ZONE_ID",
    "BIOZ_ZONE_NAME",
    "BIOZ_ACTIVE_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.BIOGEOCLIMATIC_ZONE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'STANDARD_UNIT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'STANDARD_UNIT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "STUN_SEQ_NBR",
    "SILP_SEQ_NBR",
    "STUN_ID",
    "SUTY_TYPE_ID",
    "STUN_DESCRIPTION",
    "STUN_GROSS_HA_AREA",
    "STUN_DIGITISED_IND",
    "SPSS_SEQ_NBR",
    "STUN_CRITICAL_SITE_CONDITIONS",
    "STUN_SOIL_COMPACTION",
    "STUN_SOIL_EROSION",
    "STUN_SOIL_DISPLACEMENT",
    "STUN_DEPTH_UNFAVOURABLE_MIN",
    "STUN_DEPTH_UNFAVOURABLE_MAX",
    "STUN_TYPE_UNFAVOURABLE",
    "STUN_SEDIMENT_RISK",
    "STUN_MAX_DISTURBANCE",
    "STUN_REGEN_DATE_EARLY",
    "STUN_FREEGROW_DATE_EARLY",
    "STUN_FREEGROW_DATE_LATE",
    "STUN_SILVICULTURAL_SYSTEM_COMM",
    "STUN_TEMP_ACCESS_MAX_DISTURB",
    "STUN_NCBR_HA_AREA",
    "STUN_NCBR_DIGITISED_IND",
    "STUN_TEMP_ACCESS_BANK_HEIGHT",
    "STUN_TEMP_ACCESS_AVG_BANK_HGT",
    "STUN_TEMP_ACCESS_EQUIPMENT",
    "STUN_SOIL_PUDDLING",
    "STUN_SOIL_FROST_HEAVE",
    "STUN_SOIL_WINDTHROW",
    "STUN_VEG_COMP_SEVERITY",
    "STTY_ORIGINAL_STANDARD_TYPE",
    "STUN_ORIGINAL_STD_DECLARE_DATE",
    "STUN_WET_LOW_DENSITY_IND",
    "STUN_MS_DECL_COMMENT",
    "STUN_FORMC_POSTHARV_PRN_DATE",
    "STUN_FORMC_REGEN_PRN_DATE",
    "STUN_FORMC_FREEGROW_PRN_DATE",
    "STDS_SEQ_NBR",
    "STUN_SITE_INDEX",
    "STUN_OTHER_PERF_STANDARDS",
    "STUN_PATHWAY",
    "STUN_SI_SPECIES",
    "STUN_BRUSH_HAZARD",
    "STUN_BROWSE_HAZARD",
    "STUN_SI_SOURCE",
    "STUN_NON_MAPPABLE_AREA",
    "STUN_DESIGNATION_CODE",
    "STUN_DECLARATION_AREA",
    "STUN_DECLARATION_AREA_LOCK_IND",
    "STUN_PREV_POSTHARV_PRN_DATE",
    "STUN_PREV_REGEN_PRN_DATE",
    "STUN_PREV_FREEGROW_PRN_DATE",
    "STUN_OBJECTIVE_TYPE",
    "STUN_REGEN_OBLIGATION_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "STUN_PRIMARY_OBJ",
    "STUN_SECONDARY_OBJ",
    "NP_HA_AREA"
FROM forest.STANDARD_UNIT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'PERSON' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'PERSON' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "PERS_SEQ_NBR",
    "DIVI_DIV_NBR",
    "PERS_FIRST_NAME",
    "PERS_LAST_NAME",
    "PERS_DISPLAY_NAME",
    "PERS_PERSON_NUMBER",
    "PERS_PHONE_NUMBER",
    "PERS_EMAIL",
    "PERS_FAX",
    "CTOR_SEQ_NBR",
    "TPCT_CATEGORY_ID",
    "TPDP_DEPARTMENT_ID",
    "PERS_COMMENT",
    "PERS_ACTIVE_IND",
    "PERS_INACTIVE_DATE",
    "PERS_PROFESSIONAL_NUMBER",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.PERSON' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'BRUSHING_TARGET_SPECIES' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'BRUSHING_TARGET_SPECIES' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BTRS_SEQ_NBR",
    "SILA_SEQ_NBR",
    "SISP_SPECIES_ID",
    "BTRS_COVERAGE_PCT",
    "BTRS_HEIGHT",
    "BTRS_BROADLEAF_TREE_DIAMETER",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.BRUSHING_TARGET_SPECIES' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'CUT_BLOCK_SHAPE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'CUT_BLOCK_SHAPE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    CAST("OBJECTID" AS NUMBER(38,10)) AS "OBJECTID",
    "TRANSACTION_ID",
    "CUTB_SEQ_NBR",
    "BUFFERDIST",
    "OBJECTID_1",
    "TRANSACTIO",
    "OBJECTID_2",
    "HECTARES",
    "FEATURE_LEN",
    "FEATURE_AREA",
    "SHAPE_LEN",
    "SHAPE_AREA",
    SDE.ST_AsText("SHAPE") AS "SHAPE",
    "LICN_SEQ_NBR",
    "MANU_SEQ_NBR",
    "MARK_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.CUT_BLOCK_SHAPE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'CUT_BLOCK_SHAPE_EVW' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'CUT_BLOCK_SHAPE_EVW' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "OBJECTID",
    "TRANSACTION_ID",
    "CUTB_SEQ_NBR",
    "BUFFERDIST",
    "OBJECTID_1",
    "TRANSACTIO",
    "OBJECTID_2",
    "HECTARES",
    "FEATURE_LEN",
    "FEATURE_AREA",
    "SHAPE_LEN",
    "SHAPE_AREA",
    SDE.ST_AsText("SHAPE") AS "SHAPE",
    "LICN_SEQ_NBR",
    "MANU_SEQ_NBR",
    "MARK_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    CAST("SDE_STATE_ID" AS NUMBER(38,10)) AS "SDE_STATE_ID"
FROM forest.CUT_BLOCK_SHAPE_EVW' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'FRST_COST_ITEM' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'FRST_COST_ITEM' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CSTI_COST_ITEM_ID",
    "DIVI_DIV_NBR",
    "CSTI_COST_ITEM_DESCRIPTION",
    "CSTI_COST_ITEM_ACTIVE_IND",
    "CSTI_COST_ITEM_DISPLAY_ORDER",
    "CSTI_COST_ITEM_ACCOUNT_CODE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.FRST_COST_ITEM' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'PLANTING_UNIT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'PLANTING_UNIT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "PLUN_SEQ_NBR",
    "SILA_SEQ_NBR",
    "PLUN_DIGITISED_IND",
    "PLUN_HA_AREA",
    "PLUN_QUALITY_PERCENT",
    "PLUN_PLANTING_COST",
    "PLUN_PLANTING_UNIT_ID",
    "PLUN_SURVIVAL_PCT",
    "PLUN_LAST_SURVIVAL_CHECK",
    "PLUN_CONTRACT_MIN_SPACING",
    "PLUN_CONTRACT_TARGET_SPACING",
    "PLUN_CONTRACT_MAX_SPACING",
    "PLUN_PLANTING_PLAN_COST",
    "PLUN_ROW",
    "PLUN_NET_AREA",
    "PLUN_DRILL",
    "PLUN_MIN_STOCK_SIZE",
    "PLUN_MAX_STOCK_SIZE",
    "PLUN_PLAN_COST_PER_UNIT",
    "PLUN_COST_PER_UNIT",
    "PLUN_DENSITY",
    "PLUN_LOCK_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "WHEN_AREA_IS_CHANGED",
    "PLSP_EDITING_TREES_OR_PCT",
    "TOTAL_TREES",
    "PLUN_COST_PER_TREE",
    "PLUN_PLANNED_COST_PER_TREE",
    "PLUN_COST_PER_HA",
    "PLUN_PLANNED_COST_PER_HA"
FROM forest.PLANTING_UNIT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'SILVICULTURE_ACTIVITY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'SILVICULTURE_ACTIVITY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SILA_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "SICA_SEQ_NBR",
    "SILA_STATUS",
    "SILA_GROSS_HA_AREA",
    "SILA_DIGITISED_IND",
    "SILA_START_DATE",
    "SILA_COMPLETION_DATE",
    "FUND_FUNDING_CODE",
    "RESP_RESPONSIBILITY_CODE",
    "SILA_CONTRACTOR_ID",
    "SILA_FORMB_PRINTED",
    "SILA_FORMB_DATE",
    "SISY_SILVICULTURAL_SYSTEM_ID",
    "SIVA_VARIANT_ID",
    "SIPH_CUT_PHASE_ID",
    "SILA_RESERVE_TYPE",
    "SILA_DISTURBANCE_CODE",
    "HAME_METHOD_ID",
    "SILA_HARVEST_VOLUME",
    "SILA_COST_PER_HA",
    "SILA_MACHINE_HOURS",
    "SILA_PLANTABLE_SPOTS",
    "SILA_SOIL_DISTURBANCE",
    "SILA_SEASON",
    "SILA_PERCENT_PAID",
    "SILA_BRUSH_CHEMICAL",
    "SILA_BRUSH_ACTIVE",
    "SILA_BRUSH_TARGET_SPECIES1",
    "SILA_BRUSH_TARGET_SPECIES2",
    "SILA_BRUSH_DECIDUOUS",
    "SILA_BRUSH_APPLY_RATE",
    "SILA_BRUSH_PMP_PUP_NBR",
    "CTOR_SEQ_NBR",
    "SILA_TREATMENT_UNIT_ID",
    "STST_STOCKING_STATUS_ID",
    "SILA_TOTAL_STOCKING_PCT",
    "SILA_PROJECT_NUM",
    "CLOC_SEQ_NBR",
    "SILA_COST_PER_HA_PLAN",
    "SILA_TREATED_AREA",
    "PERS_SEQ_NBR",
    "OALT_OBJECT_ID",
    "OBJT_SEQ_NBR",
    "HRUN_SEQ_NBR",
    "SILA_START_DATE_PLANNED",
    "SILA_COMPLETION_DATE_PLANNED",
    "SILA_NET_AREA",
    "SURVEYOR_SEQ_NBR",
    "RECORDER_SEQ_NBR",
    "SILA_ACTIVITY_APPROVAL_STATUS",
    "SILA_PRIORITY",
    "SILA_COMMENTS",
    "SILA_COST_UNIT",
    "SILA_BUDGETED_IND",
    "SILA_PREV_FORMB_DATE",
    "SILA_ALLOCATION_ID",
    "SILA_HARVEST_AREA_FINALIZED",
    "SILA_PLAN_LOCK_IND",
    "SILA_ACT_LOCK_IND",
    "ATU_ID",
    "SILA_RETREATMENT_IND",
    "SILA_SDD_SUBMISSION",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "DOCUMENTKEY"
FROM forest.SILVICULTURE_ACTIVITY' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'CTOR_CONTRACTOR_LOCATION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'CTOR_CONTRACTOR_LOCATION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CLOC_SEQ_NBR",
    "CTOR_SEQ_NBR",
    "CLOC_ADDRESS_LINE1",
    "CLOC_ADDRESS_LINE2",
    "CLOC_ADDRESS_LINE3",
    "CLOC_CITY",
    "CLOC_PHONE_NUMBER1",
    "CLOC_PHONE_NUMBER2",
    "CLOC_FAX_NUMBER",
    "CTRY_COUNTRY_CODE",
    "PROV_PROV_STATE_CODE",
    "CLOC_POSTAL_CODE",
    "CLOC_CLIENT_NUMBER",
    "CLOC_CLIENT_LOCATION_CODE",
    "CLOC_CLIENT_LOCATION_NAME",
    "CLOC_LOCN_EXPIRED_IND",
    "CLOC_RETURNED_MAIL_DATE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.CTOR_CONTRACTOR_LOCATION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'SILVICULTURE_STRATUM' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'SILVICULTURE_STRATUM' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SIST_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "SIST_STRATUM_NAME",
    "STST_STOCKING_STATUS_ID",
    "SIST_DIGITISED_IND",
    "SIST_HA_AREA",
    "SIST_GREEN_UP_DATE",
    "SIST_GREEN_UP_IND",
    "SIST_FREE_GROW_DATE",
    "SIST_FREE_GROW_IND",
    "SIST_REGEN_DELAY_MET_DATE",
    "SIST_REGEN_DELAY_MET_IND",
    "SILA_SEQ_NBR",
    "SIST_POST_HARVEST_DATE",
    "SIST_POST_HARVEST_IND",
    "SIST_NONMAP_PARENT",
    "SIST_ESTABLISHMENT_YEAR",
    "SIST_CROP_STEMS_PER_HA",
    "SIST_TOTAL_STEMS_PER_HA",
    "SIST_SITE_INDEX",
    "SIST_PCT_DISTRIBUTION",
    "SIST_PCT_PRODUCTIVE",
    "SIST_PREFIX",
    "SIST_CLASS",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "NP_HA_AREA",
    "DEP_IND"
FROM forest.SILVICULTURE_STRATUM' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'BLOCK_ALLOCATION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'BLOCK_ALLOCATION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CUTB_SEQ_NBR",
    "BLAL_SEQ_NBR",
    "DIVI_DIV_NBR",
    "LICN_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MARK_SEQ_NBR",
    "BLAL_GROSS_HA_AREA",
    "BLAL_MERCH_HA_AREA",
    "BLAL_CRUISE_M3_VOL",
    "BLAL_HARVESTED_M3_VOL",
    "BLAL_HARVESTED_HA_AREA",
    "BLAL_FIRS_CUTBLOCK_ID",
    "BLAL_FIRS_TIMBERMARK_ID",
    "BLAL_ESTIMATED_AREA",
    "MANU_SEQ_NBR",
    "BLAL_DIGI_IND",
    "BLAL_PRIMARY_IND",
    "BLAL_ACTUAL_PARTITION_VOL",
    "BLAL_USR_CRUISE_M3_VOL",
    "BLAL_RW_VOL",
    "BLAL_RW_HA_AREA",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "BLAL_DATA_SOURCE"
FROM forest.BLOCK_ALLOCATION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'MANAGEMENT_UNIT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'MANAGEMENT_UNIT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "MANU_SEQ_NBR",
    "DIVI_DIV_NBR",
    "MANU_ID",
    "MANU_NAME",
    "MANU_COMMENT",
    "MANU_TYPE_ID",
    "MANU_OPERATING_ZONE",
    "MANU_MGR_SEQ_NBR",
    "MANU_FST_SEQ_NBR",
    "MANU_DIGITIZE_IND",
    "MANU_AREA",
    "MANU_NET_AREA",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "NAT_RES_AREA",
    "NAT_RES_REGION",
    "MANU_AAC_EFFECT_DATE",
    "MANU_AAC",
    "NON_BCTS_AAC_IND"
FROM forest.MANAGEMENT_UNIT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'LICENCE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'LICENCE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "LICN_SEQ_NBR",
    "LICN_LICENCE_ID",
    "LICN_LICENCE_DESC",
    "DIVI_DIV_NBR",
    "LICN_CROWN_LAND",
    "LICN_ANNUAL_ALLOWABLE_CUT",
    "LSEE_LICENSEE_ID",
    "TENT_SEQ_NBR",
    "LICN_LICENCE_STATE",
    "LICN_PERMIT_EXISTS_IND",
    "LICN_SALVAGE_IND",
    "LICN_CATEGORY_ID",
    "LICN_FIELD_TEAM_ID",
    "CTOR_SEQ_NBR",
    "CLOC_SEQ_NBR",
    "BLAZ_ADMIN_ZONE_ID",
    "LICN_DIGI_IND",
    "LICL_LICENCE_CLASS",
    "LICN_PARENT_LICENCE",
    "LICN_CROWN_GRANTED_IND",
    "LICN_CLIENT_LOC_CODE",
    "LICN_CATEGORY_TYPE",
    "LICN_LICENCE_TO_CUT_CODE",
    "LINC_CERT_LEVEL_ID",
    "LICN_MGR_SEQ_NBR",
    "LICN_FST_SEQ_NBR",
    "LICN_GROSS_AREA",
    "LICN_NET_AREA",
    "LICN_COMMENT",
    "LICN_APPORTION_TENURE_TYPE",
    "TEAM_SEQ_NBR",
    "LICN_HAMMERMARK",
    "LICN_CLIENT_LOCATION_CODE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "LICN_ARCHIVE_IND",
    "LICN_ARCHIVE_DATE"
FROM forest.LICENCE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'SILV_COMPANY_ACTIVITY_COST' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'SILV_COMPANY_ACTIVITY_COST' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SCAC_SEQ_NBR",
    "SICA_SEQ_NBR",
    "CSTI_COST_ITEM_ID",
    "DIVI_DIV_NBR",
    "SCAC_ACTIVE_IND",
    "SCAC_DISPLAY_ORDER",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.SILV_COMPANY_ACTIVITY_COST' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'SILVICULTURE_PRESCRIPTION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'SILVICULTURE_PRESCRIPTION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SILP_SEQ_NBR",
    "DIVI_DIV_NBR",
    "CUTB_SEQ_NBR",
    "SILP_LTERM_MAN_OBJ",
    "SILP_WILDLIFE",
    "SILP_SENSITIVE_AREAS",
    "SILP_FISHERIES",
    "SILP_WATERSHEDS",
    "SILP_RECREATION",
    "SILP_COMMUNITY_WATERSHED_IND",
    "SILP_BIO_DIVERSITY",
    "SILP_WILDLIFE_TREE_CREDIT_HA",
    "SILP_VISUAL_LANDSCAPE",
    "SILP_VISUAL_ASSESSMENT",
    "SILP_CULTURAL_HERITAGE",
    "SILP_RANGE",
    "SILP_RANGE_TENURE_HOLDER",
    "SILP_OTHER_RESOURCES",
    "SILP_TRAPPER_REGISTRATION",
    "SILP_GUIDE_REGISTRATION",
    "SILP_NA_CONDITIONS",
    "SILP_RIPARIAN_ASSESSMENT",
    "SILP_GULLY_MANAGE_STRATEGY",
    "SILP_GULLY_ASSESSMENT",
    "SILP_FOREST_HEALTH",
    "SILP_PEST_ASSESSMENT",
    "SILP_COARSE_WOODY_DEBRIS",
    "SILP_ARCHAEOLOGICAL_SITES",
    "SILP_ARCHAEOLOGY_ASSESSMENT",
    "SILP_VEGETATION_MANAGEMENT",
    "SILP_LIVESTOCK_USED",
    "SILP_SLOPE_INSTABILITY",
    "SILP_SLOPE_ASSESSMENT",
    "SILP_PERM_ACCESS_MAX_PCT",
    "SILP_TEMP_ACCESS_MAX_REHAB",
    "SILP_TEMP_ACCESS_MAX_DISTURB",
    "SILP_TEMP_ACCESS_LOCATION",
    "SILP_TEMP_ACCESS_BANK_HEIGHT",
    "SILP_TEMP_ACCESS_AVG_BANK_HGT",
    "SILP_TEMP_ACCESS_EQUIPMENT",
    "SILP_TEMP_ACCESS_HA_AREA",
    "SILP_SOIL_CONS_COMMENTS",
    "SILP_STOCKING_REQ_COMMENTS",
    "PERS_SEQ_NBR",
    "SILP_SP_REPORT_FORMAT",
    "SILP_AMENDMENTS_COMMENTS",
    "SILP_TEMP_ACCESS_COMMENTS",
    "SILP_RIPARIAN_COMMENTS",
    "SILP_BIO_DIVERSITY_CALC_IND",
    "SILP_RPF_CERTIFY_COMMENTS",
    "SILP_ADMIN_ASSESSMENT_COMMENTS",
    "SILP_USE_BLOCK_NUM_IND",
    "SILP_COARSE_WOODY_DEBRIS_M3_HA",
    "SILP_BIO_DIVERSITY_PCT",
    "SILP_LICHEN_ASSESSMENT",
    "SILP_ROADSIDE_DISTURB_MAX_PCT",
    "SILP_BIODIV_PERF_STD_START_PCT",
    "SILP_BIO_DIVERS_BLOCK_TGT_PCT",
    "SILP_CWD_PERF_STD_PCT",
    "SILP_CWD_BLOCK_TGT_PCT",
    "SILP_PERM_ACCESS_MAX_AREA",
    "SILP_ADDITIONAL_COMMENTS",
    "SILP_PEST_REQUIRED",
    "SILP_VISUAL_REQUIRED",
    "SILP_VEGETATION_REQUIRED",
    "SILP_RAIPARIAN_REQUIRED",
    "SILP_FHEALTH_REQUIRED",
    "SILP_GULLY_REQUIRED",
    "SILP_ARCHEOSITE_REQUIRED",
    "SILP_SLOPE_REQUIRED",
    "SILP_LICHEN_REQUIRED",
    "SILP_FOREST_HEALTH_PEST",
    "SILP_BIODIV_PERF_STD_END_PCT",
    "SLIP_UPDATED_BY",
    "SLIP_UPDATED_DATE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "FSPM_SEQ_NBR"
FROM forest.SILVICULTURE_PRESCRIPTION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'V_LRM_COMMITMENTS' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_LRM_COMMITMENTS' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "LICN_SEQ_NBR",
    "COMMIT_SEQ_NBR",
    "COPA_PARTITION",
    "COPA_COMMIT_APPO",
    CAST("V_COPA_COMMIT_M3_VOL" AS NUMBER(38,10)) AS "V_COPA_COMMIT_M3_VOL",
    CAST("V_REMAIN_COMMIT_M3_VOL" AS NUMBER(38,10)) AS "V_REMAIN_COMMIT_M3_VOL",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "MANU_SEQ_NBR",
    "COPA_COMMIT_LIC_TYPE"
FROM forest.V_LRM_COMMITMENTS' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'SU_TYPE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'SU_TYPE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "SUTY_TYPE_ID",
    "SUTY_TYPE_DESC",
    CAST("SUTY_SORT_ORDER" AS NUMBER(38,10)) AS "SUTY_SORT_ORDER",
    "SUTY_KEY_IND",
    "SUTY_DEFAULT_CROWN_CLOSURE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.SU_TYPE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'MARK_ALLOCATION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'MARK_ALLOCATION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "MARK_SEQ_NBR",
    "DIVI_DIV_NBR",
    "LICN_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MANU_SEQ_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "MAAL_SEQ_NBR"
FROM forest.MARK_ALLOCATION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'CTOR_CONTRACTOR' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'CTOR_CONTRACTOR' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CTOR_SEQ_NBR",
    "CTOR_NAME",
    "CTOR_EMAIL_ADDRESS",
    "CTOR_STATUS_CODE",
    "CTOR_INTERNAL_CODE",
    "CTOR_EMS_IND",
    "CTOR_WCB_NBR",
    "CTOR_GST",
    "CTOR_WCB_CLEARANCE",
    "CTOR_INSURANCE_LIABILITY",
    "CTOR_DATE_APPROVED",
    "CTOR_EXPIRATION_DATE",
    "CTOR_APPROVED_IND",
    "CTOR_COMMENT",
    "CTOR_ID",
    "CTOR_PASSWORD",
    "CTOR_VENDOR_NBR",
    "CTOR_POST_AP_IND",
    "CTOR_GST_IND",
    "CTOR_LEGAL_FIRST_NAME",
    "CTOR_LEGAL_MIDDLE_NAME",
    "CTOR_BCTS_CLIENT_NUMBER",
    "CTOR_BCTS_OCG_CLIENT_NUMBER",
    "CTOR_BCTS_PRIME_MAILING_LOCN",
    "CTOR_BCTS_TOTAL_BALANCE",
    "CTOR_BIRTHDATE",
    "CTOR_DRIVERS_LICENSE",
    "CTOR_SIN",
    "CTOR_CLIENT_OTHER_ID",
    "CTOR_BCTS_CORP_REGN_NMBR",
    "CTOR_BCTS_CORP_REPORTING_IND",
    "CTOR_RISKY_CONTRACTOR",
    "CTOR_RATE_BENEFIT_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.CTOR_CONTRACTOR' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'LRM_VT_COMMIT_LIC_TYPE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'LRM_VT_COMMIT_LIC_TYPE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CODE",
    "DESCRIPTION",
    "ACTIVEFLAG",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING"
FROM forest.LRM_VT_COMMIT_LIC_TYPE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'CUT_BLOCK_SILV_REGIME' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'CUT_BLOCK_SILV_REGIME' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "CBSR_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "TREG_SEQ_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.CUT_BLOCK_SILV_REGIME' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'V_RES_VT_FDTM_TEAM' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_RES_VT_FDTM_TEAM' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "COLU_LOOKUP_TYPE",
    "COLU_LOOKUP_ID",
    "DIVI_DIV_NBR",
    "COLU_LOOKUP_DESC",
    "COLU_USER_DEFINED_IND",
    "COLU_DISPLAY_IND",
    "COLU_DISPLAY_ORDER"
FROM forest.V_RES_VT_FDTM_TEAM' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'COMMITMENT_PARTITION' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'COMMITMENT_PARTITION' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "COPA_SEQ_NBR",
    "COPA_PARTITION",
    "COPA_PERCENT",
    CAST("MARK_SEQ_NBR" AS NUMBER(38,10)) AS "MARK_SEQ_NBR",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "CUTB_SEQ_NBR",
    "COPA_BLOCK_ID",
    "COPA_CRUISE_M3_VOL",
    "COPA_PARTITION_TYPE",
    "COPA_COMMIT_M3_VOL",
    "COPA_COMMIT_PART_PERCENT",
    "COMMIT_SEQ_NBR"
FROM forest.COMMITMENT_PARTITION' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'BLOCK_SEED_ZONE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'BLOCK_SEED_ZONE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BLSZ_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "BLSZ_CLASS_ID",
    "BLSZ_SPECIES_ID",
    "BLSZ_ZONE_ID",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "SILP_SEQ_NBR"
FROM forest.BLOCK_SEED_ZONE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'APPORTIONMENT' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'APPORTIONMENT' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "APPO_SEQ_NBR",
    "MANU_SEQ_NBR",
    "APPO_TENURE_TYPE",
    "APPO_DATE",
    "APPO_M3_VOL",
    "PERS_SEQ_NBR",
    "CORP_USER_ID",
    "APPO_COMMENTS",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "FIYR",
    "DISPO_AGREEMENT",
    "APPO_END_DATE",
    "DISPO_RECV_LIC_NMB",
    "APPO_M3_VOL_PRORATED",
    "NON_BCTS_TENURE_HOLDER",
    "FN_CODE",
    "RESOURCES_DATA_IND",
    "FORMULA"
FROM forest.APPORTIONMENT' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'V_LRM_LICENCE_SHAPE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_LRM_LICENCE_SHAPE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "OBJECTID",
    "MANU_SEQ_NBR",
    "LICN_SEQ_NBR",
    SDE.ST_AsText("SHAPE") AS "SHAPE",
    CAST("SDE_STATE_ID" AS NUMBER(38,10)) AS "SDE_STATE_ID",
    "V_TREEFIELD",
    "SHAPE_AREA",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.V_LRM_LICENCE_SHAPE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'BLOCK_ADMIN_ZONE' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'BLOCK_ADMIN_ZONE' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "BLAZ_ADMIN_ZONE_ID",
    "DIVI_DIV_NBR",
    "BLAZ_ADMIN_ZONE_DESC",
    "BLAZ_ACTIVE_IND",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING"
FROM forest.BLOCK_ADMIN_ZONE' AS customsql_query,
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
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forest' AS source_schema_name,
            'V_LRM_CUT_BLOCK' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_LRM_CUT_BLOCK' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            'SELECT
    "MANU_SEQ_NBR",
    "LICN_SEQ_NBR",
    "PERM_SEQ_NBR",
    "MARK_SEQ_NBR",
    "CUTB_SEQ_NBR",
    "CUTB_BLOCK_ID",
    "DIVI_DIV_NBR",
    "CUTB_GROSS_HA_AREA",
    "C_CUTB_LATITUDE",
    "C_CUTB_LONGITUDE",
    "CUTB_OPENING",
    "CUTB_PHOTOS",
    "SBLK_SUPPLY_BLOCK_ID",
    "OPAR_OPERATING_AREA_ID",
    "FINZ_FOREST_INVENTORY_ZONE_ID",
    "CUTB_CELL_NUMBER",
    "CUTB_USER_MAPSHEET_ID",
    "CUTB_GREENUP_DATE",
    "GRNS_GREENUP_SOURCE",
    "PMPO_OPERATING_ZONE",
    "LSUN_LANDSCAPE_UNIT",
    "CUTB_PROV_FOREST_CONFLICT",
    "CUTB_DIGI_IND",
    "CUTB_BLOCK_NUMBER",
    "CUTB_OPENING_ID",
    CAST("MIN_ELEVATION" AS NUMBER(38,10)) AS "MIN_ELEVATION",
    CAST("MAX_ELEVATION" AS NUMBER(38,10)) AS "MAX_ELEVATION",
    "CUTB_LATITUDE",
    "CUTB_LONGITUDE",
    "MODIFIEDBY",
    "MODIFIEDON",
    "MODIFIEDUSING",
    "CREATEDBY",
    "CREATEDON",
    "CREATEDUSING",
    "CUTB_CPI_SLOPE_PCT",
    "CUTB_SYSTEM_ID",
    "CUTB_BLOCK_STATE",
    "CUTB_FILE_ID",
    "CUTB_ROW_IND",
    "SUOP_SUBOP_AREA_ID",
    "LICN_LICENCE_ID",
    "GENERATE_SYSTEM_ID",
    "CUTB_LOCATION",
    "CUTB_LATITUDE_DD",
    "CUTB_LONGITUDE_DD",
    CAST("NO_SHAPE" AS NUMBER(38,10)) AS "NO_SHAPE",
    "CUTB_ARCHIVE_REASON",
    "CUTB_ARCHIVE_DATE"
FROM forest.V_LRM_CUT_BLOCK' AS customsql_query,
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

spark.sql("""
    MERGE INTO bcts_metadata.cdc_master_table_list AS tgt
    USING (
        SELECT
            'lrm' AS business,
            'lrm' AS application_name,
            CAST(NULL AS STRING) AS custodian,
            'forestview' AS source_schema_name,
            'V_SILV_STOCKSTAT_HISTORY' AS source_table_name,
            'lrm_replication' AS target_schema_name,
            'V_SILV_STOCKSTAT_HISTORY' AS target_table_name,
            'Y' AS truncate_flag,
            CAST(NULL AS STRING) AS cdc_flag,
            CAST(NULL AS STRING) AS full_inc_flag,
            CAST(NULL AS STRING) AS cdc_column,
            'Y' AS active_ind,
            1 AS replication_order,
            CAST(NULL AS STRING) AS where_clause,
            'Y' AS customsql_ind,
            '
SELECT
    "SISH_SEQ_NBR",
    "SLAY_LAYER",
    "SRNK_RANK",
    "STST_STOCKING_STATUS_ID",
    "STTP_STOCKING_TYPE_ID",
    "SSSH_SURVEY_SOURCE",
    "SSSH_SURVEY_DATE",
    "SSSH_STOCK_AGE",
    "SSSH_STOCK_AGE_PLANT",
    "SSSH_STOCK_HEIGHT",
    "SSSH_CROWN_CLOSURE",
    "SSSH_REFERENCE_YEAR",
    "SICL_SITE_CLASS",

    CAST("SSSH_SITE_INDEX" AS NUMBER(38,10)) AS "SSSH_SITE_INDEX",
    CAST("SSSH_DENSITY" AS NUMBER(38,10)) AS "SSSH_DENSITY",
    CAST("SSSH_WELL_SPACED_DENSITY" AS NUMBER(38,10)) AS "SSSH_WELL_SPACED_DENSITY",
    CAST("SSSH_FREE_GROWING_DENSITY" AS NUMBER(38,10)) AS "SSSH_FREE_GROWING_DENSITY",

    "SRTY_RESERVE_TYPE",

    CAST("SSSH_BASAL_AREA" AS NUMBER(38,10)) AS "SSSH_BASAL_AREA",
    CAST("SSSH_CM_TOP_HEIGHT" AS NUMBER(38,10)) AS "SSSH_CM_TOP_HEIGHT",
    CAST("SSSH_CM_LEADER" AS NUMBER(38,10)) AS "SSSH_CM_LEADER",

    CAST("SSSH_VIGOUR_PCT_GOOD" AS NUMBER(38,10)) AS "SSSH_VIGOUR_PCT_GOOD",
    CAST("SSSH_VIGOUR_PCT_MEDIUM" AS NUMBER(38,10)) AS "SSSH_VIGOUR_PCT_MEDIUM",
    CAST("SSSH_VIGOUR_PCT_POOR" AS NUMBER(38,10)) AS "SSSH_VIGOUR_PCT_POOR",

    "SSSH_COMMENTS",

    CAST("SSSH_TOTAL_CONIFEROUS" AS NUMBER(38,10)) AS "SSSH_TOTAL_CONIFEROUS",
    CAST("SSSH_PLANTABLE_SPOTS" AS NUMBER(38,10)) AS "SSSH_PLANTABLE_SPOTS",
    CAST("SSSH_PREPABLE_SLASH_SPOTS" AS NUMBER(38,10)) AS "SSSH_PREPABLE_SLASH_SPOTS",
    CAST("SSSH_PREPABLE_BRUSH_SPOTS" AS NUMBER(38,10)) AS "SSSH_PREPABLE_BRUSH_SPOTS",
    CAST("SSSH_PREPABLE_DUFF_SPOTS" AS NUMBER(38,10)) AS "SSSH_PREPABLE_DUFF_SPOTS",
    CAST("SSSH_NON_PRODUCTIVE_SPOTS" AS NUMBER(38,10)) AS "SSSH_NON_PRODUCTIVE_SPOTS",
    CAST("SSSH_ROOT_COLLAR_DIAMETER" AS NUMBER(38,10)) AS "SSSH_ROOT_COLLAR_DIAMETER",

    "SSSC_SOURCE_CODE",

    CAST("SSSH_FREE_GROW_DENSITY_PREF" AS NUMBER(38,10)) AS "SSSH_FREE_GROW_DENSITY_PREF",
    CAST("SSSH_WELL_SPACED_DENSITY_PREF" AS NUMBER(38,10)) AS "SSSH_WELL_SPACED_DENSITY_PREF",
    CAST("SSSH_FREE_GROW_LCL_DENSITY" AS NUMBER(38,10)) AS "SSSH_FREE_GROW_LCL_DENSITY",
    CAST("SSSH_WELL_SPACED_LCL_DENSITY" AS NUMBER(38,10)) AS "SSSH_WELL_SPACED_LCL_DENSITY",
    CAST("SSSH_COUNTABLE_CONIFERS" AS NUMBER(38,10)) AS "SSSH_COUNTABLE_CONIFERS",

    "SSSH_REENTRY_YEAR",
    "SSSH_CVR_PATTERN",
    "SSSH_RES_OBJECTIVE",

    CAST("SSSH_TOTAL_WELL_SPACED" AS NUMBER(38,10)) AS "SSSH_TOTAL_WELL_SPACED",
    CAST("SSSH_GERMINANT_PER_HA" AS NUMBER(38,10)) AS "SSSH_GERMINANT_PER_HA",
    CAST("SSSH_GREENED_UP_TREE_PER_HA" AS NUMBER(38,10)) AS "SSSH_GREENED_UP_TREE_PER_HA"

FROM forestview.V_SILV_STOCKSTAT_HISTORY' AS customsql_query,
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
