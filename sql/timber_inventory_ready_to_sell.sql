CREATE TABLE IF NOT EXISTS bcts_staging.timber_inventory_ready_to_sell (
    id INT,
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    business_area_code STRING,
    field_team STRING,
    nav_name STRING,
    operatingarea STRING,
    location STRING,
    tenure STRING,
    licence_id STRING,
    licence_state STRING,
    permit_id STRING,
    block_id STRING,
    ubi STRING,
    block_state STRING,
    dvc_category STRING,
    dr_category STRING,
    deferred_at_report_date STRING,
    inventory_category STRING,
    spatial_flag STRING,
    cruise_vol DECIMAL(15,6),
    rw_vol DECIMAL(15,6),
    rc_date DATE,
    rc_fiscal DECIMAL(15,0),
    rc_quarter STRING,
    dr_date DATE,
    dr_fiscal DECIMAL(15,0),
    dr_quarter STRING,
    dvs_date DATE,
    dvc_date DATE,
    dvc_fiscal DECIMAL(15,0),
    dvc_quarter STRING,
    auc_date TIMESTAMP,
    auc_status STRING,
    def_change_of_op_plan DATE,
    def_first_nations DATE,
    def_loss_of_access DATE,
    def_other DATE,
    def_planning_constraint DATE,
    def_returned_to_bcts DATE,
    def_stale_dated_fieldwork DATE,
    def_stakeholder_issue DATE,
    def_environmental_stewardship_initiative DATE,
    def_reactivated DATE,
    old_growth_strategy DATE,
    ogs_reactivated_forest_health DATE,
    ogs_reactivated_fn_proceed DATE,
    ogs_reactivated_field_verified DATE,
    ogs_reactivated_minor DATE,
    ogs_reactivated_road DATE,
    ogs_reactivated_re_engineered DATE,
    salvage_any_fire_year STRING,
    salvage_fire_years STRING,
    salvage_2021_fire STRING,
    salvage_2022_fire STRING,
    salvage_2023_fire STRING,
    salvage_2024_fire STRING,
    cutb_seq_nbr DECIMAL(15,0),
    report_end_date DATE,
    deferred_activity STRING,
    latest_deferral_date DATE,
    report_run_date DATE,
    ancient STRING,
    remnant STRING,
    big_treed STRING,
    ancient_volume DECIMAL(38,18),
    remnant_volume DECIMAL(38,18),
    big_treed_volume DECIMAL(38,18),
    report_run_timestamp TIMESTAMP,
    salvage_2025_fire STRING
)
USING DELTA;



-- Report exists check is done on bcts_reporting.timber_inventory_ready_to_sell table 
-- If report exists in bcts_staging.timber_inventory_ready_to_sell, clear the staging table before inserting new records 
delete from bcts_staging.timber_inventory_ready_to_sell 
where report_end_date = '${report_end_date}';

-- Populate staging table
INSERT INTO bcts_staging.timber_inventory_ready_to_sell_hist (
    business_area_region_category,
    business_area_region,
    business_area,
    business_area_code,
    field_team,
    nav_name,
    operatingarea,
    location,
    tenure,
    licence_id,
    licence_state,
    permit_id,
    block_id,
    ubi,
    block_state,
    dvc_category,
    dr_category,
    deferred_at_report_date,
    inventory_category,
    deferred_activity,
    latest_deferral_date,
    spatial_flag,
    cruise_vol,
    rw_vol,
    rc_date,
    rc_fiscal,
    rc_quarter,
    dr_date,
    dr_fiscal,
    dr_quarter,
    dvs_date,
    dvc_date,
    dvc_fiscal,
    dvc_quarter,
    auc_date,
    auc_status,
    def_change_of_op_plan,
    def_first_nations,
    def_loss_of_access,
    def_other,
    def_planning_constraint,
    def_returned_to_bcts,
    def_stale_dated_fieldwork,
    def_stakeholder_issue,
    def_environmental_stewardship_initiative,
    def_reactivated,
    old_growth_strategy,
    ogs_reactivated_forest_health,
    ogs_reactivated_fn_proceed,
    ogs_reactivated_field_verified,
    ogs_reactivated_minor,
    ogs_reactivated_road,
    ogs_reactivated_re_engineered,
    salvage_any_fire_year,
    salvage_fire_years,
    salvage_2021_fire,
    salvage_2022_fire,
    salvage_2023_fire,
    salvage_2024_fire,
    cutb_seq_nbr,
    ancient,
    remnant,
    big_treed,
    ancient_volume,
    remnant_volume,
    big_treed_volume,
    report_end_date,
    report_run_timestamp
)

WITH A_D AS (
    SELECT
        CUTB_SEQ_NBR,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DEL' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS Deletion_Approval_Date,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DVC' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DVC_Date,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DVS' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DVS_Date,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RC' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS RC_Date,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DR' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DR_Date,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'WO' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS Write_Off_Date,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DCP' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DEF_Change_of_Op_Plan,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DFN' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DEF_First_Nations,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DLA' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DEF_Loss_of_Access,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DOR' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DEF_Other,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DPC' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DEF_Planning_Constraint,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DRB' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DEF_Returned_to_BCTS,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DSD' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DEF_Stale_dated_Fieldwork,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DSI' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DEF_Stakeholder_Issue,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DESI' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DEF_Environmental_Stewardship_Initiative,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DRD' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS DEF_REACTIVATED,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DOG' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS Old_Growth_Strategy,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RFH' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS OGS_Reactivated_Forest_Health,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RFN' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS OGS_Reactivated_FN_Proceed,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RFV' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS OGS_Reactivated_Field_Verified,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RMN' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS OGS_Reactivated_Minor,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RRD' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS OGS_Reactivated_Road,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RRE' THEN ACTIVITY_DATE ELSE NULL END) AS DATE) AS OGS_Reactivated_Re_Engineered
    FROM (
        SELECT
            A0.CUTB_SEQ_NBR,
            A0.ACTT_KEY_IND,
            A0.ACTIVITY_DATE
        FROM LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL A0
        WHERE (
            (
                A0.ACTIVITY_CLASS = 'CMB'
                AND A0.ACTT_KEY_IND IN (
                    'DVC',
                    'DVS',
                    'RC',
                    'DR',
                    'WO'
                )
            )
            OR (
                A0.ACTIVITY_CLASS = 'CSB'
                AND A0.ACTT_KEY_IND IN (
                    'DEL',
                    'DCP',
                    'DFN',
                    'DLA',
                    'DOG',
                    'DOR',
                    'DPC',
                    'DRB',
                    'DSD',
                    'DSI',
                    'DESI',
                    'DRD',
                    'RFH',
                    'RFN',
                    'RFV',
                    'RMN',
                    'RRD',
                    'RRE'
                )
            )
        )
        AND A0.ACTI_STATUS_IND = 'D'
        AND A0.ACTIVITY_DATE <= CAST('{end_date}' AS DATE)
    ) TEMP
    GROUP BY CUTB_SEQ_NBR
),

DF AS (
    SELECT
        cutb_seq_nbr,
        activity_date AS Latest_Deferral_Date,
        activity_type AS DEFERRED_ACTIVITY
    FROM (
        SELECT
            DA1.cutb_seq_nbr,
            DA1.activity_date,
            DA1.activity_type,
            ROW_NUMBER() OVER (
                PARTITION BY DA1.cutb_seq_nbr
                ORDER BY DA1.activity_date DESC
            ) AS rn
        FROM lrm_replication.v_block_activity_all DA1
        WHERE DA1.acti_status_ind = 'D'
            AND DA1.activity_class = 'CSB'
            AND DA1.ACTT_KEY_IND IN (
                'DCP',
                'DFN',
                'DLA',
                'DOG',
                'DOR',
                'DPC',
                'DRB',
                'DSD',
                'DSI',
                'DESI'
            )
            AND DA1.ACTIVITY_DATE <= CAST('{end_date}' AS DATE)
    ) DF_TEMP
    WHERE rn = 1
),

LDF AS (
    SELECT
        A2.CUTB_SEQ_NBR,
        MAX(A2.ACTIVITY_DATE) AS LATEST_DEF
    FROM LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL A2
    WHERE A2.ACTIVITY_CLASS = 'CSB'
        AND A2.ACTT_KEY_IND IN (
            'DCP',
            'DFN',
            'DLA',
            'DOR',
            'DPC',
            'DRB',
            'DSD',
            'DSI',
            'DESI'
        )
        AND A2.ACTI_STATUS_IND = 'D'
        AND A2.ACTIVITY_DATE <= CAST('{end_date}' AS DATE)
    GROUP BY A2.CUTB_SEQ_NBR
),

LRCT AS (
    SELECT
        A4.CUTB_SEQ_NBR,
        MAX(A4.ACTIVITY_DATE) AS LATEST_OGS_REACTIVATED
    FROM LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL A4
    WHERE A4.ACTIVITY_CLASS = 'CSB'
        AND A4.ACTT_KEY_IND IN (
            'RFH',
            'RFN',
            'RFV',
            'RMN',
            'RRD',
            'RRE',
            'DRD'
        )
        AND A4.ACTI_STATUS_IND = 'D'
        AND A4.ACTIVITY_DATE <= CAST('{end_date}' AS DATE)
    GROUP BY A4.CUTB_SEQ_NBR
),

AUC AS (
    SELECT
        LA1.LICN_SEQ_NBR,
        LA1.ACTI_TARGET_DATE,
        LA1.ACTIVITY_DATE,
        LA1.ACTI_STATUS_IND
    FROM LRM_REPLICATION.V_LICENCE_ACTIVITY_ALL LA1
    WHERE LA1.ACTIVITY_CLASS = 'CML'
        AND LA1.ACTT_KEY_IND = 'AUC'
),

HI AS (
    SELECT
        LA2.LICN_SEQ_NBR
    FROM LRM_REPLICATION.V_LICENCE_ACTIVITY_ALL LA2
    WHERE LA2.ACTIVITY_CLASS = 'CML'
        AND LA2.ACTT_KEY_IND = 'HI'
        AND LA2.ACTIVITY_DATE <= CAST('{end_date}' AS DATE)
        AND LA2.ACTI_STATUS_IND = 'D'
),

SALVAGE_ANY_FIRE_YEAR AS (
    SELECT
        cutb_seq_nbr,
        CONCAT_WS(
            ', ',
            SORT_ARRAY(
                COLLECT_SET(
                    SUBSTRING(activity_type, INSTR(activity_type, '2'), 4)
                )
            )
        ) AS salvage_fire_years
    FROM lrm_replication.v_block_activity_all
    WHERE activity_class = 'CSB'
        AND actt_key_ind LIKE 'SFIRE%'
    GROUP BY cutb_seq_nbr
),

SALVAGE21 AS (
    SELECT DISTINCT
        cutb_seq_nbr,
        activity_class,
        actt_key_ind,
        activity_type
    FROM LRM_REPLICATION.v_block_activity_all
    WHERE activity_class = 'CSB'
        AND actt_key_ind = 'SFIRE21'
),

SALVAGE22 AS (
    SELECT DISTINCT
        cutb_seq_nbr,
        activity_class,
        actt_key_ind,
        activity_type
    FROM LRM_REPLICATION.v_block_activity_all
    WHERE activity_class = 'CSB'
        AND actt_key_ind = 'SFIRE22'
),

SALVAGE23 AS (
    SELECT DISTINCT
        cutb_seq_nbr,
        activity_class,
        actt_key_ind,
        activity_type
    FROM LRM_REPLICATION.v_block_activity_all
    WHERE activity_class = 'CSB'
        AND actt_key_ind = 'SFIRE23'
),

SALVAGE24 AS (
    SELECT DISTINCT
        cutb_seq_nbr,
        activity_class,
        actt_key_ind,
        activity_type
    FROM LRM_REPLICATION.v_block_activity_all
    WHERE activity_class = 'CSB'
        AND actt_key_ind = 'SFIRE24'
)

SELECT DISTINCT
    CASE
        WHEN B.TSO_CODE IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN', 'TCC', 'TKA', 'TKO', 'TOC')
            THEN 'Interior'
        WHEN B.TSO_CODE IN ('TCH', 'TST', 'TSG')
            THEN 'Coast'
    END AS BUSINESS_AREA_REGION_CATEGORY,

    CASE
        WHEN B.TSO_CODE IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN')
            THEN 'North Interior'
        WHEN B.TSO_CODE IN ('TCC', 'TKA', 'TKO', 'TOC')
            THEN 'South Interior'
        WHEN B.TSO_CODE IN ('TCH', 'TST', 'TSG')
            THEN 'Coast'
    END AS BUSINESS_AREA_REGION,

    CONCAT(
        CASE
            WHEN B.TSO_NAME = 'Seaward' THEN 'Seaward-Tlasta'
            ELSE B.TSO_NAME
        END,
        ' (',
        B.TSO_CODE,
        ')'
    ) AS BUSINESS_AREA,

    B.TSO_CODE AS BUSINESS_AREA_CODE,
    L.FIELD_TEAM,
    L.NAV_NAME,
    B.OPAR_OPERATING_AREA_NAME AS OperatingArea,
    B.CUTB_LOCATION AS Location,
    L.TENURE,
    L.LICENCE_ID,
    L.LICN_LICENCE_STATE AS Licence_State,
    B.PERMIT_ID,
    B.BLOCK_ID,
    B.UBI,
    B.CUTB_BLOCK_STATE AS Block_State,

    CASE
        WHEN YEAR(ADD_MONTHS(CURRENT_DATE(), 9)) - YEAR(ADD_MONTHS(A_D.DVC_Date, 9)) > 5
            THEN CONCAT(
                'Before ',
                DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -63), 'yy'),
                '/',
                DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -51), 'yy')
            )
        ELSE CONCAT(
            DATE_FORMAT(ADD_MONTHS(A_D.DVC_Date, -3), 'yy'),
            '/',
            DATE_FORMAT(ADD_MONTHS(A_D.DVC_Date, 9), 'yy')
        )
    END AS DVC_Category,

    CASE
        WHEN YEAR(ADD_MONTHS(CURRENT_DATE(), 9)) - YEAR(ADD_MONTHS(A_D.DR_Date, 9)) > 5
            THEN CONCAT(
                'Before ',
                DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -63), 'yy'),
                '/',
                DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -51), 'yy')
            )
        ELSE CONCAT(
            DATE_FORMAT(ADD_MONTHS(A_D.DR_Date, -3), 'yy'),
            '/',
            DATE_FORMAT(ADD_MONTHS(A_D.DR_Date, 9), 'yy')
        )
    END AS DR_Category,

    CASE
        WHEN (
            A_D.Old_Growth_Strategy > COALESCE(LRCT.LATEST_OGS_REACTIVATED, DATE '1900-01-01')
            OR LDF.LATEST_DEF > COALESCE(A_D.DEF_REACTIVATED, DATE '1900-01-01')
        )
            THEN 'Y'
        ELSE 'N'
    END AS DEFERRED_AT_REPORT_DATE,

    CASE
        WHEN A_D.Old_Growth_Strategy > COALESCE(LRCT.LATEST_OGS_REACTIVATED, DATE '1900-01-01')
            THEN 'Deferred-OGS'
        WHEN LDF.LATEST_DEF > COALESCE(A_D.DEF_REACTIVATED, DATE '1900-01-01')
            THEN 'Deferred-Other'
        ELSE 'Ready to Sell'
    END AS INVENTORY_CATEGORY,

    DF.DEFERRED_ACTIVITY,
    DF.LATEST_DEFERRAL_DATE,
    BS.SPATIAL_FLAG,
    B.CRUISE_VOL,
    B.BLAL_RW_VOL AS RW_VOL,
    A_D.RC_Date,

    YEAR(ADD_MONTHS(A_D.RC_Date, 9)) AS RC_Fiscal,

    CONCAT(
        'Q',
        CAST(CEIL(MONTH(ADD_MONTHS(A_D.RC_Date, -3)) / 3.0) AS INT)
    ) AS RC_Quarter,

    A_D.DR_Date,

    YEAR(ADD_MONTHS(A_D.DR_Date, 9)) AS DR_Fiscal,

    CONCAT(
        'Q',
        CAST(CEIL(MONTH(ADD_MONTHS(A_D.DR_Date, -3)) / 3.0) AS INT)
    ) AS DR_Quarter,

    A_D.DVS_Date,
    A_D.DVC_Date,

    YEAR(ADD_MONTHS(A_D.DVC_Date, 9)) AS DVC_Fiscal,

    CONCAT(
        'Q',
        CAST(CEIL(MONTH(ADD_MONTHS(A_D.DVC_Date, -3)) / 3.0) AS INT)
    ) AS DVC_Quarter,

    AUC.ACTIVITY_DATE AS AUC_Date,
    AUC.ACTI_STATUS_IND AS AUC_Status,
    A_D.DEF_Change_of_Op_Plan,
    A_D.DEF_First_Nations,
    A_D.DEF_Loss_of_Access,
    A_D.DEF_Other,
    A_D.DEF_Planning_Constraint,
    A_D.DEF_Returned_to_BCTS,
    A_D.DEF_Stale_dated_Fieldwork,
    A_D.DEF_Stakeholder_Issue,
    A_D.DEF_Environmental_Stewardship_Initiative,
    A_D.DEF_REACTIVATED,
    A_D.Old_Growth_Strategy,
    A_D.OGS_Reactivated_Forest_Health,
    A_D.OGS_Reactivated_FN_Proceed,
    A_D.OGS_Reactivated_Field_Verified,
    A_D.OGS_Reactivated_Minor,
    A_D.OGS_Reactivated_Road,
    A_D.OGS_Reactivated_Re_Engineered,

    CASE
        WHEN SALVAGE_ANY_FIRE_YEAR.cutb_seq_nbr IS NULL THEN 'N'
        ELSE 'Y'
    END AS SALVAGE_ANY_FIRE_YEAR,

    SALVAGE_ANY_FIRE_YEAR.salvage_fire_years,

    CASE
        WHEN salvage21.actt_key_ind IS NULL THEN NULL
        ELSE CONCAT(
            salvage21.activity_type,
            ' (',
            salvage21.activity_class,
            ' - ',
            salvage21.actt_key_ind,
            ')'
        )
    END AS salvage_2021_fire,

    CASE
        WHEN salvage22.actt_key_ind IS NULL THEN NULL
        ELSE CONCAT(
            salvage22.activity_type,
            ' (',
            salvage22.activity_class,
            ' - ',
            salvage22.actt_key_ind,
            ')'
        )
    END AS salvage_2022_fire,

    CASE
        WHEN salvage23.actt_key_ind IS NULL THEN NULL
        ELSE CONCAT(
            salvage23.activity_type,
            ' (',
            salvage23.activity_class,
            ' - ',
            salvage23.actt_key_ind,
            ')'
        )
    END AS salvage_2023_fire,

    CASE
        WHEN salvage24.actt_key_ind IS NULL THEN NULL
        ELSE CONCAT(
            salvage24.activity_type,
            ' (',
            salvage24.activity_class,
            ' - ',
            salvage24.actt_key_ind,
            ')'
        )
    END AS salvage_2024_fire,

    B.CUTB_SEQ_NBR,
    OGC.ancient,
    OGC.remnant,
    OGC.big_treed,

    CASE
        WHEN OGC.ancient = 'Y' THEN B.CRUISE_VOL
        ELSE 0
    END AS ANCIENT_VOLUME,

    CASE
        WHEN OGC.remnant = 'Y' THEN B.CRUISE_VOL
        ELSE 0
    END AS REMNANT_VOLUME,

    CASE
        WHEN OGC.big_treed = 'Y' THEN B.CRUISE_VOL
        ELSE 0
    END AS BIG_TREED_VOLUME,

    CAST('{end_date}' AS DATE) AS report_end_date,
        FROM_UTC_TIMESTAMP(
        CURRENT_TIMESTAMP(),
        'America/Vancouver'
    ) AS report_run_timestamp

FROM LRM_REPLICATION.V_BLOCK B

INNER JOIN A_D
    ON B.CUTB_SEQ_NBR = A_D.CUTB_SEQ_NBR

LEFT JOIN LDF
    ON B.CUTB_SEQ_NBR = LDF.CUTB_SEQ_NBR

LEFT JOIN LRCT
    ON B.CUTB_SEQ_NBR = LRCT.CUTB_SEQ_NBR

LEFT JOIN DF
    ON B.CUTB_SEQ_NBR = DF.CUTB_SEQ_NBR

LEFT JOIN LRM_REPLICATION.V_BLOCK_SPATIAL BS
    ON B.CUTB_SEQ_NBR = BS.CUTB_SEQ_NBR

LEFT JOIN LRM_REPLICATION.V_LICENCE L
    ON B.LICN_SEQ_NBR = L.LICN_SEQ_NBR

LEFT JOIN AUC
    ON L.LICN_SEQ_NBR = AUC.LICN_SEQ_NBR

LEFT JOIN HI
    ON L.LICN_SEQ_NBR = HI.LICN_SEQ_NBR

LEFT JOIN SALVAGE_ANY_FIRE_YEAR
    ON B.cutb_seq_nbr = SALVAGE_ANY_FIRE_YEAR.cutb_seq_nbr

LEFT JOIN salvage21
    ON B.cutb_seq_nbr = salvage21.cutb_seq_nbr

LEFT JOIN salvage22
    ON B.cutb_seq_nbr = salvage22.cutb_seq_nbr

LEFT JOIN salvage23
    ON B.cutb_seq_nbr = salvage23.cutb_seq_nbr

LEFT JOIN salvage24
    ON B.cutb_seq_nbr = salvage24.cutb_seq_nbr

LEFT JOIN bcts_staging.old_growth_tap_deferral_categories OGC
    ON B.UBI = OGC.UBI

WHERE 1 = 1
    AND COALESCE(L.TENURE, ' ') <> 'B07'
    AND A_D.RC_Date IS NOT NULL
    AND A_D.DR_Date IS NOT NULL
    AND A_D.DVC_Date IS NOT NULL
    AND A_D.Deletion_Approval_Date IS NULL
    AND A_D.Write_Off_Date IS NULL
    AND HI.LICN_SEQ_NBR IS NULL;

-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
    DROP TABLE IF EXISTS BCTS_STAGING.timber_inventory_ready_to_sell;
    CREATE TABLE BCTS_STAGING.timber_inventory_ready_to_sell
    AS SELECT * 
    FROM BCTS_STAGING.timber_inventory_ready_to_sell_hist
    WHERE report_end_date = (
        SELECT MAX(report_end_date)
        FROM BCTS_STAGING.timber_inventory_ready_to_sell_hist
	);

    DROP TABLE IF EXISTS BCTS_REPORTING.timber_inventory_ready_to_sell_hist;
    CREATE TABLE BCTS_REPORTING.timber_inventory_ready_to_sell_hist
    AS SELECT * 
    FROM BCTS_STAGING.timber_inventory_ready_to_sell_hist;

    DROP TABLE IF EXISTS BCTS_REPORTING.timber_inventory_ready_to_sell;
    CREATE TABLE BCTS_REPORTING.timber_inventory_ready_to_sell
    AS SELECT * 
    FROM BCTS_STAGING.timber_inventory_ready_to_sell;