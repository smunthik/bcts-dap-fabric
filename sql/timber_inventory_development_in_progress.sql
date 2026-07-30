CREATE TABLE IF NOT EXISTS bcts_staging.timber_inventory_development_in_progress_hist (
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    business_area_code STRING,
    fieldteam STRING,
    manu_id STRING,
    licence_id STRING,
    tenure_type STRING,
    perm_permit_id STRING,
    mark_mark_id STRING,
    block_id STRING,
    ubi STRING,
    block_nbr STRING,
    sub_operating_area STRING,
    licn_licence_state STRING,
    cutb_block_state STRING,
    deferred_at_report_date STRING,
    inventory_category STRING,
    merch_area DECIMAL(11,6),
    cruise_volume DECIMAL(15,6),
    rw_volume DECIMAL(15,6),
    rc_status STRING,
    rc_date DATE,
    rc_fiscal DECIMAL(10,0),
    dr_status STRING,
    dr_date DATE,
    dr_fiscal DECIMAL(10,0),
    dvs_status STRING,
    dvs_date DATE,
    dvs_fiscal DECIMAL(10,0),
    dsc_status STRING,
    dsc_date DATE,
    dvc_status STRING,
    dvc_date DATE,
    dvc_fiscal DECIMAL(10,0),
    days_in_dip DOUBLE,
    days_in_dip_category STRING,
    woff_status STRING,
    woff_date DATE,
    woff_fiscal DECIMAL(10,0),
    auc_status STRING,
    auc_date TIMESTAMP,
    hi_status STRING,
    hi_date TIMESTAMP,
    hvs_status STRING,
    hvs_date DATE,
    hvc_status STRING,
    hvc_date DATE,
    fg_met_status STRING,
    fg_date DATE,
    def_change_of_op_plan_status STRING,
    def_change_of_op_plan DATE,
    def_first_nations_status STRING,
    def_first_nations DATE,
    def_loss_of_access_status STRING,
    def_loss_of_access DATE,
    def_other_status STRING,
    def_other DATE,
    def_planning_constraint_status STRING,
    def_planning_constraint DATE,
    def_returned_to_bcts_status STRING,
    def_returned_to_bcts DATE,
    def_stale_dated_fieldwork_status STRING,
    def_stale_dated_fieldwork DATE,
    def_stakeholder_issue_status STRING,
    def_stakeholder_issue DATE,
    def_environmental_stewardship_initiative_status STRING,
    def_environmental_stewardship_initiative DATE,
    def_reactivated_status STRING,
    def_reactivated DATE,
    old_growth_strategy_status STRING,
    old_growth_strategy DATE,
    ogs_reactivated_forest_health_status STRING,
    ogs_reactivated_forest_health DATE,
    ogs_reactivated_fn_proceed_status STRING,
    ogs_reactivated_fn_proceed DATE,
    ogs_reactivated_field_verified_status STRING,
    ogs_reactivated_field_verified DATE,
    ogs_reactivated_minor_status STRING,
    ogs_reactivated_minor DATE,
    ogs_reactivated_road_status STRING,
    ogs_reactivated_road DATE,
    ogs_reactivated_re_engineered_status STRING,
    ogs_reactivated_re_engineered DATE,
    xxx_zzz_flag STRING,
    spatial_flag STRING,
    rc_flag STRING,
    dr_flag STRING,
    dvs_flag STRING,
    dsc_flag STRING,
    dvc_flag STRING,
    count_of_blocks BIGINT,
    salvage_any_fire_year STRING,
    salvage_fire_years STRING,
    salvage_2021_fire STRING,
    salvage_2022_fire STRING,
    salvage_2023_fire STRING,
    salvage_2024_fire STRING,
    salvage_2025_fire STRING,
    licn_seq_nbr BIGINT,
    mark_seq_nbr DECIMAL(15,0),
    cutb_seq_nbr BIGINT,
    ancient STRING,
    remnant STRING,
    big_treed STRING,
    ancient_volume DECIMAL(18,6),
    remnant_volume DECIMAL(18,6),
    big_treed_volume DECIMAL(18,6),
    report_end_date DATE,
    report_run_date DATE,
    report_run_timestamp TIMESTAMP
)
USING DELTA;


-- Report exists check is done on bcts_reporting.annual_developed_volume table 
-- If report exists in bcts_staging.timber_inventory_development_in_progress_hist, clear the staging table before inserting new records 
delete from bcts_staging.timber_inventory_development_in_progress_hist 
where report_end_date = '${report_end_date}';

-- Populate staging table for development in progress history
INSERT INTO bcts_staging.timber_inventory_development_in_progress_hist(
    business_area_region_category, business_area_region, business_area, business_area_code, fieldteam, manu_id, licence_id, tenure_type, perm_permit_id, mark_mark_id, block_id, ubi, block_nbr, sub_operating_area, licn_licence_state, cutb_block_state, deferred_at_report_date, inventory_category, inventory_category_new, merch_area, cruise_volume, rw_volume, rc_status, rc_date, rc_fiscal, dr_status, dr_date, dr_fiscal, dvs_status, dvs_date, dvs_fiscal, dsc_status, dsc_date, dvc_status, dvc_date, dvc_fiscal, days_in_dip, days_in_dip_category, woff_status, woff_date, woff_fiscal, auc_status, auc_date, hi_status, hi_date, hvs_status, hvs_date, hvc_status, hvc_date, fg_met_status, fg_date, def_change_of_op_plan_status, def_change_of_op_plan, def_first_nations_status, def_first_nations, def_loss_of_access_status, def_loss_of_access, def_other_status, def_other, def_planning_constraint_status, def_planning_constraint, def_returned_to_bcts_status, def_returned_to_bcts, def_stale_dated_fieldwork_status, def_stale_dated_fieldwork, def_stakeholder_issue_status, def_stakeholder_issue, def_environmental_stewardship_initiative_status, def_environmental_stewardship_initiative, def_reactivated_status, def_reactivated, old_growth_strategy_status, old_growth_strategy, ogs_reactivated_forest_health_status, ogs_reactivated_forest_health, ogs_reactivated_fn_proceed_status, ogs_reactivated_fn_proceed, ogs_reactivated_field_verified_status, ogs_reactivated_field_verified, ogs_reactivated_minor_status, ogs_reactivated_minor, ogs_reactivated_road_status, ogs_reactivated_road, ogs_reactivated_re_engineered_status, ogs_reactivated_re_engineered, xxx_zzz_flag, spatial_flag, rc_flag, dr_flag, dvs_flag, dsc_flag, dvc_flag, count_of_blocks, salvage_any_fire_year, salvage_fire_years, salvage_2021_fire, salvage_2022_fire, salvage_2023_fire, salvage_2024_fire, salvage_2025_fire, licn_seq_nbr, mark_seq_nbr, cutb_seq_nbr, ancient, remnant, big_treed, ancient_volume, remnant_volume, big_treed_volume, report_end_date   
)
/* Block Activity (ACTB) */
WITH ACTB AS
(
    SELECT
        cutb_seq_nbr,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DEL' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DEL_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DSC' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DSC_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DVC' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DVC_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DVS' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DVS_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'FG' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS FG_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'HVC' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS HVC_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'HVS' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS HVS_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RC' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS RC_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DR' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DR_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'WO' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS WOFF_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DCP' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DEF_Change_of_Op_Plan,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DFN' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DEF_First_Nations,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DLA' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DEF_Loss_of_Access,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DOR' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DEF_Other,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DPC' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DEF_Planning_Constraint,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DRB' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DEF_Returned_to_BCTS,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DSD' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DEF_Stale_dated_Fieldwork,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DSI' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DEF_Stakeholder_Issue,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DESI' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DEF_Environmental_Stewardship_Initiative,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DRD' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS DEF_REACTIVATED,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DOG' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS Old_Growth_Strategy,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RFH' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS OGS_Reactivated_Forest_Health,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RFN' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS OGS_Reactivated_FN_Proceed,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RFV' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS OGS_Reactivated_Field_Verified,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RMN' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS OGS_Reactivated_Minor,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RRD' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS OGS_Reactivated_Road,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RRE' THEN ACTI_STATUS_DATE ELSE NULL END) AS DATE) AS OGS_Reactivated_Re_Engineered
    FROM
    (
        SELECT
            a.cutb_seq_nbr,
            atype.actt_key_ind,
            a.acti_status_date
        FROM
            LRM_REPLICATION.activity_class ac
        INNER JOIN LRM_REPLICATION.activity_type atype
            ON ac.accl_seq_nbr = atype.accl_seq_nbr
            AND ac.divi_div_nbr = atype.divi_div_nbr
        INNER JOIN LRM_REPLICATION.activity a
            ON atype.actt_seq_nbr = a.actt_seq_nbr
            AND a.plan_seq_nbr IS NULL
        WHERE
            (
                atype.actt_key_ind IN ('DSC', 'DVC', 'DVS', 'FG', 'HVC', 'HVS', 'RC', 'DR', 'WO')
                AND ac.accl_key_ind = 'CMB'
            )
            OR
            (
                atype.actt_key_ind IN (
                    'DEL',
                    'DCP', 'DFN', 'DLA', 'DOG', 'DOR', 'DPC', 'DRB', 'DSD', 'DSI',
                    'DESI', 'DRD', 'RFH', 'RFN', 'RFV', 'RMN', 'RRD', 'RRE'
                )
                AND ac.accl_key_ind = 'CSB'
            )
    ) TEMP
    GROUP BY cutb_seq_nbr
),

/* Block Activity Status (ACTB_S) */
ACTB_S AS
(
    SELECT
        CUTB_SEQ_NBR,
        MAX(CASE WHEN ACTT_KEY_IND = 'DEL' THEN ACTI_STATUS_IND ELSE NULL END) AS DEL_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DSC' THEN ACTI_STATUS_IND ELSE NULL END) AS DSC_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DVC' THEN ACTI_STATUS_IND ELSE NULL END) AS DVC_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DVS' THEN ACTI_STATUS_IND ELSE NULL END) AS DVS_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'FG' THEN ACTI_STATUS_IND ELSE NULL END) AS FG_Met_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'HVC' THEN ACTI_STATUS_IND ELSE NULL END) AS HVC_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'HVS' THEN ACTI_STATUS_IND ELSE NULL END) AS HVS_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'RC' THEN ACTI_STATUS_IND ELSE NULL END) AS RC_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DR' THEN ACTI_STATUS_IND ELSE NULL END) AS DR_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'WO' THEN ACTI_STATUS_IND ELSE NULL END) AS WOFF_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DCP' THEN ACTI_STATUS_IND ELSE NULL END) AS DEF_Change_of_Op_Plan_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DFN' THEN ACTI_STATUS_IND ELSE NULL END) AS DEF_First_Nations_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DLA' THEN ACTI_STATUS_IND ELSE NULL END) AS DEF_Loss_of_Access_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DOR' THEN ACTI_STATUS_IND ELSE NULL END) AS DEF_Other_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DPC' THEN ACTI_STATUS_IND ELSE NULL END) AS DEF_Planning_Constraint_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DRB' THEN ACTI_STATUS_IND ELSE NULL END) AS DEF_Returned_to_BCTS_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DSD' THEN ACTI_STATUS_IND ELSE NULL END) AS DEF_Stale_dated_Fieldwork_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DSI' THEN ACTI_STATUS_IND ELSE NULL END) AS DEF_Stakeholder_Issue_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DESI' THEN ACTI_STATUS_IND ELSE NULL END) AS DEF_Environmental_Stewardship_Initiative_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DRD' THEN ACTI_STATUS_IND ELSE NULL END) AS DEF_REACTIVATED_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'DOG' THEN ACTI_STATUS_IND ELSE NULL END) AS Old_Growth_Strategy_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'RFH' THEN ACTI_STATUS_IND ELSE NULL END) AS OGS_Reactivated_Forest_Health_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'RFN' THEN ACTI_STATUS_IND ELSE NULL END) AS OGS_Reactivated_FN_Proceed_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'RFV' THEN ACTI_STATUS_IND ELSE NULL END) AS OGS_Reactivated_Field_Verified_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'RMN' THEN ACTI_STATUS_IND ELSE NULL END) AS OGS_Reactivated_Minor_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'RRD' THEN ACTI_STATUS_IND ELSE NULL END) AS OGS_Reactivated_Road_Status,
        MAX(CASE WHEN ACTT_KEY_IND = 'RRE' THEN ACTI_STATUS_IND ELSE NULL END) AS OGS_Reactivated_Re_Engineered_Status
    FROM
    (
        SELECT
            a.cutb_seq_nbr,
            atype.actt_key_ind,
            a.acti_status_ind
        FROM
            LRM_REPLICATION.activity_class ac
        INNER JOIN LRM_REPLICATION.activity_type atype
            ON ac.accl_seq_nbr = atype.accl_seq_nbr
            AND ac.divi_div_nbr = atype.divi_div_nbr
        INNER JOIN LRM_REPLICATION.activity a
            ON atype.actt_seq_nbr = a.actt_seq_nbr
            AND (
                (
                    atype.actt_key_ind IN ('DSC', 'DVC', 'DVS', 'FG', 'HVC', 'HVS', 'RC', 'DR', 'WO')
                    AND ac.accl_key_ind = 'CMB'
                )
                OR
                (
                    atype.actt_key_ind IN (
                        'DEL',
                        'DCP', 'DFN', 'DLA', 'DOG', 'DOR', 'DPC', 'DRB', 'DSD', 'DSI',
                        'DESI', 'DRD', 'RFH', 'RFN', 'RFV', 'RMN', 'RRD', 'RRE'
                    )
                    AND ac.accl_key_ind = 'CSB'
                )
            )
            AND a.plan_seq_nbr IS NULL
    ) TEMP
    GROUP BY cutb_seq_nbr
),

/* Latest non-old-growth deferral activity (LDF) */
LDF AS
(
    SELECT
        A2.CUTB_SEQ_NBR,
        MAX(A2.ACTIVITY_DATE) AS LATEST_DEF
    FROM
        LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL A2
    WHERE
        A2.ACTIVITY_CLASS = 'CSB'
        AND A2.ACTT_KEY_IND IN ('DCP', 'DFN', 'DLA', 'DOR', 'DPC', 'DRB', 'DSD', 'DSI', 'DESI')
        AND A2.ACTI_STATUS_IND = 'D'
        AND A2.ACTIVITY_DATE <= DATE '${report_end_date}'
    GROUP BY
        A2.CUTB_SEQ_NBR
),

/* Latest reactivation activity, except DRD */
LRCT AS
(
    SELECT
        A4.CUTB_SEQ_NBR,
        MAX(A4.ACTIVITY_DATE) AS LATEST_OGS_REACTIVATED
    FROM
        LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL A4
    WHERE
        A4.ACTIVITY_CLASS = 'CSB'
        AND A4.ACTT_KEY_IND IN ('RFH', 'RFN', 'RFV', 'RMN', 'RRD', 'RRE')
        AND A4.ACTI_STATUS_IND = 'D'
        AND A4.ACTIVITY_DATE <= DATE '${report_end_date}'
    GROUP BY
        A4.CUTB_SEQ_NBR
),

/* Block Activity Flags (ACTB_F) */
ACTB_F AS
(
    SELECT
        cutb_seq_nbr,
        MAX(CASE WHEN actt_key_ind = 'DSC' THEN 'Y' END) AS has_dsc,
        MAX(CASE WHEN actt_key_ind = 'DVC' THEN 'Y' END) AS has_dvc,
        MAX(CASE WHEN actt_key_ind = 'DVS' THEN 'Y' END) AS has_dvs,
        MAX(CASE WHEN actt_key_ind = 'FG' THEN 'Y' END) AS has_fg,
        MAX(CASE WHEN actt_key_ind = 'HVC' THEN 'Y' END) AS has_hvc,
        MAX(CASE WHEN actt_key_ind = 'HVS' THEN 'Y' END) AS has_hvs,
        MAX(CASE WHEN actt_key_ind = 'RC' THEN 'Y' END) AS has_rc,
        MAX(CASE WHEN actt_key_ind = 'DR' THEN 'Y' END) AS has_dr,
        MAX(CASE WHEN actt_key_ind = 'WO' THEN 'Y' END) AS has_woff
    FROM
    (
        SELECT
            a.cutb_seq_nbr,
            a.acti_seq_nbr,
            atype.actt_key_ind
        FROM
            LRM_REPLICATION.activity_class ac
        JOIN LRM_REPLICATION.activity_type atype
            ON ac.accl_seq_nbr = atype.accl_seq_nbr
            AND ac.divi_div_nbr = atype.divi_div_nbr
        JOIN LRM_REPLICATION.activity a
            ON atype.actt_seq_nbr = a.actt_seq_nbr
        WHERE
            atype.actt_key_ind IN ('DSC', 'DVC', 'DVS', 'FG', 'HVC', 'HVS', 'RC', 'DR', 'WO')
            AND a.plan_seq_nbr IS NULL
            AND ac.accl_key_ind = 'CMB'
    ) sub
    GROUP BY cutb_seq_nbr
),

/* Auction status and date for licence (AUC) */
AUC AS
(
    SELECT
        A.LICN_SEQ_NBR,
        A.ACTI_STATUS_IND AS AUC_Status,
        A.ACTI_STATUS_DATE AS AUC_DATE
    FROM
        LRM_REPLICATION.activity_class ac,
        LRM_REPLICATION.activity_type atype,
        LRM_REPLICATION.activity A
    WHERE
        ac.accl_seq_nbr = atype.accl_seq_nbr
        AND ac.divi_div_nbr = atype.divi_div_nbr
        AND atype.actt_seq_nbr = a.actt_seq_nbr
        AND atype.actt_key_ind = 'AUC'
        AND ac.accl_key_ind = 'CML'
),

/* Licence Issued status and date for licence (HI) */
HI AS
(
    SELECT
        A.LICN_SEQ_NBR,
        A.ACTI_STATUS_IND AS HI_Status,
        A.ACTI_STATUS_DATE AS HI_DATE
    FROM
        LRM_REPLICATION.activity_class ac,
        LRM_REPLICATION.activity_type atype,
        LRM_REPLICATION.activity A
    WHERE
        ac.accl_seq_nbr = atype.accl_seq_nbr
        AND ac.divi_div_nbr = atype.divi_div_nbr
        AND atype.actt_seq_nbr = a.actt_seq_nbr
        AND atype.actt_key_ind = 'HI'
        AND ac.accl_key_ind = 'CML'
),

/* Salvage - Any fire year */
SALVAGE_ANY_FIRE_YEAR AS
(
    SELECT
        cutb_seq_nbr,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(fire_year))) AS salvage_fire_years
    FROM
    (
        SELECT DISTINCT
            cutb_seq_nbr,
            SUBSTRING(activity_type, INSTR(activity_type, '2'), 4) AS fire_year
        FROM
            lrm_replication.v_block_activity_all
        WHERE
            activity_class = 'CSB'
            AND actt_key_ind LIKE 'SFIRE%'
            AND INSTR(activity_type, '2') > 0
    ) s
    GROUP BY cutb_seq_nbr
),

/* Salvage - 2021 Fire */
SALVAGE21 AS
(
    SELECT DISTINCT
        cutb_seq_nbr,
        activity_class,
        actt_key_ind,
        activity_type
    FROM
        LRM_REPLICATION.v_block_activity_all
    WHERE
        activity_class = 'CSB'
        AND actt_key_ind = 'SFIRE21'
),

/* Salvage - 2022 Fire */
SALVAGE22 AS
(
    SELECT DISTINCT
        cutb_seq_nbr,
        activity_class,
        actt_key_ind,
        activity_type
    FROM
        LRM_REPLICATION.v_block_activity_all
    WHERE
        activity_class = 'CSB'
        AND actt_key_ind = 'SFIRE22'
),

/* Salvage - 2023 Fire */
SALVAGE23 AS
(
    SELECT DISTINCT
        cutb_seq_nbr,
        activity_class,
        actt_key_ind,
        activity_type
    FROM
        LRM_REPLICATION.v_block_activity_all
    WHERE
        activity_class = 'CSB'
        AND actt_key_ind = 'SFIRE23'
),

/* Salvage - 2024 Fire */
SALVAGE24 AS
(
    SELECT DISTINCT
        cutb_seq_nbr,
        activity_class,
        actt_key_ind,
        activity_type
    FROM
        LRM_REPLICATION.v_block_activity_all
    WHERE
        activity_class = 'CSB'
        AND actt_key_ind = 'SFIRE24'
),

/* Salvage - 2025 Fire */
SALVAGE25 AS
(
    SELECT DISTINCT
        cutb_seq_nbr,
        activity_class,
        actt_key_ind,
        activity_type
    FROM
        LRM_REPLICATION.v_block_activity_all
    WHERE
        activity_class = 'CSB'
        AND actt_key_ind = 'SFIRE25'
),

/* Number of Licences Per Block */
BlockCount AS
(
    SELECT
        BL0.LICN_SEQ_NBR,
        COUNT(DISTINCT BL0.CUTB_SEQ_NBR) AS Count_Of_Blocks
    FROM
        LRM_REPLICATION.BLOCK_ALLOCATION BL0
    GROUP BY
        BL0.LICN_SEQ_NBR
)

SELECT DISTINCT
    CASE
        WHEN DIV.DIVI_SHORT_CODE IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN', 'TCC', 'TKA', 'TKO', 'TOC')
        THEN 'Interior'
        WHEN DIV.DIVI_SHORT_CODE IN ('TCH', 'TST', 'TSG')
        THEN 'Coast'
    END AS BUSINESS_AREA_REGION_CATEGORY,

    CASE
        WHEN DIV.DIVI_SHORT_CODE IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN')
        THEN 'North Interior'
        WHEN DIV.DIVI_SHORT_CODE IN ('TCC', 'TKA', 'TKO', 'TOC')
        THEN 'South Interior'
        WHEN DIV.DIVI_SHORT_CODE IN ('TCH', 'TST', 'TSG')
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

    DIV.DIVI_SHORT_CODE AS BUSINESS_AREA_CODE,
    CL.COLU_LOOKUP_DESC AS FieldTeam,
    MANU.MANU_ID,
    LICN.LICN_LICENCE_ID AS LICENCE_ID,
    TN.TENT_TENURE_ID AS TENURE_TYPE,
    PERM.PERM_PERMIT_ID,
    MARK.MARK_MARK_ID,
    CUTB.CUTB_BLOCK_ID AS BLOCK_ID,
    CUTB.CUTB_SYSTEM_ID AS UBI,
    B.BLOCK_NBR,
    B.SUOP_SUBOP_AREA_NAME AS SUB_OPERATING_AREA,
    LICN.LICN_LICENCE_STATE,
    CUTB.CUTB_BLOCK_STATE,

    CASE
        WHEN
            ACTB.Old_Growth_Strategy > COALESCE(LRCT.LATEST_OGS_REACTIVATED, DATE '1900-01-01')
            OR LDF.LATEST_DEF > COALESCE(ACTB.DEF_REACTIVATED, DATE '1900-01-01')
        THEN 'Y'
        ELSE 'N'
    END AS DEFERRED_AT_REPORT_DATE,

    CASE
        WHEN ACTB.Old_Growth_Strategy > COALESCE(LRCT.LATEST_OGS_REACTIVATED, DATE '1900-01-01')
        THEN 'Deferred-OGS'
        WHEN LDF.LATEST_DEF > COALESCE(ACTB.DEF_REACTIVATED, DATE '1900-01-01')
        THEN 'Deferred-Other'
        ELSE 'No Deferral'
    END AS INVENTORY_CATEGORY,

    CASE
        WHEN ACTB.Old_Growth_Strategy > COALESCE(LRCT.LATEST_OGS_REACTIVATED, DATE '1900-01-01')
        THEN 'Deferred-OGS'

        WHEN ACTB_S.DEF_First_Nations_Status = 'D'
            AND ACTB.DEF_First_Nations > COALESCE(ACTB.DEF_REACTIVATED, DATE '1900-01-01')
        THEN 'Deferred-FN'

        WHEN LDF.LATEST_DEF > COALESCE(ACTB.DEF_REACTIVATED, DATE '1900-01-01')
        THEN 'Deferred-Other'

        ELSE 'No Deferral'
    END AS INVENTORY_CATEGORY_NEW,

    BLAL.BLAL_MERCH_HA_AREA AS MERCH_AREA,
    BLAL.BLAL_CRUISE_M3_VOL AS CRUISE_VOLUME,
    BLAL.BLAL_RW_VOL AS RW_VOLUME,

    ACTB_S.RC_Status,
    ACTB.RC_DATE,
    YEAR(ADD_MONTHS(ACTB.RC_DATE, 9)) AS rc_fiscal,

    ACTB_S.dr_status,
    ACTB.dr_date,
    YEAR(ADD_MONTHS(ACTB.dr_date, 9)) AS dr_fiscal,

    ACTB_S.dvs_status,
    ACTB.dvs_date,
    YEAR(ADD_MONTHS(ACTB.dvs_date, 9)) AS dvs_fiscal,

    ACTB_S.dsc_status,
    ACTB.dsc_date,

    ACTB_S.dvc_status,
    ACTB.dvc_date,
    YEAR(ADD_MONTHS(ACTB.dvc_date, 9)) AS dvc_fiscal,

    DATEDIFF(LEAST(CURRENT_DATE(), DATE '${report_end_date}'), ACTB.DVS_DATE) AS Days_in_DIP,

    CASE
        WHEN DATEDIFF(LEAST(CURRENT_DATE(), DATE '${report_end_date}'), ACTB.DVS_DATE) < 1 THEN 'Less than One Day'
        WHEN DATEDIFF(LEAST(CURRENT_DATE(), DATE '${report_end_date}'), ACTB.DVS_DATE) < 181 THEN '1 to 180 Days'
        WHEN DATEDIFF(LEAST(CURRENT_DATE(), DATE '${report_end_date}'), ACTB.DVS_DATE) < 366 THEN '181 to 365 Days'
        WHEN DATEDIFF(LEAST(CURRENT_DATE(), DATE '${report_end_date}'), ACTB.DVS_DATE) < 546 THEN '366 to 545 Days'
        WHEN DATEDIFF(LEAST(CURRENT_DATE(), DATE '${report_end_date}'), ACTB.DVS_DATE) < 731 THEN '546 to 730 Days'
        ELSE 'Greater Than Two Years'
    END AS Days_in_DIP_Category,

    ACTB_S.WOFF_Status,
    ACTB.WOFF_DATE,
    YEAR(ADD_MONTHS(ACTB.WOFF_DATE, 9)) AS WOFF_Fiscal,

    AUC.AUC_Status,
    AUC.AUC_DATE,
    HI.HI_Status,
    HI.HI_DATE,

    ACTB_S.HVS_Status,
    ACTB.HVS_DATE,
    ACTB_S.HVC_Status,
    ACTB.HVC_DATE,

    ACTB_S.FG_Met_Status,
    ACTB.FG_DATE,

    ACTB_S.DEF_Change_of_Op_Plan_Status,
    ACTB.DEF_Change_of_Op_Plan,
    ACTB_S.DEF_First_Nations_Status,
    ACTB.DEF_First_Nations,
    ACTB_S.DEF_Loss_of_Access_Status,
    ACTB.DEF_Loss_of_Access,
    ACTB_S.DEF_Other_Status,
    ACTB.DEF_Other,
    ACTB_S.DEF_Planning_Constraint_Status,
    ACTB.DEF_Planning_Constraint,
    ACTB_S.DEF_Returned_to_BCTS_Status,
    ACTB.DEF_Returned_to_BCTS,
    ACTB_S.DEF_Stale_dated_Fieldwork_Status,
    ACTB.DEF_Stale_dated_Fieldwork,
    ACTB_S.DEF_Stakeholder_Issue_Status,
    ACTB.DEF_Stakeholder_Issue,
    ACTB_S.DEF_Environmental_Stewardship_Initiative_Status,
    ACTB.DEF_Environmental_Stewardship_Initiative,
    ACTB_S.DEF_REACTIVATED_Status,
    ACTB.DEF_REACTIVATED,
    ACTB_S.Old_Growth_Strategy_Status,
    ACTB.Old_Growth_Strategy,
    ACTB_S.OGS_Reactivated_Forest_Health_Status,
    ACTB.OGS_Reactivated_Forest_Health,
    ACTB_S.OGS_Reactivated_FN_Proceed_Status,
    ACTB.OGS_Reactivated_FN_Proceed,
    ACTB_S.OGS_Reactivated_Field_Verified_Status,
    ACTB.OGS_Reactivated_Field_Verified,
    ACTB_S.OGS_Reactivated_Minor_Status,
    ACTB.OGS_Reactivated_Minor,
    ACTB_S.OGS_Reactivated_Road_Status,
    ACTB.OGS_Reactivated_Road,
    ACTB_S.OGS_Reactivated_Re_Engineered_Status,
    ACTB.OGS_Reactivated_Re_Engineered,

    CASE
        WHEN POSITION('XXX' IN UPPER(CUTB.CUTB_BLOCK_ID)) + POSITION('ZZZ' IN UPPER(CUTB.CUTB_BLOCK_ID)) = 0
        THEN 'NO'
        ELSE 'YES'
    END AS xxx_zzz_flag,

    CASE
        WHEN BSH.CUTB_SEQ_NBR IS NULL
        THEN 'NO'
        ELSE 'YES'
    END AS spatial_flag,

    CASE
        WHEN ACTB_F.HAS_RC = 'Y'
        THEN 'YES'
        ELSE 'NO'
    END AS rc_flag,

    CASE
        WHEN ACTB_F.HAS_DR = 'Y'
        THEN 'YES'
        ELSE 'NO'
    END AS dr_flag,

    CASE
        WHEN ACTB_F.HAS_DVS = 'Y'
        THEN 'YES'
        ELSE 'NO'
    END AS dvs_flag,

    CASE
        WHEN ACTB_F.HAS_DSC = 'Y'
        THEN 'YES'
        ELSE 'NO'
    END AS dsc_flag,

    CASE
        WHEN ACTB_F.HAS_DVC = 'Y'
        THEN 'YES'
        ELSE 'NO'
    END AS dvc_flag,

    BlockCount.Count_Of_Blocks,

    CASE
        WHEN SALVAGE_ANY_FIRE_YEAR.cutb_seq_nbr IS NULL
        THEN 'N'
        ELSE 'Y'
    END AS salvage_any_fire_year,

    SALVAGE_ANY_FIRE_YEAR.salvage_fire_years,

    CASE
        WHEN salvage21.actt_key_ind IS NULL
        THEN NULL
        ELSE CONCAT(salvage21.activity_type, ' (', salvage21.activity_class, ' - ', salvage21.actt_key_ind, ')')
    END AS salvage_2021_fire,

    CASE
        WHEN salvage22.actt_key_ind IS NULL
        THEN NULL
        ELSE CONCAT(salvage22.activity_type, ' (', salvage22.activity_class, ' - ', salvage22.actt_key_ind, ')')
    END AS salvage_2022_fire,

    CASE
        WHEN salvage23.actt_key_ind IS NULL
        THEN NULL
        ELSE CONCAT(salvage23.activity_type, ' (', salvage23.activity_class, ' - ', salvage23.actt_key_ind, ')')
    END AS salvage_2023_fire,

    CASE
        WHEN salvage24.actt_key_ind IS NULL
        THEN NULL
        ELSE CONCAT(salvage24.activity_type, ' (', salvage24.activity_class, ' - ', salvage24.actt_key_ind, ')')
    END AS salvage_2024_fire,

    CASE
        WHEN salvage25.actt_key_ind IS NULL
        THEN NULL
        ELSE CONCAT(salvage25.activity_type, ' (', salvage25.activity_class, ' - ', salvage25.actt_key_ind, ')')
    END AS salvage_2025_fire,

    BLAL.LICN_SEQ_NBR,
    BLAL.MARK_SEQ_NBR,
    CUTB.CUTB_SEQ_NBR,

    OGC.ancient,
    OGC.remnant,
    OGC.big_treed,

    CASE
        WHEN OGC.ancient = 'Y' THEN BLAL.BLAL_CRUISE_M3_VOL
        ELSE 0
    END AS ANCIENT_VOLUME,

    CASE
        WHEN OGC.remnant = 'Y' THEN BLAL.BLAL_CRUISE_M3_VOL
        ELSE 0
    END AS REMNANT_VOLUME,

    CASE
        WHEN OGC.big_treed = 'Y' THEN BLAL.BLAL_CRUISE_M3_VOL
        ELSE 0
    END AS BIG_TREED_VOLUME,

    DATE '${report_end_date}' AS report_end_date,
    CURRENT_DATE AS report_run_date,
    CURRENT_TIMESTAMP AS report_run_timestamp

FROM
    LRM_REPLICATION.CUT_BLOCK CUTB
INNER JOIN LRM_REPLICATION.DIVISION DIV
    ON DIV.DIVI_DIV_NBR = CUTB.DIVI_DIV_NBR
LEFT JOIN LRM_REPLICATION.v_block B
    ON CUTB.cutb_seq_nbr = B.cutb_seq_nbr
    AND DIV.divi_div_nbr = B.divi_div_nbr
LEFT JOIN LRM_REPLICATION.BLOCK_ALLOCATION BLAL
    ON CUTB.CUTB_SEQ_NBR = BLAL.CUTB_SEQ_NBR
LEFT JOIN LRM_REPLICATION.LICENCE LICN
    ON LICN.licn_seq_nbr = B.licn_seq_nbr
    AND BLAL.LICN_SEQ_NBR = LICN.LICN_SEQ_NBR
LEFT JOIN LRM_REPLICATION.MANAGEMENT_UNIT MANU
    ON BLAL.MANU_SEQ_NBR = MANU.MANU_SEQ_NBR
LEFT JOIN LRM_REPLICATION.DIVISION_CODE_LOOKUP dcl
    ON LICN.LICN_FIELD_TEAM_ID = dcl.COLU_LOOKUP_ID
    AND LICN.DIVI_DIV_NBR = dcl.DIVI_DIV_NBR
LEFT JOIN LRM_REPLICATION.CODE_LOOKUP cl
    ON dcl.COLU_LOOKUP_TYPE = cl.COLU_LOOKUP_TYPE
    AND dcl.COLU_LOOKUP_ID = cl.COLU_LOOKUP_ID
LEFT JOIN LRM_REPLICATION.TENURE_TYPE tn
    ON LICN.TENT_SEQ_NBR = tn.TENT_SEQ_NBR
LEFT JOIN LRM_REPLICATION.CUT_PERMIT PERM
    ON BLAL.PERM_SEQ_NBR = PERM.PERM_SEQ_NBR
LEFT JOIN LRM_REPLICATION.MARK MARK
    ON BLAL.MARK_SEQ_NBR = MARK.MARK_SEQ_NBR
LEFT JOIN LRM_REPLICATION.CUT_BLOCK_SHAPE BSH
    ON BLAL.CUTB_SEQ_NBR = BSH.CUTB_SEQ_NBR
LEFT JOIN ACTB
    ON BLAL.CUTB_SEQ_NBR = ACTB.CUTB_SEQ_NBR
LEFT JOIN ACTB_S
    ON BLAL.CUTB_SEQ_NBR = ACTB_S.CUTB_SEQ_NBR
LEFT JOIN LDF
    ON BLAL.CUTB_SEQ_NBR = LDF.CUTB_SEQ_NBR
LEFT JOIN LRCT
    ON BLAL.CUTB_SEQ_NBR = LRCT.CUTB_SEQ_NBR
LEFT JOIN ACTB_F
    ON BLAL.CUTB_SEQ_NBR = ACTB_F.CUTB_SEQ_NBR
LEFT JOIN AUC
    ON BLAL.LICN_SEQ_NBR = AUC.LICN_SEQ_NBR
LEFT JOIN HI
    ON BLAL.LICN_SEQ_NBR = HI.LICN_SEQ_NBR
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
LEFT JOIN salvage25
    ON B.cutb_seq_nbr = salvage25.cutb_seq_nbr
LEFT JOIN BlockCount
    ON BLAL.LICN_SEQ_NBR = BlockCount.LICN_SEQ_NBR
LEFT JOIN bcts_staging.old_growth_tap_deferral_categories OGC
    ON CUTB.CUTB_SYSTEM_ID = OGC.UBI

WHERE
    COALESCE(TN.TENT_TENURE_ID, ' ') <> 'B07'
    AND (
        ACTB_S.DVS_Status = 'D'
        AND ACTB.DVS_DATE <= DATE '${report_end_date}'
    )
    AND (
        COALESCE(ACTB_S.DEL_Status, ' ') <> 'D'
        OR ACTB.DEL_DATE IS NULL
        OR (
            ACTB_S.DEL_Status = 'D'
            AND ACTB.DEL_DATE > DATE '${report_end_date}'
        )
    )
    AND (
        COALESCE(ACTB_S.DVC_Status, ' ') <> 'D'
        OR ACTB.DVC_DATE IS NULL
        OR (
            ACTB_S.DVC_Status = 'D'
            AND ACTB.DVC_DATE > DATE '${report_end_date}'
        )
    )
    AND (
        COALESCE(ACTB_S.WOFF_Status, ' ') <> 'D'
        OR ACTB.WOFF_DATE IS NULL
        OR (
            ACTB_S.WOFF_Status = 'D'
            AND ACTB.WOFF_DATE > DATE '${report_end_date}'
        )
    )
    AND (
        COALESCE(AUC.AUC_Status, ' ') <> 'D'
        OR AUC.AUC_DATE IS NULL
        OR (
            AUC.AUC_Status = 'D'
            AND AUC.AUC_DATE > DATE '${report_end_date}'
        )
    )
    AND (
        COALESCE(HI.HI_Status, ' ') <> 'D'
        OR HI.HI_DATE IS NULL
        OR (
            HI.HI_Status = 'D'
            AND HI.HI_DATE > DATE '${report_end_date}'
        )
    )

ORDER BY
    business_area_region,
    business_area,
    fieldteam,
    manu_id,
    licence_id;


-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
DROP TABLE IF EXISTS BCTS_STAGING.timber_inventory_development_in_progress;
CREATE TABLE BCTS_STAGING.timber_inventory_development_in_progress
AS SELECT * 
FROM BCTS_STAGING.timber_inventory_development_in_progress_hist
WHERE report_end_date = (
SELECT MAX(report_end_date)
FROM BCTS_STAGING.timber_inventory_development_in_progress_hist
);

DROP TABLE IF EXISTS BCTS_REPORTING.timber_inventory_development_in_progress_hist;
CREATE TABLE BCTS_REPORTING.timber_inventory_development_in_progress_hist
AS SELECT * 
FROM BCTS_STAGING.timber_inventory_development_in_progress_hist;

DROP TABLE IF EXISTS BCTS_REPORTING.timber_inventory_development_in_progress;
CREATE TABLE BCTS_REPORTING.timber_inventory_development_in_progress
AS SELECT * 
FROM BCTS_STAGING.timber_inventory_development_in_progress;