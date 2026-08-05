CREATE TABLE IF NOT EXISTS bcts_staging.timber_inventory_ready_to_develop_hist
(
    id BIGINT IDENTITY(1,1) NOT NULL,

    business_area_region_category VARCHAR(MAX),
    business_area_region VARCHAR(MAX),
    business_area VARCHAR(MAX),
    business_area_code VARCHAR(15),
    field_team VARCHAR(150),
    nav_name VARCHAR(60),
    operatingarea VARCHAR(30),
    location VARCHAR(800),
    tenure VARCHAR(40),
    licence_id VARCHAR(15),
    licence_state VARCHAR(20),
    permit_id VARCHAR(40),
    block_id VARCHAR(20),
    ubi VARCHAR(15),
    block_state VARCHAR(20),
    deferred VARCHAR(MAX),
    inventory_category VARCHAR(MAX),

    cruise_vol DECIMAL(15,6),
    rw_vol DECIMAL(15,6),

    rc_status VARCHAR(MAX),
    rc_date DATE,

    dr_status VARCHAR(MAX),
    dr_date DATE,
    dr_fiscal DECIMAL(18,0),
    dr_quarter DECIMAL(18,0),
    dr_category VARCHAR(MAX),

    dvs_status VARCHAR(MAX),
    dvs_date DATE,

    dvc_status VARCHAR(MAX),
    dvc_date DATE,

    dvs_fiscal DECIMAL(18,0),
    dvs_quarter DECIMAL(18,0),

    def_change_of_op_plan_status VARCHAR(MAX),
    def_change_of_op_plan DATE,

    def_first_nations_status VARCHAR(MAX),
    def_first_nations DATE,

    def_loss_of_access_status VARCHAR(MAX),
    def_loss_of_access DATE,

    def_other_status VARCHAR(MAX),
    def_other DATE,

    def_planning_constraint_status VARCHAR(MAX),
    def_planning_constraint DATE,

    def_returned_to_bcts_status VARCHAR(MAX),
    def_returned_to_bcts DATE,

    def_stale_dated_fieldwork_status VARCHAR(MAX),
    def_stale_dated_fieldwork DATE,

    def_stakeholder_issue_status VARCHAR(MAX),
    def_stakeholder_issue DATE,

    def_environmental_stewardship_initiative_status VARCHAR(MAX),
    def_environmental_stewardship_initiative DATE,

    def_reactivated_status VARCHAR(MAX),
    def_reactivated DATE,

    old_growth_strategy_status VARCHAR(MAX),
    old_growth_strategy DATE,

    ogs_reactivated_forest_health_status VARCHAR(MAX),
    ogs_reactivated_forest_health DATE,

    ogs_reactivated_fn_proceed_status VARCHAR(MAX),
    ogs_reactivated_fn_proceed DATE,

    ogs_reactivated_field_verified_status VARCHAR(MAX),
    ogs_reactivated_field_verified DATE,

    ogs_reactivated_minor_status VARCHAR(MAX),
    ogs_reactivated_minor DATE,

    ogs_reactivated_road_status VARCHAR(MAX),
    ogs_reactivated_road DATE,

    ogs_reactivated_re_engineered_status VARCHAR(MAX),
    ogs_reactivated_re_engineered DATE,

    spatial_flag VARCHAR(3),
    cutb_seq_nbr BIGINT,

    report_end_date DATE,
    report_run_date DATE
        DEFAULT CAST(
            SYSDATETIMEOFFSET() AT TIME ZONE 'Pacific Standard Time'
            AS DATE
        ),

    report_run_timestamp DATETIME2
        DEFAULT CAST(
            SYSDATETIMEOFFSET() AT TIME ZONE 'Pacific Standard Time'
            AS DATETIME2
        )
);


-- Report exists check is done on bcts_reporting.timber_inventory_ready_to_develop table 
-- If report exists in bcts_staging.timber_inventory_ready_to_develop_hist, clear the staging table before inserting new records 
delete from bcts_staging.timber_inventory_ready_to_develop_hist 
where report_end_date = '${report_end_date}';

-- Populate staging table 
WITH ACTB AS
(
    SELECT
        cutb_seq_nbr,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DEL' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DEL_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DSC' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DSC_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DVC' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DVC_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DVS' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DVS_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'FG' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS FG_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'HVC' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS HVC_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'HVS' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS HVS_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RC' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS RC_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DR' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DR_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'WO' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS WOFF_DATE,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DCP' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DEF_Change_of_Op_Plan,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DFN' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DEF_First_Nations,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DLA' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DEF_Loss_of_Access,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DOR' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DEF_Other,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DPC' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DEF_Planning_Constraint,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DRB' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DEF_Returned_to_BCTS,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DSD' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DEF_Stale_dated_Fieldwork,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DSI' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DEF_Stakeholder_Issue,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DESI' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DEF_Environmental_Stewardship_Initiative,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DRD' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS DEF_REACTIVATED,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'DOG' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS Old_Growth_Strategy,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RFH' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS OGS_Reactivated_Forest_Health,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RFN' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS OGS_Reactivated_FN_Proceed,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RFV' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS OGS_Reactivated_Field_Verified,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RMN' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS OGS_Reactivated_Minor,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RRD' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS OGS_Reactivated_Road,
        CAST(MAX(CASE WHEN ACTT_KEY_IND = 'RRE' THEN ACTI_STATUS_DATE ELSE NULL END) AS date) AS OGS_Reactivated_Re_Engineered
    FROM
    (
        SELECT
            a.cutb_seq_nbr,
            atype.actt_key_ind,
            a.acti_status_date
        FROM BCTS_STAGING.FOREST_activity_class ac
        INNER JOIN BCTS_STAGING.FOREST_activity_type atype
            ON ac.accl_seq_nbr = atype.accl_seq_nbr
            AND ac.divi_div_nbr = atype.divi_div_nbr
        INNER JOIN BCTS_STAGING.FOREST_activity a
            ON atype.actt_seq_nbr = a.actt_seq_nbr
            AND a.plan_seq_nbr IS NULL
        WHERE
            (
                atype.actt_key_ind IN ('DSC', 'DVC', 'DVS', 'FG', 'HVC', 'HVS', 'RC', 'DR', 'WO')
                AND ac.accl_key_ind = 'CMB'
            )
            OR
            (
                atype.actt_key_ind IN
                (
                    'DEL',
                    'DCP', 'DFN', 'DLA', 'DOG', 'DOR', 'DPC', 'DRB', 'DSD', 'DSI',
                    'DESI', 'DRD', 'RFH', 'RFN', 'RFV', 'RMN', 'RRD', 'RRE'
                )
                AND ac.accl_key_ind = 'CSB'
            )
    ) TEMP
    GROUP BY cutb_seq_nbr
),
ACTB_S AS
(
    SELECT
        cutb_seq_nbr,
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
        FROM BCTS_STAGING.FOREST_activity_class ac
        INNER JOIN BCTS_STAGING.FOREST_activity_type atype
            ON ac.accl_seq_nbr = atype.accl_seq_nbr
            AND ac.divi_div_nbr = atype.divi_div_nbr
        INNER JOIN BCTS_STAGING.FOREST_activity a
            ON atype.actt_seq_nbr = a.actt_seq_nbr
            AND
            (
                (
                    atype.actt_key_ind IN ('DSC', 'DVC', 'DVS', 'FG', 'HVC', 'HVS', 'RC', 'DR', 'WO')
                    AND ac.accl_key_ind = 'CMB'
                )
                OR
                (
                    atype.actt_key_ind IN
                    (
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
LDF AS
(
    SELECT
        A2.CUTB_SEQ_NBR,
        MAX(A2.ACTIVITY_DATE) AS LATEST_DEF
    FROM LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL A2
    WHERE
        A2.ACTIVITY_CLASS = 'CSB'
        AND A2.ACTT_KEY_IND IN ('DCP', 'DFN', 'DLA', 'DOR', 'DPC', 'DRB', 'DSD', 'DSI', 'DESI')
        AND A2.ACTI_STATUS_IND = 'D'
        AND A2.ACTIVITY_DATE <= CAST('${report_end_date}' AS date)
    GROUP BY A2.CUTB_SEQ_NBR
),
LRCT AS
(
    SELECT
        A4.CUTB_SEQ_NBR,
        MAX(A4.ACTIVITY_DATE) AS LATEST_OGS_REACTIVATED
    FROM LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL A4
    WHERE
        A4.ACTIVITY_CLASS = 'CSB'
        AND A4.ACTT_KEY_IND IN ('RFH', 'RFN', 'RFV', 'RMN', 'RRD', 'RRE')
        AND A4.ACTI_STATUS_IND = 'D'
        AND A4.ACTIVITY_DATE <= CAST('${report_end_date}' AS date)
    GROUP BY
        A4.CUTB_SEQ_NBR,
        A4.ACTI_STATUS_IND
),
EXL AS
(
    SELECT DISTINCT
        LICN_SEQ_NBR
    FROM LRM_REPLICATION.V_LICENCE_ACTIVITY_ALL
    WHERE
        ACTIVITY_CLASS = 'CML'
        AND ACTT_KEY_IND IN ('HI', 'HC', 'HX', 'HS')
        AND ACTI_STATUS_IND = 'D'
)

INSERT INTO bcts_staging.timber_inventory_ready_to_develop_hist
(
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
    deferred,
    inventory_category,
    cruise_vol,
    rw_vol,
    rc_status,
    rc_date,
    dr_status,
    dr_date,
    dr_fiscal,
    dr_quarter,
    dr_category,
    dvs_status,
    dvs_date,
    dvc_status,
    dvc_date,
    dvs_fiscal,
    dvs_quarter,
    def_change_of_op_plan_status,
    def_change_of_op_plan,
    def_first_nations_status,
    def_first_nations,
    def_loss_of_access_status,
    def_loss_of_access,
    def_other_status,
    def_other,
    def_planning_constraint_status,
    def_planning_constraint,
    def_returned_to_bcts_status,
    def_returned_to_bcts,
    def_stale_dated_fieldwork_status,
    def_stale_dated_fieldwork,
    def_stakeholder_issue_status,
    def_stakeholder_issue,
    def_environmental_stewardship_initiative_status,
    def_environmental_stewardship_initiative,
    def_reactivated_status,
    def_reactivated,
    old_growth_strategy_status,
    old_growth_strategy,
    ogs_reactivated_forest_health_status,
    ogs_reactivated_forest_health,
    ogs_reactivated_fn_proceed_status,
    ogs_reactivated_fn_proceed,
    ogs_reactivated_field_verified_status,
    ogs_reactivated_field_verified,
    ogs_reactivated_minor_status,
    ogs_reactivated_minor,
    ogs_reactivated_road_status,
    ogs_reactivated_road,
    ogs_reactivated_re_engineered_status,
    ogs_reactivated_re_engineered,
    spatial_flag,
    cutb_seq_nbr,
    report_end_date
)
SELECT DISTINCT
    CASE
        WHEN BLOCK.TSO_CODE IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN', 'TCC', 'TKA', 'TKO', 'TOC') THEN 'Interior'
        WHEN BLOCK.TSO_CODE IN ('TCH', 'TST', 'TSG') THEN 'Coast'
    END AS business_area_region_category,

    CASE
        WHEN BLOCK.TSO_CODE IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN') THEN 'North Interior'
        WHEN BLOCK.TSO_CODE IN ('TCC', 'TKA', 'TKO', 'TOC') THEN 'South Interior'
        WHEN BLOCK.TSO_CODE IN ('TCH', 'TST', 'TSG') THEN 'Coast'
    END AS business_area_region,

    CONCAT(
        CASE
            WHEN BLOCK.TSO_NAME = 'Seaward' THEN 'Seaward/Tlasta'
            ELSE BLOCK.TSO_NAME
        END,
        ' (',
        BLOCK.TSO_CODE,
        ')'
    ) AS business_area,

    BLOCK.TSO_CODE AS business_area_code,
    LICENCE.FIELD_TEAM,
    LICENCE.NAV_NAME,
    BLOCK.OPAR_OPERATING_AREA_NAME AS operatingarea,
    BLOCK.CUTB_LOCATION AS location,
    LICENCE.TENURE,
    LICENCE.LICENCE_ID,
    LICENCE.LICN_LICENCE_STATE AS licence_state,
    BLOCK.PERMIT_ID,
    BLOCK.BLOCK_ID,
    BLOCK.UBI,
    BLOCK.CUTB_BLOCK_STATE AS block_state,

    CASE
        WHEN
            ACTB.Old_Growth_Strategy > COALESCE(LRCT.LATEST_OGS_REACTIVATED, CONVERT(date, '19000101'))
            OR LDF.LATEST_DEF > COALESCE(ACTB.DEF_REACTIVATED, CONVERT(date, '19000101'))
        THEN 'Y'
        ELSE 'N'
    END AS deferred,

    CASE
        WHEN ACTB.Old_Growth_Strategy > COALESCE(LRCT.LATEST_OGS_REACTIVATED, CONVERT(date, '19000101')) THEN 'Deferred-OGS'
        WHEN LDF.LATEST_DEF > COALESCE(ACTB.DEF_REACTIVATED, CONVERT(date, '19000101')) THEN 'Deferred-Other'
        ELSE 'No Deferral'
    END AS inventory_category,

    BLOCK.CRUISE_VOL,
    BLOCK.BLAL_RW_VOL AS rw_vol,
    ACTB_S.RC_Status,
    ACTB.RC_DATE,
    ACTB_S.DR_Status,
    ACTB.DR_DATE,

    YEAR(DATEADD(MONTH, 9, ACTB.DR_DATE)) AS dr_fiscal,
    CEILING(MONTH(DATEADD(MONTH, -3, ACTB.DR_DATE)) / 3.0) AS dr_quarter,

    CASE
        WHEN YEAR(DATEADD(MONTH, 9, CAST(GETDATE() AS date))) - YEAR(DATEADD(MONTH, 9, ACTB.DR_DATE)) > 5
        THEN CONCAT(
            'Before ',
            RIGHT(CAST(YEAR(DATEADD(MONTH, -63, CAST(GETDATE() AS date))) AS varchar(4)), 2),
            '/',
            RIGHT(CAST(YEAR(DATEADD(MONTH, -51, CAST(GETDATE() AS date))) AS varchar(4)), 2)
        )
        ELSE CONCAT(
            RIGHT(CAST(YEAR(DATEADD(MONTH, -3, ACTB.DR_DATE)) AS varchar(4)), 2),
            '/',
            RIGHT(CAST(YEAR(DATEADD(MONTH, 9, ACTB.DR_DATE)) AS varchar(4)), 2)
        )
    END AS dr_category,

    ACTB_S.DVS_Status,
    ACTB.DVS_DATE,
    ACTB_S.DVC_Status,
    ACTB.DVC_DATE,

    YEAR(DATEADD(MONTH, 9, ACTB.DVS_DATE)) AS dvs_fiscal,
    CEILING(MONTH(DATEADD(MONTH, -3, ACTB.DVS_DATE)) / 3.0) AS dvs_quarter,

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
    BS.SPATIAL_FLAG,
    BLOCK.CUTB_SEQ_NBR,
    CAST('${report_end_date}' AS date) AS report_end_date

FROM LRM_REPLICATION.V_BLOCK BLOCK
INNER JOIN ACTB_S
    ON BLOCK.CUTB_SEQ_NBR = ACTB_S.CUTB_SEQ_NBR
INNER JOIN ACTB
    ON BLOCK.CUTB_SEQ_NBR = ACTB.CUTB_SEQ_NBR
LEFT JOIN LRM_REPLICATION.V_BLOCK_SPATIAL BS
    ON BLOCK.CUTB_SEQ_NBR = BS.CUTB_SEQ_NBR
LEFT JOIN LDF
    ON BLOCK.CUTB_SEQ_NBR = LDF.CUTB_SEQ_NBR
LEFT JOIN LRCT
    ON BLOCK.CUTB_SEQ_NBR = LRCT.CUTB_SEQ_NBR
LEFT JOIN LRM_REPLICATION.V_LICENCE LICENCE
    ON BLOCK.LICN_SEQ_NBR = LICENCE.LICN_SEQ_NBR
LEFT JOIN EXL
    ON LICENCE.LICN_SEQ_NBR = EXL.LICN_SEQ_NBR

WHERE 1 = 1
    AND ACTB.DR_DATE <= CAST('${report_end_date}' AS date)
    AND ACTB_S.DR_STATUS = 'D'
    AND COALESCE(ACTB_S.DVC_STATUS, ' ') <> 'D'
    AND COALESCE(ACTB_S.DVS_STATUS, ' ') <> 'D'
    AND COALESCE(ACTB_S.DEL_STATUS, ' ') <> 'D'
    AND COALESCE(ACTB_S.WOFF_Status, ' ') <> 'D'
    AND EXL.LICN_SEQ_NBR IS NULL;

-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
DROP TABLE IF EXISTS BCTS_STAGING.timber_inventory_ready_to_develop;
CREATE TABLE BCTS_STAGING.timber_inventory_ready_to_develop
AS SELECT * 
FROM BCTS_STAGING.timber_inventory_ready_to_develop_hist
WHERE report_end_date = (
SELECT MAX(report_end_date)
FROM BCTS_STAGING.timber_inventory_ready_to_develop_hist
);

DROP TABLE IF EXISTS BCTS_REPORTING.timber_inventory_ready_to_develop_hist;
CREATE TABLE BCTS_REPORTING.timber_inventory_ready_to_develop_hist
AS SELECT * 
FROM BCTS_STAGING.timber_inventory_ready_to_develop_hist;

DROP TABLE IF EXISTS BCTS_REPORTING.timber_inventory_ready_to_develop;
CREATE TABLE BCTS_REPORTING.timber_inventory_ready_to_develop
AS SELECT * 
FROM BCTS_STAGING.timber_inventory_ready_to_develop;