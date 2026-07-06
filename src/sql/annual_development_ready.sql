    -- DDL and DML for Annual Development Ready report
    CREATE TABLE IF NOT EXISTS  bcts_staging.annual_development_ready_hist
    (
        id BIGINT,

        business_area_region_category STRING,
        business_area_region STRING,
        business_area STRING,
        business_area_code STRING,
        field_team STRING,
        manu_id STRING,
        licence STRING,
        tenure STRING,
        perm STRING,
        mark STRING,
        block_id STRING,
        ubi STRING,
        block_state STRING,

        cruise_volume DECIMAL(15,6),
        rw_volume DECIMAL(15,6),

        dr_done DATE,
        rc_status STRING,
        rc_status_date DATE,
        dvs_status STRING,
        dvs_status_date DATE,
        dvc_status STRING,
        dvc_status_date DATE,

        licn_seq_nbr DECIMAL(15,0),
        cutb_seq_nbr BIGINT,

        report_start_date DATE,
        report_end_date DATE,
        report_run_timestamp TIMESTAMP
    )
    USING DELTA;
    
    -- Report exists check is done on bcts_reporting.annual_development_ready table 
    -- If report exists in bcts_staging.annual_development_ready_hist, clear the staging table before inserting new records 
    delete from bcts_staging.annual_development_ready_hist 
    where  report_start_date = '${report_start_date}'
    and report_end_date = '${report_end_date}';
    
    INSERT INTO bcts_staging.annual_development_ready_hist(
        business_area_region_category,
        business_area_region,
        business_area,
        business_area_code,
        field_team,
        manu_id,
        licence,
        tenure,
        perm,
        mark,
        block_id,
        ubi,
        block_state,
        cruise_volume,
        rw_volume,
        dr_done,
        rc_status,
        rc_status_date,
        dvs_status,
        dvs_status_date,
        dvc_status,
        dvc_status_date,
        licn_seq_nbr,
        cutb_seq_nbr,
        report_start_date,
        report_end_date,
        report_run_timestamp
    )

    WITH ACTB AS
    /* Block activity status and date ACTB */
    (
            SELECT
            A.CUTB_SEQ_NBR,
            MAX(CASE WHEN T.ACTT_KEY_IND = 'DR' THEN A.ACTI_STATUS_IND ELSE NULL END) AS DR_STATUS,
            MAX(CASE WHEN T.ACTT_KEY_IND = 'DR' THEN DATE(A.ACTI_STATUS_DATE) ELSE NULL END) AS DR_STATUS_DATE,
            MAX(CASE WHEN T.ACTT_KEY_IND = 'RC' THEN A.ACTI_STATUS_IND ELSE NULL END) AS RC_STATUS,
            MAX(CASE WHEN T.ACTT_KEY_IND = 'RC' THEN DATE(A.ACTI_STATUS_DATE) ELSE NULL END) AS RC_STATUS_DATE,
            MAX(CASE WHEN T.ACTT_KEY_IND = 'DVS' THEN A.ACTI_STATUS_IND ELSE NULL END) AS DVS_STATUS,
            MAX(CASE WHEN T.ACTT_KEY_IND = 'DVS' THEN DATE(A.ACTI_STATUS_DATE) ELSE NULL END) AS DVS_STATUS_DATE,
            MAX(CASE WHEN T.ACTT_KEY_IND = 'DVC' THEN A.ACTI_STATUS_IND ELSE NULL END) AS DVC_STATUS,
            MAX(CASE WHEN T.ACTT_KEY_IND = 'DVC' THEN DATE(A.ACTI_STATUS_DATE) ELSE NULL END) AS DVC_STATUS_DATE
        FROM
            LRM_REPLICATION.ACTIVITY_CLASS C
            INNER JOIN lrm_replication.ACTIVITY_TYPE T
            ON C.ACCL_SEQ_NBR = T.ACCL_SEQ_NBR
            AND C.ACCL_DESCRIPTION = 'CMB'
            AND T.ACTT_KEY_IND IN ('RC', 'DR', 'DVS', 'DVC')
            INNER JOIN lrm_replication.ACTIVITY A
            ON T.ACTT_SEQ_NBR = A.ACTT_SEQ_NBR
        GROUP BY
            A.CUTB_SEQ_NBR
    ) 

    SELECT
        case
            when
                D.DIVI_SHORT_CODE in ('TBA', 'TPL', 'TPG', 'TSK', 'TSN', 'TCC', 'TKA', 'TKO', 'TOC')
            then
                'Interior'
            when
                D.DIVI_SHORT_CODE in ('TCH', 'TST', 'TSG')
            then
                'Coast'
            end as BUSINESS_AREA_REGION_CATEGORY,
        case
            when
                D.DIVI_SHORT_CODE in ('TBA', 'TPL', 'TPG', 'TSK', 'TSN')
            then
                'North Interior'
            when
                D.DIVI_SHORT_CODE in ('TCC', 'TKA', 'TKO', 'TOC')
            then
                'South Interior'
            when
                D.DIVI_SHORT_CODE in ('TCH', 'TST', 'TSG')
            then
                'Coast'
            end as BUSINESS_AREA_REGION,
        CASE 
            WHEN D.DIVI_DIVISION_NAME = 'Seaward' THEN 'Seaward-Tlasta' 
            ELSE D.DIVI_DIVISION_NAME 
        END || ' (' || D.DIVI_SHORT_CODE || ')' AS BUSINESS_AREA,
        D.DIVI_SHORT_CODE AS BUSINESS_AREA_CODE,
        LKF.COLU_LOOKUP_DESC AS FIELD_TEAM,
        MU.MANU_ID,
        L.LICN_LICENCE_ID AS LICENCE,
        TN.TENT_TENURE_ID AS TENURE,
        CP.PERM_PERMIT_ID AS PERM,
        M.MARK_MARK_ID AS MARK,
        CB.CUTB_BLOCK_ID AS BLOCK_ID,
        CB.CUTB_SYSTEM_ID AS UBI,
        CB.CUTB_BLOCK_STATE AS BLOCK_STATE,
        BA.BLAL_CRUISE_M3_VOL AS CRUISE_VOLUME,
        BA.BLAL_RW_VOL AS RW_VOLUME,
        ACTB.DR_STATUS_DATE AS DR_DONE,
        ACTB.RC_STATUS,
        ACTB.RC_STATUS_DATE,
        ACTB.DVS_STATUS,
        ACTB.DVS_STATUS_DATE,
        ACTB.DVC_STATUS,
        ACTB.DVC_STATUS_DATE,
        L.LICN_SEQ_NBR,
        CB.CUTB_SEQ_NBR,
        '${report_start_date}' AS report_start_date,
        '${report_end_date}' AS report_end_date,
        CURRENT_TIMESTAMP AS report_run_timestamp
    FROM
        lrm_replication.DIVISION D
        INNER JOIN lrm_replication.BLOCK_ALLOCATION BA
        ON D.DIVI_DIV_NBR = BA.DIVI_DIV_NBR
        INNER JOIN ACTB
        ON BA.cutb_seq_nbr = ACTB.cutb_seq_nbr
        AND ACTB.DR_STATUS = 'D'  -- Development Ready (DR) status is Done (D)
        AND ACTB.DR_STATUS_DATE
            BETWEEN '${report_start_date}'  -- Date: beginning of current fiscal
            AND '${report_end_date}'  -- Date: end of reporting period
        INNER JOIN lrm_replication.CUT_BLOCK CB
        ON BA.CUTB_SEQ_NBR = CB.CUTB_SEQ_NBR
        LEFT JOIN lrm_replication.MANAGEMENT_UNIT MU
        ON BA.MANU_SEQ_NBR = MU.MANU_SEQ_NBR 
        LEFT JOIN lrm_replication.LICENCE L
        ON BA.LICN_SEQ_NBR = L.LICN_SEQ_NBR
        LEFT JOIN lrm_replication.TENURE_TYPE TN
        ON L.TENT_SEQ_NBR = TN.TENT_SEQ_NBR
        LEFT JOIN lrm_replication.CODE_LOOKUP LKF
        ON L.LICN_FIELD_TEAM_ID = LKF.COLU_LOOKUP_ID
        AND LKF.COLU_LOOKUP_TYPE = 'FDTM'  -- Licence Field Team (FDTM)
        LEFT JOIN lrm_replication.MARK M
        ON BA.MARK_SEQ_NBR = M.MARK_SEQ_NBR
        LEFT JOIN lrm_replication.CUT_PERMIT CP
        ON BA.PERM_SEQ_NBR = CP.PERM_SEQ_NBR;


-- Publish data to reporting tables
DROP TABLE IF EXISTS BCTS_STAGING.annual_development_ready;
CREATE TABLE BCTS_STAGING.annual_development_ready
AS SELECT * 
FROM BCTS_STAGING.annual_development_ready_hist
WHERE report_end_date = (
    SELECT MAX(report_end_date)
    FROM BCTS_STAGING.annual_development_ready_hist
);

DROP TABLE IF EXISTS BCTS_REPORTING.annual_development_ready_hist;
CREATE TABLE BCTS_REPORTING.annual_development_ready_hist
AS SELECT * 
FROM BCTS_STAGING.annual_development_ready_hist;

DROP TABLE IF EXISTS BCTS_REPORTING.annual_development_ready;
CREATE TABLE BCTS_REPORTING.annual_development_ready
AS SELECT * 
FROM BCTS_STAGING.annual_development_ready;