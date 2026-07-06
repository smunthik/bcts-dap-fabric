CREATE TABLE IF NOT EXISTS bcts_staging.annual_developed_volume_hist (
    id BIGINT,

    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    business_area_code STRING,

    field_team STRING,
    manu_id STRING,
    licence STRING,
    file_type STRING,

    agreement_type_code STRING,
    agreement_type STRING,
    permit STRING,
    mark STRING,
    block STRING,
    ubi STRING,
    block_state STRING,

    cruise_volume DECIMAL(15,6),
    rw_volume DECIMAL(15,6),

    rc_done DATE,
    rc_done_fiscal DECIMAL(10,0),

    dr_done DATE,
    dr_done_fiscal DECIMAL(10,0),

    dvs_done DATE,
    dvs_done_fiscal DECIMAL(10,0),

    dvc_done DATE,
    dvc_done_fiscal DECIMAL(10,0),

    cutb_seq_nbr BIGINT,
    fiscal_year_start_date DATE,
    report_start_date DATE,
    report_end_date DATE,
    report_run_timestamp TIMESTAMP
)
USING DELTA;


-- Report exists check is done on bcts_reporting.annual_developed_volume table 
-- If report exists in bcts_staging.annual_developed_volume_hist, clear the staging table before inserting new records 
delete from bcts_staging.annual_developed_volume_hist 
where  report_start_date = '${report_start_date}'
and report_end_date = '${report_end_date}';

-- Populate staging table for annual developed volume history
INSERT INTO bcts_staging.annual_developed_volume_hist (
    business_area_region_category,
    business_area_region,
    business_area,
    business_area_code,
    field_team,
    manu_id,
    licence,
    file_type,
    agreement_type_code,
    agreement_type,
    permit,
    mark,
    block,
    ubi,
    block_state,
    cruise_volume,
    rw_volume,
    rc_done,
    rc_done_fiscal,
    dr_done,
    dr_done_fiscal,
    dvs_done,
    dvs_done_fiscal,
    dvc_done,
    dvc_done_fiscal,
    cutb_seq_nbr,
    fiscal_year_start_date,
    report_start_date,
    report_end_date,
    report_run_timestamp
)
WITH actb AS (
    SELECT
        a.cutb_seq_nbr,
        MAX(CASE WHEN t.actt_key_ind = 'RC'  THEN CAST(date_trunc('DAY', a.acti_status_date) AS DATE) END)  AS rc_done,
        MAX(CASE WHEN t.actt_key_ind = 'DR'  THEN CAST(date_trunc('DAY', a.acti_status_date) AS DATE) END)  AS dr_done,
        MAX(CASE WHEN t.actt_key_ind = 'DVS' THEN CAST(date_trunc('DAY', a.acti_status_date) AS DATE) END)  AS dvs_done,
        MAX(CASE WHEN t.actt_key_ind = 'DVC' THEN CAST(date_trunc('DAY', a.acti_status_date) AS DATE) END)  AS dvc_done
    FROM lrm_replication.activity a
    INNER JOIN lrm_replication.activity_type t
        ON t.actt_seq_nbr = a.actt_seq_nbr
    INNER JOIN lrm_replication.activity_class c
        ON c.accl_seq_nbr = t.accl_seq_nbr
    WHERE c.accl_description = 'CMB'
      AND t.actt_key_ind IN ('RC', 'DR', 'DVS', 'DVC')
      AND a.acti_status_ind = 'D'
    GROUP BY a.cutb_seq_nbr
),
annual_developed_volume AS (
    SELECT DISTINCT
        CASE
            WHEN d.divi_short_code IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN', 'TCC', 'TKA', 'TKO', 'TOC') THEN 'Interior'
            WHEN d.divi_short_code IN ('TCH', 'TST', 'TSG') THEN 'Coast'
        END AS business_area_region_category,

        CASE
            WHEN d.divi_short_code IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN') THEN 'North Interior'
            WHEN d.divi_short_code IN ('TCC', 'TKA', 'TKO', 'TOC') THEN 'South Interior'
            WHEN d.divi_short_code IN ('TCH', 'TST', 'TSG') THEN 'Coast'
        END AS business_area_region,

        CONCAT(
            CASE
                WHEN d.divi_division_name = 'Seaward' THEN 'Seaward-Tlasta'
                ELSE d.divi_division_name
            END,
            ' (',
            d.divi_short_code,
            ')'
        ) AS business_area,

        d.divi_short_code AS business_area_code,
        cl.colu_lookup_desc AS field_team,
        mu.manu_id,
        l.licn_licence_id AS licence,
        tn.tent_tenure_id AS file_type,
        l.blaz_admin_zone_id AS agreement_type_code,
        z.blaz_admin_zone_desc AS agreement_type,
        cp.perm_permit_id AS permit,
        m.mark_mark_id AS mark,
        b.cutb_block_id AS block,
        b.cutb_system_id AS ubi,
        b.cutb_block_state AS block_state,
        ba.blal_cruise_m3_vol AS cruise_volume,
        ba.blal_rw_vol AS rw_volume,

        actb.rc_done,
        YEAR(add_months(actb.rc_done, 9)) AS rc_done_fiscal,

        actb.dr_done,
        YEAR(add_months(actb.dr_done, 9)) AS dr_done_fiscal,

        actb.dvs_done,
        YEAR(add_months(actb.dvs_done, 9)) AS dvs_done_fiscal,

        actb.dvc_done,
        YEAR(add_months(actb.dvc_done, 9)) AS dvc_done_fiscal,

        b.cutb_seq_nbr
    FROM lrm_replication.division d
    INNER JOIN lrm_replication.block_allocation ba
        ON d.divi_div_nbr = ba.divi_div_nbr
    INNER JOIN lrm_replication.management_unit mu
        ON ba.manu_seq_nbr = mu.manu_seq_nbr
    INNER JOIN lrm_replication.licence l
        ON ba.licn_seq_nbr = l.licn_seq_nbr
    LEFT JOIN lrm_replication.block_admin_zone z
        ON l.divi_div_nbr = z.divi_div_nbr
       AND l.blaz_admin_zone_id = z.blaz_admin_zone_id
    LEFT JOIN lrm_replication.division_code_lookup dcl
        ON l.licn_field_team_id = dcl.colu_lookup_id
       AND l.divi_div_nbr = dcl.divi_div_nbr
    LEFT JOIN lrm_replication.code_lookup cl
        ON dcl.colu_lookup_type = cl.colu_lookup_type
       AND dcl.colu_lookup_id = cl.colu_lookup_id
    LEFT JOIN lrm_replication.tenure_type tn
        ON l.tent_seq_nbr = tn.tent_seq_nbr
    LEFT JOIN lrm_replication.cut_permit cp
        ON ba.perm_seq_nbr = cp.perm_seq_nbr
    LEFT JOIN lrm_replication.mark m
        ON ba.mark_seq_nbr = m.mark_seq_nbr
    INNER JOIN lrm_replication.cut_block b
        ON ba.cutb_seq_nbr = b.cutb_seq_nbr
    INNER JOIN actb
        ON ba.cutb_seq_nbr = actb.cutb_seq_nbr
    WHERE actb.dvc_done BETWEEN TO_DATE('${report_start_date}') AND TO_DATE('${report_end_date}')
)

SELECT
    business_area_region_category,
    business_area_region,
    business_area,
    business_area_code,
    field_team,
    manu_id,
    licence,
    file_type,
    agreement_type_code,
    agreement_type,
    permit,
    mark,
    block,
    ubi,
    block_state,
    cruise_volume,
    rw_volume,
    rc_done,
    rc_done_fiscal,
    dr_done,
    dr_done_fiscal,
    dvs_done,
    dvs_done_fiscal,
    dvc_done,
    dvc_done_fiscal,
    cutb_seq_nbr,
    TO_DATE('${report_start_date}') AS fiscal_year_start_date,
    TO_DATE('${report_start_date}') AS report_start_date,
    TO_DATE('${report_end_date}')   AS report_end_date,
    CURRENT_TIMESTAMP()             AS report_run_timestamp
FROM annual_developed_volume;


-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
DROP TABLE IF EXISTS BCTS_STAGING.annual_developed_volume;
    CREATE TABLE BCTS_STAGING.annual_developed_volume
    AS 
    with adv as
    (
    SELECT * 
        FROM BCTS_STAGING.annual_developed_volume_hist
        WHERE report_end_date = (
            SELECT MAX(report_end_date)
            FROM BCTS_STAGING.annual_developed_volume_hist
        )
    ),
    advt as
    (
    select *
    from bcts_staging.bcts_adv_targets
    where fiscal_year = (select max(fiscal_year) from bcts_staging.bcts_adv_targets)
    )

    select
        adv.*,
        advt.adv_target_m3
        FROM adv
        left join advt
        on adv.business_area_region = advt.business_area_region
        and adv.business_area = advt.business_area
	;

    DROP TABLE IF EXISTS BCTS_REPORTING.annual_developed_volume_hist;
    CREATE TABLE BCTS_REPORTING.annual_developed_volume_hist
    AS SELECT * 
    FROM BCTS_STAGING.annual_developed_volume_hist;

    DROP TABLE IF EXISTS BCTS_REPORTING.annual_developed_volume;
    CREATE TABLE BCTS_REPORTING.annual_developed_volume
    AS SELECT * 
    FROM BCTS_STAGING.annual_developed_volume;