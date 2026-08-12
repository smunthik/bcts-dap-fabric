-- Create _hist table if it does not exist
CREATE TABLE IF NOT EXISTS bcts_staging.roads_constructed_hist (
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    business_area_code STRING,
    road_seq_nbr DECIMAL(16,0),
    uri STRING,
    road_road_name STRING,
    rcls_accounting_type STRING,
    poc DECIMAL(13,4),
    pot DECIMAL(38,18),
    length DECIMAL(38,18),
    rcom_completion_date TIMESTAMP,
    const_method_type STRING,
    rcom_method STRING,
    rdst_steward_name STRING,
    fiscal_year_start_date DATE,
    report_end_date DATE,
    report_run_date DATE,
    report_run_timestamp TIMESTAMP,
    field_team_desc STRING
)
USING DELTA;


-- Report exists check is done on bcts_reporting table 
-- If report exists in bcts_staging.***_hist table, clear the staging table before inserting new records 
delete from bcts_staging.roads_constructed_hist
where  fiscal_year_start_date = '${report_start_date}'
and report_end_date = '${report_end_date}';

-- Populate staging table
INSERT INTO bcts_staging.roads_constructed_hist (
    business_area_region_category,
    business_area_region,
    business_area,
    business_area_code,
    road_seq_nbr,
    uri,
    road_road_name,
    rcls_accounting_type,
    poc,
    pot,
    length,
    rcom_completion_date,
    const_method_type,
    rcom_method,
    rdst_steward_name,
    field_team_desc,
    fiscal_year_start_date,
    report_end_date,
    report_run_date,
    report_run_timestamp
)
SELECT DISTINCT
    CASE
        WHEN TSO_CODE IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN', 'TCC', 'TKA', 'TKO', 'TOC')
            THEN 'Interior'
        WHEN TSO_CODE IN ('TCH', 'TST', 'TSG')
            THEN 'Coast'
    END AS business_area_region_category,

    CASE
        WHEN TSO_CODE IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN')
            THEN 'North Interior'
        WHEN TSO_CODE IN ('TCC', 'TKA', 'TKO', 'TOC')
            THEN 'South Interior'
        WHEN TSO_CODE IN ('TCH', 'TST', 'TSG')
            THEN 'Coast'
    END AS business_area_region,

    CONCAT(
        CASE
            WHEN divi_division_name = 'Seaward'
                THEN 'Seaward-Tlasta'
            ELSE divi_division_name
        END,
        ' (',
        TSO_CODE,
        ')'
    ) AS business_area,

    TSO_CODE AS business_area_code,
    ROAD_SEQ_NBR,
    URI,
    ROAD_ROAD_NAME,
    RCLS_ACCOUNTING_TYPE,
    POC,
    POT,
    ((POT - POC) / 1000) AS length,

    DATE_TRUNC('DAY', RCOM_COMPLETION_DATE) AS rcom_completion_date,

    CONST_METHOD_TYPE,
    RCOM_METHOD,
    RDST_STEWARD_NAME,
    FIELD_TEAM_DESC,

    CAST('${report_start_date}' AS DATE) AS fiscal_year_start_date,
    CAST('${report_end_date}' AS DATE) AS report_end_date,

    TO_DATE(
        FROM_UTC_TIMESTAMP(
            CURRENT_TIMESTAMP(),
            'America/Vancouver'
        )
    ) AS report_run_date,

    FROM_UTC_TIMESTAMP(
        CURRENT_TIMESTAMP(),
        'America/Vancouver'
    ) AS report_run_timestamp

FROM LRM_REPLICATION.V_ROAD_GAP_ANALYSIS

WHERE URI IS NOT NULL
    AND RCLS_ACCOUNTING_TYPE IN (
        '1 Sale = 5 yrs',
        'S.Term = 10 yrs',
        'Perm = 40 yrs'
    )
    AND RDST_STEWARD_NAME = 'BCTS'
    AND RCOM_COMPLETION_DATE BETWEEN
        CAST('${report_start_date}' AS DATE)
        AND CAST('${report_end_date}' AS DATE)
    AND (
        (
            CONST_METHOD_TYPE = 'NEW ROAD'
            AND RCOM_METHOD <> 'Road Permit'
            AND RCOM_METHOD <> 'TSL (on-block)'
        )
        OR (
            CONST_METHOD_TYPE = 'TRANSFER IN'
            AND RCOM_METHOD = 'With Costs'
        )
    );

-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
    DROP TABLE IF EXISTS BCTS_STAGING.roads_constructed;
    CREATE TABLE BCTS_STAGING.roads_constructed
    AS SELECT * 
    FROM BCTS_STAGING.roads_constructed_hist
    WHERE report_end_date = (
	    SELECT MAX(report_end_date)
	    FROM BCTS_STAGING.roads_constructed_hist
    );

    DROP TABLE IF EXISTS BCTS_REPORTING.roads_constructed_hist;
    CREATE TABLE BCTS_REPORTING.roads_constructed_hist
    AS SELECT * 
    FROM BCTS_STAGING.roads_constructed_hist;

    DROP TABLE IF EXISTS BCTS_REPORTING.roads_constructed;
    CREATE TABLE BCTS_REPORTING.roads_constructed
    AS SELECT * 
    FROM BCTS_STAGING.roads_constructed;
   
    