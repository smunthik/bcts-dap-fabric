-- Create _hist table if it does not exist
CREATE TABLE IF NOT EXISTS bcts_staging.roads_transferred_out_hist (
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
    transfer_date DATE,
    deac_method_type STRING,
    deac_level_type STRING,
    rdst_steward_name STRING,
    fiscal_year_start_date DATE,
    report_end_date DATE,
    report_run_date DATE,
    report_run_timestamp TIMESTAMP
)
USING DELTA;


-- Report exists check is done on bcts_reporting table 
-- If report exists in bcts_staging.***_hist table, clear the staging table before inserting new records 
delete from bcts_staging.roads_transferred_out_hist
where  report_start_date = '${report_start_date}'
and report_end_date = '${report_end_date}';

-- Populate staging table
INSERT INTO bcts_staging.roads_transferred_out_hist (
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
    transfer_date,
    deac_method_type,
    deac_level_type,
    rdst_steward_name,
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
            WHEN DIVI_DIVISION_NAME = 'Seaward'
                THEN 'Seaward-Tlasta'
            ELSE DIVI_DIVISION_NAME
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
    CAST(DEAC_END_DATE AS DATE) AS transfer_date,
    DEAC_METHOD_TYPE,
    DEAC_LEVEL_TYPE,
    RDST_STEWARD_NAME,

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

WHERE UPPER(DEAC_METHOD_TYPE) = 'TRANSFER OUT'
    AND URI IS NOT NULL
    AND RCLS_ACCOUNTING_TYPE IN (
        '1 Sale = 5 yrs',
        'S.Term = 10 yrs',
        'Perm = 40 yrs'
    )
    AND DEAC_END_DATE BETWEEN
        CAST('${report_start_date}' AS DATE)
        AND CAST('${report_end_date}' AS DATE)

ORDER BY
    business_area_region_category DESC,
    business_area_region,
    business_area,
    road_road_name;

-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
   DROP TABLE IF EXISTS BCTS_STAGING.roads_transferred_out;
    CREATE TABLE BCTS_STAGING.roads_transferred_out
    AS SELECT * 
    FROM BCTS_STAGING.roads_transferred_out_hist
    WHERE report_end_date = (
	    SELECT MAX(report_end_date)
	    FROM BCTS_STAGING.roads_transferred_out_hist
    );

    DROP TABLE IF EXISTS BCTS_REPORTING.roads_transferred_out_hist;
    CREATE TABLE BCTS_REPORTING.roads_transferred_out_hist
    AS SELECT * 
    FROM BCTS_STAGING.roads_transferred_out_hist;

    DROP TABLE IF EXISTS BCTS_REPORTING.roads_transferred_out;
    CREATE TABLE BCTS_REPORTING.roads_transferred_out
    AS SELECT * 
    FROM BCTS_STAGING.roads_transferred_out;   
    