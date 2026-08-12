-- Create _hist table if it does not exist
CREATE TABLE IF NOT EXISTS bcts_staging.roads_deactivated_hist (
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    business_area_code STRING,
    field_team_desc STRING,
    road_seq_nbr DECIMAL(16,0),
    road_road_name STRING,
    uri STRING,
    poc DECIMAL(38,18),
    pot DECIMAL(38,18),
    length DECIMAL(38,18),
    rcls_accounting_type STRING,
    rdst_steward_name STRING,
    deac_end_date TIMESTAMP,
    deac_level_type STRING,
    deac_method_type STRING,
    fiscal_year_start_date DATE,
    report_end_date DATE,
    report_run_date DATE,
    report_run_timestamp TIMESTAMP
)
USING DELTA;


-- Report exists check is done on bcts_reporting table 
-- If report exists in bcts_staging.***_hist table, clear the staging table before inserting new records 
delete from bcts_staging.roads_deactivated_hist
where  report_start_date = '${report_start_date}'
and report_end_date = '${report_end_date}';

-- Populate staging table
INSERT INTO bcts_staging.roads_deactivated_hist (
    business_area_region_category,
    business_area_region,
    business_area,
    business_area_code,
    field_team_desc,
    road_seq_nbr,
    road_road_name,
    uri,
    poc,
    pot,
    length,
    rcls_accounting_type,
    rdst_steward_name,
    deac_end_date,
    deac_level_type,
    deac_method_type,
    fiscal_year_start_date,
    report_end_date,
    report_run_date,
    report_run_timestamp
)
SELECT
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
    FIELD_TEAM_DESC,
    ROAD_SEQ_NBR,
    ROAD_ROAD_NAME,
    URI,
    MIN(POC) AS poc,
    MAX(POT) AS pot,
    (MAX(POT) - MIN(POC)) / 1000 AS length,
    RCLS_ACCOUNTING_TYPE,
    RDST_STEWARD_NAME,
    DEAC_END_DATE,
    DEAC_LEVEL_TYPE,
    DEAC_METHOD_TYPE,

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

FROM (
    SELECT
        a.DIVI_DIVISION_NAME,
        a.TSO_CODE,
        a.FIELD_TEAM_DESC,
        a.ROAD_SEQ_NBR,
        a.ROAD_ROAD_NAME,
        a.URI,
        a.POC,
        a.POT,

        LEAD(a.POC, 1) OVER (
            PARTITION BY a.ROAD_SEQ_NBR
            ORDER BY
                a.ROAD_SEQ_NBR,
                a.POC
        ) AS POC_NEXT,

        LAG(a.POT, 1) OVER (
            PARTITION BY a.ROAD_SEQ_NBR
            ORDER BY
                a.ROAD_SEQ_NBR,
                a.POC
        ) AS POT_PREV,

        a.RCLS_ACCOUNTING_TYPE,
        a.RDST_STEWARD_NAME,
        DATE_TRUNC('DAY', a.DEAC_END_DATE) AS DEAC_END_DATE,
        a.DEAC_LEVEL_TYPE,
        a.DEAC_METHOD_TYPE

    FROM LRM_REPLICATION.V_ROAD_GAP_ANALYSIS a

    WHERE a.URI IS NOT NULL
        AND a.RDST_STEWARD_NAME IN ('BCTS', 'former BCTS')
        AND a.RCLS_ACCOUNTING_TYPE IN (
            '1 Sale = 5 yrs',
            'S.Term = 10 yrs',
            'Perm = 40 yrs'
        )
        AND a.DEAC_END_DATE BETWEEN
            CAST('${report_start_date}' AS DATE)
            AND CAST('${report_end_date}' AS DATE)
        AND a.DEAC_METHOD_TYPE IN ('DEACT', 'REHAB')
) RGA

GROUP BY
    DIVI_DIVISION_NAME,
    TSO_CODE,
    FIELD_TEAM_DESC,
    ROAD_SEQ_NBR,
    ROAD_ROAD_NAME,
    URI,
    RCLS_ACCOUNTING_TYPE,
    RDST_STEWARD_NAME,
    DEAC_END_DATE,
    DEAC_LEVEL_TYPE,
    DEAC_METHOD_TYPE,
    CASE
        WHEN POC_NEXT IS NULL
            AND POT_PREV IS NULL
            THEN 'N'
        WHEN POT < POC_NEXT
            THEN 'Before'
        WHEN POC > POT_PREV
            THEN 'After'
        WHEN POC = POC_NEXT
            OR POC = POT_PREV
            OR POT = POC_NEXT
            OR POT = POT_PREV
            THEN 'Y'
        ELSE 'G'
    END;

-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
    DROP TABLE IF EXISTS BCTS_STAGING.roads_deactivated;
    CREATE TABLE BCTS_STAGING.roads_deactivated
    AS SELECT * 
    FROM BCTS_STAGING.roads_deactivated_hist
    WHERE report_end_date = (
	    SELECT MAX(report_end_date)
	    FROM BCTS_STAGING.roads_deactivated_hist
    );

    DROP TABLE IF EXISTS BCTS_REPORTING.roads_deactivated_hist;
    CREATE TABLE BCTS_REPORTING.roads_deactivated_hist
    AS SELECT * 
    FROM BCTS_STAGING.roads_deactivated_hist;

    DROP TABLE IF EXISTS BCTS_REPORTING.roads_deactivated;
    CREATE TABLE BCTS_REPORTING.roads_deactivated
    AS SELECT * 
    FROM BCTS_STAGING.roads_deactivated;    
    