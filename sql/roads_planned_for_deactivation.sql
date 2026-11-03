-- Create _hist table if it does not exist
CREATE TABLE IF NOT EXISTS bcts_staging.roads_planned_deactivation_hist
(
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    business_area_code STRING,
    road_seq_nbr DECIMAL(16,0),
    deac_seq_nbr DECIMAL(15,0),
    uri STRING,
    road_road_name STRING,
    field_team_desc STRING,
    poc DECIMAL(38,18),
    pot DECIMAL(38,18),
    total_length DECIMAL(38,18),
    effective_planned_cost DECIMAL(38,18),
    deac_budgeted_cost DECIMAL(15,2),
    deac_budgeted_item_cost DECIMAL(38,18),
    rcls_accounting_type STRING,
    rdst_steward_name STRING,
    deac_planned_date DATE,
    deac_end_date TIMESTAMP,
    deac_method_type STRING,
    deac_level_type STRING,
    fiscal_year DECIMAL(38,18),
    fiscal STRING,
    report_end_date DATE,
    report_run_date DATE,
    report_run_timestamp TIMESTAMP
)
USING DELTA;


-- Report exists check is done on bcts_reporting table 
-- If report exists in bcts_staging.***_hist table, clear the staging table before inserting new records 
delete from bcts_staging.roads_planned_deactivation_hist
where report_end_date = '${report_end_date}';

-- Populate staging table
INSERT INTO bcts_staging.roads_planned_deactivation_hist (
    business_area_region_category,
    business_area_region,
    business_area,
    business_area_code,
    road_seq_nbr,
    deac_seq_nbr,
    uri,
    road_road_name,
    field_team_desc,
    poc,
    pot,
    total_length,
    effective_planned_cost,
    deac_budgeted_cost,
    deac_budgeted_item_cost,
    rcls_accounting_type,
    rdst_steward_name,
    deac_planned_date,
    deac_end_date,
    deac_method_type,
    deac_level_type,
    fiscal_year,
    fiscal,
    report_end_date,
    report_run_date,
    report_run_timestamp
)
WITH GAP AS (
    SELECT
        G.divi_division_name,
        G.tso_code,
        G.road_seq_nbr,
        G.deac_seq_nbr,
        G.uri,
        G.road_road_name,
        G.field_team_desc,
        G.poc,
        G.pot,

        LEAD(G.poc, 1) OVER (
            PARTITION BY G.deac_seq_nbr
            ORDER BY G.deac_seq_nbr, G.poc
        ) AS poc_next,

        LAG(G.pot, 1) OVER (
            PARTITION BY G.deac_seq_nbr
            ORDER BY G.deac_seq_nbr, G.poc
        ) AS pot_prev,

        G.deac_budgeted_cost,
        G.rcls_accounting_type,
        G.rdst_steward_name,
        TO_DATE(G.deac_planned_date) AS deac_planned_date,
        G.deac_end_date,
        G.deac_method_type,
        G.deac_level_type

    FROM lrm_replication.v_road_gap_analysis G

    WHERE G.rdst_steward_name IN (
              'BCTS',
              'former BCTS'
          )
      AND G.uri IS NOT NULL
      AND G.deac_end_date IS NULL
      AND G.rcls_accounting_type IN (
              '1 Sale = 5 yrs',
              'S.Term = 10 yrs',
              'Perm = 40 yrs'
          )
      AND G.deac_method_type IN (
              'DEACT',
              'REHAB',
              'TRANSFER OUT'
          )
),

R AS (
    SELECT
        divi_division_name,
        tso_code,
        road_seq_nbr,
        deac_seq_nbr,
        uri,
        road_road_name,
        field_team_desc,
        MIN(poc) AS poc,
        MAX(pot) AS pot,
        (MAX(pot) - MIN(poc)) / 1000 AS total_length,
        deac_budgeted_cost,
        rcls_accounting_type,
        rdst_steward_name,
        deac_planned_date,
        deac_end_date,
        deac_method_type,
        deac_level_type

    FROM GAP

    GROUP BY
        divi_division_name,
        tso_code,
        road_seq_nbr,
        deac_seq_nbr,
        uri,
        road_road_name,
        field_team_desc,
        deac_budgeted_cost,
        rcls_accounting_type,
        rdst_steward_name,
        deac_planned_date,
        deac_end_date,
        deac_method_type,
        deac_level_type,

        CASE
            WHEN poc_next IS NULL
             AND pot_prev IS NULL
                THEN 'N'

            WHEN pot < poc_next
                THEN 'Before'

            WHEN poc > pot_prev
                THEN 'After'

            WHEN poc = poc_next
              OR poc = pot_prev
              OR pot = poc_next
              OR pot = pot_prev
                THEN 'Y'

            ELSE 'G'
        END
),

DC AS (
    SELECT
        C.deac_seq_nbr,
        SUM(C.raco_item_cost) AS deac_budgeted_item_cost

    FROM lrm_replication.v_road_activity_cost C

    WHERE C.deac_seq_nbr IS NOT NULL
      AND UPPER(C.raco_cost_type) = 'BUDGETED_COST'

    GROUP BY
        C.deac_seq_nbr
),

FINAL AS (
    SELECT
        R.divi_division_name,
        R.tso_code,
        R.road_seq_nbr,
        R.deac_seq_nbr,
        R.uri,
        R.road_road_name,
        R.field_team_desc,
        R.poc,
        R.pot,
        R.total_length,
        R.deac_budgeted_cost,
        DC.deac_budgeted_item_cost,
        R.rcls_accounting_type,
        R.rdst_steward_name,
        R.deac_planned_date,
        R.deac_end_date,
        R.deac_method_type,
        R.deac_level_type,

        YEAR(
            ADD_MONTHS(
                R.deac_planned_date,
                9
            )
        ) AS planned_fiscal_year,

        YEAR(
            ADD_MONTHS(
                CAST('${report_end_date}' AS DATE),
                9
            )
        ) AS report_fiscal_year

    FROM R

    LEFT JOIN DC
        ON R.deac_seq_nbr = DC.deac_seq_nbr
)

SELECT
    CASE
        WHEN tso_code IN (
            'TBA',
            'TPL',
            'TPG',
            'TSK',
            'TSN',
            'TCC',
            'TKA',
            'TKO',
            'TOC'
        )
            THEN 'Interior'

        WHEN tso_code IN (
            'TCH',
            'TST',
            'TSG'
        )
            THEN 'Coast'
    END AS business_area_region_category,

    CASE
        WHEN tso_code IN (
            'TBA',
            'TPL',
            'TPG',
            'TSK',
            'TSN'
        )
            THEN 'North Interior'

        WHEN tso_code IN (
            'TCC',
            'TKA',
            'TKO',
            'TOC'
        )
            THEN 'South Interior'

        WHEN tso_code IN (
            'TCH',
            'TST',
            'TSG'
        )
            THEN 'Coast'
    END AS business_area_region,

    CONCAT(
        CASE
            WHEN divi_division_name = 'Seaward'
                THEN 'Seaward-Tlasta'
            ELSE divi_division_name
        END,
        ' (',
        tso_code,
        ')'
    ) AS business_area,

    tso_code AS business_area_code,
    road_seq_nbr,
    deac_seq_nbr,
    uri,
    road_road_name,
    field_team_desc,
    poc,
    pot,
    total_length,

    CASE
        WHEN COALESCE(deac_budgeted_cost, 0)
             >= COALESCE(deac_budgeted_item_cost, 0)
            THEN deac_budgeted_cost
        ELSE deac_budgeted_item_cost
    END AS effective_planned_cost,

    deac_budgeted_cost,
    deac_budgeted_item_cost,
    rcls_accounting_type,
    rdst_steward_name,
    deac_planned_date,
    deac_end_date,
    deac_method_type,
    deac_level_type,
    planned_fiscal_year AS fiscal_year,

    CASE
        WHEN planned_fiscal_year < report_fiscal_year
            THEN CONCAT(
                'Past Fiscals (Pre-',
                CAST(report_fiscal_year AS STRING),
                ')'
            )

        WHEN planned_fiscal_year = report_fiscal_year
            THEN CONCAT(
                'Current Fiscal (',
                CAST(planned_fiscal_year AS STRING),
                ')'
            )

        WHEN planned_fiscal_year - report_fiscal_year = 1
            THEN CONCAT(
                '1st Fiscal (',
                CAST(planned_fiscal_year AS STRING),
                ')'
            )

        WHEN planned_fiscal_year - report_fiscal_year = 2
            THEN CONCAT(
                '2nd Fiscal (',
                CAST(planned_fiscal_year AS STRING),
                ')'
            )

        WHEN planned_fiscal_year - report_fiscal_year = 3
            THEN CONCAT(
                '3rd Fiscal (',
                CAST(planned_fiscal_year AS STRING),
                ')'
            )

        WHEN planned_fiscal_year - report_fiscal_year = 4
            THEN CONCAT(
                '4th Fiscal (',
                CAST(planned_fiscal_year AS STRING),
                ')'
            )

        WHEN planned_fiscal_year - report_fiscal_year = 5
            THEN CONCAT(
                '5th Fiscal (',
                CAST(planned_fiscal_year AS STRING),
                ')'
            )

        WHEN planned_fiscal_year - report_fiscal_year > 5
            THEN CONCAT(
                '6th Fiscal-Onwards (',
                CAST(report_fiscal_year + 6 AS STRING),
                '+)'
            )
    END AS fiscal,

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

FROM FINAL;

-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.

DROP TABLE IF EXISTS BCTS_STAGING.roads_planned_deactivation;
CREATE TABLE BCTS_STAGING.roads_planned_deactivation
AS SELECT *,
report_start_date as fiscal_year_start_date 
FROM BCTS_STAGING.roads_planned_deactivation_hist
WHERE report_end_date = (
    SELECT MAX(report_end_date)
    FROM BCTS_STAGING.roads_planned_deactivation_hist
);

DROP TABLE IF EXISTS BCTS_REPORTING.roads_planned_deactivation_hist;
CREATE TABLE BCTS_REPORTING.roads_planned_deactivation_hist
AS SELECT *,
report_start_date as fiscal_year_start_date 
FROM BCTS_STAGING.roads_planned_deactivation_hist;

DROP TABLE IF EXISTS BCTS_REPORTING.roads_planned_deactivation;
CREATE TABLE BCTS_REPORTING.roads_planned_deactivation
AS SELECT * 
FROM BCTS_STAGING.roads_planned_deactivation;
   
    