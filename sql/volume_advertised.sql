-- Create _hist table if it does not exist
CREATE TABLE IF NOT EXISTS bcts_staging.volume_advertised_official (
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    business_area_code STRING,
    mgmt_unit_type STRING,
    mgmt_unit_id STRING,
    description STRING,
    forest_file_id STRING,
    bcts_category_code STRING,
    category STRING,
    auction_date TIMESTAMP,
    sale_volume DECIMAL(13,1),
    fta_volume DECIMAL(10,0),
    client_count BIGINT,
    eligible_client_count BIGINT,
    ineligible_client_count BIGINT,
    client_count_eligibility_indicator_missing BIGINT,
    no_sale_rationale_code STRING,
    no_sale_rationale STRING,
    auction_date_fiscal DECIMAL(38,18),
    auction_date_quarter STRING,
    tenure_term DECIMAL(5,0),
    initial_expiry_dt DATE,
    current_expiry_dt DATE,
    awarded_ind STRING,
    file_status_st STRING,
    first_auction_date TIMESTAMP,
    last_auction_date TIMESTAMP,
    auction_count BIGINT,
    first_auction STRING,
    last_auction STRING,
    last_auction_no_sale STRING,
    report_start_date DATE,
    report_end_date DATE,
    fiscal_year DECIMAL(38,18),
    report_run_date DATE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS bcts_staging.volume_advertised_main_hist (
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    business_area_code STRING,
    mgmt_unit_type STRING,
    mgmt_unit_id STRING,
    nav_name STRING,
    district_name STRING,
    field_team STRING,
    geographiclocation STRING,
    operatingarea STRING,
    description STRING,
    forest_file_id STRING,
    bcts_category_code STRING,
    category STRING,
    auction_date TIMESTAMP,
    auction_date_fiscal DECIMAL(38,18),
    auction_date_quarter STRING,
    lrm_auction_date TIMESTAMP,
    lrm_auction_status STRING,
    sale_volume DECIMAL(13,1),
    fta_volume DECIMAL(10,0),
    lrm_total_volume DECIMAL(38,18),
    lrm_cruise_volume DECIMAL(38,18),
    lrm_rw_volume DECIMAL(38,18),
    lrm_total_volume_salvage_all_fire_years DECIMAL(38,18),
    lrm_total_volume_salvage_2021_fire DECIMAL(38,18),
    lrm_total_volume_salvage_2022_fire DECIMAL(38,18),
    lrm_total_volume_salvage_2023_fire DECIMAL(38,18),
    lrm_total_volume_salvage_2024_fire DECIMAL(38,18),
    lrm_total_volume_salvage_2025_fire DECIMAL(38,18),
    client_count BIGINT,
    eligible_client_count BIGINT,
    ineligible_client_count BIGINT,
    client_count_eligibility_indicator_missing BIGINT,
    file_status_st STRING,
    awarded_ind STRING,
    no_sale_rationale_code STRING,
    no_sale_rationale STRING,
    lrm_auc_fiscal DECIMAL(38,18),
    lrm_auc_quarter STRING,
    first_auction_date TIMESTAMP,
    last_auction_date TIMESTAMP,
    auction_count BIGINT,
    first_auction STRING,
    last_auction STRING,
    last_auction_no_sale STRING,
    report_start_date DATE,
    report_end_date DATE,
    fiscal_year DECIMAL(38,18),
    report_run_date DATE,
    report_run_timestamp TIMESTAMP
)
USING DELTA;

-- Report exists check is done on bcts_reporting table 
-- If report exists in bcts_staging.***_hist table, clear the staging table before inserting new records 
delete from bcts_staging.volume_advertised_main_hist
where  report_start_date = '${report_start_date}'
and report_end_date = '${report_end_date}';


-- Populate staging table
INSERT INTO bcts_staging.volume_advertised_official (
    business_area_region_category,
    business_area_region,
    business_area,
    business_area_code,
    mgmt_unit_type,
    mgmt_unit_id,
    description,
    forest_file_id,
    bcts_category_code,
    category,
    auction_date,
    sale_volume,
    fta_volume,
    client_count,
    eligible_client_count,
    ineligible_client_count,
    client_count_eligibility_indicator_missing,
    no_sale_rationale_code,
    no_sale_rationale,
    auction_date_fiscal,
    auction_date_quarter,
    tenure_term,
    initial_expiry_dt,
    current_expiry_dt,
    awarded_ind,
    file_status_st,
    first_auction_date,
    last_auction_date,
    auction_count,
    first_auction,
    last_auction,
    last_auction_no_sale,
    report_start_date,
    report_end_date,
    fiscal_year,
    report_run_date
)

WITH AU_CNT AS (
    SELECT
        ts.forest_file_id,
        COUNT(ts.auction_date) AS auction_count,
        MIN(ts.auction_date) AS first_auction_date,
        MAX(ts.auction_date) AS last_auction_date
    FROM
        bctsadmin_replication.bcts_timber_sale ts,
        (
            SELECT
                forest_file_id,
                auction_date
            FROM bctsadmin_replication.bcts_timber_sale
            WHERE
                COALESCE(no_sale_rationale_code, ' ') <> 'TB'
                AND auction_date <= TO_DATE('{end_date}', 'yyyy-MM-dd')
        ) auction_filter
    WHERE
        ts.forest_file_id = auction_filter.forest_file_id
        AND ts.auction_date = auction_filter.auction_date
    GROUP BY
        ts.forest_file_id
),

bid_info AS (
    SELECT
        b.forest_file_id,
        b.auction_date,
        COUNT(client_number) AS client_count,
        COUNT(
            CASE
                WHEN UPPER(ineligible_ind) = 'N'
                    THEN client_number
            END
        ) AS eligible_client_count,
        COUNT(
            CASE
                WHEN UPPER(ineligible_ind) = 'Y'
                    THEN client_number
            END
        ) AS ineligible_client_count,
        COUNT(
            CASE
                WHEN ineligible_ind IS NULL
                    THEN client_number
            END
        ) AS client_count_eligibility_indicator_missing,
        MAX(b.sale_awarded_ind) AS awarded_ind
    FROM
        bctsadmin_replication.bcts_tenure_bidder b
    WHERE
        b.auction_date BETWEEN TO_DATE('{start_date}', 'yyyy-MM-dd')
                           AND TO_DATE('{end_date}', 'yyyy-MM-dd')
    GROUP BY
        b.forest_file_id,
        b.auction_date
)

SELECT DISTINCT
    CASE
        WHEN ou.org_unit_code IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN', 'TCC', 'TKA', 'TKO', 'TOC')
            THEN 'Interior'
        WHEN ou.org_unit_code IN ('TCH', 'TST', 'TSG')
            THEN 'Coast'
    END AS business_area_region_category,

    CASE
        WHEN ou.org_unit_code IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN')
            THEN 'North Interior'
        WHEN ou.org_unit_code IN ('TCC', 'TKA', 'TKO', 'TOC')
            THEN 'South Interior'
        WHEN ou.org_unit_code IN ('TCH', 'TST', 'TSG')
            THEN 'Coast'
    END AS business_area_region,

    REPLACE(
        CONCAT(
            CASE
                WHEN ou.org_unit_name = 'Seaward Timber Sales Office'
                    THEN 'Seaward-Tlasta'
                ELSE ou.org_unit_name
            END,
            ' (',
            ou.org_unit_code,
            ')'
        ),
        ' Timber Sales Office',
        ''
    ) AS business_area,

    ou.org_unit_code AS business_area_code,
    pfu.mgmt_unit_type,
    pfu.mgmt_unit_id,
    ta.description,
    ts.forest_file_id,
    ts.bcts_category_code,
    c.description AS category,
    ts.auction_date,
    ts.sale_volume,
    hs.sale_volume AS fta_volume,
    bid_info.client_count,
    bid_info.eligible_client_count,
    bid_info.ineligible_client_count,
    bid_info.client_count_eligibility_indicator_missing,
    ts.no_sale_rationale_code,

    CASE
        WHEN ts.no_sale_rationale_code IS NULL THEN NULL
        ELSE CONCAT(
            no_sale.description,
            ' (',
            ts.no_sale_rationale_code,
            ')'
        )
    END AS no_sale_rationale,

    YEAR(ADD_MONTHS(ts.auction_date, 9)) AS auction_date_fiscal,

    CONCAT(
        'Q',
        CAST(CEIL(MONTH(ADD_MONTHS(ts.auction_date, -3)) / 3.0) AS INT)
    ) AS auction_date_quarter,

    te.tenure_term,
    te.initial_expiry_dt,
    te.current_expiry_dt,
    bid_info.awarded_ind,
    pfu.file_status_st,
    au_cnt.first_auction_date,
    au_cnt.last_auction_date,
    au_cnt.auction_count,

    CASE
        WHEN ts.auction_date = au_cnt.first_auction_date THEN 'Y'
        ELSE 'N'
    END AS first_auction,

    CASE
        WHEN ts.auction_date = au_cnt.last_auction_date THEN 'Y'
        ELSE 'N'
    END AS last_auction,

    CASE
        WHEN ts.auction_date = au_cnt.last_auction_date
             AND ts.no_sale_rationale_code IS NOT NULL
            THEN 'This auction is the last auction for the licence in the report period and is a no sale.'
    END AS last_auction_no_sale,

    CAST('{start_date}' AS DATE) AS report_start_date,
    CAST('{end_date}' AS DATE) AS report_end_date,

    CASE
        WHEN MONTH(CAST('{end_date}' AS DATE)) >= 4
            THEN YEAR(CAST('{end_date}' AS DATE))
        ELSE YEAR(CAST('{end_date}' AS DATE)) - 1
    END AS fiscal_year,

        TO_DATE(
        FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Vancouver')
    ) AS report_run_date

FROM
    bctsadmin_replication.bcts_timber_sale ts

    LEFT JOIN bcts_staging.fta_harvest_sale hs
        ON ts.forest_file_id = hs.forest_file_id

    LEFT JOIN bctsadmin_replication.bcts_category_code c
        ON ts.bcts_category_code = c.bcts_category_code

    INNER JOIN bcts_staging.fta_prov_forest_use pfu
        ON ts.forest_file_id = pfu.forest_file_id

    INNER JOIN mofclient_replication.org_unit ou
        ON pfu.bcts_org_unit = ou.org_unit_no

    LEFT JOIN bcts_staging.fta_tenure_term te
        ON ts.forest_file_id = te.forest_file_id

    LEFT JOIN bctsadmin_replication.no_sale_rationale_code no_sale
        ON ts.no_sale_rationale_code = no_sale.no_sale_rationale_code

    LEFT JOIN bcts_staging.fta_tsa_number_code ta
        ON pfu.mgmt_unit_id = ta.tsa_number

    LEFT JOIN bcts_staging.fta_tfl_number_code tf
        ON pfu.mgmt_unit_id = tf.tfl_number

    LEFT JOIN AU_CNT
        ON ts.forest_file_id = au_cnt.forest_file_id

    LEFT JOIN bid_info
        ON ts.forest_file_id = bid_info.forest_file_id
        AND ts.auction_date = bid_info.auction_date

WHERE
    ts.auction_date BETWEEN TO_DATE('{start_date}', 'yyyy-MM-dd')
                        AND TO_DATE('{end_date}', 'yyyy-MM-dd')
    AND COALESCE(ts.no_sale_rationale_code, ' ') <> 'TB';


INSERT INTO bcts_staging.volume_advertised_main_hist (
    business_area_region_category,
    business_area_region,
    business_area,
    business_area_code,
    mgmt_unit_type,
    mgmt_unit_id,
    nav_name,
    district_name,
    field_team,
    geographiclocation,
    operatingarea,
    description,
    forest_file_id,
    bcts_category_code,
    category,
    auction_date,
    auction_date_fiscal,
    auction_date_quarter,
    lrm_auction_date,
    lrm_auction_status,
    sale_volume,
    fta_volume,
    lrm_total_volume,
    lrm_cruise_volume,
    lrm_rw_volume,
    lrm_total_volume_salvage_all_fire_years,
    lrm_total_volume_salvage_2021_fire,
    lrm_total_volume_salvage_2022_fire,
    lrm_total_volume_salvage_2023_fire,
    lrm_total_volume_salvage_2024_fire,
    lrm_total_volume_salvage_2025_fire,
    client_count,
    eligible_client_count,
    ineligible_client_count,
    client_count_eligibility_indicator_missing,
    file_status_st,
    awarded_ind,
    no_sale_rationale_code,
    no_sale_rationale,
    lrm_auc_fiscal,
    lrm_auc_quarter,
    first_auction_date,
    last_auction_date,
    auction_count,
    first_auction,
    last_auction,
    last_auction_no_sale,
    report_start_date,
    report_end_date,
    fiscal_year,
    report_run_date,
    report_run_timestamp
)
SELECT
    AD.BUSINESS_AREA_REGION_CATEGORY,
    AD.BUSINESS_AREA_REGION,
    AD.BUSINESS_AREA,
    AD.BUSINESS_AREA_CODE,
    AD.MGMT_UNIT_TYPE,
    AD.MGMT_UNIT_ID,
    LRM.NAV_NAME,
    LRM.DISTRICT_NAME,
    LRM.FIELD_TEAM,
    LRM.GEOGRAPHICLOCATION,
    LRM.OPERATINGAREA,
    AD.DESCRIPTION,
    AD.FOREST_FILE_ID,
    AD.BCTS_CATEGORY_CODE,
    AD.CATEGORY,
    AD.AUCTION_DATE,
    AD.AUCTION_DATE_FISCAL,
    AD.AUCTION_DATE_QUARTER,
    LRM.LRM_AUCTION_DATE,
    LRM.LRM_AUCTION_STATUS,
    AD.SALE_VOLUME,
    AD.FTA_VOLUME,
    LRM.LRM_TOTAL_VOLUME,
    LRM.LRM_CRUISE_VOLUME,
    LRM.LRM_RW_VOLUME,
    LRM.LRM_TOTAL_VOLUME_SALVAGE_ALL_FIRE_YEARS,
    LRM.LRM_TOTAL_VOLUME_SALVAGE_2021_FIRE,
    LRM.LRM_TOTAL_VOLUME_SALVAGE_2022_FIRE,
    LRM.LRM_TOTAL_VOLUME_SALVAGE_2023_FIRE,
    LRM.LRM_TOTAL_VOLUME_SALVAGE_2024_FIRE,
    LRM.LRM_TOTAL_VOLUME_SALVAGE_2025_FIRE,
    AD.CLIENT_COUNT,
    AD.ELIGIBLE_CLIENT_COUNT,
    AD.INELIGIBLE_CLIENT_COUNT,
    AD.CLIENT_COUNT_ELIGIBILITY_INDICATOR_MISSING,
    AD.FILE_STATUS_ST,
    AD.AWARDED_IND,
    AD.NO_SALE_RATIONALE_CODE,
    AD.NO_SALE_RATIONALE,

    YEAR(ADD_MONTHS(LRM.LRM_AUCTION_DATE, 9)) AS lrm_auc_fiscal,

    CONCAT(
        'Q',
        CAST(
            CEIL(
                MONTH(ADD_MONTHS(LRM.LRM_AUCTION_DATE, -3)) / 3.0
            ) AS INT
        )
    ) AS lrm_auc_quarter,

    AD.FIRST_AUCTION_DATE,
    AD.LAST_AUCTION_DATE,
    AD.AUCTION_COUNT,
    AD.FIRST_AUCTION,
    AD.LAST_AUCTION,
    AD.LAST_AUCTION_NO_SALE,
    AD.REPORT_START_DATE,
    AD.REPORT_END_DATE,
    AD.FISCAL_YEAR,

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

FROM bcts_staging.volume_advertised_official AD
LEFT JOIN bcts_staging.v_volume_advertised_lrm LRM
    ON AD.FOREST_FILE_ID = LRM.LICENCE_ID;


-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
    DROP TABLE IF EXISTS BCTS_STAGING.volume_advertised_main;
    CREATE TABLE BCTS_STAGING.volume_advertised_main
    AS SELECT * 
    FROM BCTS_STAGING.volume_advertised_main_hist
    WHERE report_end_date = (
        SELECT MAX(report_end_date)
        FROM BCTS_STAGING.volume_advertised_main_hist
	);

    DROP TABLE IF EXISTS BCTS_REPORTING.volume_advertised_main;
    CREATE TABLE BCTS_REPORTING.volume_advertised_main
    AS SELECT * 
    FROM BCTS_STAGING.volume_advertised_main;

    DROP TABLE IF EXISTS BCTS_REPORTING.volume_advertised_main_hist;
    CREATE TABLE BCTS_REPORTING.volume_advertised_main_hist
    AS SELECT * 
    FROM BCTS_STAGING.volume_advertised_main_hist;
   