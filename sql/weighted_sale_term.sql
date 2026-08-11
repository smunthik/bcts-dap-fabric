-- Create _hist table if it does not exist
CREATE TABLE IF NOT EXISTS bcts_staging.weighted_sale_term_hist (
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    auction_fiscal DECIMAL(38,18),
    awarded_licence_volume_class STRING,
    sum_awarded_licence_volume DECIMAL(38,18),
    sum_awarded_licence_volume_x_tenure_term DECIMAL(38,18),
    weighted_tenure_term DECIMAL(38,18),
    count_awarded_licences BIGINT,
    report_start_date DATE,
    report_end_date DATE,
    report_run_date DATE,
    report_run_timestamp TIMESTAMP
)
USING DELTA;

-- Report exists check is done on bcts_reporting table 
-- If report exists in bcts_staging.***_hist table, clear the staging table before inserting new records 
delete from bcts_staging.weighted_sale_term_hist
where  report_start_date = '${report_start_date}'
and report_end_date = '${report_end_date}';

-- Populate staging table
INSERT INTO bcts_staging.weighted_sale_term_hist (
    business_area_region_category,
    business_area_region,
    business_area,
    auction_fiscal,
    awarded_licence_volume_class,
    sum_awarded_licence_volume,
    sum_awarded_licence_volume_x_tenure_term,
    weighted_tenure_term,
    count_awarded_licences,
    report_start_date,
    report_end_date,
    report_run_date,
    report_run_timestamp
)

WITH sold_licence_bid_info AS (
    SELECT
        ts0.forest_file_id,
        ts0.auction_date,
        ts0.total_upset_value AS cruise_total_upset_value,
        ts0.upset_rate AS scale_upset_rate,
        ts0.sale_volume AS sale_volume,
        tb.bonus_bid AS sold_licence_bonus_bid,
        tb.bonus_offer AS sold_licence_bonus_offer,

        CASE
            WHEN ts0.total_upset_value > 0 THEN
                ROUND(
                    ts0.total_upset_value + tb.bonus_offer,
                    2
                )
            ELSE
                ROUND(
                    (ts0.upset_rate + tb.bonus_bid) * ts0.sale_volume,
                    2
                )
        END AS sold_licence_maximum_value,

        tb.client_number AS sold_licence_client_number

    FROM bctsadmin_replication.bcts_timber_sale ts0

    INNER JOIN bctsadmin_replication.bcts_tenure_bidder tb
        ON ts0.forest_file_id = tb.forest_file_id
        AND ts0.auction_date = tb.auction_date

    INNER JOIN bcts_staging.fta_prov_forest_use pfu
        ON pfu.forest_file_id = ts0.forest_file_id

    INNER JOIN bcts_staging.fta_tenure_term tt
        ON tt.forest_file_id = pfu.forest_file_id

    WHERE UPPER(tb.sale_awarded_ind) = 'Y'
        AND ts0.no_sale_rationale_code IS NULL
        AND pfu.file_status_st IN (
            'HI',
            'HC',
            'LC',
            'HX',
            'HS',
            'HRS'
        )
        AND tt.legal_effective_dt BETWEEN
            TO_DATE('${start_date}', 'yyyy-MM-dd')
            AND TO_DATE('${end_date}', 'yyyy-MM-dd')
),

awarded_sale_info AS (
    SELECT
        ts1.forest_file_id,
        ts1.auction_date,
        ts1.total_upset_value AS cruise_total_upset_value,
        ts1.upset_rate AS scale_upset_rate,
        ts1.sale_volume AS sale_volume,
        tb.bonus_bid AS awarded_sale_bonus_bid,
        tb.bonus_offer AS awarded_sale_bonus_offer,

        CASE
            WHEN ts1.total_upset_value > 0 THEN
                ROUND(
                    ts1.total_upset_value + tb.bonus_offer,
                    2
                )
            ELSE
                ROUND(
                    (ts1.upset_rate + tb.bonus_bid) * ts1.sale_volume,
                    2
                )
        END AS awarded_licence_maximum_value,

        tb.client_number AS awarded_licence_client_number

    FROM bctsadmin_replication.bcts_timber_sale ts1

    INNER JOIN bctsadmin_replication.bcts_tenure_bidder tb
        ON ts1.forest_file_id = tb.forest_file_id
        AND ts1.auction_date = tb.auction_date

    WHERE UPPER(tb.sale_awarded_ind) = 'Y'
        AND ts1.auction_date BETWEEN
            TO_DATE('${start_date}', 'yyyy-MM-dd')
            AND TO_DATE('${end_date}', 'yyyy-MM-dd')
),

per_licence AS (
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
        ts.forest_file_id,
        pfu.file_type_code,

        CASE
            WHEN cc.description IS NULL THEN ts.bcts_category_code
            ELSE CONCAT(
                cc.description,
                ' (',
                ts.bcts_category_code,
                ')'
            )
        END AS bcts_category,

        ts.auction_date AS bcts_admin_auction_date,

        YEAR(ADD_MONTHS(ts.auction_date, 9)) AS auction_fiscal,

        CASE
            WHEN ts.auction_date IS NULL THEN NULL
            ELSE CONCAT(
                'Q',
                CAST(
                    CEIL(
                        MONTH(ADD_MONTHS(ts.auction_date, -3)) / 3.0
                    ) AS INT
                )
            )
        END AS auction_quarter,

        tt.legal_effective_dt AS fta_legal_effective_date,

        YEAR(ADD_MONTHS(tt.legal_effective_dt, 9)) AS legal_effective_fiscal,

        CASE
            WHEN tt.legal_effective_dt IS NULL THEN NULL
            ELSE CONCAT(
                'Q',
                CAST(
                    CEIL(
                        MONTH(ADD_MONTHS(tt.legal_effective_dt, -3)) / 3.0
                    ) AS INT
                )
            )
        END AS legal_effective_quarter,

        tt.tenure_term,

        sold_licence_bid_info.sale_volume AS sold_licence_volume,

        tt.tenure_term * sold_licence_bid_info.sale_volume
            AS sold_licence_volume_x_tenure_term,

        fc_sold.client_number AS sold_licence_client_number,

        CONCAT(
            COALESCE(CONCAT(fc_sold.legal_first_name, ' '), ''),
            COALESCE(CONCAT(fc_sold.legal_middle_name, ' '), ''),
            fc_sold.client_name
        ) AS sold_licence_client_name,

        awarded_sale_info.sale_volume AS awarded_licence_volume,

        CASE
            WHEN awarded_sale_info.sale_volume <= 5000 THEN '0.0 to 5,000.0 m3'
            WHEN awarded_sale_info.sale_volume <= 15000 THEN '5,000.1 to 15,000.0 m3'
            WHEN awarded_sale_info.sale_volume <= 30000 THEN '15,000.1 to 30,000.0 m3'
            WHEN awarded_sale_info.sale_volume <= 75000 THEN '30,000.1 to 75,000.0 m3'
            ELSE '75,000.1 m3 and above'
        END AS awarded_licence_volume_class,

        CASE
            WHEN awarded_sale_info.sale_volume <= 5000 THEN 1
            WHEN awarded_sale_info.sale_volume <= 15000 THEN 2
            WHEN awarded_sale_info.sale_volume <= 30000 THEN 3
            WHEN awarded_sale_info.sale_volume <= 75000 THEN 4
            ELSE 5
        END AS awarded_licence_volume_class_sort_order,

        tt.tenure_term * awarded_sale_info.sale_volume
            AS awarded_licence_volume_x_tenure_term,

        fc_awarded.client_number AS awarded_licence_client_number,

        CONCAT(
            COALESCE(CONCAT(fc_awarded.legal_first_name, ' '), ''),
            COALESCE(CONCAT(fc_awarded.legal_middle_name, ' '), ''),
            fc_awarded.client_name
        ) AS awarded_licence_client_name,

        CASE
            WHEN pfu.file_status_st IS NOT NULL THEN
                CONCAT(
                    tfsc.description,
                    ' (',
                    pfu.file_status_st,
                    ')'
                )
            ELSE NULL
        END AS fta_file_status,

        pfu.file_status_date AS fta_file_status_date,

        CASE
            WHEN ts.no_sale_rationale_code IS NULL
                AND pfu.file_status_st IN (
                    'HI',
                    'HC',
                    'LC',
                    'HX',
                    'HS',
                    'HRS'
                )
                AND tt.legal_effective_dt BETWEEN
                    TO_DATE('${start_date}', 'yyyy-MM-dd')
                    AND TO_DATE('${end_date}', 'yyyy-MM-dd')
            THEN 'Y'
            ELSE 'N'
        END AS sold_in_report_period,

        CASE
            WHEN ts.auction_date BETWEEN
                TO_DATE('${start_date}', 'yyyy-MM-dd')
                AND TO_DATE('${end_date}', 'yyyy-MM-dd')
            THEN 'Y'
            ELSE 'N'
        END AS auction_in_report_period,

        CASE
            WHEN pfu.file_type_code = 'B20' THEN NULL
            ELSE pfu.file_type_code
        END AS qa_non_b20_licence,

        CASE
            WHEN ts.auction_date > CURRENT_DATE()
                THEN 'Future auction (BCTS Admin)'
            WHEN (
                    sold_licence_bid_info.sold_licence_maximum_value IS NULL
                    OR sold_licence_bid_info.sold_licence_maximum_value = 0
                 )
                 AND (
                    awarded_sale_info.awarded_licence_maximum_value IS NULL
                    OR awarded_sale_info.awarded_licence_maximum_value = 0
                 )
                 AND ts.no_sale_rationale_code IS NULL
                THEN 'Auction result data missing (BCTS Admin)'
        END AS qa_auction_results_missing_bcts_admin

    FROM bcts_staging.fta_tenure_term tt

    LEFT JOIN bctsadmin_replication.bcts_timber_sale ts
        ON ts.forest_file_id = tt.forest_file_id

    LEFT JOIN bcts_staging.fta_prov_forest_use pfu
        ON ts.forest_file_id = pfu.forest_file_id

    LEFT JOIN bcts_staging.fta_org_unit ou
        ON pfu.bcts_org_unit = ou.org_unit_no

    LEFT JOIN bcts_staging.fta_tenure_file_status_code tfsc
        ON pfu.file_status_st = tfsc.tenure_file_status_code

    LEFT JOIN bctsadmin_replication.bcts_category_code cc
        ON ts.bcts_category_code = cc.bcts_category_code

    LEFT JOIN sold_licence_bid_info
        ON ts.forest_file_id = sold_licence_bid_info.forest_file_id
        AND ts.auction_date = sold_licence_bid_info.auction_date

    LEFT JOIN awarded_sale_info
        ON ts.forest_file_id = awarded_sale_info.forest_file_id
        AND ts.auction_date = awarded_sale_info.auction_date

    LEFT JOIN mofclient_replication.v_client_public fc_sold
        ON sold_licence_bid_info.sold_licence_client_number = fc_sold.client_number

    LEFT JOIN mofclient_replication.v_client_public fc_awarded
        ON awarded_sale_info.awarded_licence_client_number = fc_awarded.client_number

    WHERE (
        ts.no_sale_rationale_code IS NULL
        AND pfu.file_status_st IN ('HI', 'HC', 'LC', 'HX', 'HS', 'HRS')
        AND tt.legal_effective_dt BETWEEN
            TO_DATE('${start_date}', 'yyyy-MM-dd')
            AND TO_DATE('${end_date}', 'yyyy-MM-dd')
    )
    OR (
        ts.auction_date BETWEEN
            TO_DATE('${start_date}', 'yyyy-MM-dd')
            AND TO_DATE('${end_date}', 'yyyy-MM-dd')
    )
)

SELECT
    business_area_region_category,
    business_area_region,
    business_area,
    auction_fiscal,
    awarded_licence_volume_class,

    SUM(awarded_licence_volume) AS sum_awarded_licence_volume,

    SUM(awarded_licence_volume_x_tenure_term)
        AS sum_awarded_licence_volume_x_tenure_term,

    ROUND(
        SUM(awarded_licence_volume_x_tenure_term)
            / SUM(awarded_licence_volume),
        1
    ) AS weighted_tenure_term,

    COUNT(awarded_licence_volume) AS count_awarded_licences,

    TO_DATE('${start_date}', 'yyyy-MM-dd') AS report_start_date,
    TO_DATE('${end_date}', 'yyyy-MM-dd') AS report_end_date,

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

FROM per_licence

GROUP BY
    business_area_region_category,
    business_area_region,
    business_area,
    auction_fiscal,
    awarded_licence_volume_class,
    awarded_licence_volume_class_sort_order;

-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
    DROP TABLE IF EXISTS BCTS_STAGING.weighted_sale_term;
    CREATE TABLE BCTS_STAGING.weighted_sale_term
    AS SELECT * 
    FROM BCTS_STAGING.weighted_sale_term_hist
    WHERE report_end_date = (
	    SELECT MAX(report_end_date)
	    FROM BCTS_STAGING.weighted_sale_term_hist
    );

    DROP TABLE IF EXISTS BCTS_REPORTING.weighted_sale_term_hist;
    CREATE TABLE BCTS_REPORTING.weighted_sale_term_hist
    AS SELECT * 
    FROM BCTS_STAGING.weighted_sale_term_hist;

    DROP TABLE IF EXISTS BCTS_REPORTING.weighted_sale_term;
    CREATE TABLE BCTS_REPORTING.weighted_sale_term
    AS SELECT * 
    FROM BCTS_STAGING.weighted_sale_term;    
    