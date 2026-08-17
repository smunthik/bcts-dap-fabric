-- Create _hist table if it does not exist
CREATE TABLE IF NOT EXISTS bcts_staging.licence_issued_advertised_main_hist
(
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    management_unit STRING,
    district STRING,
    x_axis_date TIMESTAMP,
    x_axis_fiscal STRING,
    x_axis_quarter STRING,
    licence STRING,
    file_type_code STRING,
    auction_count_all_time_to_report_period_end BIGINT,
    first_auction_date TIMESTAMP,
    first_auction_fiscal STRING,
    first_auction_quarter STRING,
    first_bcts_category_code STRING,
    first_auction_volume DECIMAL(38,18),
    first_auction_category_a_and_1_volume DECIMAL(38,18),
    first_auction_category_2_and_4_volume DECIMAL(38,18),
    first_auction_volume_is_in_report_period DECIMAL(38,18),
    first_auction_category_a_and_1_volume_is_in_report_period DECIMAL(38,18),
    first_auction_category_2_and_4_volume_is_in_report_period DECIMAL(38,18),
    last_auction_date TIMESTAMP,
    last_auction_fiscal STRING,
    last_auction_quarter STRING,
    last_auction_bcts_category_code STRING,
    last_auction_volume DECIMAL(38,18),
    last_auction_category_a_and_1_volume DECIMAL(38,18),
    last_auction_category_2_and_4_volume DECIMAL(38,18),
    original_cat_2_and_4_readvertised_cat_a_and_1_volume DECIMAL(38,18),
    original_cat_a_and_1_readvertised_cat_2_and_4_volume DECIMAL(38,18),
    last_auction_no_sale_rationale STRING,
    last_auction_no_sale_volume DECIMAL(38,18),
    last_auction_no_sale_category_a_1_volume DECIMAL(38,18),
    last_auction_no_sale_category_2_4_volume DECIMAL(38,18),
    last_auction_no_sale STRING,
    last_auction_no_sale_cat_a STRING,
    last_auction_no_sale_cat_2_4 STRING,
    issued_licence_legal_effective_date DATE,
    issued_licence_legal_effective_fiscal STRING,
    issued_licence_legal_effective_quarter STRING,
    issued_licence_bcts_category_code STRING,
    issued_licence_volume DECIMAL(38,18),
    category_a_and_1_issued_volume DECIMAL(38,18),
    category_2_and_4_issued_volume DECIMAL(38,18),
    issued_licence_maximum_value DECIMAL(38,18),
    issued_licence_maximum_value_cat_a DECIMAL(38,18),
    issued_licence_maximum_value_cat_2_4 DECIMAL(38,18),
    issued_licence_client_number STRING,
    issued_licence_client_name STRING,
    issued_in_report_period STRING,
    issued_in_report_period_cat_a STRING,
    issued_in_report_period_cat_2_4 STRING,
    advertised_in_report_period STRING,
    total_volume_salvage_all_fire_year_lrm DECIMAL(38,18),
    fta_file_status STRING,
    fta_file_status_date DATE,
    bidder_count DECIMAL(38,18),
    report_start_date DATE,
    report_end_date DATE,
    fiscal_year DECIMAL(38,18),
    semi_monthly_report_start_date DATE,
    include_in_semi_monthly_report STRING,
    report_run_date DATE,
    report_run_timestamp TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS bcts_staging.licence_issued_advertised_official
(
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    business_area_code STRING,
    forest_file_id STRING,
    x_axis_date TIMESTAMP,
    x_axis_fiscal STRING,
    x_axis_quarter STRING,
    file_type_code STRING,
    auction_count_all_time_to_report_period_end BIGINT,
    first_auction_date TIMESTAMP,
    first_auction_fiscal STRING,
    first_auction_quarter STRING,
    first_bcts_category_code STRING,
    first_auction_volume DECIMAL(38,18),
    first_auction_category_a_and_1_volume DECIMAL(38,18),
    first_auction_category_2_and_4_volume DECIMAL(38,18),
    first_auction_volume_is_in_report_period DECIMAL(38,18),
    first_auction_category_a_and_1_volume_is_in_report_period DECIMAL(38,18),
    first_auction_category_2_and_4_volume_is_in_report_period DECIMAL(38,18),
    last_auction_date TIMESTAMP,
    last_auction_fiscal STRING,
    last_auction_quarter STRING,
    last_auction_bcts_category_code STRING,
    last_auction_volume DECIMAL(38,18),
    last_auction_category_a_and_1_volume DECIMAL(38,18),
    last_auction_category_2_and_4_volume DECIMAL(38,18),
    original_cat_2_and_4_readvertised_cat_a_and_1_volume DECIMAL(38,18),
    original_cat_a_and_1_readvertised_cat_2_and_4_volume DECIMAL(38,18),
    last_auction_no_sale_rationale STRING,
    last_auction_no_sale_volume DECIMAL(38,18),
    last_auction_no_sale_category_a_1_volume DECIMAL(38,18),
    last_auction_no_sale_category_2_4_volume DECIMAL(38,18),
    last_auction_no_sale STRING,
    last_auction_no_sale_cat_a STRING,
    last_auction_no_sale_cat_2_4 STRING,
    issued_licence_legal_effective_date DATE,
    issued_licence_legal_effective_fiscal STRING,
    issued_licence_legal_effective_quarter STRING,
    issued_licence_bcts_category_code STRING,
    issued_licence_volume DECIMAL(38,18),
    category_a_and_1_issued_volume DECIMAL(38,18),
    category_2_and_4_issued_volume DECIMAL(38,18),
    issued_licence_maximum_value DECIMAL(38,18),
    issued_licence_maximum_value_cat_a DECIMAL(38,18),
    issued_licence_maximum_value_cat_2_4 DECIMAL(38,18),
    issued_licence_client_number STRING,
    issued_licence_client_name STRING,
    issued_in_report_period STRING,
    issued_in_report_period_cat_a STRING,
    issued_in_report_period_cat_2_4 STRING,
    advertised_in_report_period STRING,
    fta_file_status STRING,
    fta_file_status_date DATE,
    bidder_count DECIMAL(38,18),
    report_start_date DATE,
    report_end_date DATE,
    fiscal_year DECIMAL(38,18),
    report_run_date DATE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS bcts_staging.currently_in_market_hist
(
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    business_area_code STRING,
    nav_name STRING,
    field_team STRING,
    licence_id STRING,
    tenure STRING,
    lrm_category_code STRING,
    lrm_category_description STRING,
    lrm_category STRING,
    lrm_tender_posted_done_status STRING,
    lrm_tender_posted_done_date TIMESTAMP,
    lrm_licence_awarded_done_date TIMESTAMP,
    lrm_auction_done_date TIMESTAMP,
    lrm_total_volume DECIMAL(38,18),
    lrm_total_volume_cat_a DECIMAL(38,18),
    lrm_total_volume_cat_2_4 DECIMAL(38,18),
    licn_seq_nbr DECIMAL(38,18),
    include_in_currently_in_market_report STRING,
    in_currentlyinmarket_query STRING,
    on_bc_bid STRING,
    data_error STRING,
    report_end_date DATE,
    report_run_date DATE,
    report_run_timestamp TIMESTAMP
)
USING DELTA;

CREATE OR REPLACE VIEW bcts_staging.v_licence_issued_advertised_lrm AS

WITH licence AS
(
    SELECT
        licn_seq_nbr,

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
                WHEN TSO_NAME = 'Seaward' THEN 'Seaward-Tlasta'
                ELSE TSO_NAME
            END,
            ' (',
            TSO_CODE,
            ')'
        ) AS business_area,

        licence_id,
        nav_name AS management_unit,
        district_name AS district,
        licn_category_id AS category_id_lrm,
        category AS category_lrm

    FROM lrm_replication.v_licence
),

total_licence_info AS
(
    SELECT
        b.licn_seq_nbr,
        COUNT(*) AS count_all_blocks_in_licence,
        SUM(b.blal_rw_vol) AS lrm_rw_volume,
        SUM(b.cruise_vol) AS lrm_cruise_volume,
        SUM(COALESCE(b.cruise_vol, 0) + COALESCE(b.blal_rw_vol, 0)) AS lrm_total_volume

    FROM lrm_replication.v_block b

    GROUP BY
        b.licn_seq_nbr
),

salvage_all_fire_year AS
(
    SELECT
        b.licn_seq_nbr,
        COUNT(*) AS count_blocks_salvage_any_fire_year,
        SUM(b.blal_rw_vol) AS lrm_rw_volume_salvage_all_fire_years,
        SUM(b.cruise_vol) AS lrm_cruise_volume_salvage_all_fire_years,
        SUM(COALESCE(b.cruise_vol, 0) + COALESCE(b.blal_rw_vol, 0)) AS lrm_total_volume_salvage_all_fire_years

    FROM lrm_replication.v_block b

    INNER JOIN
    (
        SELECT DISTINCT
            cutb_seq_nbr

        FROM lrm_replication.v_block_activity_all

        WHERE activity_class = 'CSB'
          AND actt_key_ind LIKE 'SFIRE%'
    ) block_with_any_sfire_year
        ON b.cutb_seq_nbr = block_with_any_sfire_year.cutb_seq_nbr

    GROUP BY
        b.licn_seq_nbr
),

salvage21fire AS
(
    SELECT
        b.licn_seq_nbr,
        COUNT(*) AS count_blocks_salvage_21_fire,
        SUM(b.blal_rw_vol) AS lrm_rw_volume_salvage_2021_fire,
        SUM(b.cruise_vol) AS lrm_cruise_volume_salvage_2021_fire,
        SUM(COALESCE(b.cruise_vol, 0) + COALESCE(b.blal_rw_vol, 0)) AS lrm_total_volume_salvage_2021_fire

    FROM lrm_replication.v_block b

    INNER JOIN
    (
        SELECT DISTINCT
            cutb_seq_nbr

        FROM lrm_replication.v_block_activity_all

        WHERE activity_class = 'CSB'
          AND actt_key_ind = 'SFIRE21'
    ) block_with_sfire21
        ON b.cutb_seq_nbr = block_with_sfire21.cutb_seq_nbr

    GROUP BY
        b.licn_seq_nbr
),

salvage22fire AS
(
    SELECT
        b.licn_seq_nbr,
        COUNT(*) AS count_blocks_salvage_22_fire,
        SUM(b.blal_rw_vol) AS lrm_rw_volume_salvage_2022_fire,
        SUM(b.cruise_vol) AS lrm_cruise_volume_salvage_2022_fire,
        SUM(COALESCE(b.cruise_vol, 0) + COALESCE(b.blal_rw_vol, 0)) AS lrm_total_volume_salvage_2022_fire

    FROM lrm_replication.v_block b

    INNER JOIN
    (
        SELECT DISTINCT
            cutb_seq_nbr

        FROM lrm_replication.v_block_activity_all

        WHERE activity_class = 'CSB'
          AND actt_key_ind = 'SFIRE22'
    ) block_with_sfire22
        ON b.cutb_seq_nbr = block_with_sfire22.cutb_seq_nbr

    GROUP BY
        b.licn_seq_nbr
),

salvage23fire AS
(
    SELECT
        b.licn_seq_nbr,
        COUNT(*) AS count_blocks_salvage_23_fire,
        SUM(b.blal_rw_vol) AS lrm_rw_volume_salvage_2023_fire,
        SUM(b.cruise_vol) AS lrm_cruise_volume_salvage_2023_fire,
        SUM(COALESCE(b.cruise_vol, 0) + COALESCE(b.blal_rw_vol, 0)) AS lrm_total_volume_salvage_2023_fire

    FROM lrm_replication.v_block b

    INNER JOIN
    (
        SELECT DISTINCT
            cutb_seq_nbr

        FROM lrm_replication.v_block_activity_all

        WHERE activity_class = 'CSB'
          AND actt_key_ind = 'SFIRE23'
    ) block_with_sfire23
        ON b.cutb_seq_nbr = block_with_sfire23.cutb_seq_nbr

    GROUP BY
        b.licn_seq_nbr
),

salvage24fire AS
(
    SELECT
        b.licn_seq_nbr,
        COUNT(*) AS count_blocks_salvage_24_fire,
        SUM(b.blal_rw_vol) AS lrm_rw_volume_salvage_2024_fire,
        SUM(b.cruise_vol) AS lrm_cruise_volume_salvage_2024_fire,
        SUM(COALESCE(b.cruise_vol, 0) + COALESCE(b.blal_rw_vol, 0)) AS lrm_total_volume_salvage_2024_fire

    FROM lrm_replication.v_block b

    INNER JOIN
    (
        SELECT DISTINCT
            cutb_seq_nbr

        FROM lrm_replication.v_block_activity_all

        WHERE activity_class = 'CSB'
          AND actt_key_ind = 'SFIRE24'
    ) block_with_sfire24
        ON b.cutb_seq_nbr = block_with_sfire24.cutb_seq_nbr

    GROUP BY
        b.licn_seq_nbr
),

salvage25fire AS
(
    SELECT
        b.licn_seq_nbr,
        COUNT(*) AS count_blocks_salvage_25_fire,
        SUM(b.blal_rw_vol) AS lrm_rw_volume_salvage_2025_fire,
        SUM(b.cruise_vol) AS lrm_cruise_volume_salvage_2025_fire,
        SUM(COALESCE(b.cruise_vol, 0) + COALESCE(b.blal_rw_vol, 0)) AS lrm_total_volume_salvage_2025_fire

    FROM lrm_replication.v_block b

    INNER JOIN
    (
        SELECT DISTINCT
            cutb_seq_nbr

        FROM lrm_replication.v_block_activity_all

        WHERE activity_class = 'CSB'
          AND actt_key_ind = 'SFIRE25'
    ) block_with_sfire25
        ON b.cutb_seq_nbr = block_with_sfire25.cutb_seq_nbr

    GROUP BY
        b.licn_seq_nbr
)

SELECT
    licence.business_area_region_category,
    licence.business_area_region,
    licence.business_area,
    licence.licence_id,
    licence.management_unit,
    licence.district,
    licence.category_id_lrm,
    total_licence_info.lrm_total_volume,
    total_licence_info.count_all_blocks_in_licence,
    salvage_all_fire_year.lrm_total_volume_salvage_all_fire_years,
    salvage_all_fire_year.count_blocks_salvage_any_fire_year,
    salvage21fire.lrm_total_volume_salvage_2021_fire,
    salvage21fire.count_blocks_salvage_21_fire,
    salvage22fire.lrm_total_volume_salvage_2022_fire,
    salvage22fire.count_blocks_salvage_22_fire,
    salvage23fire.lrm_total_volume_salvage_2023_fire,
    salvage23fire.count_blocks_salvage_23_fire,
    salvage24fire.lrm_total_volume_salvage_2024_fire,
    salvage24fire.count_blocks_salvage_24_fire,
    salvage25fire.lrm_total_volume_salvage_2025_fire,
    salvage25fire.count_blocks_salvage_25_fire,
    licence.licn_seq_nbr

FROM licence

LEFT JOIN total_licence_info
    ON licence.licn_seq_nbr = total_licence_info.licn_seq_nbr

LEFT JOIN salvage_all_fire_year
    ON licence.licn_seq_nbr = salvage_all_fire_year.licn_seq_nbr

LEFT JOIN salvage21fire
    ON licence.licn_seq_nbr = salvage21fire.licn_seq_nbr

LEFT JOIN salvage22fire
    ON licence.licn_seq_nbr = salvage22fire.licn_seq_nbr

LEFT JOIN salvage23fire
    ON licence.licn_seq_nbr = salvage23fire.licn_seq_nbr

LEFT JOIN salvage24fire
    ON licence.licn_seq_nbr = salvage24fire.licn_seq_nbr

LEFT JOIN salvage25fire
    ON licence.licn_seq_nbr = salvage25fire.licn_seq_nbr;


-- Report exists check is done on bcts_reporting table 
-- If report exists in bcts_staging.***_hist table, clear the staging table before inserting new records 
delete from bcts_staging.licence_issued_advertised_main_hist
where  report_start_date = '${report_start_date}'
and report_end_date = '${report_end_date}';

delete from bcts_staging.licence_issued_advertised_official;

-- Populate staging table
INSERT INTO bcts_staging.licence_issued_advertised_official
(
    business_area_region_category,
    business_area_region,
    business_area,
    business_area_code,
    forest_file_id,
    x_axis_date,
    x_axis_fiscal,
    x_axis_quarter,
    file_type_code,
    auction_count_all_time_to_report_period_end,
    first_auction_date,
    first_auction_fiscal,
    first_auction_quarter,
    first_bcts_category_code,
    first_auction_volume,
    first_auction_category_a_and_1_volume,
    first_auction_category_2_and_4_volume,
    first_auction_volume_is_in_report_period,
    first_auction_category_a_and_1_volume_is_in_report_period,
    first_auction_category_2_and_4_volume_is_in_report_period,
    last_auction_date,
    last_auction_fiscal,
    last_auction_quarter,
    last_auction_bcts_category_code,
    last_auction_volume,
    last_auction_category_a_and_1_volume,
    last_auction_category_2_and_4_volume,
    original_cat_2_and_4_readvertised_cat_a_and_1_volume,
    original_cat_a_and_1_readvertised_cat_2_and_4_volume,
    last_auction_no_sale_rationale,
    last_auction_no_sale_volume,
    last_auction_no_sale_category_a_1_volume,
    last_auction_no_sale_category_2_4_volume,
    last_auction_no_sale,
    last_auction_no_sale_cat_a,
    last_auction_no_sale_cat_2_4,
    issued_licence_legal_effective_date,
    issued_licence_legal_effective_fiscal,
    issued_licence_legal_effective_quarter,
    issued_licence_bcts_category_code,
    issued_licence_volume,
    category_a_and_1_issued_volume,
    category_2_and_4_issued_volume,
    issued_licence_maximum_value,
    issued_licence_maximum_value_cat_a,
    issued_licence_maximum_value_cat_2_4,
    issued_licence_client_number,
    issued_licence_client_name,
    issued_in_report_period,
    issued_in_report_period_cat_a,
    issued_in_report_period_cat_2_4,
    advertised_in_report_period,
    fta_file_status,
    fta_file_status_date,
    bidder_count,
    report_start_date,
    report_end_date,
    fiscal_year,
    report_run_date
)

WITH issued AS
(
    SELECT
        ts0.forest_file_id,
        tt.legal_effective_dt AS issued_licence_legal_effective_date,

        CASE
            WHEN tt.legal_effective_dt IS NULL THEN NULL
            ELSE CONCAT(
                'Fiscal ',
                CAST(YEAR(ADD_MONTHS(tt.legal_effective_dt, 9)) - 1 AS STRING),
                '/',
                CAST(YEAR(ADD_MONTHS(tt.legal_effective_dt, 9)) AS STRING)
            )
        END AS issued_licence_legal_effective_fiscal,

        CASE
            WHEN tt.legal_effective_dt IS NULL THEN NULL
            ELSE CONCAT(
                'Q',
                CAST(CEIL(MONTH(ADD_MONTHS(tt.legal_effective_dt, -3)) / 3.0) AS STRING)
            )
        END AS issued_licence_legal_effective_quarter,

        ts0.auction_date,
        ts0.bcts_category_code AS issued_licence_bcts_category_code,
        ts0.sale_volume AS issued_licence_volume,

        CASE
            WHEN ts0.bcts_category_code IN ('2', '4')
                THEN ts0.sale_volume
        END AS category_2_and_4_issued_volume,

        CASE
            WHEN ts0.bcts_category_code IN ('A', '1')
                THEN ts0.sale_volume
        END AS category_a_and_1_issued_volume,

        ts0.total_upset_value AS cruise_total_upset_value,
        ts0.upset_rate AS scale_upset_rate,
        tb.bonus_bid AS issued_licence_bonus_bid,
        tb.bonus_offer AS issued_licence_bonus_offer,

        CASE
            WHEN ts0.total_upset_value > 0
                THEN ROUND(ts0.total_upset_value + tb.bonus_offer, 2)
            ELSE ROUND((ts0.upset_rate + tb.bonus_bid) * ts0.sale_volume, 2)
        END AS issued_licence_maximum_value,

        tb.client_number AS issued_licence_client_number,

        CONCAT(
            COALESCE(CONCAT(fc.legal_first_name, ' '), ''),
            COALESCE(CONCAT(fc.legal_middle_name, ' '), ''),
            COALESCE(fc.client_name, '')
        ) AS issued_licence_client_name

    FROM bcts_staging.the_bcts_timber_sale ts0

    INNER JOIN bcts_staging.the_bcts_tenure_bidder tb
        ON ts0.forest_file_id = tb.forest_file_id
        AND ts0.auction_date = tb.auction_date
        AND ts0.no_sale_rationale_code IS NULL
        AND UPPER(tb.sale_awarded_ind) = 'Y'

    INNER JOIN bcts_staging.fta_prov_forest_use pfu
        ON pfu.forest_file_id = ts0.forest_file_id
        AND pfu.file_status_st IN ('HI', 'HC', 'LC', 'HX', 'HS', 'HRS')

    INNER JOIN bcts_staging.fta_tenure_term tt
        ON pfu.forest_file_id = tt.forest_file_id
        AND tt.legal_effective_dt BETWEEN TO_DATE($report_start_date, 'yyyy-MM-dd')
                                      AND TO_DATE($report_end_date, 'yyyy-MM-dd')

    INNER JOIN bcts_staging.the_v_client_public fc
        ON tb.client_number = fc.client_number
),

advertised AS
(
    SELECT
        ts.forest_file_id,
        all_auctions_to_date.first_auction_date,

        CASE
            WHEN all_auctions_to_date.first_auction_date IS NULL THEN NULL
            ELSE CONCAT(
                'Fiscal ',
                CAST(YEAR(ADD_MONTHS(all_auctions_to_date.first_auction_date, 9)) - 1 AS STRING),
                '/',
                CAST(YEAR(ADD_MONTHS(all_auctions_to_date.first_auction_date, 9)) AS STRING)
            )
        END AS first_auction_fiscal,

        CASE
            WHEN all_auctions_to_date.first_auction_date IS NULL THEN NULL
            ELSE CONCAT(
                'Q',
                CAST(CEIL(MONTH(ADD_MONTHS(all_auctions_to_date.first_auction_date, -3)) / 3.0) AS STRING)
            )
        END AS first_auction_quarter,

        all_auctions_to_date.last_auction_date,

        CASE
            WHEN all_auctions_to_date.last_auction_date IS NULL THEN NULL
            ELSE CONCAT(
                'Fiscal ',
                CAST(YEAR(ADD_MONTHS(all_auctions_to_date.last_auction_date, 9)) - 1 AS STRING),
                '/',
                CAST(YEAR(ADD_MONTHS(all_auctions_to_date.last_auction_date, 9)) AS STRING)
            )
        END AS last_auction_fiscal,

        CASE
            WHEN all_auctions_to_date.last_auction_date IS NULL THEN NULL
            ELSE CONCAT(
                'Q',
                CAST(CEIL(MONTH(ADD_MONTHS(all_auctions_to_date.last_auction_date, -3)) / 3.0) AS STRING)
            )
        END AS last_auction_quarter,

        all_auctions_to_date.auction_count_all_time_to_report_period_end,
        first_auction.bcts_category_code AS first_bcts_category_code,
        ts.bcts_category_code AS last_auction_bcts_category_code,

        CASE
            WHEN first_auction.bcts_category_code = ts.bcts_category_code
                THEN 'No category change'
            ELSE 'Category change'
        END AS category_change,

        CASE
            WHEN nsrc.description IS NULL THEN ts.no_sale_rationale_code
            ELSE CONCAT(nsrc.description, ' (', ts.no_sale_rationale_code, ')')
        END AS last_auction_no_sale_rationale,

        CAST(
            CASE
                WHEN ts.no_sale_rationale_code IS NULL THEN NULL
                ELSE ts.sale_volume
            END AS DECIMAL(38,18)
        ) AS last_auction_no_sale_volume,

        CASE
            WHEN ts.no_sale_rationale_code IS NOT NULL
                AND ts.bcts_category_code IN ('2', '4')
                THEN ts.sale_volume
        END AS last_auction_no_sale_category_2_4_volume,

        CASE
            WHEN ts.no_sale_rationale_code IS NOT NULL
                AND ts.bcts_category_code IN ('A', '1')
                THEN ts.sale_volume
        END AS last_auction_no_sale_category_a_1_volume,

        first_auction.sale_volume AS first_auction_volume,
        ts.sale_volume AS last_auction_volume,

        CASE
            WHEN first_auction.bcts_category_code IN ('2', '4')
                THEN first_auction.sale_volume
        END AS first_auction_category_2_and_4_volume,

        CASE
            WHEN first_auction.bcts_category_code IN ('A', '1')
                THEN first_auction.sale_volume
        END AS first_auction_category_a_and_1_volume,

        CASE
            WHEN ts.bcts_category_code IN ('2', '4')
                THEN ts.sale_volume
        END AS last_auction_category_2_and_4_volume,

        CASE
            WHEN ts.bcts_category_code IN ('A', '1')
                THEN ts.sale_volume
        END AS last_auction_category_a_and_1_volume,

        CASE
            WHEN first_auction.bcts_category_code IN ('2', '4')
                AND ts.bcts_category_code IN ('A', '1')
                THEN ts.sale_volume
        END AS original_cat_2_and_4_readvertised_cat_a_and_1_volume,

        CASE
            WHEN first_auction.bcts_category_code IN ('A', '1')
                AND ts.bcts_category_code IN ('2', '4')
                THEN ts.sale_volume
        END AS original_cat_a_and_1_readvertised_cat_2_and_4_volume

    FROM bcts_staging.the_bcts_timber_sale ts

    LEFT JOIN bcts_staging.the_no_sale_rationale_code nsrc
        ON ts.no_sale_rationale_code = nsrc.no_sale_rationale_code

    INNER JOIN
    (
        SELECT
            ts.forest_file_id,
            COUNT(ts.auction_date) AS auction_count_all_time_to_report_period_end,
            MIN(ts.auction_date) AS first_auction_date,
            MAX(ts.auction_date) AS last_auction_date

        FROM bcts_staging.the_bcts_timber_sale ts

        WHERE COALESCE(no_sale_rationale_code, ' ') <> 'TB'
          AND ts.auction_date <= TO_DATE($report_end_date, 'yyyy-MM-dd')

        GROUP BY
            ts.forest_file_id
    ) all_auctions_to_date
        ON ts.forest_file_id = all_auctions_to_date.forest_file_id
        AND ts.auction_date = all_auctions_to_date.last_auction_date

    INNER JOIN
    (
        SELECT
            forest_file_id,
            auction_date,
            bcts_category_code,
            sale_volume

        FROM bcts_staging.the_bcts_timber_sale
    ) first_auction
        ON ts.forest_file_id = first_auction.forest_file_id
        AND all_auctions_to_date.first_auction_date = first_auction.auction_date
),

advertised_in_report_period AS
(
    SELECT DISTINCT
        forest_file_id

    FROM bcts_staging.the_bcts_timber_sale

    WHERE auction_date BETWEEN TO_DATE($report_start_date, 'yyyy-MM-dd')
                           AND TO_DATE($report_end_date, 'yyyy-MM-dd')
),

bidder_count AS
(
    SELECT
        forest_file_id,
        auction_date,
        COUNT(DISTINCT client_number) AS bidder_count

    FROM bcts_staging.the_bcts_tenure_bidder

    GROUP BY
        forest_file_id,
        auction_date
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

    CASE
        WHEN ou.org_unit_code IS NULL THEN NULL
        ELSE REPLACE(
            CONCAT(
                CASE
                    WHEN ou.org_unit_name = 'Seaward Timber Sales Office' THEN 'Seaward-Tlasta'
                    ELSE ou.org_unit_name
                END,
                ' (',
                ou.org_unit_code,
                ')'
            ),
            ' Timber Sales Office',
            ''
        )
    END AS business_area,

    ou.org_unit_code AS business_area_code,
    ts.forest_file_id,

    CASE
        WHEN issued.issued_licence_legal_effective_date IS NULL THEN advertised.last_auction_date
        ELSE issued.issued_licence_legal_effective_date
    END AS x_axis_date,

    CASE
        WHEN issued.issued_licence_legal_effective_fiscal IS NULL THEN advertised.last_auction_fiscal
        ELSE issued.issued_licence_legal_effective_fiscal
    END AS x_axis_fiscal,

    CASE
        WHEN issued.issued_licence_legal_effective_quarter IS NULL THEN advertised.last_auction_quarter
        ELSE issued.issued_licence_legal_effective_quarter
    END AS x_axis_quarter,

    pfu.file_type_code,
    advertised.auction_count_all_time_to_report_period_end,
    advertised.first_auction_date,
    advertised.first_auction_fiscal,
    advertised.first_auction_quarter,
    advertised.first_bcts_category_code,
    advertised.first_auction_volume,
    advertised.first_auction_category_a_and_1_volume,
    advertised.first_auction_category_2_and_4_volume,

    CASE
        WHEN advertised.first_auction_date BETWEEN TO_DATE($report_start_date, 'yyyy-MM-dd')
                                               AND TO_DATE($report_end_date, 'yyyy-MM-dd')
            THEN advertised.first_auction_volume
    END AS first_auction_volume_is_in_report_period,

    CASE
        WHEN advertised.first_auction_date BETWEEN TO_DATE($report_start_date, 'yyyy-MM-dd')
                                               AND TO_DATE($report_end_date, 'yyyy-MM-dd')
            THEN advertised.first_auction_category_a_and_1_volume
    END AS first_auction_category_a_and_1_volume_is_in_report_period,

    CASE
        WHEN advertised.first_auction_date BETWEEN TO_DATE($report_start_date, 'yyyy-MM-dd')
                                               AND TO_DATE($report_end_date, 'yyyy-MM-dd')
            THEN advertised.first_auction_category_2_and_4_volume
    END AS first_auction_category_2_and_4_volume_is_in_report_period,

    advertised.last_auction_date,
    advertised.last_auction_fiscal,
    advertised.last_auction_quarter,
    advertised.last_auction_bcts_category_code,
    advertised.last_auction_volume,
    advertised.last_auction_category_a_and_1_volume,
    advertised.last_auction_category_2_and_4_volume,
    advertised.original_cat_2_and_4_readvertised_cat_a_and_1_volume,
    advertised.original_cat_a_and_1_readvertised_cat_2_and_4_volume,
    advertised.last_auction_no_sale_rationale,
    advertised.last_auction_no_sale_volume,
    advertised.last_auction_no_sale_category_a_1_volume,
    advertised.last_auction_no_sale_category_2_4_volume,

    CASE
        WHEN advertised.last_auction_no_sale_rationale IS NULL THEN NULL
        ELSE 'Y'
    END AS last_auction_no_sale,

    CASE
        WHEN advertised.last_auction_no_sale_rationale IS NULL THEN NULL
        WHEN advertised.last_auction_no_sale_category_a_1_volume IS NULL THEN NULL
        ELSE 'Y'
    END AS last_auction_no_sale_cat_a,

    CASE
        WHEN advertised.last_auction_no_sale_rationale IS NULL THEN NULL
        WHEN advertised.last_auction_no_sale_category_2_4_volume IS NULL THEN NULL
        ELSE 'Y'
    END AS last_auction_no_sale_cat_2_4,

    issued.issued_licence_legal_effective_date,
    issued.issued_licence_legal_effective_fiscal,
    issued.issued_licence_legal_effective_quarter,
    issued.issued_licence_bcts_category_code,
    issued.issued_licence_volume,
    issued.category_a_and_1_issued_volume,
    issued.category_2_and_4_issued_volume,
    issued.issued_licence_maximum_value,

    CASE
        WHEN issued.issued_licence_bcts_category_code IN ('A')
            THEN issued.issued_licence_maximum_value
    END AS issued_licence_maximum_value_cat_a,

    CASE
        WHEN issued.issued_licence_bcts_category_code IN ('2', '4')
            THEN issued.issued_licence_maximum_value
    END AS issued_licence_maximum_value_cat_2_4,

    issued.issued_licence_client_number,
    issued.issued_licence_client_name,

    CASE
        WHEN issued.forest_file_id IS NULL THEN NULL
        ELSE 'Y'
    END AS issued_in_report_period,

    CASE
        WHEN issued.forest_file_id IS NULL THEN NULL
        WHEN issued.category_a_and_1_issued_volume IS NULL THEN NULL
        ELSE 'Y'
    END AS issued_in_report_period_cat_a,

    CASE
        WHEN issued.forest_file_id IS NULL THEN NULL
        WHEN issued.category_2_and_4_issued_volume IS NULL THEN NULL
        ELSE 'Y'
    END AS issued_in_report_period_cat_2_4,

    CASE
        WHEN advertised_in_report_period.forest_file_id IS NULL THEN NULL
        ELSE 'Y'
    END AS advertised_in_report_period,

    CASE
        WHEN tfsc.description IS NOT NULL
            THEN CONCAT(tfsc.description, ' (', pfu.file_status_st, ')')
        ELSE pfu.file_status_st
    END AS fta_file_status,

    pfu.file_status_date AS fta_file_status_date,
    bc.bidder_count,
    TO_DATE($report_start_date, 'yyyy-MM-dd') AS report_start_date,
    TO_DATE($report_end_date, 'yyyy-MM-dd') AS report_end_date,

    CASE
        WHEN MONTH(CURRENT_DATE()) >= 4 THEN YEAR(CURRENT_DATE())
        ELSE YEAR(CURRENT_DATE()) - 1
    END AS fiscal_year,

    TO_DATE(
        FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Vancouver')
    ) AS report_run_date

FROM bcts_staging.fta_prov_forest_use pfu

INNER JOIN bcts_staging.the_org_unit ou
    ON pfu.bcts_org_unit = ou.org_unit_no

LEFT JOIN bcts_staging.the_bcts_timber_sale ts
    ON pfu.forest_file_id = ts.forest_file_id

LEFT JOIN bcts_staging.fta_tenure_term tt
    ON pfu.forest_file_id = tt.forest_file_id

LEFT JOIN bcts_staging.fta_tenure_file_status_code tfsc
    ON pfu.file_status_st = tfsc.tenure_file_status_code

LEFT JOIN issued
    ON ts.forest_file_id = issued.forest_file_id

LEFT JOIN advertised
    ON ts.forest_file_id = advertised.forest_file_id

LEFT JOIN advertised_in_report_period
    ON ts.forest_file_id = advertised_in_report_period.forest_file_id

LEFT JOIN bidder_count bc
    ON ts.forest_file_id = bc.forest_file_id
    AND advertised.last_auction_date = bc.auction_date

WHERE 1 = 1
    AND (
        issued.forest_file_id IS NOT NULL
        OR advertised_in_report_period.forest_file_id IS NOT NULL
    )
    AND ts.forest_file_id IS NOT NULL

UNION

SELECT
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

    CASE
        WHEN ou.org_unit_code IS NULL THEN NULL
        ELSE REPLACE(
            CONCAT(
                CASE
                    WHEN ou.org_unit_name = 'Seaward Timber Sales Office' THEN 'Seaward-Tlasta'
                    ELSE ou.org_unit_name
                END,
                ' (',
                ou.org_unit_code,
                ')'
            ),
            ' Timber Sales Office',
            ''
        )
    END AS business_area,

    ou.org_unit_code AS business_area_code,
    CAST(NULL AS STRING) AS forest_file_id,
    CAST(NULL AS TIMESTAMP) AS x_axis_date,
    CAST(NULL AS STRING) AS x_axis_fiscal,
    CAST(NULL AS STRING) AS x_axis_quarter,
    CAST(NULL AS STRING) AS file_type_code,
    CAST(NULL AS BIGINT) AS auction_count_all_time_to_report_period_end,
    CAST(NULL AS TIMESTAMP) AS first_auction_date,
    CAST(NULL AS STRING) AS first_auction_fiscal,
    CAST(NULL AS STRING) AS first_auction_quarter,
    CAST(NULL AS STRING) AS first_bcts_category_code,
    CAST(NULL AS DECIMAL(38,18)) AS first_auction_volume,
    CAST(NULL AS DECIMAL(38,18)) AS first_auction_category_a_and_1_volume,
    CAST(NULL AS DECIMAL(38,18)) AS first_auction_category_2_and_4_volume,
    CAST(NULL AS DECIMAL(38,18)) AS first_auction_volume_is_in_report_period,
    CAST(NULL AS DECIMAL(38,18)) AS first_auction_category_a_and_1_volume_is_in_report_period,
    CAST(NULL AS DECIMAL(38,18)) AS first_auction_category_2_and_4_volume_is_in_report_period,
    CAST(NULL AS TIMESTAMP) AS last_auction_date,
    CAST(NULL AS STRING) AS last_auction_fiscal,
    CAST(NULL AS STRING) AS last_auction_quarter,
    CAST(NULL AS STRING) AS last_auction_bcts_category_code,
    CAST(NULL AS DECIMAL(38,18)) AS last_auction_volume,
    CAST(NULL AS DECIMAL(38,18)) AS last_auction_category_a_and_1_volume,
    CAST(NULL AS DECIMAL(38,18)) AS last_auction_category_2_and_4_volume,
    CAST(NULL AS DECIMAL(38,18)) AS original_cat_2_and_4_readvertised_cat_a_and_1_volume,
    CAST(NULL AS DECIMAL(38,18)) AS original_cat_a_and_1_readvertised_cat_2_and_4_volume,
    CAST(NULL AS STRING) AS last_auction_no_sale_rationale,
    CAST(NULL AS DECIMAL(38,18)) AS last_auction_no_sale_volume,
    CAST(NULL AS DECIMAL(38,18)) AS last_auction_no_sale_category_a_1_volume,
    CAST(NULL AS DECIMAL(38,18)) AS last_auction_no_sale_category_2_4_volume,
    CAST(NULL AS STRING) AS last_auction_no_sale,
    CAST(NULL AS STRING) AS last_auction_no_sale_cat_a,
    CAST(NULL AS STRING) AS last_auction_no_sale_cat_2_4,
    CAST(NULL AS DATE) AS issued_licence_legal_effective_date,
    CAST(NULL AS STRING) AS issued_licence_legal_effective_fiscal,
    CAST(NULL AS STRING) AS issued_licence_legal_effective_quarter,
    CAST(NULL AS STRING) AS issued_licence_bcts_category_code,
    CAST(NULL AS DECIMAL(38,18)) AS issued_licence_volume,
    CAST(NULL AS DECIMAL(38,18)) AS category_a_and_1_issued_volume,
    CAST(NULL AS DECIMAL(38,18)) AS category_2_and_4_issued_volume,
    CAST(NULL AS DECIMAL(38,18)) AS issued_licence_maximum_value,
    CAST(NULL AS DECIMAL(38,18)) AS issued_licence_maximum_value_cat_a,
    CAST(NULL AS DECIMAL(38,18)) AS issued_licence_maximum_value_cat_2_4,
    CAST(NULL AS STRING) AS issued_licence_client_number,
    CAST(NULL AS STRING) AS issued_licence_client_name,
    CAST(NULL AS STRING) AS issued_in_report_period,
    CAST(NULL AS STRING) AS issued_in_report_period_cat_a,
    CAST(NULL AS STRING) AS issued_in_report_period_cat_2_4,
    CAST(NULL AS STRING) AS advertised_in_report_period,
    CAST(NULL AS STRING) AS fta_file_status,
    CAST(NULL AS DATE) AS fta_file_status_date,
    CAST(NULL AS DECIMAL(38,18)) AS bidder_count,
    TO_DATE($report_start_date, 'yyyy-MM-dd') AS report_start_date,
    TO_DATE($report_end_date, 'yyyy-MM-dd') AS report_end_date,

    CASE
        WHEN MONTH(CURRENT_DATE()) >= 4 THEN YEAR(CURRENT_DATE())
        ELSE YEAR(CURRENT_DATE()) - 1
    END AS fiscal_year,

    TO_DATE(
        FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Vancouver')
    ) AS report_run_date

FROM bcts_staging.the_org_unit ou

WHERE ou.org_unit_no IN
(
    1808,
    1812,
    1816,
    1813,
    1815,
    1814,
    1810,
    1811,
    1817,
    1807,
    1809,
    1818
);

INSERT INTO bcts_staging.licence_issued_advertised_main_hist
(
    business_area_region_category,
    business_area_region,
    business_area,
    management_unit,
    district,
    x_axis_date,
    x_axis_fiscal,
    x_axis_quarter,
    licence,
    file_type_code,
    auction_count_all_time_to_report_period_end,
    first_auction_date,
    first_auction_fiscal,
    first_auction_quarter,
    first_bcts_category_code,
    first_auction_volume,
    first_auction_category_a_and_1_volume,
    first_auction_category_2_and_4_volume,
    first_auction_volume_is_in_report_period,
    first_auction_category_a_and_1_volume_is_in_report_period,
    first_auction_category_2_and_4_volume_is_in_report_period,
    last_auction_date,
    last_auction_fiscal,
    last_auction_quarter,
    last_auction_bcts_category_code,
    last_auction_volume,
    last_auction_category_a_and_1_volume,
    last_auction_category_2_and_4_volume,
    original_cat_2_and_4_readvertised_cat_a_and_1_volume,
    original_cat_a_and_1_readvertised_cat_2_and_4_volume,
    last_auction_no_sale_rationale,
    last_auction_no_sale_volume,
    last_auction_no_sale_category_a_1_volume,
    last_auction_no_sale_category_2_4_volume,
    last_auction_no_sale,
    last_auction_no_sale_cat_a,
    last_auction_no_sale_cat_2_4,
    issued_licence_legal_effective_date,
    issued_licence_legal_effective_fiscal,
    issued_licence_legal_effective_quarter,
    issued_licence_bcts_category_code,
    issued_licence_volume,
    category_a_and_1_issued_volume,
    category_2_and_4_issued_volume,
    issued_licence_maximum_value,
    issued_licence_maximum_value_cat_a,
    issued_licence_maximum_value_cat_2_4,
    issued_licence_client_number,
    issued_licence_client_name,
    issued_in_report_period,
    issued_in_report_period_cat_a,
    issued_in_report_period_cat_2_4,
    advertised_in_report_period,
    total_volume_salvage_all_fire_year_lrm,
    fta_file_status,
    fta_file_status_date,
    bidder_count,
    report_start_date,
    report_end_date,
    fiscal_year,
    semi_monthly_report_start_date,
    include_in_semi_monthly_report,
    report_run_date,
    report_run_timestamp
)
SELECT
    official.business_area_region_category,
    official.business_area_region,
    official.business_area,
    lrm.management_unit,
    lrm.district,
    official.x_axis_date,
    official.x_axis_fiscal,
    official.x_axis_quarter,
    official.forest_file_id AS licence,
    official.file_type_code,
    official.auction_count_all_time_to_report_period_end,
    official.first_auction_date,
    official.first_auction_fiscal,
    official.first_auction_quarter,
    official.first_bcts_category_code,
    official.first_auction_volume,
    official.first_auction_category_a_and_1_volume,
    official.first_auction_category_2_and_4_volume,
    official.first_auction_volume_is_in_report_period,
    official.first_auction_category_a_and_1_volume_is_in_report_period,
    official.first_auction_category_2_and_4_volume_is_in_report_period,
    official.last_auction_date,
    official.last_auction_fiscal,
    official.last_auction_quarter,
    official.last_auction_bcts_category_code,
    official.last_auction_volume,
    official.last_auction_category_a_and_1_volume,
    official.last_auction_category_2_and_4_volume,
    official.original_cat_2_and_4_readvertised_cat_a_and_1_volume,
    official.original_cat_a_and_1_readvertised_cat_2_and_4_volume,
    official.last_auction_no_sale_rationale,
    official.last_auction_no_sale_volume,
    official.last_auction_no_sale_category_a_1_volume,
    official.last_auction_no_sale_category_2_4_volume,
    official.last_auction_no_sale,
    official.last_auction_no_sale_cat_a,
    official.last_auction_no_sale_cat_2_4,
    official.issued_licence_legal_effective_date,
    official.issued_licence_legal_effective_fiscal,
    official.issued_licence_legal_effective_quarter,
    official.issued_licence_bcts_category_code,
    official.issued_licence_volume,
    official.category_a_and_1_issued_volume,
    official.category_2_and_4_issued_volume,
    official.issued_licence_maximum_value,
    official.issued_licence_maximum_value_cat_a,
    official.issued_licence_maximum_value_cat_2_4,
    official.issued_licence_client_number,
    official.issued_licence_client_name,
    official.issued_in_report_period,
    official.issued_in_report_period_cat_a,
    official.issued_in_report_period_cat_2_4,
    official.advertised_in_report_period,
    ROUND(lrm.lrm_total_volume_salvage_all_fire_years) AS total_volume_salvage_all_fire_year_lrm,
    official.fta_file_status,
    official.fta_file_status_date,
    official.bidder_count,
    official.report_start_date,
    official.report_end_date,
    official.fiscal_year,

    CASE
        WHEN DAY(official.report_end_date) = 15 THEN
            DATE_SUB(official.report_end_date, 14)
        ELSE
            DATE_ADD(TRUNC(official.report_end_date, 'MM'), 15)
    END AS semi_monthly_report_start_date,

    CASE
        WHEN official.x_axis_date >=
            CASE
                WHEN DAY(official.report_end_date) = 15 THEN
                    DATE_SUB(official.report_end_date, 14)
                ELSE
                    DATE_ADD(TRUNC(official.report_end_date, 'MM'), 15)
            END
        THEN 'Y'
        ELSE 'N'
    END AS include_in_semi_monthly_report,

    TO_DATE(
        FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Vancouver')
    ) AS report_run_date,

    FROM_UTC_TIMESTAMP(
        CURRENT_TIMESTAMP(),
        'America/Vancouver'
    ) AS report_run_timestamp

FROM bcts_staging.licence_issued_advertised_official official

LEFT JOIN bcts_staging.v_licence_issued_advertised_lrm lrm
    ON official.forest_file_id = lrm.licence_id;

INSERT INTO bcts_staging.currently_in_market_hist
(
    business_area_region_category,
    business_area_region,
    business_area,
    business_area_code,
    nav_name,
    field_team,
    licence_id,
    tenure,
    lrm_category_code,
    lrm_category_description,
    lrm_category,
    lrm_tender_posted_done_status,
    lrm_tender_posted_done_date,
    lrm_licence_awarded_done_date,
    lrm_auction_done_date,
    lrm_total_volume,
    lrm_total_volume_cat_a,
    lrm_total_volume_cat_2_4,
    licn_seq_nbr,
    include_in_currently_in_market_report,
    in_currentlyinmarket_query,
    on_bc_bid,
    data_error,
    report_end_date,
    report_run_date,
    report_run_timestamp
)

WITH tenpost AS
(
    SELECT
        a.licn_seq_nbr,

        MAX(
            CASE
                WHEN atype.actt_key_ind = 'TENPOST'
                    AND a.acti_status_ind = 'D'
                THEN a.acti_status_ind
            END
        ) AS lrm_tender_posted_done_status,

        MAX(
            CASE
                WHEN atype.actt_key_ind = 'TENPOST'
                    AND a.acti_status_ind = 'D'
                THEN a.acti_status_date
            END
        ) AS lrm_tender_posted_done_date

    FROM lrm_replication.activity_class ac

    INNER JOIN lrm_replication.activity_type atype
        ON ac.accl_seq_nbr = atype.accl_seq_nbr
        AND ac.divi_div_nbr = atype.divi_div_nbr

    INNER JOIN lrm_replication.activity a
        ON atype.actt_seq_nbr = a.actt_seq_nbr

    WHERE atype.actt_key_ind = 'TENPOST'
      AND ac.accl_key_ind = 'CML'

    GROUP BY
        a.licn_seq_nbr

    HAVING
        MAX(
            CASE
                WHEN atype.actt_key_ind = 'TENPOST'
                    AND a.acti_status_ind = 'D'
                THEN a.acti_status_ind
            END
        ) = 'D'
),

ha AS
(
    SELECT
        a.licn_seq_nbr,

        MAX(
            CASE
                WHEN atype.actt_key_ind = 'HA'
                    AND a.acti_status_ind = 'D'
                THEN a.acti_status_ind
            END
        ) AS lrm_licence_awarded_status,

        MAX(
            CASE
                WHEN atype.actt_key_ind = 'HA'
                    AND a.acti_status_ind = 'D'
                THEN a.acti_status_date
            END
        ) AS lrm_licence_awarded_done_date

    FROM lrm_replication.activity_class ac

    INNER JOIN lrm_replication.activity_type atype
        ON ac.accl_seq_nbr = atype.accl_seq_nbr
        AND ac.divi_div_nbr = atype.divi_div_nbr

    INNER JOIN lrm_replication.activity a
        ON atype.actt_seq_nbr = a.actt_seq_nbr

    WHERE atype.actt_key_ind = 'HA'
      AND ac.accl_key_ind = 'CML'

    GROUP BY
        a.licn_seq_nbr

    HAVING
        MAX(
            CASE
                WHEN atype.actt_key_ind = 'HA'
                    AND a.acti_status_ind = 'D'
                THEN a.acti_status_ind
            END
        ) = 'D'
),

auc AS
(
    SELECT
        a.licn_seq_nbr,

        MAX(
            CASE
                WHEN atype.actt_key_ind = 'AUC'
                    AND a.acti_status_ind = 'D'
                THEN a.acti_status_ind
            END
        ) AS lrm_auction_status,

        MAX(
            CASE
                WHEN atype.actt_key_ind = 'AUC'
                    AND a.acti_status_ind = 'D'
                THEN a.acti_status_date
            END
        ) AS lrm_auction_done_date

    FROM lrm_replication.activity_class ac

    INNER JOIN lrm_replication.activity_type atype
        ON ac.accl_seq_nbr = atype.accl_seq_nbr
        AND ac.divi_div_nbr = atype.divi_div_nbr

    INNER JOIN lrm_replication.activity a
        ON atype.actt_seq_nbr = a.actt_seq_nbr

    WHERE atype.actt_key_ind = 'AUC'
      AND ac.accl_key_ind = 'CML'

    GROUP BY
        a.licn_seq_nbr

    HAVING
        MAX(
            CASE
                WHEN atype.actt_key_ind = 'AUC'
                    AND a.acti_status_ind = 'D'
                THEN a.acti_status_ind
            END
        ) = 'D'
),

lv AS
(
    SELECT
        b.licn_seq_nbr,
        SUM(b.cruise_vol) AS lrm_cruise_volume,
        SUM(b.blal_rw_vol) AS lrm_rw_volume,
        SUM(COALESCE(b.cruise_vol, 0) + COALESCE(b.blal_rw_vol, 0)) AS lrm_total_volume

    FROM lrm_replication.v_block b

    GROUP BY
        b.licn_seq_nbr
)

SELECT
    CASE
        WHEN d.divi_short_code IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN', 'TCC', 'TKA', 'TKO', 'TOC')
            THEN 'Interior'
        WHEN d.divi_short_code IN ('TCH', 'TST', 'TSG')
            THEN 'Coast'
    END AS business_area_region_category,

    CASE
        WHEN d.divi_short_code IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN')
            THEN 'North Interior'
        WHEN d.divi_short_code IN ('TCC', 'TKA', 'TKO', 'TOC')
            THEN 'South Interior'
        WHEN d.divi_short_code IN ('TCH', 'TST', 'TSG')
            THEN 'Coast'
    END AS business_area_region,

    CONCAT(
        CASE
            WHEN d.divi_division_name = 'Seaward' THEN 'Seaward-Tlasta'
            ELSE d.divi_division_name
        END,
        ' (',
        l.tso_code,
        ')'
    ) AS business_area,

    l.tso_code AS business_area_code,
    l.nav_name,
    l.field_team,
    l.licence_id,
    l.tenure,
    l.licn_category_id AS lrm_category_code,
    l.category AS lrm_category_description,

    CASE
        WHEN l.category IS NULL THEN l.licn_category_id
        ELSE CONCAT(l.category, ' (', l.licn_category_id, ')')
    END AS lrm_category,

    tenpost.lrm_tender_posted_done_status,
    tenpost.lrm_tender_posted_done_date,
    ha.lrm_licence_awarded_done_date,
    auc.lrm_auction_done_date,
    lv.lrm_total_volume,

    CASE
        WHEN l.licn_category_id NOT IN ('2', '4')
            THEN lv.lrm_total_volume
    END AS lrm_total_volume_cat_a,

    CASE
        WHEN l.licn_category_id IN ('2', '4')
            THEN lv.lrm_total_volume
    END AS lrm_total_volume_cat_2_4,

    l.licn_seq_nbr,
    CAST(NULL AS STRING) AS include_in_currently_in_market_report,
    'Y' AS in_currentlyinmarket_query,
    CAST(NULL AS STRING) AS on_bc_bid,
    CAST(NULL AS STRING) AS data_error,
    TO_DATE($report_end_date, 'yyyy-MM-dd') AS report_end_date,

    TO_DATE(
        FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Vancouver')
    ) AS report_run_date,

    FROM_UTC_TIMESTAMP(
        CURRENT_TIMESTAMP(),
        'America/Vancouver'
    ) AS report_run_timestamp

FROM lrm_replication.division d

LEFT JOIN lrm_replication.v_licence l
    ON d.divi_short_code = l.tso_code

LEFT JOIN tenpost
    ON l.licn_seq_nbr = tenpost.licn_seq_nbr

LEFT JOIN ha
    ON l.licn_seq_nbr = ha.licn_seq_nbr

LEFT JOIN auc
    ON l.licn_seq_nbr = auc.licn_seq_nbr

LEFT JOIN lv
    ON l.licn_seq_nbr = lv.licn_seq_nbr

WHERE 1 = 1
    AND tenpost.licn_seq_nbr IS NOT NULL
    AND tenpost.lrm_tender_posted_done_date <= TO_DATE($report_end_date, 'yyyy-MM-dd')
    AND (
        ha.licn_seq_nbr IS NULL
        OR ha.lrm_licence_awarded_done_date > TO_DATE($report_end_date, 'yyyy-MM-dd')
    )
    AND (
        NOT (
            auc.lrm_auction_done_date BETWEEN tenpost.lrm_tender_posted_done_date
                                          AND TO_DATE($report_end_date, 'yyyy-MM-dd')
        )
        OR auc.lrm_auction_done_date IS NULL
    )

UNION

SELECT
    CASE
        WHEN d.divi_short_code IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN', 'TCC', 'TKA', 'TKO', 'TOC')
            THEN 'Interior'
        WHEN d.divi_short_code IN ('TCH', 'TST', 'TSG')
            THEN 'Coast'
    END AS business_area_region_category,

    CASE
        WHEN d.divi_short_code IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN')
            THEN 'North Interior'
        WHEN d.divi_short_code IN ('TCC', 'TKA', 'TKO', 'TOC')
            THEN 'South Interior'
        WHEN d.divi_short_code IN ('TCH', 'TST', 'TSG')
            THEN 'Coast'
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
    CAST(NULL AS STRING) AS nav_name,
    CAST(NULL AS STRING) AS field_team,
    CAST(NULL AS STRING) AS licence_id,
    CAST(NULL AS STRING) AS tenure,
    CAST(NULL AS STRING) AS lrm_category_code,
    CAST(NULL AS STRING) AS lrm_category_description,
    CAST(NULL AS STRING) AS lrm_category,
    CAST(NULL AS STRING) AS lrm_tender_posted_done_status,
    CAST(NULL AS TIMESTAMP) AS lrm_tender_posted_done_date,
    CAST(NULL AS TIMESTAMP) AS lrm_licence_awarded_done_date,
    CAST(NULL AS TIMESTAMP) AS lrm_auction_done_date,
    CAST(NULL AS DECIMAL(38,18)) AS lrm_total_volume,
    CAST(NULL AS DECIMAL(38,18)) AS lrm_total_volume_cat_a,
    CAST(NULL AS DECIMAL(38,18)) AS lrm_total_volume_cat_2_4,
    CAST(NULL AS DECIMAL(38,18)) AS licn_seq_nbr,
    'Include' AS include_in_currently_in_market_report,
    'Y' AS in_currentlyinmarket_query,
    'Not applicable' AS on_bc_bid,
    'Not applicable' AS data_error,
    TO_DATE($report_end_date, 'yyyy-MM-dd') AS report_end_date,

    TO_DATE(
        FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Vancouver')
    ) AS report_run_date,

    FROM_UTC_TIMESTAMP(
        CURRENT_TIMESTAMP(),
        'America/Vancouver'
    ) AS report_run_timestamp

FROM lrm_replication.division d;

-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
    
DROP TABLE IF EXISTS bcts_staging.currently_in_market;

CREATE TABLE bcts_staging.currently_in_market
USING DELTA
AS
SELECT *
FROM bcts_staging.currently_in_market_hist
WHERE report_run_timestamp =
(
    SELECT MAX(report_run_timestamp)
    FROM bcts_staging.currently_in_market_hist
);


DROP TABLE IF EXISTS bcts_reporting.currently_in_market;

CREATE TABLE bcts_reporting.currently_in_market
USING DELTA
AS
SELECT *
FROM bcts_staging.currently_in_market;


DROP TABLE IF EXISTS bcts_reporting.currently_in_market_hist;

CREATE TABLE bcts_reporting.currently_in_market_hist
USING DELTA
AS
SELECT *
FROM bcts_staging.currently_in_market_hist;


DROP TABLE IF EXISTS bcts_staging.licence_issued_advertised_main;

CREATE TABLE bcts_staging.licence_issued_advertised_main
USING DELTA
AS
SELECT *
FROM bcts_staging.licence_issued_advertised_main_hist
WHERE report_end_date =
(
    SELECT MAX(report_end_date)
    FROM bcts_staging.licence_issued_advertised_main_hist
);


DROP TABLE IF EXISTS bcts_reporting.licence_issued_advertised_main;

CREATE TABLE bcts_reporting.licence_issued_advertised_main
USING DELTA
AS
SELECT *
FROM bcts_staging.licence_issued_advertised_main;


DROP TABLE IF EXISTS bcts_reporting.licence_issued_advertised_main_hist;

CREATE TABLE bcts_reporting.licence_issued_advertised_main_hist
USING DELTA
AS
SELECT *
FROM bcts_staging.licence_issued_advertised_main_hist;


DROP TABLE IF EXISTS bcts_staging.currently_in_market_summary;

CREATE TABLE bcts_staging.currently_in_market_summary
USING DELTA
AS
SELECT
    business_area_region_category,
    business_area_region,
    business_area,
    SUM(lrm_total_volume) AS `Currently in Market`,
    SUM(lrm_total_volume_cat_2_4) AS `Volume: Value Added`,
    COUNT(licence_id) AS `Number of Auctions`,
    COUNT(lrm_total_volume_cat_2_4) AS `Number of Auctions Value Added`
FROM bcts_staging.currently_in_market
GROUP BY
    business_area_region_category,
    business_area_region,
    business_area;


DROP TABLE IF EXISTS bcts_staging.ytd_auctioned_issued_not_awarded;

CREATE TABLE bcts_staging.ytd_auctioned_issued_not_awarded
USING DELTA
AS
SELECT
    business_area_region_category,
    business_area_region,
    business_area,
    SUM(first_auction_volume_is_in_report_period) AS `Auctioned (First Auction is in Report Period)`,
    SUM(issued_licence_volume) AS `Licence Issued`,
    SUM(last_auction_no_sale_volume) AS `Not Awarded (Last Auction in Report Period is No Sale)`,
    SUM(first_auction_category_2_and_4_volume_is_in_report_period) AS `Auctioned (First Auction is in Report Period): Category 2/4`,
    SUM(category_2_and_4_issued_volume) AS `Licence Issued: Cat 2/4`,
    SUM(last_auction_no_sale_category_2_4_volume) AS `Not Awarded: Category 2/4`
FROM bcts_staging.licence_issued_advertised_main
GROUP BY
    business_area_region_category,
    business_area_region,
    business_area;


DROP TABLE IF EXISTS bcts_staging.recent_auction_results;

CREATE TABLE bcts_staging.recent_auction_results
USING DELTA
AS
WITH temp AS
(
    SELECT
        d.business_area_region_category,
        d.business_area_region,
        d.business_area,
        SUM(COALESCE(m.issued_licence_volume, 0)) AS `Licence Issued`,
        SUM(COALESCE(m.category_2_and_4_issued_volume, 0)) AS `Licence Issued: Value Added`,
        SUM(COALESCE(m.last_auction_no_sale_volume, 0)) AS `Not Awarded`,
        SUM(COALESCE(m.last_auction_no_sale_category_2_4_volume, 0)) AS `Not Awarded: Value Added`
    FROM bcts_reporting.v_forest_division d

    LEFT JOIN bcts_staging.licence_issued_advertised_main m
        ON m.business_area_region_category = d.business_area_region_category
        AND m.business_area_region = d.business_area_region
        AND m.business_area = d.business_area
        AND m.include_in_semi_monthly_report = 'Y'

    GROUP BY
        d.business_area_region_category,
        d.business_area_region,
        d.business_area
)

SELECT
    *,
    GREATEST(
        `Licence Issued`,
        `Licence Issued: Value Added`,
        `Not Awarded`,
        `Not Awarded: Value Added`
    ) * 1.1 AS y_max_business_area,

    GREATEST(
        SUM(`Licence Issued`) OVER (PARTITION BY business_area_region),
        SUM(`Licence Issued: Value Added`) OVER (PARTITION BY business_area_region),
        SUM(`Not Awarded`) OVER (PARTITION BY business_area_region),
        SUM(`Not Awarded: Value Added`) OVER (PARTITION BY business_area_region)
    ) * 1.1 AS y_max_region,

    CASE
        WHEN business_area_region = 'North Interior' THEN 1
        WHEN business_area_region = 'South Interior' THEN 2
        ELSE 3
    END AS business_area_region_sort_order

FROM temp;


DROP TABLE IF EXISTS bcts_staging.bcts_performance_report_not_awarded_details;

CREATE TABLE bcts_staging.bcts_performance_report_not_awarded_details
USING DELTA
AS
SELECT
    business_area AS `Business Area`,
    business_area_region AS `Business Area Region`,
    licence,
    CASE
        WHEN last_auction_bcts_category_code IN ('2', '4') THEN 'Value Added'
        ELSE '-'
    END AS `Value Added`,
    CASE
        WHEN total_volume_salvage_all_fire_year_lrm > 0 THEN 'Fire salvage'
        ELSE '-'
    END AS `Includes Fire Salvage`,
    last_auction_date AS auction,
    FORMAT_NUMBER(last_auction_no_sale_volume, 0) AS `Volume (cubic metres)`,
    last_auction_no_sale_rationale AS `No Sale Rationale`,
    CASE
        WHEN business_area_region = 'North Interior' THEN 1
        WHEN business_area_region = 'South Interior' THEN 2
        ELSE 3
    END AS business_area_region_sort_order
FROM bcts_staging.licence_issued_advertised_main
WHERE advertised_in_report_period = 'Y'
  AND last_auction_no_sale_rationale IS NOT NULL
  AND last_auction_date BETWEEN semi_monthly_report_start_date AND report_end_date;


DROP TABLE IF EXISTS bcts_staging.bcts_performance_report_licence_issued_details;

CREATE TABLE bcts_staging.bcts_performance_report_licence_issued_details
USING DELTA
AS
SELECT
    business_area AS `Business Area`,
    business_area_region AS `Business Area Region`,
    licence,
    CASE
        WHEN issued_licence_bcts_category_code IN ('2', '4') THEN 'Value Added'
        ELSE '-'
    END AS `Value Added`,
    CASE
        WHEN total_volume_salvage_all_fire_year_lrm > 0 THEN 'Fire salvage'
        ELSE '-'
    END AS `Includes Fire Salvage`,
    bidder_count AS `# of Bidders`,
    issued_licence_legal_effective_date AS issued,
    FORMAT_NUMBER(issued_licence_volume, 0) AS `Volume (cubic metres)`,
    CONCAT('$', FORMAT_NUMBER(issued_licence_maximum_value, 0)) AS `Max Value`,
    issued_licence_client_name AS client,
    CASE
        WHEN business_area_region = 'North Interior' THEN 1
        WHEN business_area_region = 'South Interior' THEN 2
        ELSE 3
    END AS business_area_region_sort_order
FROM bcts_staging.licence_issued_advertised_main
WHERE issued_in_report_period = 'Y'
  AND issued_licence_legal_effective_date BETWEEN semi_monthly_report_start_date AND report_end_date;


DROP TABLE IF EXISTS bcts_staging.recent_auctions_chart_2;

CREATE TABLE bcts_staging.recent_auctions_chart_2
USING DELTA
AS
SELECT
    'Licence Issued' AS metric,
    COALESCE(SUM(issued_licence_volume), 0) - COALESCE(SUM(category_2_and_4_issued_volume), 0) AS `Total Excluding Value Added`,
    COALESCE(SUM(issued_licence_volume), 0) AS `Total`,
    COALESCE(SUM(category_2_and_4_issued_volume), 0) AS `Value Added`
FROM bcts_staging.licence_issued_advertised_main
WHERE include_in_semi_monthly_report = 'Y'

UNION ALL

SELECT
    'Not Awarded' AS metric,
    COALESCE(SUM(last_auction_no_sale_volume), 0) - COALESCE(SUM(last_auction_no_sale_category_2_4_volume), 0) AS `Total Excluding Value Added`,
    COALESCE(SUM(last_auction_no_sale_volume), 0) AS `Total`,
    COALESCE(SUM(last_auction_no_sale_category_2_4_volume), 0) AS `Value Added`
FROM bcts_staging.licence_issued_advertised_main
WHERE include_in_semi_monthly_report = 'Y';


DROP TABLE IF EXISTS bcts_staging.bcts_performance_report_ytd_all;

CREATE TABLE bcts_staging.bcts_performance_report_ytd_all
USING DELTA
AS
WITH base AS
(
    SELECT
        st.business_area_region_category,
        st.business_area_region,
        st.business_area,

        COALESCE(st.q1_ytd_sales_target_volume, 0) AS `Q1 Licence Issued Target`,
        COALESCE(st.q2_ytd_sales_target_volume, 0) AS `Q2 Licence Issued Target`,
        COALESCE(st.q3_ytd_sales_target_volume, 0) AS `Q3 Licence Issued Target`,
        COALESCE(st.total_fiscal_year_sales_target_volume, 0) AS `Fiscal Year Licence Issued Target`,

        COALESCE(st.q1_ytd_sales_target_volume_cat_4, 0) AS `Q1 Licence Issued Target: Value Added`,
        COALESCE(st.q2_ytd_sales_target_volume_cat_4, 0) AS `Q2 Licence Issued Target: Value Added`,
        COALESCE(st.q3_ytd_sales_target_volume_cat_4, 0) AS `Q3 Licence Issued Target: Value Added`,
        COALESCE(st.total_fiscal_year_sales_target_volume_cat_4, 0) AS `Fiscal Year Licence Issued Target: Value Added`,

        COALESCE(cms.`Currently in Market`, 0) AS `Currently in Market`,
        COALESCE(ain.`Auctioned (First Auction is in Report Period)`, 0) AS `Auctioned`,
        COALESCE(ain.`Licence Issued`, 0) AS `Licence Issued`,
        COALESCE(ain.`Not Awarded (Last Auction in Report Period is No Sale)`, 0) AS `Not Awarded`,

        COALESCE(cms.`Volume: Value Added`, 0) AS `Currently in Market: Value Added`,
        COALESCE(ain.`Auctioned (First Auction is in Report Period): Category 2/4`, 0) AS `Auctioned: Value Added`,
        COALESCE(ain.`Licence Issued: Cat 2/4`, 0) AS `Licence Issued: Value Added`,
        COALESCE(ain.`Not Awarded: Category 2/4`, 0) AS `Not Awarded: Value Added`

    FROM bcts_staging.bcts_sales_targets st

    LEFT JOIN bcts_staging.currently_in_market_summary cms
        ON cms.business_area_region = st.business_area_region
        AND cms.business_area = st.business_area

    LEFT JOIN bcts_staging.ytd_auctioned_issued_not_awarded ain
        ON cms.business_area_region = ain.business_area_region
        AND cms.business_area = ain.business_area
)

SELECT
    *,

    GREATEST(
        SUM(`Licence Issued`) OVER (PARTITION BY business_area),
        SUM(`Licence Issued: Value Added`) OVER (PARTITION BY business_area),
        SUM(`Not Awarded`) OVER (PARTITION BY business_area),
        SUM(`Not Awarded: Value Added`) OVER (PARTITION BY business_area),
        SUM(`Auctioned`) OVER (PARTITION BY business_area),
        SUM(`Auctioned: Value Added`) OVER (PARTITION BY business_area),
        MAX(
            CASE
                WHEN MONTH(DATE_SUB(CURRENT_DATE(), 15)) BETWEEN 4 AND 6 THEN `Q1 Licence Issued Target`
                WHEN MONTH(DATE_SUB(CURRENT_DATE(), 15)) BETWEEN 7 AND 9 THEN `Q2 Licence Issued Target`
                WHEN MONTH(DATE_SUB(CURRENT_DATE(), 15)) BETWEEN 10 AND 12 THEN `Q3 Licence Issued Target`
                ELSE `Fiscal Year Licence Issued Target`
            END
        ) OVER (PARTITION BY business_area)
    ) * 1.1 AS y_max_business_area,

    GREATEST(
        SUM(`Licence Issued`) OVER (PARTITION BY business_area_region),
        SUM(`Licence Issued: Value Added`) OVER (PARTITION BY business_area_region),
        SUM(`Not Awarded`) OVER (PARTITION BY business_area_region),
        SUM(`Not Awarded: Value Added`) OVER (PARTITION BY business_area_region),
        SUM(`Auctioned`) OVER (PARTITION BY business_area_region),
        SUM(`Auctioned: Value Added`) OVER (PARTITION BY business_area_region),
        SUM(
            CASE
                WHEN MONTH(DATE_SUB(CURRENT_DATE(), 15)) BETWEEN 4 AND 6 THEN `Q1 Licence Issued Target`
                WHEN MONTH(DATE_SUB(CURRENT_DATE(), 15)) BETWEEN 7 AND 9 THEN `Q2 Licence Issued Target`
                WHEN MONTH(DATE_SUB(CURRENT_DATE(), 15)) BETWEEN 10 AND 12 THEN `Q3 Licence Issued Target`
                ELSE `Fiscal Year Licence Issued Target`
            END
        ) OVER (PARTITION BY business_area_region)
    ) * 1.1 AS y_max_region,

    GREATEST(
        SUM(`Licence Issued`) OVER (),
        SUM(`Licence Issued: Value Added`) OVER (),
        SUM(`Not Awarded`) OVER (),
        SUM(`Not Awarded: Value Added`) OVER (),
        SUM(`Auctioned`) OVER (),
        SUM(`Auctioned: Value Added`) OVER (),
        SUM(
            CASE
                WHEN MONTH(DATE_SUB(CURRENT_DATE(), 15)) BETWEEN 4 AND 6 THEN `Q1 Licence Issued Target`
                WHEN MONTH(DATE_SUB(CURRENT_DATE(), 15)) BETWEEN 7 AND 9 THEN `Q2 Licence Issued Target`
                WHEN MONTH(DATE_SUB(CURRENT_DATE(), 15)) BETWEEN 10 AND 12 THEN `Q3 Licence Issued Target`
                ELSE `Fiscal Year Licence Issued Target`
            END
        ) OVER ()
    ) * 1.1 AS y_max_province,

    CASE
        WHEN business_area_region = 'North Interior' THEN 1
        WHEN business_area_region = 'South Interior' THEN 2
        ELSE 3
    END AS business_area_region_sort_order,

    CASE
        WHEN business_area_region_category = 'Interior' THEN 1
        ELSE 2
    END AS business_area_region_category_sort_order

FROM base;


DROP TABLE IF EXISTS bcts_staging.bcts_volume_summary_chart_2;

CREATE TABLE bcts_staging.bcts_volume_summary_chart_2
USING DELTA
AS
SELECT
    'Q1 Licence Issued Target' AS metric,
    SUM(`Q1 Licence Issued Target`) AS `Total`,
    SUM(`Q1 Licence Issued Target: Value Added`) AS `Value Added`
FROM bcts_staging.bcts_performance_report_ytd_all

UNION ALL

SELECT
    'Q2 Licence Issued Target' AS metric,
    SUM(`Q2 Licence Issued Target`) AS `Total`,
    SUM(`Q2 Licence Issued Target: Value Added`) AS `Value Added`
FROM bcts_staging.bcts_performance_report_ytd_all

UNION ALL

SELECT
    'Q3 Licence Issued Target' AS metric,
    SUM(`Q3 Licence Issued Target`) AS `Total`,
    SUM(`Q3 Licence Issued Target: Value Added`) AS `Value Added`
FROM bcts_staging.bcts_performance_report_ytd_all

UNION ALL

SELECT
    'Fiscal Year Licence Issued Target' AS metric,
    SUM(`Fiscal Year Licence Issued Target`) AS `Total`,
    SUM(`Fiscal Year Licence Issued Target: Value Added`) AS `Value Added`
FROM bcts_staging.bcts_performance_report_ytd_all

UNION ALL

SELECT
    'Auctioned' AS metric,
    SUM(`Auctioned`) AS `Total`,
    SUM(`Auctioned: Value Added`) AS `Value Added`
FROM bcts_staging.bcts_performance_report_ytd_all

UNION ALL

SELECT
    'Currently in Market' AS metric,
    SUM(`Currently in Market`) AS `Total`,
    SUM(`Currently in Market: Value Added`) AS `Value Added`
FROM bcts_staging.bcts_performance_report_ytd_all

UNION ALL

SELECT
    'Licence Issued' AS metric,
    SUM(`Licence Issued`) AS `Total`,
    SUM(`Licence Issued: Value Added`) AS `Value Added`
FROM bcts_staging.bcts_performance_report_ytd_all

UNION ALL

SELECT
    'Not Awarded' AS metric,
    SUM(`Not Awarded`) AS `Total`,
    SUM(`Not Awarded: Value Added`) AS `Value Added`
FROM bcts_staging.bcts_performance_report_ytd_all;


DROP TABLE IF EXISTS bcts_reporting.bcts_performance_report_ytd_all;

CREATE TABLE bcts_reporting.bcts_performance_report_ytd_all
USING DELTA
AS
SELECT *
FROM bcts_staging.bcts_performance_report_ytd_all;


DROP TABLE IF EXISTS bcts_reporting.bcts_volume_summary_chart_2;

CREATE TABLE bcts_reporting.bcts_volume_summary_chart_2
USING DELTA
AS
SELECT *
FROM bcts_staging.bcts_volume_summary_chart_2;


DROP TABLE IF EXISTS bcts_reporting.recent_auctions_chart_2;

CREATE TABLE bcts_reporting.recent_auctions_chart_2
USING DELTA
AS
SELECT *
FROM bcts_staging.recent_auctions_chart_2;


DROP TABLE IF EXISTS bcts_reporting.recent_auction_results;

CREATE TABLE bcts_reporting.recent_auction_results
USING DELTA
AS
SELECT *
FROM bcts_staging.recent_auction_results;


DROP TABLE IF EXISTS bcts_reporting.bcts_performance_report_not_awarded_details;

CREATE TABLE bcts_reporting.bcts_performance_report_not_awarded_details
USING DELTA
AS
SELECT *
FROM bcts_staging.bcts_performance_report_not_awarded_details;


DROP TABLE IF EXISTS bcts_reporting.bcts_performance_report_licence_issued_details;

CREATE TABLE bcts_reporting.bcts_performance_report_licence_issued_details
USING DELTA
AS
SELECT *
FROM bcts_staging.bcts_performance_report_licence_issued_details;


DROP TABLE IF EXISTS bcts_staging.bcts_performance_report_current_prev_ytd_issued_lic_volume;

CREATE TABLE bcts_staging.bcts_performance_report_current_prev_ytd_issued_lic_volume
USING DELTA
AS
WITH current_ytd AS
(
    SELECT
        business_area_region_category,
        business_area_region,
        business_area,
        COALESCE(SUM(issued_licence_volume), 0) AS `Current YTD Licence Issued`,
        COALESCE(SUM(category_2_and_4_issued_volume), 0) AS `Current YTD Licence Issued: Value Added`
    FROM bcts_reporting.licence_issued_advertised_main
    GROUP BY
        business_area_region_category,
        business_area_region,
        business_area
),

previous_ytd AS
(
    SELECT
        business_area_region_category,
        business_area_region,
        business_area,
        COALESCE(SUM(issued_licence_volume), 0) AS `Previous YTD Licence Issued`,
        COALESCE(SUM(category_2_and_4_issued_volume), 0) AS `Previous YTD Licence Issued: Value Added`
    FROM bcts_reporting.licence_issued_advertised_main_hist
    WHERE report_start_date =
    (
        SELECT ADD_MONTHS(MAX(report_start_date), -12)
        FROM bcts_reporting.licence_issued_advertised_main
    )
    AND report_end_date =
    (
        SELECT ADD_MONTHS(MAX(report_end_date), -12)
        FROM bcts_reporting.licence_issued_advertised_main
    )
    GROUP BY
        business_area_region_category,
        business_area_region,
        business_area
),

base AS
(
    SELECT
        current_ytd.business_area_region_category,
        current_ytd.business_area_region,
        current_ytd.business_area,
        current_ytd.`Current YTD Licence Issued`,
        current_ytd.`Current YTD Licence Issued: Value Added`,
        current_ytd.`Current YTD Licence Issued` - current_ytd.`Current YTD Licence Issued: Value Added` AS `Current YTD Licence Issued: Other`,
        previous_ytd.`Previous YTD Licence Issued`,
        previous_ytd.`Previous YTD Licence Issued: Value Added`,
        previous_ytd.`Previous YTD Licence Issued` - previous_ytd.`Previous YTD Licence Issued: Value Added` AS `Previous YTD Licence Issued: Other`,

        GREATEST(
            SUM(current_ytd.`Current YTD Licence Issued`) OVER (PARTITION BY current_ytd.business_area),
            SUM(previous_ytd.`Previous YTD Licence Issued`) OVER (PARTITION BY current_ytd.business_area),
            SUM(current_ytd.`Current YTD Licence Issued: Value Added`) OVER (PARTITION BY current_ytd.business_area),
            SUM(previous_ytd.`Previous YTD Licence Issued: Value Added`) OVER (PARTITION BY current_ytd.business_area)
        ) AS y_business_area

    FROM previous_ytd

    LEFT JOIN current_ytd
        ON previous_ytd.business_area_region_category = current_ytd.business_area_region_category
        AND previous_ytd.business_area_region = current_ytd.business_area_region
        AND previous_ytd.business_area = current_ytd.business_area
)

SELECT
    business_area_region_category,
    business_area_region,
    business_area,
    `Current YTD Licence Issued`,
    `Current YTD Licence Issued: Value Added`,
    `Current YTD Licence Issued: Other`,
    `Previous YTD Licence Issued`,
    `Previous YTD Licence Issued: Value Added`,
    `Previous YTD Licence Issued` - `Previous YTD Licence Issued: Value Added` AS `Previous YTD Licence Issued: Other`,
    y_business_area * 1.1 AS y_max_business_area
FROM base;


DROP TABLE IF EXISTS bcts_reporting.bcts_performance_report_current_prev_ytd_issued_lic_volume;

CREATE TABLE bcts_reporting.bcts_performance_report_current_prev_ytd_issued_lic_volume
USING DELTA
AS
SELECT *
FROM bcts_staging.bcts_performance_report_current_prev_ytd_issued_lic_volume;    