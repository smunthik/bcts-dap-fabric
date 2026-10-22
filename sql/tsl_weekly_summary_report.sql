-- Create _hist table if it does not exist
CREATE TABLE IF NOT EXISTS bcts_staging.bcbids_tsl_weekly_report
(
    `Status` STRING,
    `Opportunity ID` STRING,
    `Opportunity Description` STRING,
    `Commodities` STRING,
    `Type` STRING,
    `Issue Date
and Time
(Pacific Time)` STRING,
    `Closing Date
and Time
(Pacific Time)` STRING,
    `Ends in` STRING,
    `# of Amendments` STRING,
    `Last Updated` STRING,
    `Organization (Issued by)` STRING,
    `Organization (Issued for)` STRING,
    `Interested
Vendor
List
Available` STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS bcts_staging.tsl_summary_lrm
(
    licence_id STRING,
    geographic_location STRING,
    field_team STRING,
    species STRING,
    report_run_date DATE DEFAULT CURRENT_DATE(),
    report_run_timestamp TIMESTAMP DEFAULT FROM_UTC_TIMESTAMP(
        CURRENT_TIMESTAMP(),
        'America/Vancouver'
    )
)
USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);


CREATE TABLE IF NOT EXISTS bcts_staging.tsl_summary_official
(
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    business_area_code STRING,
    forest_file_id STRING,
    status STRING,
    volume_advertised_m3 DECIMAL(38,18),
    volume_readvertised_m3 DECIMAL(38,18),
    last_auction_date TIMESTAMP,
    last_auction_bcts_category_code STRING,
    last_auction_volume DECIMAL(13,1),
    auction_count_all_time_to_report_period_end BIGINT,
    bidder_count STRING,
    readvertised_auction STRING,
    no_bid STRING,
    no_bid_info STRING,
    upset_rate_value DECIMAL(38,18),
    bonus_bid_offer DECIMAL(38,18),
    report_start_date DATE,
    report_end_date DATE,
    report_run_date DATE DEFAULT CURRENT_DATE(),
    report_run_timestamp TIMESTAMP DEFAULT FROM_UTC_TIMESTAMP(
        CURRENT_TIMESTAMP(),
        'America/Vancouver'
    )
)
USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);


CREATE TABLE IF NOT EXISTS bcts_staging.tsl_summary_main_hist
(
    business_area_region_category STRING,
    business_area_region STRING,
    business_area_code STRING,
    business_area STRING,
    licence_number STRING,
    geographic_location STRING,
    field_team STRING,
    species STRING,
    volume_advertised_m3 DECIMAL(38,18),
    volume_readvertised_m3 DECIMAL(38,18),
    auctioned_bcts_category_code STRING,
    total_auction_count BIGINT,
    readvertised_auction STRING,
    auction_closing_date TIMESTAMP,
    number_of_bidders STRING,
    no_bid STRING,
    upset_rate_or_upset_value DECIMAL(38,18),
    bonus_bid_or_bonus_value DECIMAL(38,18),
    total_stumpage_or_total_stumpage_value DECIMAL(38,18),
    no_bid_info STRING,
    report_start_date DATE,
    report_end_date DATE,
    report_run_date DATE DEFAULT CURRENT_DATE(),
    report_run_timestamp TIMESTAMP DEFAULT FROM_UTC_TIMESTAMP(
        CURRENT_TIMESTAMP(),
        'America/Vancouver'
    )
)
USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);


-- Report exists check is done on bcts_reporting table 
-- If report exists in bcts_staging.***_hist table, clear the staging table before inserting new records 
delete from bcts_staging.tsl_summary_main_hist
where  report_start_date = '${report_start_date}'
and report_end_date = '${report_end_date}';

delete from bcts_staging.tsl_summary_official;

delete from bcts_staging.tsl_summary_lrm;

-- Populate staging table
INSERT INTO bcts_staging.tsl_summary_lrm
(
    licence_id,
    geographic_location,
    field_team,
    species
)
WITH licence_ids AS
(
    SELECT DISTINCT
        LEFT(`Opportunity ID`, 6) AS licence_id
    FROM bcts_staging.bcbids_tsl_weekly_report
    WHERE `Opportunity ID` IS NOT NULL
      AND TRIM(`Opportunity ID`) <> ''
),

split_species AS
(
    SELECT
        ss.licence_id,
        ss.block_id,
        ss.block_volume,
        ss.geographic_location,
        ss.field_team,
        TRIM(species_detail) AS species_detail
    FROM lrm_replication.sv_sales_schedule ss
    LATERAL VIEW EXPLODE(
        SPLIT(COALESCE(ss.species_composition, ''), ';')
    ) exploded_species AS species_detail
    WHERE ss.block_volume IS NOT NULL
      AND ss.block_volume > 0
),

parsed_species AS
(
    SELECT
        licence_id,
        block_id,
        block_volume,
        REGEXP_EXTRACT(
            species_detail,
            '^([A-Z]+)',
            1
        ) AS species_cd,
        TRY_CAST(
            REGEXP_EXTRACT(
                species_detail,
                '([0-9.]+)',
                1
            ) AS DECIMAL(18,4)
        ) AS species_pct
    FROM split_species
    WHERE species_detail <> ''
),

licence_volume AS
(
    SELECT
        licence_id,
        SUM(block_volume) AS total_volume
    FROM lrm_replication.sv_sales_schedule
    WHERE block_volume IS NOT NULL
      AND block_volume > 0
    GROUP BY licence_id
),

calc_weight AS
(
    SELECT
        p.licence_id,
        p.species_cd,
        SUM(p.block_volume * p.species_pct) AS weighted_pct_numerator,
        lv.total_volume,
        CASE
            WHEN lv.total_volume = 0 THEN 0
            ELSE
                SUM(p.block_volume * p.species_pct)
                / lv.total_volume
        END AS final_pct
    FROM parsed_species p
    INNER JOIN licence_volume lv
        ON p.licence_id = lv.licence_id
    WHERE p.species_cd <> ''
      AND p.species_pct IS NOT NULL
    GROUP BY
        p.licence_id,
        p.species_cd,
        lv.total_volume
),

species_summary AS
(
    SELECT
        licence_id,
        CONCAT_WS(
            ', ',
            TRANSFORM(
                SORT_ARRAY(
                    COLLECT_LIST(
                        NAMED_STRUCT(
                            'sort_order', -final_pct,
                            'description',
                            CONCAT(
                                species_cd,
                                ' ',
                                CAST(
                                    CAST(
                                        ROUND(final_pct, 0) AS BIGINT
                                    ) AS STRING
                                ),
                                '%'
                            )
                        )
                    )
                ),
                item -> item.description
            )
        ) AS species
    FROM calc_weight
    WHERE ROUND(final_pct, 0) > 0
    GROUP BY licence_id
),

schedule_details AS
(
    SELECT DISTINCT
        licence_id,
        geographic_location,
        field_team
    FROM lrm_replication.sv_sales_schedule
)

SELECT DISTINCT
    ids.licence_id,
    schedule.geographic_location,
    schedule.field_team,
    species.species
FROM licence_ids ids
LEFT JOIN schedule_details schedule
    ON ids.licence_id = schedule.licence_id
LEFT JOIN species_summary species
    ON ids.licence_id = species.licence_id;



INSERT INTO bcts_staging.tsl_summary_official
(
    business_area_region_category,
    business_area_region,
    business_area,
    business_area_code,
    forest_file_id,
    status,
    volume_advertised_m3,
    volume_readvertised_m3,
    last_auction_date,
    last_auction_bcts_category_code,
    last_auction_volume,
    auction_count_all_time_to_report_period_end,
    bidder_count,
    readvertised_auction,
    no_bid,
    no_bid_info,
    upset_rate_value,
    bonus_bid_offer,
    report_start_date,
    report_end_date
)

WITH all_auctions_to_date AS
(
    SELECT
        ts.forest_file_id,
        COUNT(ts.auction_date) AS auction_count_all_time_to_report_period_end,
        MIN(ts.auction_date) AS first_auction_date,
        MAX(ts.auction_date) AS last_auction_date
    FROM bctsadmin_replication.bcts_timber_sale ts
    WHERE COALESCE(ts.no_sale_rationale_code, ' ') <> 'TB'
      AND TO_DATE(ts.auction_date) <= CAST('${report_end_date}' AS DATE)
    GROUP BY
        ts.forest_file_id
),

first_auction AS
(
    SELECT
        forest_file_id,
        auction_date,
        bcts_category_code,
        sale_volume
    FROM bctsadmin_replication.bcts_timber_sale
),

last_auction AS
(
    SELECT
        ts.forest_file_id,
        MAX(ts.auction_date) AS last_auction_date
    FROM bctsadmin_replication.bcts_timber_sale ts
    WHERE COALESCE(ts.no_sale_rationale_code, ' ') <> 'TB'
      AND TO_DATE(ts.auction_date)
          BETWEEN CAST('${report_start_date}' AS DATE)
              AND CAST('${report_end_date}' AS DATE)
    GROUP BY
        ts.forest_file_id
),

advertised AS
(
    SELECT
        ts.forest_file_id,
        all_auctions_to_date.first_auction_date,
        last_auction.last_auction_date,
        all_auctions_to_date.auction_count_all_time_to_report_period_end,
        first_auction.bcts_category_code AS first_bcts_category_code,
        ts.bcts_category_code AS last_auction_bcts_category_code,
        first_auction.sale_volume AS first_auction_volume,
        ts.sale_volume AS last_auction_volume
    FROM bctsadmin_replication.bcts_timber_sale ts

    LEFT JOIN bctsadmin_replication.no_sale_rationale_code nsrc
        ON ts.no_sale_rationale_code = nsrc.no_sale_rationale_code

    INNER JOIN all_auctions_to_date
        ON ts.forest_file_id = all_auctions_to_date.forest_file_id
       AND ts.auction_date = all_auctions_to_date.last_auction_date

    INNER JOIN last_auction
        ON ts.forest_file_id = last_auction.forest_file_id

    INNER JOIN first_auction
        ON ts.forest_file_id = first_auction.forest_file_id
       AND all_auctions_to_date.first_auction_date = first_auction.auction_date
),

bonus_bid AS
(
    SELECT
        forest_file_id,
        auction_date,
        MAX(bonus_bid) AS bonus_bid
    FROM bctsadmin_replication.bcts_tenure_bidder
    WHERE sale_awarded_ind = 'Y'
    GROUP BY
        forest_file_id,
        auction_date
),

bonus_offer AS
(
    SELECT
        forest_file_id,
        auction_date,
        MAX(bonus_offer) AS bonus_offer
    FROM bctsadmin_replication.bcts_tenure_bidder
    WHERE sale_awarded_ind = 'Y'
    GROUP BY
        forest_file_id,
        auction_date
),

qo AS
(
    SELECT DISTINCT

        CASE
            WHEN ou.org_unit_code IN
            (
                'TBA', 'TPL', 'TPG', 'TSK', 'TSN',
                'TCC', 'TKA', 'TKO', 'TOC'
            )
                THEN 'Interior'

            WHEN ou.org_unit_code IN
            (
                'TCH', 'TST', 'TSG'
            )
                THEN 'Coast'
        END AS business_area_region_category,

        CASE
            WHEN ou.org_unit_code IN
            (
                'TBA', 'TPL', 'TPG', 'TSK', 'TSN'
            )
                THEN 'North Interior'

            WHEN ou.org_unit_code IN
            (
                'TCC', 'TKA', 'TKO', 'TOC'
            )
                THEN 'South Interior'

            WHEN ou.org_unit_code IN
            (
                'TCH', 'TST', 'TSG'
            )
                THEN 'Coast'
        END AS business_area_region,

        CASE
            WHEN ou.org_unit_code IS NULL THEN NULL

            ELSE REPLACE(
                CONCAT(
                    CASE
                        WHEN ou.org_unit_name =
                             'Seaward-tlasta Timber Sales Office'
                            THEN 'Seaward-Tlasta'
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

        pfu.forest_file_id,

        CAST(NULL AS STRING) AS status,

        ts.auction_date,

        ts.upset_rate,

        tb.bonus_bid,

        tm.cruise_based_ind AS cruise_based,

        ts.total_upset_value,

        tb1.bonus_offer,

        advertised.auction_count_all_time_to_report_period_end,

        advertised.first_auction_date,

        advertised.first_bcts_category_code,

        advertised.first_auction_volume,

        advertised.last_auction_date,

        advertised.last_auction_bcts_category_code,

        advertised.last_auction_volume,

        CASE
            WHEN tfsc.description IS NOT NULL
                THEN CONCAT(
                    tfsc.description,
                    ' (',
                    pfu.file_status_st,
                    ')'
                )
            ELSE pfu.file_status_st
        END AS fta_file_status,

        pfu.file_status_date AS fta_file_status_date

    FROM bcts_staging.fta_prov_forest_use pfu

    LEFT JOIN bctsadmin_replication.bcts_timber_sale ts
        ON pfu.forest_file_id = ts.forest_file_id

    INNER JOIN mofclient_replication.org_unit ou
        ON pfu.bcts_org_unit = ou.org_unit_no

    LEFT JOIN bcts_staging.fta_timber_mark tm
        ON pfu.forest_file_id = tm.forest_file_id

    LEFT JOIN bonus_bid tb
        ON ts.forest_file_id = tb.forest_file_id
       AND ts.auction_date = tb.auction_date

    LEFT JOIN bonus_offer tb1
        ON ts.forest_file_id = tb1.forest_file_id
       AND ts.auction_date = tb1.auction_date

    LEFT JOIN bcts_staging.fta_tenure_term tt
        ON pfu.forest_file_id = tt.forest_file_id

    LEFT JOIN bcts_staging.fta_tenure_file_status_code tfsc
        ON pfu.file_status_st = tfsc.tenure_file_status_code

    LEFT JOIN advertised
        ON ts.forest_file_id = advertised.forest_file_id
),

bidder_count AS
(
    SELECT
        forest_file_id,
        auction_date,
        COUNT(DISTINCT client_number) AS bidder_count
    FROM bctsadmin_replication.bcts_tenure_bidder
    GROUP BY
        forest_file_id,
        auction_date
),

ts_max AS
(
    SELECT
        forest_file_id,
        auction_date,
        upset_rate,
        total_upset_value
    FROM bctsadmin_replication.bcts_timber_sale
)

SELECT DISTINCT

    qo.business_area_region_category,

    qo.business_area_region,

    qo.business_area,

    qo.business_area_code,

    qo.forest_file_id,

    qo.status,

    CASE
        WHEN qo.auction_count_all_time_to_report_period_end = 1
            THEN qo.last_auction_volume
        ELSE NULL
    END AS volume_advertised_m3,

    CASE
        WHEN qo.auction_count_all_time_to_report_period_end > 1
            THEN qo.last_auction_volume
        ELSE NULL
    END AS volume_readvertised_m3,

    qo.last_auction_date,

    qo.last_auction_bcts_category_code,

    qo.last_auction_volume,

    qo.auction_count_all_time_to_report_period_end,

    CASE
        WHEN COALESCE(bidder_count.bidder_count, 0) > 0
            THEN CAST(bidder_count.bidder_count AS STRING)

        WHEN COALESCE(bidder_count.bidder_count, 0) = 0
            THEN '0'

        ELSE ''
    END AS bidder_count,

    CASE
        WHEN qo.auction_count_all_time_to_report_period_end > 1
            THEN 'Y'

        WHEN qo.auction_count_all_time_to_report_period_end = 1
            THEN 'N'

        WHEN qo.auction_count_all_time_to_report_period_end IS NULL
            THEN ''
    END AS readvertised_auction,

    CASE
        WHEN COALESCE(bidder_count.bidder_count, 0) > 0
            THEN ''

        WHEN TO_DATE(ts_max.auction_date) > CURRENT_DATE()
             OR qo.auction_count_all_time_to_report_period_end IS NULL
            THEN ''

        ELSE 'Y'
    END AS no_bid,

    CASE
        WHEN COALESCE(bidder_count.bidder_count, 0) > 0
            THEN 'N'

        WHEN TO_DATE(ts_max.auction_date) > CURRENT_DATE()
             OR qo.auction_count_all_time_to_report_period_end IS NULL
            THEN ''

        ELSE 'Y'
    END AS no_bid_info,

    CASE
        WHEN qo.cruise_based = 'N'
            THEN ts_max.upset_rate
        ELSE ts_max.total_upset_value
    END AS upset_rate_value,

    CASE
        WHEN qo.cruise_based = 'N'
            THEN qo.bonus_bid
        ELSE qo.bonus_offer
    END AS bonus_bid_offer,

    CAST('${report_start_date}' AS DATE) AS report_start_date,

    CAST('${report_end_date}' AS DATE) AS report_end_date

FROM qo

LEFT JOIN bidder_count
    ON qo.forest_file_id = bidder_count.forest_file_id
   AND qo.last_auction_date = bidder_count.auction_date

LEFT JOIN ts_max
    ON qo.forest_file_id = ts_max.forest_file_id
   AND qo.last_auction_date = ts_max.auction_date;



INSERT INTO bcts_staging.tsl_summary_main_hist
(
    business_area_region_category,
    business_area_region,
    business_area_code,
    business_area,
    licence_number,
    geographic_location,
    field_team,
    species,
    volume_advertised_m3,
    volume_readvertised_m3,
    auctioned_bcts_category_code,
    total_auction_count,
    readvertised_auction,
    auction_closing_date,
    number_of_bidders,
    no_bid,
    upset_rate_or_upset_value,
    bonus_bid_or_bonus_value,
    total_stumpage_or_total_stumpage_value,
    no_bid_info,
    report_start_date,
    report_end_date,
    client_name
)

SELECT
    official.business_area_region_category,
    official.business_area_region,
    official.business_area_code,
    official.business_area,

    lrm.licence_id AS licence_number,

    lrm.geographic_location,

    lrm.field_team,

    lrm.species,

    official.volume_advertised_m3,

    official.volume_readvertised_m3,

    official.last_auction_bcts_category_code
        AS auctioned_bcts_category_code,

    official.auction_count_all_time_to_report_period_end
        AS total_auction_count,

    official.readvertised_auction,

    official.last_auction_date
        AS auction_closing_date,

    official.bidder_count
        AS number_of_bidders,

    official.no_bid,

    official.upset_rate_value
        AS upset_rate_or_upset_value,

    official.bonus_bid_offer
        AS bonus_bid_or_bonus_value,

    official.upset_rate_value + official.bonus_bid_offer
        AS total_stumpage_or_total_stumpage_value,

    official.no_bid_info,

    CAST('${report_start_date}' AS DATE)
        AS report_start_date,

    CAST('${report_end_date}' AS DATE)
        AS report_end_date,

    fc.client_name

FROM bcts_staging.tsl_summary_lrm lrm

LEFT JOIN bcts_staging.tsl_summary_official official
    ON official.forest_file_id = lrm.licence_id
   AND official.report_start_date = CAST('${report_start_date}' AS DATE)
   AND official.report_end_date = CAST('${report_end_date}' AS DATE)

LEFT JOIN bctsadmin_replication.bcts_tenure_bidder tb
    ON lrm.licence_id = tb.forest_file_id
   AND tb.sale_awarded_ind = 'Y'
   AND official.last_auction_date = tb.auction_date

LEFT JOIN mofclient_replication.v_client_public fc
    ON tb.client_number = fc.client_number
ORDER BY
    official.last_auction_date DESC;

-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
DROP TABLE IF EXISTS bcts_staging.tsl_summary_main;

CREATE TABLE bcts_staging.tsl_summary_main
USING DELTA
AS
SELECT *
FROM bcts_staging.tsl_summary_main_hist
WHERE report_start_date = CAST('${report_start_date}' AS DATE)
  AND report_end_date = CAST('${report_end_date}' AS DATE);


DROP TABLE IF EXISTS bcts_reporting.tsl_summary_main;

CREATE TABLE bcts_reporting.tsl_summary_main
USING DELTA
AS
SELECT
    *,
    CASE
        WHEN business_area_region = 'North Interior' THEN 1
        WHEN business_area_region = 'South Interior' THEN 2
        WHEN business_area_region = 'Coast' THEN 3
    END AS business_area_region_sort_order
FROM bcts_staging.tsl_summary_main;


CREATE OR REPLACE VIEW bcts_reporting.v_tsl_summary AS

WITH tsl_summary1 AS
(
    SELECT
        *,
        COALESCE(volume_advertised_m3, 0)
        + COALESCE(volume_readvertised_m3, 0) AS total_volume
    FROM bcts_reporting.tsl_summary_main
),

tsl_summary AS
(
    SELECT
        business_area_region_category,
        business_area_region,
        business_area,

        SUM(total_volume)
            AS `Total Volume Auctioned (Including Re-auctions)`,

        SUM(
            CASE
                WHEN readvertised_auction = 'N'
                    THEN volume_advertised_m3
                ELSE 0
            END
        ) AS `Volume Auctioned (First Auction Only)`,

        SUM(
            CASE
                WHEN readvertised_auction = 'Y'
                    THEN total_volume
                ELSE 0
            END
        ) AS `Volume Re-auctioned`,

        SUM(
            CASE
                WHEN no_bid_info = 'N'
                    THEN total_volume
                ELSE 0
            END
        ) AS `Licence Issued`,

        COUNT(
            CASE
                WHEN no_bid = ''
                    THEN licence_number
                ELSE NULL
            END
        ) AS `Number of Licence Issued`,

        SUM(
            CASE
                WHEN no_bid_info = 'Y'
                    THEN total_volume
                ELSE 0
            END
        ) AS `Volume Not Awarded`,

        COUNT(
            CASE
                WHEN no_bid_info = 'Y'
                    THEN licence_number
                ELSE NULL
            END
        ) AS `Number of Licence Not Awarded`,

        SUM(
            CASE
                WHEN auctioned_bcts_category_code = '4'
                     AND no_bid_info <> 'Y'
                    THEN total_volume
                ELSE 0
            END
        ) AS `Volume of Value Added (Category 4) Auctioned`,

        COUNT(
            CASE
                WHEN auctioned_bcts_category_code = '4'
                     AND no_bid_info <> 'Y'
                    THEN licence_number
                ELSE NULL
            END
        ) AS `Number of Value Added (Category 4) Licence Auctioned`

    FROM tsl_summary1

    GROUP BY
        business_area_region_category,
        business_area_region,
        business_area
)

SELECT
    business_area_region_category,
    business_area_region,
    business_area,

    `Total Volume Auctioned (Including Re-auctions)`,
    `Volume Auctioned (First Auction Only)`,
    `Volume Re-auctioned`,
    `Licence Issued`,
    `Number of Licence Issued`,
    `Volume Not Awarded`,
    `Number of Licence Not Awarded`,
    `Volume of Value Added (Category 4) Auctioned`,
    `Number of Value Added (Category 4) Licence Auctioned`,

    CASE
        WHEN business_area_region = 'North Interior' THEN 1
        WHEN business_area_region = 'South Interior' THEN 2
        WHEN business_area_region = 'Coast' THEN 3
    END AS business_area_region_sort_order

FROM tsl_summary;


DROP TABLE IF EXISTS bcts_reporting.tsl_summary_main_hist;

CREATE TABLE bcts_reporting.tsl_summary_main_hist
USING DELTA
AS
SELECT
    *,
    CASE
        WHEN business_area_region = 'North Interior' THEN 1
        WHEN business_area_region = 'South Interior' THEN 2
        WHEN business_area_region = 'Coast' THEN 3
    END AS business_area_region_sort_order
FROM bcts_staging.tsl_summary_main_hist;    
    