== SQL ==
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
                ''Fiscal '',
                CAST(YEAR(ADD_MONTHS(tt.legal_effective_dt, 9)) - 1 AS STRING),
                ''/'',
                CAST(YEAR(ADD_MONTHS(tt.legal_effective_dt, 9)) AS STRING)
            )
        END AS issued_licence_legal_effective_fiscal,

        CASE
            WHEN tt.legal_effective_dt IS NULL THEN NULL
            ELSE CONCAT(
                ''Q'',
                CAST(CEIL(MONTH(ADD_MONTHS(tt.legal_effective_dt, -3)) / 3.0) AS STRING)
            )
        END AS issued_licence_legal_effective_quarter,

        ts0.auction_date,
        ts0.bcts_category_code AS issued_licence_bcts_category_code,
        ts0.sale_volume AS issued_licence_volume,

        CASE
            WHEN ts0.bcts_category_code IN (''2'', ''4'')
                THEN ts0.sale_volume
        END AS category_2_and_4_issued_volume,

        CASE
            WHEN ts0.bcts_category_code IN (''A'', ''1'')
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
            COALESCE(CONCAT(fc.legal_first_name, '' ''), ''''),
            COALESCE(CONCAT(fc.legal_middle_name, '' ''), ''''),
            COALESCE(fc.client_name, '''')
        ) AS issued_licence_client_name

    FROM bcts_staging.the_bcts_timber_sale ts0

    INNER JOIN bcts_staging.the_bcts_tenure_bidder tb
        ON ts0.forest_file_id = tb.forest_file_id
        AND ts0.auction_date = tb.auction_date
        AND ts0.no_sale_rationale_code IS NULL
        AND UPPER(tb.sale_awarded_ind) = ''Y''

    INNER JOIN bcts_staging.fta_prov_forest_use pfu
        ON pfu.forest_file_id = ts0.forest_file_id
        AND pfu.file_status_st IN (''HI'', ''HC'', ''LC'', ''HX'', ''HS'', ''HRS'')

    INNER JOIN bcts_staging.fta_tenure_term tt
        ON pfu.forest_file_id = tt.forest_file_id
        AND tt.legal_effective_dt BETWEEN TO_DATE(''2026-04-01'', ''yyyy-MM-dd'')
                                      AND TO_DATE(''2026-09-30'', ''yyyy-MM-dd'')

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
                ''Fiscal '',
                CAST(YEAR(ADD_MONTHS(all_auctions_to_date.first_auction_date, 9)) - 1 AS STRING),
                ''/'',
                CAST(YEAR(ADD_MONTHS(all_auctions_to_date.first_auction_date, 9)) AS STRING)
            )
        END AS first_auction_fiscal,

        CASE
            WHEN all_auctions_to_date.first_auction_date IS NULL THEN NULL
            ELSE CONCAT(
                ''Q'',
                CAST(CEIL(MONTH(ADD_MONTHS(all_auctions_to_date.first_auction_date, -3)) / 3.0) AS STRING)
            )
        END AS first_auction_quarter,

        all_auctions_to_date.last_auction_date,

        CASE
            WHEN all_auctions_to_date.last_auction_date IS NULL THEN NULL
            ELSE CONCAT(
                ''Fiscal '',
                CAST(YEAR(ADD_MONTHS(all_auctions_to_date.last_auction_date, 9)) - 1 AS STRING),
                ''/'',
                CAST(YEAR(ADD_MONTHS(all_auctions_to_date.last_auction_date, 9)) AS STRING)
            )
        END AS last_auction_fiscal,

        CASE
            WHEN all_auctions_to_date.last_auction_date IS NULL THEN NULL
            ELSE CONCAT(
                ''Q'',
                CAST(CEIL(MONTH(ADD_MONTHS(all_auctions_to_date.last_auction_date, -3)) / 3.0) AS STRING)
            )
        END AS last_auction_quarter,

        all_auctions_to_date.auction_count_all_time_to_report_period_end,
        first_auction.bcts_category_code AS first_bcts_category_code,
        ts.bcts_category_code AS last_auction_bcts_category_code,

        CASE
            WHEN first_auction.bcts_category_code = ts.bcts_category_code
                THEN ''No category change''
            ELSE ''Category change''
        END AS category_change,

        CASE
            WHEN nsrc.description IS NULL THEN ts.no_sale_rationale_code
            ELSE CONCAT(nsrc.description, '' ('', ts.no_sale_rationale_code, '')'')
        END AS last_auction_no_sale_rationale,

        CAST(
            CASE
                WHEN ts.no_sale_rationale_code IS NULL THEN NULL
                ELSE ts.sale_volume
            END AS DECIMAL(38,18)
        ) AS last_auction_no_sale_volume,

        CASE
            WHEN ts.no_sale_rationale_code IS NOT NULL
                AND ts.bcts_category_code IN (''2'', ''4'')
                THEN ts.sale_volume
        END AS last_auction_no_sale_category_2_4_volume,

        CASE
            WHEN ts.no_sale_rationale_code IS NOT NULL
                AND ts.bcts_category_code IN (''A'', ''1'')
                THEN ts.sale_volume
        END AS last_auction_no_sale_category_a_1_volume,

        first_auction.sale_volume AS first_auction_volume,
        ts.sale_volume AS last_auction_volume,

        CASE
            WHEN first_auction.bcts_category_code IN (''2'', ''4'')
                THEN first_auction.sale_volume
        END AS first_auction_category_2_and_4_volume,

        CASE
            WHEN first_auction.bcts_category_code IN (''A'', ''1'')
                THEN first_auction.sale_volume
        END AS first_auction_category_a_and_1_volume,

        CASE
            WHEN ts.bcts_category_code IN (''2'', ''4'')
                THEN ts.sale_volume
        END AS last_auction_category_2_and_4_volume,

        CASE
            WHEN ts.bcts_category_code IN (''A'', ''1'')
                THEN ts.sale_volume
        END AS last_auction_category_a_and_1_volume,

        CASE
            WHEN first_auction.bcts_category_code IN (''2'', ''4'')
                AND ts.bcts_category_code IN (''A'', ''1'')
                THEN ts.sale_volume
        END AS original_cat_2_and_4_readvertised_cat_a_and_1_volume,

        CASE
            WHEN first_auction.bcts_category_code IN (''A'', ''1'')
                AND ts.bcts_category_code IN (''2'', ''4'')
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

        WHERE COALESCE(no_sale_rationale_code, '' '') <> ''TB''
          AND ts.auction_date <= TO_DATE('${report_end_date}', ''yyyy-MM-dd'')
----------------------------------------^^^

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

    WHERE auction_date BETWEEN TO_DATE(''2026-04-01'', ''yyyy-MM-dd'')
                           AND TO_DATE(''2026-09-30'', ''yyyy-MM-dd'')
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
        WHEN ou.org_unit_code IN (''TBA'', ''TPL'', ''TPG'', ''TSK'', ''TSN'', ''TCC'', ''TKA'', ''TKO'', ''TOC'')
            THEN ''Interior''
        WHEN ou.org_unit_code IN (''TCH'', ''TST'', ''TSG'')
            THEN ''Coast''
    END AS business_area_region_category,

    CASE
        WHEN ou.org_unit_code IN (''TBA'', ''TPL'', ''TPG'', ''TSK'', ''TSN'')
            THEN ''North Interior''
        WHEN ou.org_unit_code IN (''TCC'', ''TKA'', ''TKO'', ''TOC'')
            THEN ''South Interior''
        WHEN ou.org_unit_code IN (''TCH'', ''TST'', ''TSG'')
            THEN ''Coast''
    END AS business_area_region,

    CASE
        WHEN ou.org_unit_code IS NULL THEN NULL
        ELSE REPLACE(
            CONCAT(
                CASE
                    WHEN ou.org_unit_name = ''Seaward Timber Sales Office'' THEN ''Seaward-Tlasta''
                    ELSE ou.org_unit_name
                END,
                '' ('',
                ou.org_unit_code,
                '')''
            ),
            '' Timber Sales Office'',
            ''''
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
        WHEN advertised.first_auction_date BETWEEN TO_DATE(''2026-04-01'', ''yyyy-MM-dd'')
                                               AND TO_DATE(''2026-09-30'', ''yyyy-MM-dd'')
            THEN advertised.first_auction_volume
    END AS first_auction_volume_is_in_report_period,

    CASE
        WHEN advertised.first_auction_date BETWEEN TO_DATE(''2026-04-01'', ''yyyy-MM-dd'')
                                               AND TO_DATE(''2026-09-30'', ''yyyy-MM-dd'')
            THEN advertised.first_auction_category_a_and_1_volume
    END AS first_auction_category_a_and_1_volume_is_in_report_period,

    CASE
        WHEN advertised.first_auction_date BETWEEN TO_DATE(''2026-04-01'', ''yyyy-MM-dd'')
                                               AND TO_DATE(''2026-09-30'', ''yyyy-MM-dd'')
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
        ELSE ''Y''
    END AS last_auction_no_sale,

    CASE
        WHEN advertised.last_auction_no_sale_rationale IS NULL THEN NULL
        WHEN advertised.last_auction_no_sale_category_a_1_volume IS NULL THEN NULL
        ELSE ''Y''
    END AS last_auction_no_sale_cat_a,

    CASE
        WHEN advertised.last_auction_no_sale_rationale IS NULL THEN NULL
        WHEN advertised.last_auction_no_sale_category_2_4_volume IS NULL THEN NULL
        ELSE ''Y''
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
        WHEN issued.issued_licence_bcts_category_code IN (''A'')
            THEN issued.issued_licence_maximum_value
    END AS issued_licence_maximum_value_cat_a,

    CASE
        WHEN issued.issued_licence_bcts_category_code IN (''2'', ''4'')
            THEN issued.issued_licence_maximum_value
    END AS issued_licence_maximum_value_cat_2_4,

    issued.issued_licence_client_number,
    issued.issued_licence_client_name,

    CASE
        WHEN issued.forest_file_id IS NULL THEN NULL
        ELSE ''Y''
    END AS issued_in_report_period,

    CASE
        WHEN issued.forest_file_id IS NULL THEN NULL
        WHEN issued.category_a_and_1_issued_volume IS NULL THEN NULL
        ELSE ''Y''
    END AS issued_in_report_period_cat_a,

    CASE
        WHEN issued.forest_file_id IS NULL THEN NULL
        WHEN issued.category_2_and_4_issued_volume IS NULL THEN NULL
        ELSE ''Y''
    END AS issued_in_report_period_cat_2_4,

    CASE
        WHEN advertised_in_report_period.forest_file_id IS NULL THEN NULL
        ELSE ''Y''
    END AS advertised_in_report_period,

    CASE
        WHEN tfsc.description IS NOT NULL
            THEN CONCAT(tfsc.description, '' ('', pfu.file_status_st, '')'')
        ELSE pfu.file_status_st
    END AS fta_file_status,

    pfu.file_status_date AS fta_file_status_date,
    bc.bidder_count,
    TO_DATE(''2026-04-01'', ''yyyy-MM-dd'') AS report_start_date,
    TO_DATE(''2026-09-30'', ''yyyy-MM-dd'') AS report_end_date,

    CASE
        WHEN MONTH(CURRENT_DATE()) >= 4 THEN YEAR(CURRENT_DATE())
        ELSE YEAR(CURRENT_DATE()) - 1
    END AS fiscal_year,

    TO_DATE(
        FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), ''America/Vancouver'')
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
        WHEN ou.org_unit_code IN (''TBA'', ''TPL'', ''TPG'', ''TSK'', ''TSN'', ''TCC'', ''TKA'', ''TKO'', ''TOC'')
            THEN ''Interior''
        WHEN ou.org_unit_code IN (''TCH'', ''TST'', ''TSG'')
            THEN ''Coast''
    END AS business_area_region_category,

    CASE
        WHEN ou.org_unit_code IN (''TBA'', ''TPL'', ''TPG'', ''TSK'', ''TSN'')
            THEN ''North Interior''
        WHEN ou.org_unit_code IN (''TCC'', ''TKA'', ''TKO'', ''TOC'')
            THEN ''South Interior''
        WHEN ou.org_unit_code IN (''TCH'', ''TST'', ''TSG'')
            THEN ''Coast''
    END AS business_area_region,

    CASE
        WHEN ou.org_unit_code IS NULL THEN NULL
        ELSE REPLACE(
            CONCAT(
                CASE
                    WHEN ou.org_unit_name = ''Seaward Timber Sales Office'' THEN ''Seaward-Tlasta''
                    ELSE ou.org_unit_name
                END,
                '' ('',
                ou.org_unit_code,
                '')''
            ),
            '' Timber Sales Office'',
            ''''
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
    CAST(NULL AS STRING) AS first_auction_quarter,… 
Show all (22.7 kB)
ParseException
[PARSE_SYNTAX_ERROR] Syntax error at or near '('.(line 272, pos 40) == SQL == -- Populate staging table

[PARSE_SYNTAX_ERROR] Syntax error at or near '('.(line 272, pos 40)

== SQL ==
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
        AND tt.legal_effective_dt BETWEEN TO_DATE('2026-04-01', 'yyyy-MM-dd')
                                      AND TO_DATE('2026-09-30', 'yyyy-MM-dd')

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
          AND ts.auction_date <= TO_DATE('${report_end_date}', 'yyyy-MM-dd')

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

    WHERE auction_date BETWEEN TO_DATE('2026-04-01', 'yyyy-MM-dd')
                           AND TO_DATE('2026-09-30', 'yyyy-MM-dd')
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
        WHEN advertised.first_auction_date BETWEEN TO_DATE('2026-04-01', 'yyyy-MM-dd')
                                               AND TO_DATE('2026-09-30', 'yyyy-MM-dd')
            THEN advertised.first_auction_volume
    END AS first_auction_volume_is_in_report_period,

    CASE
        WHEN advertised.first_auction_date BETWEEN TO_DATE('2026-04-01', 'yyyy-MM-dd')
                                               AND TO_DATE('2026-09-30', 'yyyy-MM-dd')
            THEN advertised.first_auction_category_a_and_1_volume
    END AS first_auction_category_a_and_1_volume_is_in_report_period,

    CASE
        WHEN advertised.first_auction_date BETWEEN TO_DATE('2026-04-01', 'yyyy-MM-dd')
                                               AND TO_DATE('2026-09-30', 'yyyy-MM-dd')
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
    TO_DATE('2026-04-01', 'yyyy-MM-dd') AS report_start_date,
    TO_DATE('2026-09-30', 'yyyy-MM-dd') AS report_end_date,

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
    TO_DATE('2026-04-01', 'yyyy-MM-dd') AS report_start_date,
    TO_DATE('2026-09-30', 'yyyy-MM-dd') AS report_end_date,

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
)

---------------------------------------------------------------------------ParseException                            Traceback (most recent call last)Cell In[4], line 70
     68 try:
     69     for stmt in [s.strip() for s in final_sql.split(";") if s.strip()]:
---> 70         spark.sql(stmt)
     71     print(f" {target_schema}.{target_table} report for {start_date} and {end_date} completed.")
     73 except Exception as e:
File /opt/spark/python/lib/pyspark.zip/pyspark/sql/session.py:1631, in SparkSession.sql(self, sqlQuery, args, **kwargs)
   1627         assert self._jvm is not None
   1628         litArgs = self._jvm.PythonUtils.toArray(
   1629             [_to_java_column(lit(v)) for v in (args or [])]
   1630         )
-> 1631     return DataFrame(self._jsparkSession.sql(sqlQuery, litArgs), self)
   1632 finally:
   1633     if len(kwargs) > 0:
File ~/cluster-env/trident_env/lib/python3.11/site-packages/py4j/java_gateway.py:1322, in JavaMember.__call__(self, *args)
   1316 command = proto.CALL_COMMAND_NAME +\
   1317     self.command_header +\
   1318     args_command +\
   1319     proto.END_COMMAND_PART
   1321 answer = self.gateway_client.send_command(command)
-> 1322 return_value = get_return_value(
   1323     answer, self.gateway_client, self.target_id, self.name)
   1325 for temp_arg in temp_args:
   1326     if hasattr(temp_arg, "_detach"):
File /opt/spark/python/lib/pyspark.zip/pyspark/errors/exceptions/captured.py:185, in capture_sql_exception.<locals>.deco(*a, **kw)
    181 converted = convert_exception(e.java_exception)
    182 if not isinstance(converted, UnknownException):
    183     # Hide where the exception came from that shows a non-Pythonic
    184     # JVM exception message.
--> 185     raise converted from None
    186 else:
    187     raise
ParseException: 
[PARSE_SYNTAX_ERROR] Syntax error at or near '('.(line 272, pos 40)

== SQL ==
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
        AND tt.legal_effective_dt BETWEEN TO_DATE('2026-04-01', 'yyyy-MM-dd')
                                      AND TO_DATE('2026-09-30', 'yyyy-MM-dd')

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
          AND ts.auction_date <= TO_DATE('${report_end_date}', 'yyyy-MM-dd')

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

    WHERE auction_date BETWEEN TO_DATE('2026-04-01', 'yyyy-MM-dd')
                           AND TO_DATE('2026-09-30', 'yyyy-MM-dd')
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
        WHEN advertised.first_auction_date BETWEEN TO_DATE('2026-04-01', 'yyyy-MM-dd')
                                               AND TO_DATE('2026-09-30', 'yyyy-MM-dd')
            THEN advertised.first_auction_volume
    END AS first_auction_volume_is_in_report_period,

    CASE
        WHEN advertised.first_auction_date BETWEEN TO_DATE('2026-04-01', 'yyyy-MM-dd')
                                               AND TO_DATE('2026-09-30', 'yyyy-MM-dd')
            THEN advertised.first_auction_category_a_and_1_volume
    END AS first_auction_category_a_and_1_volume_is_in_report_period,

    CASE
        WHEN advertised.first_auction_date BETWEEN TO_DATE('2026-04-01', 'yyyy-MM-dd')
                                               AND TO_DATE('2026-09-30', 'yyyy-MM-dd')
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
    TO_DATE('2026-04-01', 'yyyy-MM-dd') AS report_start_date,
    TO_DATE('2026-09-30', 'yyyy-MM-dd') AS report_end_date,

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
    TO_DATE('2026-04-01', 'yyyy-MM-dd') AS report_start_date,
    TO_DATE('2026-09-30', 'yyyy-MM-dd') AS report_end_date,

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
)