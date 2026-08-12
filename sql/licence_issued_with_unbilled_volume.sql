-- Create _hist table if it does not exist
CREATE TABLE IF NOT EXISTS bcts_staging.licence_issued_with_unbilled_volume_hist (
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    field_team STRING,
    licence_id STRING,
    tenure_type_fta STRING,
    cruise_based_ind STRING,
    fta_file_status STRING,
    client_number STRING,
    client STRING,
    fta_file_status_date TIMESTAMP,
    legal_effective_date_fta DATE,
    legal_effective_fiscal_fta STRING,
    issued_done_lrm TIMESTAMP,
    issued_done_fiscal_lrm STRING,
    initial_expiry_fta DATE,
    current_expiry_fta DATE,
    expiry_fta DATE,
    expire_extend_lrm TIMESTAMP,
    advertised_licence_term INT,
    extension_term DECIMAL(38,18),
    total_tenure_term DECIMAL(38,18),
    sale_volume DECIMAL(13,1),
    billed_volume DECIMAL(38,18),
    unbilled_volume DECIMAL(38,18),
    percent_unbilled DECIMAL(38,18),
    licn_seq_nbr DECIMAL(15,0),
    last_logging_started_date TIMESTAMP,
    last_logging_completed_date TIMESTAMP,
    harvesting_status STRING,
    report_date DATE,
    unbilled_volume_over_20_month DECIMAL(38,18),
    unbilled_volume_expire_in_6_month DECIMAL(38,18),
    report_run_date DATE,
    report_run_timestamp TIMESTAMP
)
USING DELTA;



-- Report is run for the current date.
-- If report exists in bcts_reporting.licence_issued_with_unbilled_volume_hist for the current month, clear the staging table before inserting new records
-- It deletes the records from the staging table for the current month and year, and then populates the staging table with the latest data.
DELETE FROM bcts_staging.licence_issued_with_unbilled_volume_hist
WHERE YEAR(report_date) = YEAR(CAST('${report_date}' AS DATE))
  AND MONTH(report_date) = MONTH(CAST('${report_date}' AS DATE));

-- Populate staging table
CREATE OR REPLACE VIEW bcts_staging.licence_issued_with_unbilled_volume_lrm AS

WITH licence AS (
    SELECT
        licn_seq_nbr,
        MAX(tso_name) AS business_area,
        MAX(licence_id) AS licence_id,
        MAX(field_team) AS field_team,
        MAX(tenure) AS tenure
    FROM lrm_replication.v_licence
    GROUP BY licn_seq_nbr
),

issued AS (
    SELECT
        licn_seq_nbr,
        MAX(activity_date) AS issued_done_lrm
    FROM lrm_replication.v_licence_activity_all
    WHERE activity_class = 'CML'
        AND actt_key_ind = 'HI'
        AND acti_status_ind = 'D'
    GROUP BY licn_seq_nbr
),

closed AS (
    SELECT
        licn_seq_nbr,
        MAX(activity_date) AS closed_done_lrm
    FROM lrm_replication.v_licence_activity_all
    WHERE activity_class = 'CML'
        AND actt_key_ind = 'HC'
        AND acti_status_ind = 'D'
    GROUP BY licn_seq_nbr
),

substantial_completion AS (
    SELECT
        licn_seq_nbr,
        MAX(activity_date) AS substantial_completion_done_lrm
    FROM lrm_replication.v_licence_activity_all
    WHERE activity_class = 'CML'
        AND actt_key_ind = 'LC'
        AND acti_status_ind = 'D'
    GROUP BY licn_seq_nbr
),

surrendered AS (
    SELECT
        licn_seq_nbr,
        MAX(activity_date) AS surrendered_done_lrm
    FROM lrm_replication.v_licence_activity_all
    WHERE activity_class = 'CML'
        AND actt_key_ind = 'HS'
        AND acti_status_ind = 'D'
    GROUP BY licn_seq_nbr
),

cancelled AS (
    SELECT
        licn_seq_nbr,
        MAX(activity_date) AS cancelled_done_lrm
    FROM lrm_replication.v_licence_activity_all
    WHERE activity_class = 'CML'
        AND actt_key_ind = 'HX'
        AND acti_status_ind = 'D'
    GROUP BY licn_seq_nbr
),

expire_extend AS (
    SELECT
        licn_seq_nbr,
        MAX(activity_date) AS expire_extend_lrm
    FROM lrm_replication.v_licence_activity_all
    WHERE activity_class = 'CML'
        AND actt_key_ind IN (
            'EXPIRE',
            'EXTEND'
        )
    GROUP BY licn_seq_nbr
),

logging_started AS (
    SELECT
        licn_seq_nbr,
        MAX(activity_date) AS logging_started_date
    FROM lrm_replication.v_block_activity_all
    WHERE actt_key_ind = 'HVS'
        AND acti_status_ind = 'D'
    GROUP BY licn_seq_nbr
),

logging_completed AS (
    SELECT
        licn_seq_nbr,
        MAX(activity_date) AS logging_completed_date
    FROM lrm_replication.v_block_activity_all
    WHERE actt_key_ind = 'HVC'
        AND acti_status_ind = 'D'
    GROUP BY licn_seq_nbr
)

SELECT
    licence.licn_seq_nbr,
    licence.business_area,
    licence.licence_id,

    logging_started.logging_started_date AS last_logging_started_date,

    logging_completed.logging_completed_date AS last_logging_completed_date,

    CASE
        WHEN logging_started.logging_started_date IS NULL
            AND logging_completed.logging_completed_date IS NULL
        THEN NULL
        ELSE 'Active_Harvesting'
    END AS harvesting_status,

    licence.field_team,
    licence.tenure,

    issued.issued_done_lrm,

    substantial_completion.substantial_completion_done_lrm,

    CASE
        WHEN issued.issued_done_lrm IS NULL THEN NULL
        ELSE CONCAT(
            'Fiscal ',
            CAST(YEAR(ADD_MONTHS(issued.issued_done_lrm, 9)) - 1 AS STRING),
            '/',
            CAST(YEAR(ADD_MONTHS(issued.issued_done_lrm, 9)) AS STRING)
        )
    END AS issued_done_fiscal_lrm,

    expire_extend.expire_extend_lrm

FROM licence

LEFT JOIN issued
    ON licence.licn_seq_nbr = issued.licn_seq_nbr

LEFT JOIN closed
    ON licence.licn_seq_nbr = closed.licn_seq_nbr

LEFT JOIN substantial_completion
    ON licence.licn_seq_nbr = substantial_completion.licn_seq_nbr

LEFT JOIN surrendered
    ON licence.licn_seq_nbr = surrendered.licn_seq_nbr

LEFT JOIN cancelled
    ON licence.licn_seq_nbr = cancelled.licn_seq_nbr

LEFT JOIN expire_extend
    ON licence.licn_seq_nbr = expire_extend.licn_seq_nbr

LEFT JOIN logging_started
    ON licence.licn_seq_nbr = logging_started.licn_seq_nbr

LEFT JOIN logging_completed
    ON licence.licn_seq_nbr = logging_completed.licn_seq_nbr;


-- licence_issued_with_unbilled_volume_official
CREATE OR REPLACE VIEW bcts_staging.licence_issued_with_unbilled_volume_official AS

WITH auction_with_winner AS (
    SELECT
        tb.forest_file_id,
        tb.auction_date,
        tb.client_number,
        CONCAT(
            COALESCE(CONCAT(fc.legal_first_name, ' '), ''),
            COALESCE(CONCAT(fc.legal_middle_name, ' '), ''),
            fc.client_name
        ) AS client
    FROM bctsadmin_replication.bcts_tenure_bidder tb
    INNER JOIN mofclient_replication.v_client_public fc
        ON tb.client_number = fc.client_number
    WHERE UPPER(tb.sale_awarded_ind) = 'Y'
),

latest_auction_with_winner AS (
    SELECT
        forest_file_id,
        MAX(auction_date) AS latest_auction_date
    FROM bctsadmin_replication.bcts_tenure_bidder
    WHERE UPPER(sale_awarded_ind) = 'Y'
    GROUP BY forest_file_id
),

bid_info AS (
    SELECT DISTINCT
        auction_with_winner.forest_file_id,
        auction_with_winner.auction_date,
        auction_with_winner.client_number,
        auction_with_winner.client
    FROM auction_with_winner
    INNER JOIN latest_auction_with_winner
        ON auction_with_winner.forest_file_id = latest_auction_with_winner.forest_file_id
        AND auction_with_winner.auction_date = latest_auction_with_winner.latest_auction_date
),

issued_active AS (
    SELECT
        CASE
            WHEN ou.org_unit_code IN ('TBA','TPL','TPG','TSK','TSN','TCC','TKA','TKO','TOC')
                THEN 'Interior'
            WHEN ou.org_unit_code IN ('TCH','TST','TSG')
                THEN 'Coast'
        END AS business_area_region_category,

        CASE
            WHEN ou.org_unit_code IN ('TBA','TPL','TPG','TSK','TSN')
                THEN 'North Interior'
            WHEN ou.org_unit_code IN ('TCC','TKA','TKO','TOC')
                THEN 'South Interior'
            WHEN ou.org_unit_code IN ('TCH','TST','TSG')
                THEN 'Coast'
        END AS business_area_region,

        CASE
            WHEN ou.org_unit_code IS NULL THEN NULL
            ELSE REPLACE(
                CONCAT(
                    CASE
                        WHEN ou.org_unit_name = 'Seaward-tlasta Timber Sales Office'
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

        ts.forest_file_id,
        pfu.file_type_code,
        ts.bcts_category_code,

        tt.legal_effective_dt AS legal_effective_date_fta,

        TO_DATE(
            FROM_UTC_TIMESTAMP(
                CURRENT_TIMESTAMP(),
                'America/Vancouver'
            )
        ) AS report_date,

        CASE
            WHEN ADD_MONTHS(tt.legal_effective_dt, 20) <
                TO_DATE(
                    FROM_UTC_TIMESTAMP(
                        CURRENT_TIMESTAMP(),
                        'America/Vancouver'
                    )
                )
            THEN 1
            ELSE 0
        END AS effective_date_add_20_flag,

        CASE
            WHEN ADD_MONTHS(
                    COALESCE(
                        tt.current_expiry_dt,
                        tt.initial_expiry_dt
                    ),
                    -6
                 ) <
                 TO_DATE(
                    FROM_UTC_TIMESTAMP(
                        CURRENT_TIMESTAMP(),
                        'America/Vancouver'
                    )
                 )
            THEN 1
            ELSE 0
        END AS expire_date_minus_6_flag,

        CASE
            WHEN tt.legal_effective_dt IS NULL THEN NULL
            ELSE CONCAT(
                'Fiscal ',
                YEAR(ADD_MONTHS(tt.legal_effective_dt, 9)) - 1,
                '/',
                YEAR(ADD_MONTHS(tt.legal_effective_dt, 9))
            )
        END AS legal_effective_fiscal_fta,

        tt.initial_expiry_dt AS initial_expiry_fta,
        tt.current_expiry_dt AS current_expiry_fta,
        COALESCE(tt.current_expiry_dt, tt.initial_expiry_dt) AS expiry_fta,

        tt.tenure_term AS advertised_licence_term,

        ROUND(
            MONTHS_BETWEEN(
                tt.current_expiry_dt,
                tt.initial_expiry_dt
            )
        ) AS extension_term,

        ROUND(
            MONTHS_BETWEEN(
                COALESCE(
                    tt.current_expiry_dt,
                    tt.initial_expiry_dt
                ),
                tt.legal_effective_dt
            )
        ) AS total_tenure_term,

        bid_info.client_number,
        bid_info.client,

        CASE
            WHEN tfsc.description IS NULL
                THEN pfu.file_status_st
            ELSE CONCAT(
                tfsc.description,
                ' (',
                pfu.file_status_st,
                ')'
            )
        END AS fta_file_status,

        pfu.file_status_date AS fta_file_status_date,
        ts.sale_volume

    FROM bctsadmin_replication.bcts_timber_sale ts

    INNER JOIN bcts_staging.fta_prov_forest_use pfu
        ON pfu.forest_file_id = ts.forest_file_id

    INNER JOIN bcts_staging.fta_tenure_term tt
        ON pfu.forest_file_id = tt.forest_file_id

    INNER JOIN bid_info
        ON ts.forest_file_id = bid_info.forest_file_id
        AND ts.auction_date = bid_info.auction_date

    INNER JOIN mofclient_replication.org_unit ou
        ON pfu.bcts_org_unit = ou.org_unit_no

    LEFT JOIN bcts_staging.fta_tenure_file_status_code tfsc
        ON pfu.file_status_st = tfsc.tenure_file_status_code

    WHERE ts.no_sale_rationale_code IS NULL
        AND pfu.file_type_code = 'B20'
        AND pfu.file_status_st IN ('HI', 'HS')
        AND tt.legal_effective_dt <
            TO_DATE(
                FROM_UTC_TIMESTAMP(
                    CURRENT_TIMESTAMP(),
                    'America/Vancouver'
                )
            )
),

hbs AS (
    SELECT
        p.bcts_org_unit,
        p.forest_file_id,
        m.timber_mark,
        m.cruise_based_ind,
        s.sale_volume,

        SUM(h.volume_scaled) AS billed_volume,

        SUM(
            CASE
                WHEN h.billing_type_code = 'WU'
                    THEN h.volume_scaled
            END
        ) AS billed_wu_volume,

        SUM(
            CASE
                WHEN h.billing_type_code = 'WA'
                    THEN h.volume_scaled
            END
        ) AS billed_wa_volume,

        SUM(h.total_amount) AS billed_amount,
        SUM(h.royalty_amount) AS royalty_amount,
        SUM(h.reserve_stmpg_amt) AS reserve_stumpage_amount,
        SUM(h.bonus_stumpage_amt) AS bonus_stumpage_amount,
        SUM(h.dev_levy_amount) AS dev_levy_amount

    FROM bcts_staging.fta_harvest_sale s

    INNER JOIN bcts_staging.fta_prov_forest_use p
        ON s.forest_file_id = p.forest_file_id

    INNER JOIN bcts_staging.fta_timber_mark m
        ON p.forest_file_id = m.forest_file_id

    LEFT JOIN lrm_replication.v_scaling_history h
        ON m.timber_mark = h.timber_mark

    WHERE p.bcts_org_unit IS NOT NULL

    GROUP BY
        p.bcts_org_unit,
        p.forest_file_id,
        m.timber_mark,
        m.cruise_based_ind,
        s.sale_volume
)

SELECT
    issued_active.*,
    hbs.cruise_based_ind,
    hbs.billed_volume,

    COALESCE(issued_active.sale_volume, 0)
        - COALESCE(hbs.billed_volume, 0) AS unbilled_volume,

    ROUND(
        (
            COALESCE(issued_active.sale_volume, 0)
            - COALESCE(hbs.billed_volume, 0)
        ) / issued_active.sale_volume * 100,
        1
    ) AS percent_unbilled

FROM issued_active

LEFT JOIN hbs
    ON issued_active.forest_file_id = hbs.forest_file_id

WHERE
    issued_active.expiry_fta >
    TO_DATE(
        FROM_UTC_TIMESTAMP(
            CURRENT_TIMESTAMP(),
            'America/Vancouver'
        )
    )
    AND (
        COALESCE(issued_active.sale_volume, 0)
        - COALESCE(hbs.billed_volume, 0)
    ) > 100;

CREATE OR REPLACE VIEW bcts_staging.licence_issued_with_unbilled_volume_main AS

SELECT
    official.business_area_region_category,
    official.business_area_region,
    official.business_area,
    lrm.field_team,
    official.forest_file_id AS licence_id,
    official.file_type_code AS tenure_type_fta,
    official.cruise_based_ind,
    official.fta_file_status,
    official.client_number,
    official.client,
    official.fta_file_status_date,
    official.legal_effective_date_fta,
    official.legal_effective_fiscal_fta,
    lrm.issued_done_lrm,
    lrm.issued_done_fiscal_lrm,
    official.initial_expiry_fta,
    official.current_expiry_fta,
    official.expiry_fta,
    lrm.expire_extend_lrm,
    official.advertised_licence_term,
    official.extension_term,
    official.total_tenure_term,
    official.sale_volume,
    official.billed_volume,
    official.unbilled_volume,
    official.percent_unbilled,
    lrm.licn_seq_nbr,
    lrm.last_logging_started_date,
    lrm.last_logging_completed_date,
    lrm.harvesting_status,
    official.report_date,

    official.effective_date_add_20_flag
        * official.unbilled_volume
        AS unbilled_volume_over_20_month,

    official.unbilled_volume
        * official.expire_date_minus_6_flag
        AS unbilled_volume_expire_in_6_month

FROM bcts_staging.licence_issued_with_unbilled_volume_official official

LEFT JOIN bcts_staging.licence_issued_with_unbilled_volume_lrm lrm
    ON official.forest_file_id = lrm.licence_id

WHERE lrm.substantial_completion_done_lrm IS NULL;

-- Insert the data into the _hist table
INSERT INTO bcts_staging.licence_issued_with_unbilled_volume_hist (
    business_area_region_category,
    business_area_region,
    business_area,
    field_team,
    licence_id,
    tenure_type_fta,
    cruise_based_ind,
    fta_file_status,
    client_number,
    client,
    fta_file_status_date,
    legal_effective_date_fta,
    legal_effective_fiscal_fta,
    issued_done_lrm,
    issued_done_fiscal_lrm,
    initial_expiry_fta,
    current_expiry_fta,
    expiry_fta,
    expire_extend_lrm,
    advertised_licence_term,
    extension_term,
    total_tenure_term,
    sale_volume,
    billed_volume,
    unbilled_volume,
    percent_unbilled,
    licn_seq_nbr,
    last_logging_started_date,
    last_logging_completed_date,
    harvesting_status,
    report_date,
    unbilled_volume_over_20_month,
    unbilled_volume_expire_in_6_month,
    report_run_date,
    report_run_timestamp
)
SELECT
    business_area_region_category,
    business_area_region,
    business_area,
    field_team,
    licence_id,
    tenure_type_fta,
    cruise_based_ind,
    fta_file_status,
    client_number,
    client,
    fta_file_status_date,
    legal_effective_date_fta,
    legal_effective_fiscal_fta,
    issued_done_lrm,
    issued_done_fiscal_lrm,
    initial_expiry_fta,
    current_expiry_fta,
    expiry_fta,
    expire_extend_lrm,
    advertised_licence_term,
    extension_term,
    total_tenure_term,
    sale_volume,
    billed_volume,
    unbilled_volume,
    percent_unbilled,
    licn_seq_nbr,
    last_logging_started_date,
    last_logging_completed_date,
    harvesting_status,
    report_date,
    unbilled_volume_over_20_month,
    unbilled_volume_expire_in_6_month,

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

FROM bcts_staging.licence_issued_with_unbilled_volume_main;

-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.

DROP TABLE IF EXISTS bcts_reporting.licence_issued_with_unbilled_volume_main cascade;
CREATE TABLE bcts_reporting.licence_issued_with_unbilled_volume_main AS SELECT * FROM bcts_staging.licence_issued_with_unbilled_volume_main;

DROP TABLE IF EXISTS bcts_reporting.licence_issued_with_unbilled_volume_hist cascade;
CREATE TABLE bcts_reporting.licence_issued_with_unbilled_volume_hist AS SELECT * FROM bcts_staging.licence_issued_with_unbilled_volume_hist;    
    