# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c35a1880-4d21-42b2-b055-983b7ac0563b",
# META       "default_lakehouse_name": "bcts_lakehouse",
# META       "default_lakehouse_workspace_id": "7a798b59-e76b-4af6-9ffb-fa0a2959519c",
# META       "known_lakehouses": [
# META         {
# META           "id": "c35a1880-4d21-42b2-b055-983b7ac0563b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Run ad-hoc queries against the lakehouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql = \
"""
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


"""

sql = sql.replace('$report_start_date', '2026-04-01')
sql = sql.replace('$report_end_date', '2026-07-31')
spark.sql(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql = "select 1 from 2 where start_date = $start_date"
sql = sql.replace('$start_date', '2024-02-02')
sql

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
