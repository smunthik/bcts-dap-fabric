-- Create _hist table if it does not exist
CREATE TABLE IF NOT EXISTS bcts_staging.silviliability_main_hist
(
    business_area_region_category STRING,
    business_area_region STRING,
    business_area STRING,
    business_area_code STRING,
    field_team STRING,
    manu_id STRING,
    tenure STRING,
    licence STRING,
    hbs_first_billing TIMESTAMP,
    hbs_volume_billed DECIMAL(38,18),
    block_id STRING,
    block_nbr STRING,
    ubi STRING,
    block_state STRING,
    fg_done TIMESTAMP,
    results_fg_declared TIMESTAMP,
    cutb_opening STRING,
    sp_exempt STRING,
    cruise_volume DECIMAL(15,6),
    nar_area DECIMAL(38,18),
    largest_eco_area DECIMAL(38,18),
    count_bec_with_largest_eco_area BIGINT,
    bec STRING,
    bio_zone STRING,
    bio_subzone STRING,
    bio_variant STRING,
    hvs_status STRING,
    hvs_date TIMESTAMP,
    hvs_fiscal DECIMAL(38,18),
    hvc_status STRING,
    hvc_date TIMESTAMP,
    hvc_fiscal DECIMAL(38,18),
    treatment_unit STRING,
    block_funding STRING,
    activity_funding STRING,
    base STRING,
    technique STRING,
    method STRING,
    activity STRING,
    status STRING,
    start_date TIMESTAMP,
    start_fiscal DECIMAL(38,18),
    complete_date TIMESTAMP,
    complete_fiscal DECIMAL(38,18),
    treatment_area DECIMAL(38,18),
    sila_gross_area_char STRING,
    cost_per_ha_plan DECIMAL(38,18),
    cost_per_ha DECIMAL(38,18),
    planned_total_cost DECIMAL(38,18),
    cost_per_ha_plan_x_ha DECIMAL(38,18),
    actual_total_cost DECIMAL(38,18),
    cost_per_ha_x_ha DECIMAL(38,18),
    planned_service_line STRING,
    planned_sl_description STRING,
    items_planned_total_cost DECIMAL(38,18),
    actual_service_line STRING,
    actual_sl_description STRING,
    items_actual_total_cost DECIMAL(38,18),
    planting_unit DECIMAL(3,0),
    plun_digitised_ind STRING,
    plun_planned_total_cost DECIMAL(15,5),
    plun_item_planned_total_cost DECIMAL(38,18),
    plun_actual_total_cost DECIMAL(15,5),
    plun_item_actual_total_cost DECIMAL(38,18),
    plun_ha_area DECIMAL(11,6),
    plun_net_area DECIMAL(11,6),
    plun_density DECIMAL(7,2),
    total_trees DECIMAL(12,0),
    seedling_request STRING,
    sica_key_ind STRING,
    plun_modifiedby STRING,
    plun_modifiedon TIMESTAMP,
    plun_modifiedusing STRING,
    modifiedby STRING,
    modifiedon TIMESTAMP,
    primary_record STRING,
    need_planting STRING,
    salvage_any_fire_year STRING,
    salvage_fire_years STRING,
    salvage_2021_fire STRING,
    salvage_2022_fire STRING,
    salvage_2023_fire STRING,
    salvage_2024_fire STRING,
    salvage_2025_fire STRING,

    cfs_total_amount DECIMAL(38,18),
    cfs_first_layout_amount DECIMAL(38,18),
    cfs_last_layout_amount DECIMAL(38,18),
    cfs_layout_amount_no_7017_7018 DECIMAL(38,18),
    cfs_layout_amount_7017 DECIMAL(38,18),
    cfs_layout_amount_7018 DECIMAL(38,18),
    cfs_layout_amount DECIMAL(38,18),
    cfs_planning_amount DECIMAL(38,18),
    cfs_silviculture_amount DECIMAL(38,18),
    cfs_service_line_42420 DECIMAL(38,18),
    cfs_service_line_42421 DECIMAL(38,18),
    cfs_service_line_42430 DECIMAL(38,18),
    cfs_service_line_42467 DECIMAL(38,18),
    cfs_service_line_42468 DECIMAL(38,18),
    cfs_service_line_42469 DECIMAL(38,18),
    cfs_service_line_42470 DECIMAL(38,18),
    cfs_service_line_42471 DECIMAL(38,18),
    cfs_service_line_42472 DECIMAL(38,18),
    cfs_service_line_42501 DECIMAL(38,18),
    cfs_service_line_42503 DECIMAL(38,18),
    cfs_service_line_42504 DECIMAL(38,18),
    cfs_service_line_42506 DECIMAL(38,18),
    cfs_service_line_42507 DECIMAL(38,18),
    cfs_service_line_42520 DECIMAL(38,18),
    cfs_service_line_42521 DECIMAL(38,18),
    cfs_service_line_42524 DECIMAL(38,18),
    cfs_service_line_42550 DECIMAL(38,18),
    cfs_service_line_42552 DECIMAL(38,18),
    cfs_service_line_42553 DECIMAL(38,18),
    cfs_service_line_42554 DECIMAL(38,18),
    cfs_service_line_42556 DECIMAL(38,18),
    cfs_service_line_42558 DECIMAL(38,18),
    cfs_service_line_42559 DECIMAL(38,18),
    cfs_service_line_42579 DECIMAL(38,18),
    plun_seq_nbr DECIMAL(38,18),
    sila_seq_nbr DECIMAL(15,0),
    sica_seq_nbr DECIMAL(15,0),
    cutb_seq_nbr BIGINT,
    licn_seq_nbr BIGINT,
    report_run_date DATE,
    report_run_timestamp TIMESTAMP,
    report_end_date DATE
)
USING DELTA;


CREATE OR REPLACE VIEW BCTS_STAGING.SILVILIABILITY_LRM AS

/* Licence Selection */
WITH BL AS
(
    SELECT
        A.CUTB_SEQ_NBR
    FROM
        LRM_REPLICATION.ACTIVITY_CLASS AC
        INNER JOIN LRM_REPLICATION.ACTIVITY_TYPE AT
            ON AC.ACCL_SEQ_NBR = AT.ACCL_SEQ_NBR
           AND AC.DIVI_DIV_NBR = AT.DIVI_DIV_NBR
        INNER JOIN LRM_REPLICATION.ACTIVITY A
            ON AT.ACTT_SEQ_NBR = A.ACTT_SEQ_NBR
    WHERE
        AC.ACCL_DESCRIPTION = 'CMB'
        AND AT.ACTT_KEY_IND IN ('HVC', 'HVS')
        AND A.PLAN_SEQ_NBR IS NULL
        AND A.ACTI_STATUS_IND = 'D'
        AND A.ACTI_STATUS_DATE >= DATE '1994-04-01'
    GROUP BY
        A.CUTB_SEQ_NBR
),

M AS
(
    SELECT
        HV.MARK_SEQ_NBR,
        SUM(HV.BCHH_VOLUME_BILLED) AS HBS_VOLUME_BILLED,
        MIN(HV.BCHH_BILLING_PERIOD) AS FIRST_BILLING
    FROM
        LRM_REPLICATION.BCTS_HARVEST_HISTORY HV
    GROUP BY
        HV.MARK_SEQ_NBR
),

LS AS
(
    SELECT
        BA.LICN_SEQ_NBR,
        MAX(M.HBS_VOLUME_BILLED) AS HBS_VOLUME_BILLED,
        MIN(M.FIRST_BILLING) AS HBS_FIRST_BILLING
    FROM
        LRM_REPLICATION.BLOCK_ALLOCATION BA
        LEFT JOIN BL
            ON BA.CUTB_SEQ_NBR = BL.CUTB_SEQ_NBR
        LEFT JOIN M
            ON BA.MARK_SEQ_NBR = M.MARK_SEQ_NBR
    WHERE
        BL.CUTB_SEQ_NBR IS NOT NULL
        OR M.MARK_SEQ_NBR IS NOT NULL
    GROUP BY
        BA.LICN_SEQ_NBR
),

/* Harvest started and completed */
HV AS
(
    SELECT
        A.CUTB_SEQ_NBR,
        MAX(
            CASE
                WHEN AT.ACTT_KEY_IND = 'HVC'
                THEN A.ACTI_STATUS_IND
            END
        ) AS HVC_STATUS,
        MAX(
            CASE
                WHEN AT.ACTT_KEY_IND = 'HVC'
                THEN A.ACTI_STATUS_DATE
            END
        ) AS HVC_DATE,
        MAX(
            CASE
                WHEN AT.ACTT_KEY_IND = 'HVS'
                THEN A.ACTI_STATUS_IND
            END
        ) AS HVS_STATUS,
        MAX(
            CASE
                WHEN AT.ACTT_KEY_IND = 'HVS'
                THEN A.ACTI_STATUS_DATE
            END
        ) AS HVS_DATE
    FROM
        LRM_REPLICATION.ACTIVITY_CLASS AC
        INNER JOIN LRM_REPLICATION.ACTIVITY_TYPE AT
            ON AC.ACCL_SEQ_NBR = AT.ACCL_SEQ_NBR
           AND AC.DIVI_DIV_NBR = AT.DIVI_DIV_NBR
        INNER JOIN LRM_REPLICATION.ACTIVITY A
            ON AT.ACTT_SEQ_NBR = A.ACTT_SEQ_NBR
    WHERE
        AC.ACCL_DESCRIPTION = 'CMB'
        AND AT.ACTT_KEY_IND IN ('HVC', 'HVS')
        AND A.PLAN_SEQ_NBR IS NULL
    GROUP BY
        A.CUTB_SEQ_NBR
),

/* NAR */
NAR AS
(
    SELECT
        SILP.CUTB_SEQ_NBR,
        SUM(SU.STUN_GROSS_HA_AREA) AS NAR_AREA
    FROM
        LRM_REPLICATION.STANDARD_UNIT SU
        INNER JOIN LRM_REPLICATION.SILVICULTURE_PRESCRIPTION SILP
            ON SU.SILP_SEQ_NBR = SILP.SILP_SEQ_NBR
        INNER JOIN LRM_REPLICATION.SU_TYPE SUTY
            ON SU.SUTY_TYPE_ID = SUTY.SUTY_TYPE_ID
    WHERE
        SUTY.SUTY_KEY_IND = 'PRODV'
    GROUP BY
        SILP.CUTB_SEQ_NBR
),

/* Need Planting - ART */
ART AS
(
    SELECT
        M.CUTB_SEQ_NBR
    FROM
        LRM_REPLICATION.SILVICULTURE_STRATUM M
        INNER JOIN LRM_REPLICATION.SILVICULTURE_STOCKING_STATUS K
            ON M.SIST_SEQ_NBR = K.SIST_SEQ_NBR
    WHERE
        K.STTP_STOCKING_TYPE_ID = 'ART'
    GROUP BY
        M.CUTB_SEQ_NBR
),

/*
Largest BEC in Block.

The area of each matching BEC variant (Zone, Subzone, and Variant must match)
is summed together. The summed area of each BEC variant is compared; the
variant with the largest area is identified. If multiple variants tie for
maximum summed area, each BEC variant is listed.
*/
AREA_EACH_BEC AS
(
    SELECT
        CUTB_SEQ_NBR,
        GROSS_AREA_BEC,
        BEC,
        BIO_ZONE,
        BIO_SUBZONE,
        BIO_VARIANT,
        RANK() OVER
        (
            PARTITION BY CUTB_SEQ_NBR
            ORDER BY GROSS_AREA_BEC DESC
        ) AS BEC_RANK
    FROM
    (
        SELECT
            E.CUTB_SEQ_NBR,
            SUM(E.GROSS_AREA) AS GROSS_AREA_BEC,
            CONCAT(
                E.BIOZ_ZONE_ID,
                E.BIOS_SUBZONE_ID,
                E.BIOV_VARIANT_ID
            ) AS BEC,
            CASE
                WHEN BZ.BIOZ_ZONE_NAME IS NULL
                    THEN E.BIOZ_ZONE_ID
                ELSE CONCAT(
                    BZ.BIOZ_ZONE_NAME,
                    ' (',
                    E.BIOZ_ZONE_ID,
                    ')'
                )
            END AS BIO_ZONE,
            CASE
                WHEN BSZ.BIOS_SUBZONE_NAME IS NOT NULL
                    THEN CONCAT(
                        BSZ.BIOS_SUBZONE_NAME,
                        ' (',
                        E.BIOZ_ZONE_ID,
                        E.BIOS_SUBZONE_ID,
                        ')'
                    )
                WHEN E.BIOS_SUBZONE_ID IS NOT NULL
                    THEN CONCAT(
                        E.BIOZ_ZONE_ID,
                        E.BIOS_SUBZONE_ID
                    )
            END AS BIO_SUBZONE,
            CASE
                WHEN BV.BIOV_VARIANT_NAME IS NOT NULL
                    THEN CONCAT(
                        BV.BIOV_VARIANT_NAME,
                        ' (',
                        E.BIOZ_ZONE_ID,
                        E.BIOS_SUBZONE_ID,
                        E.BIOV_VARIANT_ID,
                        ')'
                    )
                WHEN E.BIOS_SUBZONE_ID IS NOT NULL
                     AND E.BIOV_VARIANT_ID IS NOT NULL
                    THEN CONCAT(
                        E.BIOZ_ZONE_ID,
                        E.BIOS_SUBZONE_ID,
                        E.BIOV_VARIANT_ID
                    )
            END AS BIO_VARIANT
        FROM
            LRM_REPLICATION.V_ECOLOGY E
            LEFT JOIN LRM_REPLICATION.BIOGEOCLIMATIC_ZONE BZ
                ON E.BIOZ_ZONE_ID = BZ.BIOZ_ZONE_ID
            LEFT JOIN LRM_REPLICATION.BIOGEOCLIMATIC_SUBZONE BSZ
                ON E.BIOZ_ZONE_ID = BSZ.BIOZ_ZONE_ID
               AND E.BIOS_SUBZONE_ID = BSZ.BIOS_SUBZONE_ID
            LEFT JOIN LRM_REPLICATION.BIOGEOCLIMATIC_VARIANT BV
                ON E.BIOZ_ZONE_ID = BV.BIOZ_ZONE_ID
               AND E.BIOS_SUBZONE_ID = BV.BIOS_SUBZONE_ID
               AND E.BIOV_VARIANT_ID = BV.BIOV_VARIANT_ID
        GROUP BY
            E.CUTB_SEQ_NBR,
            E.BIOZ_ZONE_ID,
            E.BIOS_SUBZONE_ID,
            E.BIOV_VARIANT_ID,
            BZ.BIOZ_ZONE_NAME,
            BSZ.BIOS_SUBZONE_NAME,
            BV.BIOV_VARIANT_NAME
    ) AB
),

BIO AS
(
    SELECT
        CUTB_SEQ_NBR,
        MAX(GROSS_AREA_BEC) AS LARGEST_ECO_AREA,
        COUNT(DISTINCT BEC) AS COUNT_BEC_WITH_LARGEST_ECO_AREA,
        CONCAT_WS(
            ', ',
            SORT_ARRAY(COLLECT_SET(BEC))
        ) AS BEC,
        CONCAT_WS(
            ', ',
            SORT_ARRAY(COLLECT_SET(BIO_ZONE))
        ) AS BIO_ZONE,
        CONCAT_WS(
            ', ',
            SORT_ARRAY(COLLECT_SET(BIO_SUBZONE))
        ) AS BIO_SUBZONE,
        CONCAT_WS(
            ', ',
            SORT_ARRAY(COLLECT_SET(BIO_VARIANT))
        ) AS BIO_VARIANT
    FROM
        AREA_EACH_BEC
    WHERE
        BEC_RANK = 1
    GROUP BY
        CUTB_SEQ_NBR
),

/* Silviculture */
SA AS
(
    SELECT
        SA0.SILA_TREATMENT_UNIT_ID,
        SA0.FUND_FUNDING_CODE AS ACTIVITY_FUNDING,
        SA0.SILA_STATUS,
        SA0.RESP_RESPONSIBILITY_CODE,
        SA0.SILA_START_DATE,
        SA0.SILA_COMPLETION_DATE,
        SA0.SILA_GROSS_HA_AREA,
        SA0.SILA_COST_PER_HA_PLAN,
        SA0.SILA_COST_PER_HA,
        SUBSTR(SA0.SILA_COMMENTS, 1, 4000) AS COMMENTS,
        SA0.SILA_FORMB_DATE,
        SA0.SILA_FORMB_PRINTED,
        SA0.MODIFIEDBY,
        SA0.MODIFIEDON,
        PLU.PLUN_PLANTING_UNIT_ID,
        PLU.PLUN_DIGITISED_IND,
        PLU.PLUN_PLANTING_PLAN_COST,
        PLU.PLUN_PLANTING_COST,
        PLU.PLUN_HA_AREA,
        PLU.PLUN_NET_AREA,
        PLU.PLUN_DENSITY,
        PLU.TOTAL_TREES,
        PLU.MODIFIEDBY AS PLUN_MODIFIEDBY,
        PLU.MODIFIEDON AS PLUN_MODIFIEDON,
        PLU.MODIFIEDUSING AS PLUN_MODIFIEDUSING,
        COALESCE(PLU.PLUN_SEQ_NBR, 0) AS PLUN_SEQ_NBR,
        SA0.SILA_SEQ_NBR,
        SA0.SICA_SEQ_NBR,
        SA0.CUTB_SEQ_NBR
    FROM
        LRM_REPLICATION.SILVICULTURE_ACTIVITY SA0
        LEFT JOIN LRM_REPLICATION.PLANTING_UNIT PLU
            ON SA0.SILA_SEQ_NBR = PLU.SILA_SEQ_NBR
),

/* Species */
SP AS
(
    SELECT
        BTS.SILA_SEQ_NBR,
        CONCAT_WS(
            ', ',
            SORT_ARRAY(COLLECT_LIST(BTS.SISP_SPECIES_ID))
        ) AS TARGET_SPECIES
    FROM
        LRM_REPLICATION.BRUSHING_TARGET_SPECIES BTS
    GROUP BY
        BTS.SILA_SEQ_NBR
),

SAC0 AS
(
    SELECT
        C0.SILA_SEQ_NBR,
        C0.PLUN_SEQ_NBR,
        MAX(
            CASE
                WHEN C0.SL_RANK = 1
                     AND UPPER(C0.SACC_SERIES) = 'PLANNED'
                THEN C0.SCAC_SEQ_NBR
            END
        ) AS P_SCAC_SEQ_NBR,
        MAX(
            CASE
                WHEN C0.SL_RANK = 1
                     AND UPPER(C0.SACC_SERIES) = 'ACTUAL'
                THEN C0.SCAC_SEQ_NBR
            END
        ) AS A_SCAC_SEQ_NBR,
        SUM(
            CASE
                WHEN UPPER(C0.SACC_SERIES) = 'PLANNED'
                THEN C0.SUB_COST
                ELSE 0
            END
        ) AS ITEM_TOTAL_PLANNED,
        SUM(
            CASE
                WHEN UPPER(C0.SACC_SERIES) = 'ACTUAL'
                THEN C0.SUB_COST
                ELSE 0
            END
        ) AS ITEM_TOTAL_ACTUAL
    FROM
    (
        SELECT
            C.SILA_SEQ_NBR,
            C.PLUN_SEQ_NBR,
            C.SCAC_SEQ_NBR,
            SUM(C.SACC_ITEM_COST) AS SUB_COST,
            RANK() OVER
            (
                PARTITION BY
                    C.SILA_SEQ_NBR,
                    C.PLUN_SEQ_NBR,
                    C.SACC_SERIES
                ORDER BY
                    SUM(COALESCE(C.SACC_ITEM_COST, 0))
            ) AS SL_RANK,
            C.SACC_SERIES
        FROM
            LRM_REPLICATION.SILV_ACTIVITY_COST C
            LEFT JOIN
            (
                /* Planting activities */
                SELECT
                    SILA_SEQ_NBR
                FROM
                    LRM_REPLICATION.SILV_ACTIVITY_COST
                GROUP BY
                    SILA_SEQ_NBR
                HAVING
                    MAX(PLUN_SEQ_NBR) IS NOT NULL
            ) SPL
                ON C.SILA_SEQ_NBR = SPL.SILA_SEQ_NBR
        WHERE
            SPL.SILA_SEQ_NBR IS NULL
            OR
            (
                SPL.SILA_SEQ_NBR IS NOT NULL
                AND C.PLUN_SEQ_NBR IS NOT NULL
            )
        GROUP BY
            C.SILA_SEQ_NBR,
            C.PLUN_SEQ_NBR,
            C.SCAC_SEQ_NBR,
            C.SACC_SERIES
    ) C0
    GROUP BY
        C0.SILA_SEQ_NBR,
        C0.PLUN_SEQ_NBR
),

/* Total by items and get service line ranked by total cost */
SAC AS
(
    SELECT
        SAC0.SILA_SEQ_NBR,
        COALESCE(SAC0.PLUN_SEQ_NBR, 0) AS PLUN_SEQ_NBR,
        SAC0.ITEM_TOTAL_PLANNED,
        SAC0.ITEM_TOTAL_ACTUAL,
        CI0.CSTI_COST_ITEM_ACCOUNT_CODE AS PLANNED_SERVICE_LINE,
        CI0.CSTI_COST_ITEM_DESCRIPTION AS PLANNED_SL_DESCRIPTION,
        CI1.CSTI_COST_ITEM_ACCOUNT_CODE AS ACTUAL_SERVICE_LINE,
        CI1.CSTI_COST_ITEM_DESCRIPTION AS ACTUAL_SL_DESCRIPTION,
        SCAC0.CSTI_COST_ITEM_ID AS PLANNED_COST_ITEM_ID,
        SCAC1.CSTI_COST_ITEM_ID AS ACTUAL_COST_ITEM_ID
    FROM
        SAC0
        LEFT JOIN LRM_REPLICATION.SILV_COMPANY_ACTIVITY_COST SCAC0
            ON SAC0.P_SCAC_SEQ_NBR = SCAC0.SCAC_SEQ_NBR
        LEFT JOIN LRM_REPLICATION.FRST_COST_ITEM CI0
            ON SCAC0.DIVI_DIV_NBR = CI0.DIVI_DIV_NBR
           AND SCAC0.CSTI_COST_ITEM_ID = CI0.CSTI_COST_ITEM_ID
        LEFT JOIN LRM_REPLICATION.SILV_COMPANY_ACTIVITY_COST SCAC1
            ON SAC0.A_SCAC_SEQ_NBR = SCAC1.SCAC_SEQ_NBR
        LEFT JOIN LRM_REPLICATION.FRST_COST_ITEM CI1
            ON SCAC1.DIVI_DIV_NBR = CI1.DIVI_DIV_NBR
           AND SCAC1.CSTI_COST_ITEM_ID = CI1.CSTI_COST_ITEM_ID
),

SACT AS
(
    /* All items total */
    SELECT
        C.SILA_SEQ_NBR,
        SUM(
            CASE
                WHEN UPPER(C.SACC_SERIES) = 'PLANNED'
                THEN C.SACC_ITEM_COST
                ELSE 0
            END
        ) AS ITEMS_TOTAL_PLANNED,
        SUM(
            CASE
                WHEN UPPER(C.SACC_SERIES) = 'ACTUAL'
                THEN C.SACC_ITEM_COST
                ELSE 0
            END
        ) AS ITEMS_TOTAL_ACTUAL
    FROM
        LRM_REPLICATION.SILV_ACTIVITY_COST C
        LEFT JOIN
        (
            /* Planting activities */
            SELECT
                SILA_SEQ_NBR
            FROM
                LRM_REPLICATION.SILV_ACTIVITY_COST
            GROUP BY
                SILA_SEQ_NBR
            HAVING
                MAX(PLUN_SEQ_NBR) IS NOT NULL
        ) SPL
            ON C.SILA_SEQ_NBR = SPL.SILA_SEQ_NBR
    WHERE
        SPL.SILA_SEQ_NBR IS NULL
        OR
        (
            SPL.SILA_SEQ_NBR IS NOT NULL
            AND C.PLUN_SEQ_NBR IS NOT NULL
        )
    GROUP BY
        C.SILA_SEQ_NBR
),

/* Request key */
RK AS
(
    SELECT
        SQ.PLUN_SEQ_NBR,
        CONCAT_WS(
            ' ',
            TRANSFORM(
                SORT_ARRAY(
                    COLLECT_LIST(
                        NAMED_STRUCT(
                            'species_id',
                            SQ.SISP_SPECIES_ID,
                            'request_key',
                            SQ.SIRK_REQUEST_KEY,
                            'seed_lot_number',
                            SQ.SISL_SEED_LOT_NUMBER,
                            'request_text',
                            CONCAT(
                                CAST(SQ.SISP_SPECIES_ID AS STRING),
                                '/Lot:',
                                CAST(SQ.SISL_SEED_LOT_NUMBER AS STRING),
                                '/$',
                                CAST(SQ.PLSP_PRICE_PER_TREE AS STRING),
                                '/Trees:',
                                CAST(SQ.PLSP_NUMBER_TREES AS STRING),
                                '/Req_Key:',
                                CAST(SQ.SIRK_REQUEST_KEY AS STRING)
                            )
                        )
                    )
                ),
                X -> X.REQUEST_TEXT
            )
        ) AS SEEDLING_REQUEST
    FROM
    (
        SELECT
            PS.PLUN_SEQ_NBR,
            PS.SISP_SPECIES_ID,
            PS.SISL_SEED_LOT_NUMBER,
            PS.PLSP_PRICE_PER_TREE,
            PS.PLSP_NUMBER_TREES,
            PS.SIRK_REQUEST_KEY
        FROM
            LRM_REPLICATION.PLANTING_SPECIES PS
    ) SQ
    GROUP BY
        SQ.PLUN_SEQ_NBR
),

S AS
(
    SELECT
        SA.SILA_TREATMENT_UNIT_ID,
        SA.ACTIVITY_FUNDING,
        SCA.SIAB_BASE_CODE,
        SCA.SIAT_TECHNIQUE_CODE,
        SCA.SIAM_METHOD_CODE,
        SCA.SIAO_OBJECTIVE_CODE AS OBJECTIVE1,
        SCA.SIAO_OBJECTIVE_CODE2 AS OBJECTIVE2,
        SCA.SIAO_OBJECTIVE_CODE3 AS OBJECTIVE3,
        SCA.SICA_KEY_IND,
        SCA.SICA_ACTIVITY_NAME,
        SA.SILA_STATUS,
        SA.RESP_RESPONSIBILITY_CODE,
        SA.SILA_START_DATE,
        SA.SILA_COMPLETION_DATE,
        SA.SILA_GROSS_HA_AREA,
        SA.SILA_COST_PER_HA_PLAN,
        SA.SILA_COST_PER_HA,
        SAC.PLANNED_SERVICE_LINE,
        SAC.PLANNED_SL_DESCRIPTION,
        SACT.ITEMS_TOTAL_PLANNED,
        SAC.ITEM_TOTAL_PLANNED,
        SAC.ACTUAL_SERVICE_LINE,
        SAC.ACTUAL_SL_DESCRIPTION,
        SACT.ITEMS_TOTAL_ACTUAL,
        SAC.ITEM_TOTAL_ACTUAL,
        SA.PLUN_PLANTING_UNIT_ID,
        SA.PLUN_DIGITISED_IND,
        SA.PLUN_PLANTING_PLAN_COST,
        SA.PLUN_PLANTING_COST,
        SA.PLUN_HA_AREA,
        SA.PLUN_NET_AREA,
        SA.PLUN_DENSITY,
        SA.TOTAL_TREES,
        RK.SEEDLING_REQUEST,
        SA.PLUN_MODIFIEDBY,
        SA.PLUN_MODIFIEDON,
        SA.PLUN_MODIFIEDUSING,
        DS.DSAS_APPLY_RATE AS APPLY_RATE,
        DS.PMPL_PMP_NBR AS PMP_NBR,
        SP.TARGET_SPECIES,
        SA.COMMENTS,
        SA.SILA_FORMB_DATE,
        SA.SILA_FORMB_PRINTED,
        SA.MODIFIEDBY,
        SA.MODIFIEDON,
        RANK() OVER
        (
            PARTITION BY SA.SILA_SEQ_NBR
            ORDER BY
                SA.PLUN_PLANTING_UNIT_ID,
                SA.PLUN_SEQ_NBR,
                SAC.ACTUAL_COST_ITEM_ID,
                SAC.PLANNED_COST_ITEM_ID
        ) AS SILA_RANK,
        CASE
            WHEN SA.PLUN_SEQ_NBR = 0 THEN NULL
            ELSE SA.PLUN_SEQ_NBR
        END AS PLUN_SEQ_NBR,
        SA.SILA_SEQ_NBR,
        SA.SICA_SEQ_NBR,
        SA.CUTB_SEQ_NBR
    FROM
        LRM_REPLICATION.SILVICULTURE_COMPANY_ACTIVITY SCA
        INNER JOIN SA
            ON SCA.SICA_SEQ_NBR = SA.SICA_SEQ_NBR
        LEFT JOIN RK
            ON SA.PLUN_SEQ_NBR = RK.PLUN_SEQ_NBR
        LEFT JOIN SAC
            ON SA.SILA_SEQ_NBR = SAC.SILA_SEQ_NBR
           AND SA.PLUN_SEQ_NBR = SAC.PLUN_SEQ_NBR
        LEFT JOIN SACT
            ON SA.SILA_SEQ_NBR = SACT.SILA_SEQ_NBR
        LEFT JOIN LRM_REPLICATION.DETAILED_SITE_ASSESSMENT DS
            ON SA.SILA_SEQ_NBR = DS.SILA_SEQ_NBR
        LEFT JOIN SP
            ON SA.SILA_SEQ_NBR = SP.SILA_SEQ_NBR
    WHERE
        COALESCE(SCA.SICA_KEY_IND, ' ') <> 'HV'
        AND SCA.SIAB_BASE_CODE IS NOT NULL
),

/* Salvage - any fire year */
SALVAGE_ANY_FIRE_YEAR AS
(
    SELECT
        CUTB_SEQ_NBR,
        CONCAT_WS(
            ', ',
            SORT_ARRAY(
                COLLECT_SET(
                    SUBSTRING(
                        ACTIVITY_TYPE,
                        INSTR(ACTIVITY_TYPE, '2'),
                        4
                    )
                )
            )
        ) AS SALVAGE_FIRE_YEARS
    FROM
        LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL
    WHERE
        ACTIVITY_CLASS = 'CSB'
        AND ACTT_KEY_IND LIKE 'SFIRE%'
    GROUP BY
        CUTB_SEQ_NBR
),

/* Salvage - 2021 Fire */
SALVAGE21 AS
(
    SELECT DISTINCT
        CUTB_SEQ_NBR,
        ACTIVITY_CLASS,
        ACTT_KEY_IND,
        ACTIVITY_TYPE
    FROM
        LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL
    WHERE
        ACTIVITY_CLASS = 'CSB'
        AND ACTT_KEY_IND = 'SFIRE21'
),

/* Salvage - 2022 Fire */
SALVAGE22 AS
(
    SELECT DISTINCT
        CUTB_SEQ_NBR,
        ACTIVITY_CLASS,
        ACTT_KEY_IND,
        ACTIVITY_TYPE
    FROM
        LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL
    WHERE
        ACTIVITY_CLASS = 'CSB'
        AND ACTT_KEY_IND = 'SFIRE22'
),

/* Salvage - 2023 Fire */
SALVAGE23 AS
(
    SELECT DISTINCT
        CUTB_SEQ_NBR,
        ACTIVITY_CLASS,
        ACTT_KEY_IND,
        ACTIVITY_TYPE
    FROM
        LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL
    WHERE
        ACTIVITY_CLASS = 'CSB'
        AND ACTT_KEY_IND = 'SFIRE23'
),

/* Salvage - 2024 Fire */
SALVAGE24 AS
(
    SELECT DISTINCT
        CUTB_SEQ_NBR,
        ACTIVITY_CLASS,
        ACTT_KEY_IND,
        ACTIVITY_TYPE
    FROM
        LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL
    WHERE
        ACTIVITY_CLASS = 'CSB'
        AND ACTT_KEY_IND = 'SFIRE24'
),

/* Salvage - 2025 Fire */
SALVAGE25 AS
(
    SELECT DISTINCT
        CUTB_SEQ_NBR,
        ACTIVITY_CLASS,
        ACTT_KEY_IND,
        ACTIVITY_TYPE
    FROM
        LRM_REPLICATION.V_BLOCK_ACTIVITY_ALL
    WHERE
        ACTIVITY_CLASS = 'CSB'
        AND ACTT_KEY_IND = 'SFIRE25'
),

/* Free Growing Met, Done (FG) */
FG AS
(
    SELECT
        AF.CUTB_SEQ_NBR,
        AF.ACTI_STATUS_DATE AS FG_DONE
    FROM
        LRM_REPLICATION.ACTIVITY_CLASS ACF
        INNER JOIN LRM_REPLICATION.ACTIVITY_TYPE ATYPEF
            ON ACF.ACCL_SEQ_NBR = ATYPEF.ACCL_SEQ_NBR
           AND ACF.DIVI_DIV_NBR = ATYPEF.DIVI_DIV_NBR
        INNER JOIN LRM_REPLICATION.ACTIVITY AF
            ON ATYPEF.ACTT_SEQ_NBR = AF.ACTT_SEQ_NBR
    WHERE
        ATYPEF.ACTT_KEY_IND = 'FG'
        AND ACF.ACCL_DESCRIPTION = 'CMB'
        AND AF.ACTI_STATUS_IND = 'D'
)

SELECT
    CASE
        WHEN FD.DIVI_SHORT_CODE IN (
            'TBA', 'TPL', 'TPG', 'TSK', 'TSN',
            'TCC', 'TKA', 'TKO', 'TOC'
        ) THEN 'Interior'
        WHEN FD.DIVI_SHORT_CODE IN (
            'TCH', 'TST', 'TSG'
        ) THEN 'Coast'
    END AS BUSINESS_AREA_REGION_CATEGORY,

    CASE
        WHEN FD.DIVI_SHORT_CODE IN (
            'TBA', 'TPL', 'TPG', 'TSK', 'TSN'
        ) THEN 'North Interior'
        WHEN FD.DIVI_SHORT_CODE IN (
            'TCC', 'TKA', 'TKO', 'TOC'
        ) THEN 'South Interior'
        WHEN FD.DIVI_SHORT_CODE IN (
            'TCH', 'TST', 'TSG'
        ) THEN 'Coast'
    END AS BUSINESS_AREA_REGION,

    CONCAT(
        COALESCE(
            NULLIF(FD.DIVI_DIVISION_NAME, 'Seaward'),
            'Seaward-Tlasta'
        ),
        ' (',
        FD.DIVI_SHORT_CODE,
        ')'
    ) AS BUSINESS_AREA,

    FD.DIVI_ABBREVIATION_CODE AS BUSINESS_AREA_CODE,
    LKF.COLU_LOOKUP_DESC AS FIELD_TEAM,
    MU.MANU_ID,
    TENT.TENT_TENURE_ID AS TENURE,
    LICN.LICN_LICENCE_ID AS LICENCE,
    LS.HBS_FIRST_BILLING,
    LS.HBS_VOLUME_BILLED,
    CB.CUTB_BLOCK_ID AS BLOCK_ID,
    CB.CUTB_BLOCK_NUMBER AS BLOCK_NBR,
    CB.CUTB_SYSTEM_ID AS UBI,
    CB.CUTB_BLOCK_STATE AS BLOCK_STATE,
    CB.CUTB_OPENING,
    CB.CUTB_SITE_PLAN_EXEMPT_IND AS SP_EXEMPT,
    BLAL.BLAL_CRUISE_M3_VOL AS CRUISE_VOLUME,
    NAR.NAR_AREA,
    BIO.LARGEST_ECO_AREA,
    BIO.COUNT_BEC_WITH_LARGEST_ECO_AREA,
    BIO.BEC,
    BIO.BIO_ZONE,
    BIO.BIO_SUBZONE,
    BIO.BIO_VARIANT,
    HV.HVS_STATUS,
    HV.HVS_DATE,
    YEAR(ADD_MONTHS(HV.HVS_DATE, 9)) AS HVS_FISCAL,
    HV.HVC_STATUS,
    HV.HVC_DATE,
    YEAR(ADD_MONTHS(HV.HVC_DATE, 9)) AS HVC_FISCAL,
    S.SILA_TREATMENT_UNIT_ID AS TREATMENT_UNIT,
    CB.FUND_FUNDING_CODE AS BLOCK_FUNDING,
    S.ACTIVITY_FUNDING,
    S.SIAB_BASE_CODE AS BASE,
    S.SIAT_TECHNIQUE_CODE AS TECHNIQUE,
    S.SIAM_METHOD_CODE AS METHOD,
    S.SICA_ACTIVITY_NAME AS ACTIVITY,

    CASE
        WHEN S.SILA_STATUS = 'P' THEN 'PLANNED'
        WHEN S.SILA_STATUS = 'D' THEN 'DONE'
        ELSE S.SILA_STATUS
    END AS STATUS,

    S.SILA_START_DATE AS START_DATE,
    YEAR(ADD_MONTHS(S.SILA_START_DATE, 9)) AS START_FISCAL,
    S.SILA_COMPLETION_DATE AS COMPLETE_DATE,
    YEAR(ADD_MONTHS(S.SILA_COMPLETION_DATE, 9)) AS COMPLETE_FISCAL,

    CASE
        WHEN S.SILA_RANK = 1
        THEN S.SILA_GROSS_HA_AREA
    END AS TREATMENT_AREA,

    CASE
        WHEN S.SILA_RANK = 1
        THEN CAST(ROUND(S.SILA_GROSS_HA_AREA, 1) AS STRING)
    END AS SILA_GROSS_AREA_CHAR,

    CASE
        WHEN S.SILA_RANK = 1 THEN
            CASE
                WHEN S.ITEMS_TOTAL_PLANNED > 0
                     AND S.SILA_GROSS_HA_AREA > 0
                THEN ROUND(
                    S.ITEMS_TOTAL_PLANNED / S.SILA_GROSS_HA_AREA,
                    2
                )
                ELSE S.SILA_COST_PER_HA_PLAN
            END
    END AS COST_PER_HA_PLAN,

    CASE
        WHEN S.SILA_RANK = 1 THEN
            CASE
                WHEN S.ITEMS_TOTAL_ACTUAL > 0
                     AND S.SILA_GROSS_HA_AREA > 0
                THEN ROUND(
                    S.ITEMS_TOTAL_ACTUAL / S.SILA_GROSS_HA_AREA,
                    2
                )
                ELSE S.SILA_COST_PER_HA
            END
    END AS COST_PER_HA,

    CASE
        WHEN S.SILA_RANK = 1 THEN
            ROUND(
                CASE
                    WHEN COALESCE(S.ITEMS_TOTAL_PLANNED, 0) = 0
                    THEN
                        S.SILA_COST_PER_HA_PLAN
                        * S.SILA_GROSS_HA_AREA
                    ELSE S.ITEMS_TOTAL_PLANNED
                END,
                2
            )
    END AS PLANNED_TOTAL_COST,

    CASE
        WHEN S.SILA_RANK = 1
        THEN ROUND(
            S.SILA_GROSS_HA_AREA
            * S.SILA_COST_PER_HA_PLAN,
            2
        )
    END AS COST_PER_HA_PLAN_X_HA,

    CASE
        WHEN S.SILA_RANK = 1 THEN
            ROUND(
                CASE
                    WHEN COALESCE(S.ITEMS_TOTAL_ACTUAL, 0) = 0
                    THEN
                        S.SILA_COST_PER_HA
                        * S.SILA_GROSS_HA_AREA
                    ELSE S.ITEMS_TOTAL_ACTUAL
                END,
                2
            )
    END AS ACTUAL_TOTAL_COST,

    CASE
        WHEN S.SILA_RANK = 1
        THEN ROUND(
            S.SILA_GROSS_HA_AREA
            * S.SILA_COST_PER_HA,
            2
        )
    END AS COST_PER_HA_X_HA,

    S.PLANNED_SERVICE_LINE,
    S.PLANNED_SL_DESCRIPTION,

    CASE
        WHEN S.SILA_RANK = 1
        THEN S.ITEMS_TOTAL_PLANNED
    END AS ITEMS_PLANNED_TOTAL_COST,

    S.ACTUAL_SERVICE_LINE,
    S.ACTUAL_SL_DESCRIPTION,

    CASE
        WHEN S.SILA_RANK = 1
        THEN S.ITEMS_TOTAL_ACTUAL
    END AS ITEMS_ACTUAL_TOTAL_COST,

    S.PLUN_PLANTING_UNIT_ID AS PLANTING_UNIT,
    S.PLUN_DIGITISED_IND,
    S.PLUN_PLANTING_PLAN_COST AS PLUN_PLANNED_TOTAL_COST,

    CASE
        WHEN S.SIAB_BASE_CODE = 'PL'
        THEN S.ITEM_TOTAL_PLANNED
    END AS PLUN_ITEM_PLANNED_TOTAL_COST,

    S.PLUN_PLANTING_COST AS PLUN_ACTUAL_TOTAL_COST,

    CASE
        WHEN S.SIAB_BASE_CODE = 'PL'
        THEN S.ITEM_TOTAL_ACTUAL
    END AS PLUN_ITEM_ACTUAL_TOTAL_COST,

    S.PLUN_HA_AREA,
    S.PLUN_NET_AREA,
    S.PLUN_DENSITY,
    S.TOTAL_TREES,
    S.SEEDLING_REQUEST,
    S.SICA_KEY_IND,
    S.PLUN_MODIFIEDBY,
    S.PLUN_MODIFIEDON,
    S.PLUN_MODIFIEDUSING,
    S.MODIFIEDBY,
    S.MODIFIEDON,

    CASE
        WHEN S.SILA_RANK = 1 THEN 'Y'
        ELSE 'N'
    END AS PRIMARY_RECORD,

    CASE
        WHEN ART.CUTB_SEQ_NBR IS NULL THEN 'N'
        ELSE 'Y'
    END AS NEED_PLANTING,

    CASE
        WHEN SALVAGE_ANY_FIRE_YEAR.CUTB_SEQ_NBR IS NULL THEN 'N'
        ELSE 'Y'
    END AS SALVAGE_ANY_FIRE_YEAR,

    SALVAGE_ANY_FIRE_YEAR.SALVAGE_FIRE_YEARS,

    CASE
        WHEN SALVAGE21.ACTT_KEY_IND IS NULL THEN NULL
        ELSE CONCAT(
            SALVAGE21.ACTIVITY_TYPE,
            ' (',
            SALVAGE21.ACTIVITY_CLASS,
            ' - ',
            SALVAGE21.ACTT_KEY_IND,
            ')'
        )
    END AS SALVAGE_2021_FIRE,

    CASE
        WHEN SALVAGE22.ACTT_KEY_IND IS NULL THEN NULL
        ELSE CONCAT(
            SALVAGE22.ACTIVITY_TYPE,
            ' (',
            SALVAGE22.ACTIVITY_CLASS,
            ' - ',
            SALVAGE22.ACTT_KEY_IND,
            ')'
        )
    END AS SALVAGE_2022_FIRE,

    CASE
        WHEN SALVAGE23.ACTT_KEY_IND IS NULL THEN NULL
        ELSE CONCAT(
            SALVAGE23.ACTIVITY_TYPE,
            ' (',
            SALVAGE23.ACTIVITY_CLASS,
            ' - ',
            SALVAGE23.ACTT_KEY_IND,
            ')'
        )
    END AS SALVAGE_2023_FIRE,

    CASE
        WHEN SALVAGE24.ACTT_KEY_IND IS NULL THEN NULL
        ELSE CONCAT(
            SALVAGE24.ACTIVITY_TYPE,
            ' (',
            SALVAGE24.ACTIVITY_CLASS,
            ' - ',
            SALVAGE24.ACTT_KEY_IND,
            ')'
        )
    END AS SALVAGE_2024_FIRE,

    CASE
        WHEN SALVAGE25.ACTT_KEY_IND IS NULL THEN NULL
        ELSE CONCAT(
            SALVAGE25.ACTIVITY_TYPE,
            ' (',
            SALVAGE25.ACTIVITY_CLASS,
            ' - ',
            SALVAGE25.ACTT_KEY_IND,
            ')'
        )
    END AS SALVAGE_2025_FIRE,

    FG.FG_DONE,
    S.PLUN_SEQ_NBR,
    S.SILA_SEQ_NBR,
    S.SICA_SEQ_NBR,
    BLAL.CUTB_SEQ_NBR,
    LICN.LICN_SEQ_NBR

FROM
    LRM_REPLICATION.DIVISION FD

    INNER JOIN LRM_REPLICATION.BLOCK_ALLOCATION BLAL
        ON FD.DIVI_DIV_NBR = BLAL.DIVI_DIV_NBR

    INNER JOIN LRM_REPLICATION.MANAGEMENT_UNIT MU
        ON BLAL.MANU_SEQ_NBR = MU.MANU_SEQ_NBR

    INNER JOIN LS
        ON BLAL.LICN_SEQ_NBR = LS.LICN_SEQ_NBR

    LEFT JOIN LRM_REPLICATION.LICENCE LICN
        ON BLAL.LICN_SEQ_NBR = LICN.LICN_SEQ_NBR

    LEFT JOIN LRM_REPLICATION.TENURE_TYPE TENT
        ON LICN.TENT_SEQ_NBR = TENT.TENT_SEQ_NBR

    LEFT JOIN LRM_REPLICATION.CODE_LOOKUP LKF
        ON LICN.LICN_FIELD_TEAM_ID = LKF.COLU_LOOKUP_ID
       AND LKF.COLU_LOOKUP_TYPE = 'FDTM'

    INNER JOIN LRM_REPLICATION.CUT_BLOCK CB
        ON BLAL.CUTB_SEQ_NBR = CB.CUTB_SEQ_NBR

    LEFT JOIN HV
        ON BLAL.CUTB_SEQ_NBR = HV.CUTB_SEQ_NBR

    LEFT JOIN NAR
        ON BLAL.CUTB_SEQ_NBR = NAR.CUTB_SEQ_NBR

    LEFT JOIN ART
        ON BLAL.CUTB_SEQ_NBR = ART.CUTB_SEQ_NBR

    LEFT JOIN BIO
        ON BLAL.CUTB_SEQ_NBR = BIO.CUTB_SEQ_NBR

    INNER JOIN S
        ON BLAL.CUTB_SEQ_NBR = S.CUTB_SEQ_NBR

    LEFT JOIN SALVAGE_ANY_FIRE_YEAR
        ON BLAL.CUTB_SEQ_NBR =
           SALVAGE_ANY_FIRE_YEAR.CUTB_SEQ_NBR

    LEFT JOIN SALVAGE21
        ON BLAL.CUTB_SEQ_NBR = SALVAGE21.CUTB_SEQ_NBR

    LEFT JOIN SALVAGE22
        ON BLAL.CUTB_SEQ_NBR = SALVAGE22.CUTB_SEQ_NBR

    LEFT JOIN SALVAGE23
        ON BLAL.CUTB_SEQ_NBR = SALVAGE23.CUTB_SEQ_NBR

    LEFT JOIN SALVAGE24
        ON BLAL.CUTB_SEQ_NBR = SALVAGE24.CUTB_SEQ_NBR

    LEFT JOIN SALVAGE25
        ON BLAL.CUTB_SEQ_NBR = SALVAGE25.CUTB_SEQ_NBR

    LEFT JOIN FG
        ON BLAL.CUTB_SEQ_NBR = FG.CUTB_SEQ_NBR
;


CREATE OR REPLACE VIEW BCTS_STAGING.SILVILIABILITY_MAIN AS

SELECT
    SL.BUSINESS_AREA_REGION_CATEGORY,
    SL.BUSINESS_AREA_REGION,
    SL.BUSINESS_AREA,
    SL.BUSINESS_AREA_CODE,
    SL.FIELD_TEAM,
    SL.MANU_ID,
    SL.TENURE,
    SL.LICENCE,
    SL.HBS_FIRST_BILLING,
    SL.HBS_VOLUME_BILLED,
    SL.BLOCK_ID,
    SL.BLOCK_NBR,
    SL.UBI,
    SL.BLOCK_STATE,
    SL.FG_DONE,
    RST.RESULTS_FG_DECLARED,
    SL.CUTB_OPENING,
    SL.SP_EXEMPT,
    SL.CRUISE_VOLUME,
    SL.NAR_AREA,
    SL.LARGEST_ECO_AREA,
    SL.COUNT_BEC_WITH_LARGEST_ECO_AREA,
    SL.BEC,
    SL.BIO_ZONE,
    SL.BIO_SUBZONE,
    SL.BIO_VARIANT,
    SL.HVS_STATUS,
    SL.HVS_DATE,
    SL.HVS_FISCAL,
    SL.HVC_STATUS,
    SL.HVC_DATE,
    SL.HVC_FISCAL,
    SL.TREATMENT_UNIT,
    SL.BLOCK_FUNDING,
    SL.ACTIVITY_FUNDING,
    SL.BASE,
    SL.TECHNIQUE,
    SL.METHOD,
    SL.ACTIVITY,
    SL.STATUS,
    SL.START_DATE,
    SL.START_FISCAL,
    SL.COMPLETE_DATE,
    SL.COMPLETE_FISCAL,
    SL.TREATMENT_AREA,
    SL.SILA_GROSS_AREA_CHAR,
    SL.COST_PER_HA_PLAN,
    SL.COST_PER_HA,
    SL.PLANNED_TOTAL_COST,
    SL.COST_PER_HA_PLAN_X_HA,
    SL.ACTUAL_TOTAL_COST,
    SL.COST_PER_HA_X_HA,
    SL.PLANNED_SERVICE_LINE,
    SL.PLANNED_SL_DESCRIPTION,
    SL.ITEMS_PLANNED_TOTAL_COST,
    SL.ACTUAL_SERVICE_LINE,
    SL.ACTUAL_SL_DESCRIPTION,
    SL.ITEMS_ACTUAL_TOTAL_COST,
    SL.PLANTING_UNIT,
    SL.PLUN_DIGITISED_IND,
    SL.PLUN_PLANNED_TOTAL_COST,
    SL.PLUN_ITEM_PLANNED_TOTAL_COST,
    SL.PLUN_ACTUAL_TOTAL_COST,
    SL.PLUN_ITEM_ACTUAL_TOTAL_COST,
    SL.PLUN_HA_AREA,
    SL.PLUN_NET_AREA,
    SL.PLUN_DENSITY,
    SL.TOTAL_TREES,
    SL.SEEDLING_REQUEST,
    SL.SICA_KEY_IND,
    SL.PLUN_MODIFIEDBY,
    SL.PLUN_MODIFIEDON,
    SL.PLUN_MODIFIEDUSING,
    SL.MODIFIEDBY,
    SL.MODIFIEDON,
    SL.PRIMARY_RECORD,
    SL.NEED_PLANTING,
    SL.SALVAGE_ANY_FIRE_YEAR,
    SL.SALVAGE_FIRE_YEARS,
    SL.SALVAGE_2021_FIRE,
    SL.SALVAGE_2022_FIRE,
    SL.SALVAGE_2023_FIRE,
    SL.SALVAGE_2024_FIRE,
    SL.SALVAGE_2025_FIRE,

    cfs.total_amount AS cfs_total_amount,
    cfs.first_layout_amount AS cfs_first_layout_amount,
    cfs.last_layout_amount AS cfs_last_layout_amount,
    cfs.layout_amount_no_7017_7018 AS cfs_layout_amount_no_7017_7018,
    cfs.layout_amount_7017 AS cfs_layout_amount_7017,
    cfs.layout_amount_7018 AS cfs_layout_amount_7018,
    cfs.layout_amount AS cfs_layout_amount,
    cfs.planning_amount AS cfs_planning_amount,
    cfs.silviculture_amount AS cfs_silviculture_amount,

    cfs.service_line_42420 AS cfs_service_line_42420,
    cfs.service_line_42421 AS cfs_service_line_42421,
    cfs.service_line_42430 AS cfs_service_line_42430,
    cfs.service_line_42467 AS cfs_service_line_42467,
    cfs.service_line_42468 AS cfs_service_line_42468,
    cfs.service_line_42469 AS cfs_service_line_42469,
    cfs.service_line_42470 AS cfs_service_line_42470,
    cfs.service_line_42471 AS cfs_service_line_42471,
    cfs.service_line_42472 AS cfs_service_line_42472,
    cfs.service_line_42501 AS cfs_service_line_42501,
    cfs.service_line_42503 AS cfs_service_line_42503,
    cfs.service_line_42504 AS cfs_service_line_42504,
    cfs.service_line_42506 AS cfs_service_line_42506,
    cfs.service_line_42507 AS cfs_service_line_42507,
    cfs.service_line_42520 AS cfs_service_line_42520,
    cfs.service_line_42521 AS cfs_service_line_42521,
    cfs.service_line_42524 AS cfs_service_line_42524,
    cfs.service_line_42550 AS cfs_service_line_42550,
    cfs.service_line_42552 AS cfs_service_line_42552,
    cfs.service_line_42553 AS cfs_service_line_42553,
    cfs.service_line_42554 AS cfs_service_line_42554,
    cfs.service_line_42556 AS cfs_service_line_42556,
    cfs.service_line_42558 AS cfs_service_line_42558,
    cfs.service_line_42559 AS cfs_service_line_42559,
    cfs.service_line_42579 AS cfs_service_line_42579,

    SL.PLUN_SEQ_NBR,
    SL.SILA_SEQ_NBR,
    SL.SICA_SEQ_NBR,
    SL.CUTB_SEQ_NBR,
    SL.LICN_SEQ_NBR,

    CURRENT_DATE() AS report_run_date,
    DATE('${report_end_date}') AS report_end_date


FROM
    BCTS_STAGING.SILVILIABILITY_LRM SL

LEFT JOIN RESULTS_REPLICATION.SILVILIABILITY_RESULTS RST
    ON SL.LICENCE = RST.FOREST_FILE_ID
    AND SL.BLOCK_ID = RST.CUT_BLOCK_ID

LEFT JOIN BCTS_STAGING.FINANCIAL_TRANSACTIONS_SUMMARY cfs
    ON cfs.cfs_ubi = SL.UBI;


-- Report exists check is done on bcts_reporting table 
-- If report exists in bcts_staging.***_hist table, clear the staging table before inserting new records 
DELETE FROM bcts_staging.silviliability_main_hist
WHERE report_end_date = DATE('${report_end_date}');


-- Populate staging table
INSERT INTO bcts_staging.silviliability_main_hist
SELECT
    SL.* EXCEPT (report_end_date),
    FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Vancouver') AS report_run_timestamp,
    SL.report_end_date
FROM bcts_staging.silviliability_main SL;

-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
 -- ============================================================
-- 1. Create the main reporting table
-- ============================================================

DROP TABLE IF EXISTS bcts_reporting.silviliability_main;

CREATE TABLE bcts_reporting.silviliability_main
USING DELTA
AS
SELECT
    *
FROM bcts_staging.silviliability_main;


-- ============================================================
-- 2. Create the current-fiscal-year reporting table
--    The VW_ prefix is retained, but this is a Delta table.
-- ============================================================

DROP TABLE IF EXISTS bcts_reporting.VW_SILVILIABILITY_MAIN;

CREATE TABLE bcts_reporting.VW_SILVILIABILITY_MAIN
USING DELTA
AS

WITH TEMP AS
(
    SELECT
        *,
        CASE
            WHEN MONTH(CURRENT_DATE()) >= 4
            THEN ADD_MONTHS(TRUNC(CURRENT_DATE(), 'YEAR'), 3)
            ELSE ADD_MONTHS(TRUNC(CURRENT_DATE(), 'YEAR'), -9)
        END AS fiscal_year_start_date

    FROM bcts_reporting.silviliability_main
)

SELECT
    *
FROM TEMP
WHERE fg_done >= fiscal_year_start_date;


-- ============================================================
-- 3. Create the Power BI summary table
--    The VW_ prefix is retained, but this is a Delta table.
-- ============================================================

DROP TABLE IF EXISTS bcts_reporting.VW_SILVILIABILITY_MAIN_PBI;

CREATE TABLE bcts_reporting.VW_SILVILIABILITY_MAIN_PBI
USING DELTA
AS

WITH ubis AS
(
    SELECT
        business_area_region_category,
        business_area_region,
        business_area,
        ubi,

        MAX(
            DATEDIFF(fg_done, hvc_date)
        ) AS days_to_free_growing,

        MAX(
            DATEDIFF(fg_done, hvc_date) / 365.25
        ) AS years_to_free_growing,

        MAX(
            DATEDIFF(hvc_date, hvs_date)
        ) AS duration_of_harvesting,

        SUM(planned_total_cost) AS planned_total_cost,
        SUM(cfs_silviculture_amount) AS actual_total_cost,
        MAX(nar_area) AS nar_area,
        SUM(total_trees) AS total_trees

    FROM bcts_reporting.VW_SILVILIABILITY_MAIN

    GROUP BY
        business_area_region_category,
        business_area_region,
        business_area,
        ubi
)

SELECT
    business_area_region_category,
    business_area_region,
    business_area,

    COUNT(ubi) AS `Number of Blocks`,

    ROUND(
        SUM(nar_area),
        0
    ) AS `Total Net Area to be Reforested`,

    SUM(total_trees) AS `Total Trees Planted`,

    ROUND(
        SUM(total_trees) / SUM(nar_area),
        0
    ) AS `Total trees planted per ha of NAR`,

    ROUND(
        AVG(years_to_free_growing),
        1
    ) AS `Average years to free growing`,

    ROUND(
        AVG(days_to_free_growing / nar_area),
        0
    ) AS `Average days to free growing per ha of NAR`,

    ROUND(
        AVG(duration_of_harvesting),
        0
    ) AS `Average duration of harvesting in days`,

    ROUND(
        SUM(planned_total_cost),
        0
    ) AS `Average planned total cost`,

    ROUND(
        SUM(actual_total_cost),
        0
    ) AS `Average actual total cost`,

    ROUND(
        SUM(planned_total_cost) / SUM(nar_area),
        0
    ) AS `Average planned total cost per ha of NAR`,

    ROUND(
        SUM(actual_total_cost) / SUM(nar_area),
        0
    ) AS `Average actual total cost per ha of NAR`

FROM ubis

GROUP BY
    business_area_region_category,
    business_area_region,
    business_area;   
    