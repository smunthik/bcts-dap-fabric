-- Create _hist table if it does not exist
CREATE TABLE IF NOT EXISTS bcts_staging.licence_transfer_hist (
    business_area_region STRING,
    business_area STRING,
    management_unit_type STRING,
    management_unit_id STRING,
    management_unit STRING,
    forest_file_id STRING,
    bcts_category_code STRING,
    category STRING,
    auction_date TIMESTAMP,
    sale_volume DECIMAL(13,1),
    fta_file_status STRING,
    legal_effective_date DATE,
    client_number STRING,
    client_name STRING,
    client_type STRING,
    registry_company_type_code STRING,
    licensee_start_date DATE,
    licensee_end_date DATE,
    revision_count DECIMAL(5,0),
    licensee_order BIGINT,
    report_start_date DATE,
    report_end_date DATE,
    report_run_date DATE,
    report_run_timestamp TIMESTAMP
)
USING DELTA;


-- Report exists check is done on bcts_reporting table 
-- If report exists in bcts_staging.***_hist table, clear the staging table before inserting new records 
delete from bcts_staging.licence_transfer_hist
where  report_start_date = '${report_start_date}'
and report_end_date = '${report_end_date}';

-- Populate staging table
INSERT INTO bcts_staging.licence_transfer_hist (
    business_area_region,
    business_area,
    management_unit_type,
    management_unit_id,
    management_unit,
    forest_file_id,
    bcts_category_code,
    category,
    auction_date,
    sale_volume,
    fta_file_status,
    legal_effective_date,
    client_number,
    client_name,
    client_type,
    registry_company_type_code,
    licensee_start_date,
    licensee_end_date,
    revision_count,
    licensee_order,
    report_start_date,
    report_end_date,
    report_run_date,
    report_run_timestamp
)

WITH CL AS (
    SELECT
        ff.FOREST_FILE_ID,
        ff.CLIENT_NUMBER,
        ff.LICENSEE_START_DATE,
        ff.LICENSEE_END_DATE,
        ff.REVISION_COUNT,
        fc.legal_first_name,
        fc.legal_middle_name,
        fc.CLIENT_NAME,
        ff.FOREST_FILE_CLIENT_TYPE_CODE,
        'REDACTED' AS CLIENT_ACRONYM,
        'REDACTED' AS REGISTRY_COMPANY_TYPE_CODE
    FROM BCTS_STAGING.FTA_FOREST_FILE_CLIENT ff
    INNER JOIN MOFCLIENT_REPLICATION.V_CLIENT_PUBLIC fc
        ON ff.CLIENT_NUMBER = fc.CLIENT_NUMBER
    WHERE fc.CLIENT_NAME NOT LIKE 'TIMBER SALES MANAGER%'
),

CC AS (
    SELECT
        ff1.FOREST_FILE_ID,
        COUNT(DISTINCT ff1.CLIENT_NUMBER) AS CLIENT_COUNT
    FROM BCTS_STAGING.FTA_FOREST_FILE_CLIENT ff1
    INNER JOIN MOFCLIENT_REPLICATION.V_CLIENT_PUBLIC fc1
        ON ff1.CLIENT_NUMBER = fc1.CLIENT_NUMBER
    INNER JOIN BCTS_STAGING.FTA_TENURE_TERM tt1
        ON ff1.FOREST_FILE_ID = tt1.FOREST_FILE_ID
    WHERE fc1.CLIENT_NAME NOT LIKE 'TIMBER SALES MANAGER%'
        AND ff1.FOREST_FILE_ID IN (
            SELECT
                ff0.FOREST_FILE_ID
            FROM BCTS_STAGING.FTA_FOREST_FILE_CLIENT ff0
            WHERE ff0.LICENSEE_START_DATE BETWEEN
                TO_DATE('{start_date}', 'yyyy-MM-dd')
                AND TO_DATE('{end_date}', 'yyyy-MM-dd')
        )
    GROUP BY
        ff1.FOREST_FILE_ID
    HAVING
        COUNT(*) > 1
        AND COUNT(DISTINCT ff1.FOREST_FILE_CLIENT_TYPE_CODE) > 1
        AND MAX(ff1.LICENSEE_START_DATE) >= MAX(tt1.LEGAL_EFFECTIVE_DT)
)

SELECT DISTINCT
    CASE
        WHEN ou.ORG_UNIT_CODE IN ('TBA', 'TPL', 'TPG', 'TSK', 'TSN')
            THEN 'North Interior'
        WHEN ou.ORG_UNIT_CODE IN ('TCC', 'TKA', 'TKO', 'TOC')
            THEN 'South Interior'
        WHEN ou.ORG_UNIT_CODE IN ('TCH', 'TST', 'TSG')
            THEN 'Coast'
    END AS business_area_region,

    REPLACE(
        CONCAT(
            CASE
                WHEN ou.ORG_UNIT_NAME = 'Seaward Timber Sales Office'
                    THEN 'Seaward/Tlasta'
                ELSE ou.ORG_UNIT_NAME
            END,
            ' (',
            ou.ORG_UNIT_CODE,
            ')'
        ),
        ' Timber Sales Office',
        ''
    ) AS business_area,

    CASE
        WHEN mutc.DESCRIPTION IS NULL THEN pfu.MGMT_UNIT_TYPE
        ELSE CONCAT(
            mutc.DESCRIPTION,
            ' (',
            pfu.MGMT_UNIT_TYPE,
            ')'
        )
    END AS management_unit_type,

    pfu.MGMT_UNIT_ID AS management_unit_id,

    CASE
        WHEN pfu.MGMT_UNIT_TYPE = 'U' THEN ta.DESCRIPTION
        ELSE tf.DESCRIPTION
    END AS management_unit,

    ts.FOREST_FILE_ID,
    ts.BCTS_CATEGORY_CODE,
    c.DESCRIPTION AS category,
    ts.AUCTION_DATE,
    ts.SALE_VOLUME,

    CONCAT(
        tfsc.DESCRIPTION,
        ' (',
        pfu.FILE_STATUS_ST,
        ')'
    ) AS fta_file_status,

    tt.LEGAL_EFFECTIVE_DT AS legal_effective_date,
    cl.CLIENT_NUMBER,

    CONCAT(
        COALESCE(CONCAT(cl.LEGAL_FIRST_NAME, ' '), ''),
        COALESCE(CONCAT(cl.LEGAL_MIDDLE_NAME, ' '), ''),
        cl.CLIENT_NAME
    ) AS client_name,

    CASE
        WHEN fctc.DESCRIPTION IS NULL THEN cl.FOREST_FILE_CLIENT_TYPE_CODE
        ELSE CONCAT(
            fctc.DESCRIPTION,
            ' (',
            cl.FOREST_FILE_CLIENT_TYPE_CODE,
            ')'
        )
    END AS client_type,

    cl.REGISTRY_COMPANY_TYPE_CODE,
    cl.LICENSEE_START_DATE,
    cl.LICENSEE_END_DATE,
    cl.REVISION_COUNT,

    RANK() OVER (
        PARTITION BY ts.FOREST_FILE_ID
        ORDER BY cl.LICENSEE_START_DATE
    ) AS licensee_order,

    CAST('{start_date}' AS DATE) AS report_start_date,
    CAST('{end_date}' AS DATE) AS report_end_date,

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

FROM bctsadmin_replication.bcts_timber_sale ts

INNER JOIN BCTS_STAGING.FTA_PROV_FOREST_USE pfu
    ON ts.FOREST_FILE_ID = pfu.FOREST_FILE_ID

INNER JOIN BCTS_STAGING.FTA_ORG_UNIT ou
    ON pfu.BCTS_ORG_UNIT = ou.ORG_UNIT_NO

INNER JOIN CL
    ON ts.FOREST_FILE_ID = cl.FOREST_FILE_ID

INNER JOIN CC
    ON ts.FOREST_FILE_ID = cc.FOREST_FILE_ID

LEFT JOIN BCTS_STAGING.FTA_TENURE_FILE_STATUS_CODE tfsc
    ON pfu.FILE_STATUS_ST = tfsc.TENURE_FILE_STATUS_CODE

LEFT JOIN MOFCLIENT_REPLICATION.FILE_CLIENT_TYPE_CODE fctc
    ON cl.FOREST_FILE_CLIENT_TYPE_CODE = fctc.FILE_CLIENT_TYPE_CODE

LEFT JOIN BCTS_STAGING.FTA_MGMT_UNIT_TYPE_CODE mutc
    ON pfu.MGMT_UNIT_TYPE = mutc.MGMT_UNIT_TYPE_CODE

LEFT JOIN BCTS_STAGING.FTA_TSA_NUMBER_CODE ta
    ON pfu.MGMT_UNIT_ID = ta.TSA_NUMBER

LEFT JOIN BCTS_STAGING.FTA_TFL_NUMBER_CODE tf
    ON pfu.MGMT_UNIT_ID = tf.TFL_NUMBER

LEFT JOIN BCTS_STAGING.FTA_TENURE_TERM tt
    ON pfu.FOREST_FILE_ID = tt.FOREST_FILE_ID

LEFT JOIN bctsadmin_replication.BCTS_CATEGORY_CODE c
    ON ts.BCTS_CATEGORY_CODE = c.BCTS_CATEGORY_CODE

WHERE pfu.FILE_STATUS_ST IN ('HI', 'HC', 'LC', 'HX', 'HS', 'HRS')
    AND ts.NO_SALE_RATIONALE_CODE IS NULL;

-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
    DROP TABLE IF EXISTS BCTS_STAGING.licence_transfer;
    CREATE TABLE BCTS_STAGING.licence_transfer
    AS SELECT * 
    FROM BCTS_STAGING.licence_transfer_hist
    WHERE report_end_date = (
        SELECT MAX(report_end_date)
        FROM BCTS_STAGING.licence_transfer_hist
	);

    DROP TABLE IF EXISTS BCTS_REPORTING.licence_transfer;
    CREATE TABLE BCTS_REPORTING.licence_transfer
    AS SELECT * 
    FROM BCTS_STAGING.licence_transfer;

    DROP TABLE IF EXISTS BCTS_REPORTING.licence_transfer_hist;
    CREATE TABLE BCTS_REPORTING.licence_transfer_hist
    AS SELECT * 
    FROM BCTS_STAGING.licence_transfer_hist;    
    