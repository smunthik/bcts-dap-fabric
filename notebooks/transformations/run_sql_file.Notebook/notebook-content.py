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

from pathlib import Path

# Workaround for legacy/bad datetime values (e.g., 0100-01-01) present in lrm_replication.activity table.
# These invalid or placeholder dates cause Spark 3.x to fail with READ_ANCIENT_DATETIME errors
# due to calendar differences (Julian vs Proleptic Gregorian) when reading Parquet/Delta data.
# Setting to LEGACY ensures backward-compatible reading and prevents pipeline failures
# during Fabric migration until the source data is cleaned.

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")



def load_sql(relative_path: str) -> str:
    full_path = f"/lakehouse/default/Files/{relative_path}"
    return Path(full_path).read_text(encoding="utf-8")

def render_sql(sql: str, start_date: str, end_date: str) -> str:
    return (sql
        .replace("${report_start_date}", start_date)
        .replace("${report_end_date}", end_date)
    )

def report_exists(target_schema: str, table_name: str, start_date: str, end_date: str) -> bool:
    # Check if table exists
    if not spark.catalog.tableExists(table_name):
        return False  # ✅ Table doesn't exist → force run

    query = f"""
        SELECT 1
        FROM {target_schema}.{table_name}
        WHERE report_start_date = DATE '{start_date}'
          AND report_end_date = DATE '{end_date}'
        LIMIT 1
    """
    
    df = spark.sql(query)
    return len(df.take(1)) > 0

    
    df = spark.sql(query)
    return df.count() > 0

# Check before running SQL
if report_exists(target_schema, target_table, start_date, end_date):
    print(f"Skipping {target_schema.target_table} - already exists for {start_date} → {end_date}")
else:
    print(f"Running {target_schema}.{target_table} report for {start_date} and {end_date}...")
    sql_text = load_sql(sql_path)
    final_sql = render_sql(sql_text, start_date, end_date)

    # statements execute in the same order as in the file
    try:
        for stmt in [s.strip() for s in final_sql.split(";") if s.strip()]:
            spark.sql(stmt)
        print(f" {target_schema}.{target_table} report for {start_date} and {end_date} completed.")

    except Exception as e:
        error_msg = str(e).replace("'", "''")
        spark.sql(f"""
        UPDATE bcts_metadata.run_log
        SET status = 'FAILED',
            end_time = current_timestamp(),
            error_message = '{error_msg}'
        WHERE run_id = '{run_id}'
            AND report_name = '{report_name}'
        """)

        raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
