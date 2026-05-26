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

def load_sql(relative_path: str) -> str:
    full_path = f"/lakehouse/default/Files/{relative_path}"
    return Path(full_path).read_text(encoding="utf-8")

def render_sql(sql: str, start_date: str, end_date: str) -> str:
    return (sql
        .replace("${report_start_date}", start_date)
        .replace("${report_end_date}", end_date)
    )

sql_text = load_sql(sql_path)
final_sql = render_sql(sql_text, report_start_date, report_end_date)

# statements execute in the same order as in the file
try:
    for stmt in [s.strip() for s in final_sql.split(";") if s.strip()]:
        spark.sql(stmt)

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

# CELL ********************

spark.sql("delete from bcts_reporting.bidder_details where total_bids < 25")

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
