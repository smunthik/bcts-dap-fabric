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

from datetime import date
import calendar
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


# -------------------------
# Date logic 
# -------------------------
today = date.today()

fy_start_year = today.year if today.month >= 4 else today.year - 1
report_start_date = date(fy_start_year, 4, 1)

prev_month_year = today.year if today.month > 1 else today.year - 1
prev_month = today.month - 1 if today.month > 1 else 12
last_day = calendar.monthrange(prev_month_year, prev_month)[1]
report_end_date = date(prev_month_year, prev_month, last_day)

print("report_start_date:", report_start_date)
print("report_end_date:", report_end_date)

sql_folder = Path("/lakehouse/default/Files/transformation_sql")

# -------------------------
# SQL helpers 
# -------------------------


def process_file(file_path: Path):
    try:
        print(f"\n===== Running {file_path.name} =====")

        sql_text = file_path.read_text(encoding="utf-8")
        final_sql = render_sql(sql_text, report_start_date, report_end_date)

        # ✅ Sequential execution inside file
        statements = [s.strip() for s in final_sql.split(";") if s.strip()]
        
        for i, stmt in enumerate(statements, 1):
            print(f"\n\n[{file_path.name}] Statement {i}")
            print(stmt)
            # spark.sql(stmt)

        print(f"✅ Completed {file_path.name}")
        return file_path.name

    except Exception as e:
        print(f"❌ Failed {file_path.name}: {e}")
        raise



def render_sql(sql: str, start_date, end_date) -> str:
    # $ placeholder approach 
    return (sql
        .replace("${report_start_date}", start_date.isoformat())
        .replace("${report_end_date}", end_date.isoformat())
    )


def run_sql_script(sql_text: str):
    # Handles multi-statement SQL safely
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    for stmt in statements:
        print("Executing:\n", stmt)
        # spark.sql(stmt)


# ✅ Parallel execution across files
files = sorted(sql_folder.glob("*.sql"))

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(process_file, f) for f in files]

    for future in as_completed(futures):
        try:
            result = future.result()
            print(f"✔ Done: {result}")
        except Exception as e:
            print(f"🔥 Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql_text = load_sql("/lakehouse/default/Files/transformation_sql")
sql_text



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
