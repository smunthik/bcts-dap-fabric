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

from datetime import datetime, date
import calendar
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


from datetime import date, timedelta
import calendar

start_time = datetime.utcnow()

def get_report_interval(frequency):
    today = date.today()

    # ---------------------------------------------------------
    # BCTS Performance Report - Bi-monthly
    # Start: Fiscal year start (April 1)
    # End:
    #   - Day >= 16 -> 15th of current month
    #   - Day < 16  -> Last day of previous month
    # ---------------------------------------------------------
    if frequency == "BI_MONTHLY":

        if today.day >= 16:
            report_end_date = today.replace(day=15)
        else:
            report_end_date = today.replace(day=1) - timedelta(days=1)

        # Determine fiscal year based on the report end date
        fy_start_year = (
            report_end_date.year
            if report_end_date.month >= 4
            else report_end_date.year - 1
        )

        report_start_date = date(fy_start_year, 4, 1)

    # ---------------------------------------------------------
    # TSL Weekly Summary Report - Weekly
    # Start: Monday of previous week
    # End: Sunday of previous week
    # ---------------------------------------------------------
    elif frequency == "WEEKLY":

        # Monday of the current week
        current_monday = today - timedelta(days=today.weekday())

        # Previous Monday through Sunday
        report_start_date = current_monday - timedelta(days=7)
        report_end_date = current_monday - timedelta(days=1)

    # ---------------------------------------------------------
    # All other reports - Monthly
    # Start: Fiscal year start (April 1)
    # End: Last day of previous month
    # ---------------------------------------------------------
    elif frequency == "MONTHLY":

        # Last day of previous month
        report_end_date = today.replace(day=1) - timedelta(days=1)

        # Fiscal year start based on report end date
        fy_start_year = (
            report_end_date.year
            if report_end_date.month >= 4
            else report_end_date.year - 1
        )

        report_start_date = date(fy_start_year, 4, 1)

    else:
        raise ValueError(f"Unsupported report frequency: {frequency}")

    return report_start_date.isoformat(), report_end_date.isoformat()

    # Return consistent YYYY-MM-DD strings
    return report_start_date.isoformat(), report_end_date.isoformat()

pipeline_name = "run_transformations"

# --- Updated run_log schema ---
run_log_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), True),
    StructField("source_schema", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("report_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("error_message", StringType(), True),
])

# --- Read config table ---
cfg = spark.sql("""
SELECT report_name, sql_path, enabled_ind, target_table, target_schema, branch_name, execution_order, depends_on, has_start_date, has_end_date, frequency
FROM bcts_metadata.transformation_config
WHERE enabled_ind = 'Y'
""").collect()

# Enable running one or all transformations. Specify report_name_parameter if only one transformation needs to be run
if report_name_parameter:
    normalized_parameter = "".join(report_name_parameter.split()).casefold()

    cfg = [
        row for row in cfg
        if "".join(row["report_name"].split()).casefold() == normalized_parameter
    ]

# --- Build DAG activities ---
activities = []
all_names = set()

def parse_depends(dep_str):
    if dep_str is None:
        return []
    dep_str = dep_str.strip()
    if not dep_str:
        return []
    return [d.strip() for d in dep_str.split(",") if d.strip()]

for row in cfg:
    name = row["report_name"]
    all_names.add(name)

for row in cfg:
    name = row["report_name"]
    frequency = row["frequency"]
    # Get report start and end dates
    start_str, end_str = get_report_interval(frequency)
    deps = parse_depends(row["depends_on"])
    print(row["sql_path"])

    # Keep only dependencies that exist in config
    deps = [d for d in deps if (not report_param) or d == report_param]

    activities.append({
        "name": name,
        "path": "run_sql_file",   # child notebook
        "timeoutPerCellInSeconds": 900,
        "args": {
            "sql_path": row["sql_path"],
            "start_date": start_str,
            "end_date": end_str,
            "target_table": row["target_table"],
            "target_schema": row["target_schema"],
            "branch_name": row["branch_name"],
            "run_id": run_id,
            "report_name": name,
            "has_start_date": row["has_start_date"],
            "has_end_date": row["has_end_date"]
        },
        "dependencies": deps
    })

dag = {
    "activities": activities,
    "concurrency": 3,
    "timeoutInSeconds": 43200
}

# --- Validate + run ---
notebookutils.notebook.validateDAG(dag)

try:
    results = notebookutils.notebook.runMultiple(dag)
except Exception as ex:
    # Fabric often still returns partial results
    results = getattr(ex, "result", {})

# --- Log each result ---
log_rows = []

for report_name, result in results.items():

    status = "SUCCESS" if result["exception"] is None else "FAILED"
    error_msg = str(result["exception"]) if result["exception"] else None
    end_time = datetime.utcnow()

    log_rows.append(Row(
        run_id=run_id,
        pipeline_name=pipeline_name,
        source_schema=None,      # transformation step → keep NULL
        source_table=None,       # transformation step → keep NULL
        report_name=report_name,
        status=status,
        start_time=start_time,
        end_time=end_time,
        error_message=error_msg
    ))

if log_rows:
    df = spark.createDataFrame(log_rows, schema=run_log_schema)
    df.write.mode("append").insertInto("bcts_metadata.run_log")

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
