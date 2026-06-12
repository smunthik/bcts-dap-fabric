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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lit


spark = SparkSession.builder.getOrCreate()

# Load CSV (adjust path if needed)
df = spark.read \
    .option("header", True) \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv("Files/cdc_master_table_list_ods/lrm_all.csv")


df = df.withColumn("application_name", lit("lrm"))

def format_value(val):
    if val is None or val == "":
        return "NULL"
    return f"'{val.strip()}'"

rows_sql = []

for row in df.collect():
    customsql_ind = row["customsql_ind"]
    customsql_query = row["customsql_query"]

    # Handle custom SQL block
    if customsql_ind == 'Y' and customsql_query:
        custom_sql_formatted = f'"""\n{customsql_query.strip()}\n"""'
    else:
        custom_sql_formatted = "NULL"

    values = [
        "NULL",  # business
        format_value(row["application_name"]),
        "NULL",  # custodian
        format_value(row["source_schema_name"]),
        format_value(row["source_table_name"]),
        format_value(row["target_schema_name"]),
        format_value(row["target_table_name"]),
        format_value(row["truncate_flag"]),
        format_value(row["cdc_flag"]),
        format_value(row["full_inc_flag"]),
        format_value(row["cdc_column"]),
        format_value(row["active_ind"]),
        str(int(row["replication_order"])) if row["replication_order"] else "NULL",
        format_value(row["where_clause"]),
        format_value(row["customsql_ind"]),
        custom_sql_formatted,
        format_value(row["replication_source"])
    ]

    row_sql = "(\n    " + ",\n    ".join(values) + "\n)"
    rows_sql.append(row_sql)

# Build full INSERT statement
final_sql = "INSERT INTO bcts_metadata.cdc_master_table_list VALUES\n\n"
final_sql += ",\n\n".join(rows_sql) + "\n"

# Print result
# print(final_sql)
spark.sql(final_sql)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# spark.sql(\
# """
# delete from bcts_metadata.cdc_master_table_list;
# """)

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
