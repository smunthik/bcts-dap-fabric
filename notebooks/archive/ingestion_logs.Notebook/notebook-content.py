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

# Parameters are ingested at rutime from the pipeline


def sql_value(v):
    if v is None or str(v).strip() == "":
        return "NULL"
    # escape single quotes
    return "'" + str(v).replace("'", "''") + "'"

run_id_sql = sql_value(run_id)
pipeline_name_sql = sql_value(pipeline_name)
report_name_sql = sql_value(report_name)
status_sql = sql_value(status)
error_message_sql = sql_value(error_message)
source_schema_sql = sql_value(source_schema)
source_table_sql = sql_value(source_table)


spark.sql(f"""
    INSERT INTO bcts_metadata.run_log (
        run_id,
        pipeline_name,
        report_name,
        status,
        start_time,
        end_time,
        error_message,
        source_schema,
        source_table
    )
    VALUES (
        {run_id_sql},
        {pipeline_name_sql},
        {report_name_sql},
        {status_sql},
        current_timestamp(),
        current_timestamp(),
        {error_message_sql},
        {source_schema_sql},
        {source_table_sql}
    )
""")



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
