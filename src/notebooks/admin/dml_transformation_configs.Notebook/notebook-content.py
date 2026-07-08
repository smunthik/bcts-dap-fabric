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

# MARKDOWN ********************

# ### Upsert Helper Function
# Utility function that performs an idempotent upsert (update or insert) into the bcts_metadata.transformation_config table.
# It dynamically generates and executes a Delta Lake MERGE statement using report_name as the key—updating an existing record if it already exists, or inserting a new one if it does not—ensuring consistent and duplicate-free configuration management in your metadata-driven pipeline.

# CELL ********************

def upsert_transformation_config(
    report_name,
    sql_path,
    enabled_ind,
    target_table,
    target_schema,
    branch_name,
    execution_order=None,
    depends_on=None,
    verbose=False
):
    def format_value(val):
        if val is None:
            return "NULL"
        elif isinstance(val, str):
            return f"'{val}'"
        else:
            return str(val)

    source_sql = f"""
        SELECT
            {format_value(report_name)} AS report_name,
            {format_value(sql_path)} AS sql_path,
            {format_value(enabled_ind)} AS enabled_ind,
            {format_value(target_table)} AS target_table,
            {format_value(target_schema)} AS target_schema,
            {format_value(branch_name)} AS branch_name,
            {format_value(execution_order)} AS execution_order,
            {format_value(depends_on)} AS depends_on
    """

    merge_sql = f"""
    MERGE INTO bcts_metadata.transformation_config AS target
    USING ({source_sql}) AS source
    ON target.report_name = source.report_name

    WHEN MATCHED THEN
    UPDATE SET
        target.sql_path = source.sql_path,
        target.enabled_ind = source.enabled_ind,
        target.target_table = source.target_table,
        target.target_schema = source.target_schema,
        target.branch_name = source.branch_name,
        target.execution_order = source.execution_order,
        target.depends_on = source.depends_on,
        target.date_updated = current_timestamp()

    WHEN NOT MATCHED THEN
    INSERT (
        report_name,
        sql_path,
        enabled_ind,
        target_table,
        target_schema,
        branch_name,
        execution_order,
        depends_on,
        date_updated
    )
    VALUES (
        source.report_name,
        source.sql_path,
        source.enabled_ind,
        source.target_table,
        source.target_schema,
        source.branch_name,
        source.execution_order,
        source.depends_on,
        current_timestamp()
    )
    """

    if verbose:
        print(merge_sql)

    spark.sql(merge_sql)

    return "SUCCESS!"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Annual Developed Volume

# MARKDOWN ********************

#  report_name        STRING,  
#   sql_path           STRING,     -- e.g. sql/bidder_details.sql (relative to Files/)  
#   enabled_ind        STRING,     -- 'Y'/'N'  
#   target_table       STRING,  
#   target_schema       STRING,  
#   execution_order    INT,      -- optional ordering hint  
#   depends_on         STRING   

# CELL ********************

upsert_transformation_config(
    report_name="Annual Developed Volume",
    sql_path="sql/annual_developed_volume.sql",
    enabled_ind="Y",
    target_table="annual_developed_volume",
    target_schema="bcts_staging",
    branch_name="bcts-initial-migration",
    execution_order=1,
    depends_on=None,
    verbose=False
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Annual Development Ready

# CELL ********************

# 
upsert_transformation_config(
    report_name="Annual Development Ready",
    sql_path="sql/annual_development_ready.sql",
    enabled_ind="Y",
    target_table="annual_development_ready",
    target_schema="bcts_staging",
    branch_name="bcts-initial-migration",
    execution_order=1,
    depends_on=None,
    verbose=False
)

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
