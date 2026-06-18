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

# Test transformation
spark.sql(\
"""
INSERT INTO bcts_metadata.transformation_config VALUES
('Annual Developed Volume', 'transformation_sql/annual_developed_volume.sql', 'Y', 'annual_developed_volume', 'bcts_staging', 1, NULL);
"""
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  report_name        STRING,  
#   sql_path           STRING,     -- e.g. sql/bidder_details.sql (relative to Files/)  
#   enabled_ind        STRING,     -- 'Y'/'N'  
#   target_table       STRING,  
#   target_schema       STRING,  
#   execution_order    INT,      -- optional ordering hint  
#   depends_on         STRING   

# CELL ********************

spark.sql(\
"""
delete from bcts_metadata.transformation_config
"""
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
