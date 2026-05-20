CREATE TABLE [bcts_metada].[cdc_master_table_list] (

	[business] varchar(100) NULL, 
	[application_name] varchar(100) NULL, 
	[custodian] varchar(100) NULL, 
	[source_schema_name] varchar(100) NULL, 
	[source_table_name] varchar(100) NULL, 
	[target_schema_name] varchar(100) NULL, 
	[target_table_name] varchar(100) NULL, 
	[truncate_flag] varchar(1) NULL, 
	[cdc_flag] varchar(1) NULL, 
	[full_inc_flag] varchar(1) NULL, 
	[cdc_column] varchar(50) NULL, 
	[active_ind] varchar(1) NULL, 
	[replication_order] int NULL, 
	[where_clause] varchar(1000) NULL, 
	[customsql_ind] varchar(1) NULL, 
	[customsql_query] varchar(max) NULL, 
	[replication_source] varchar(100) NULL
);