-- Create _hist table if it does not exist



-- Report exists check is done on bcts_reporting table 
-- If report exists in bcts_staging.***_hist table, clear the staging table before inserting new records 
delete from bcts_staging.timber_inventory_ready_to_sell_hist
where  report_start_date = '${report_start_date}'
and report_end_date = '${report_end_date}';

-- Populate staging table


-- Publish the latest report to reporting area. This will overwrite the existing report in reporting area with the same report_end_date.
    
    