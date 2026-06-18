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

# ### LRM FOREST TABLES

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS lrm_replication")
spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.cut_block (
    `cutb_seq_nbr` DECIMAL(15,0),
    `cutb_block_id` STRING,
    `cutb_block_number` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `cutb_devt_plan_status_code` STRING,
    `cutb_devt_plan_apprvl_date` TIMESTAMP,
    `cutb_silvi_plan_status_code` STRING,
    `cutb_silvi_plan_apprvl_date` TIMESTAMP,
    `cutb_block_memo` STRING,
    `cutb_woods_run_km_nbr` DECIMAL(15,6),
    `cutb_hauling_km_nbr` DECIMAL(15,6),
    `cutb_hlcptr_drop_cruise_ind` STRING,
    `cutb_logging_plan_status_code` STRING,
    `cutb_logging_plan_apprvl_date` TIMESTAMP,
    `opar_operating_area_id` STRING,
    `bioz_zone_id` STRING,
    `cutb_gross_ha_area` DECIMAL(11,6),
    `cutb_bcgs` STRING,
    `cutb_opening` STRING,
    `cutb_nts` STRING,
    `cutb_ext_ha_area` DECIMAL(11,6),
    `cutb_photos` STRING,
    `cutb_field_work_by` STRING,
    `cutb_field_work_date` TIMESTAMP,
    `cutb_site_class` STRING,
    `fdps_status_id` STRING,
    `cutb_site_index` DECIMAL(5,0),
    `cutb_cpi_slope_pct` DECIMAL(7,4),
    `cutb_cpi_road_access_km` DECIMAL(13,3),
    `cutb_cpi_road_block_km` DECIMAL(13,3),
    `cutb_cpi_road_num_landings` DECIMAL(10,0),
    `cutb_cpi_road_haul_km` DECIMAL(13,3),
    `sblk_supply_block_id` STRING,
    `cutb_formc_harvest_print_date` TIMESTAMP,
    `cutb_formc_regen_print_date` TIMESTAMP,
    `cutb_formc_freegrow_print_date` TIMESTAMP,
    `fund_funding_code` STRING,
    `cutb_highway_ind` STRING,
    `cutb_winter_road_ind` STRING,
    `finz_forest_inventory_zone_id` STRING,
    `cutb_user_mapsheet_id` STRING,
    `cutb_cell_number` STRING,
    `fjap_fdp_joint_approval` STRING,
    `pmod_modifier_id` STRING,
    `cutb_traverse_method_code` STRING,
    `cutb_critical_date_ind` STRING,
    `cutb_greenup_date` TIMESTAMP,
    `grns_greenup_source` STRING,
    `pmpo_operating_zone` STRING,
    `lsun_landscape_unit` STRING,
    `plan_seq_nbr` DECIMAL(15,0),
    `cutb_forma_print_date` TIMESTAMP,
    `cutb_block_state` STRING,
    `cutb_digi_ind` STRING,
    `sttp_stand_type` STRING,
    `ttac_timbertype_age_class` STRING,
    `ttsc_timbertype_stock_class` STRING,
    `tthc_timbertype_height_class` STRING,
    `cutb_site_plan_exempt_ind` STRING,
    `sssc_source_code` STRING,
    `cutb_vol_data_source` STRING,
    `cutb_vol_data_source_type` STRING,
    `cutb_heli_flight_dist` DECIMAL(10,0),
    `cutb_harvest_sequence` DECIMAL(6,0),
    `cutb_prov_forest_conflict` STRING,
    `treg_seq_nbr` DECIMAL(15,0),
    `bcat_category_code` STRING,
    `cutb_traverse_start_point` STRING,
    `cutb_traverse_end_point` STRING,
    `cutb_forma_printed` STRING,
    `cutb_system_id` STRING,
    `cutb_block_status` STRING,
    `cutb_latitude_bak` STRING,
    `cutb_longitude_bak` STRING,
    `cutb_damage_type` STRING,
    `cutb_opening_id` DECIMAL(10,0),
    `silp_use_block_num_ind` STRING,
    `cutb_location` STRING,
    `cutb_silv_act_harv_link` STRING,
    `cutb_file_id` STRING,
    `suop_subop_area_id` STRING,
    `cutb_selling_price_period` TIMESTAMP,
    `cutb_synch_status` STRING,
    `cutb_block_grouping_id` STRING,
    `pers_seq_nbr` DECIMAL(15,0),
    `ctor_seq_nbr` DECIMAL(15,0),
    `cutb_variant_id` STRING,
    `cutb_season_id` STRING,
    `cutb_volume` DECIMAL(15,6),
    `cloc_seq_nbr` DECIMAL(15,0),
    `cutb_parent_seq` DECIMAL(15,0),
    `cutb_prev_forma_print_date` TIMESTAMP,
    `sila_seq_nbr` DECIMAL(15,0),
    `cutb_cruise_lock_ind` STRING,
    `cutb_ref_dec` STRING,
    `cutb_reg_code` STRING,
    `cutb_apr_eff_date` TIMESTAMP,
    `siph_seq_nbr` DECIMAL(15,0),
    `cdat_seq_nbr` DECIMAL(38,10),
    `cutb_access_restriction` STRING,
    `cutb_planned_vol` DECIMAL(15,6),
    `cutb_row_ind` STRING,
    `cutb_tim_dev_date` TIMESTAMP,
    `cutb_ebm_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `documentkey` DECIMAL(9,0),
    `log_start_cascade_date_ind` STRING,
    `pricing_date` TIMESTAMP,
    `cutb_latitude_dd` DECIMAL(9,6),
    `cutb_longitude_dd` DECIMAL(9,6),
    `cutb_latitude` STRING,
    `cutb_longitude` STRING,
    `marked_for_del_ind` DECIMAL(1,0),
    `marked_for_del_by` STRING,
    `marked_for_del_on` TIMESTAMP,
    `cutb_archive_reason` STRING,
    `cutb_archive_date` TIMESTAMP,
    `cutb_cprp_protection_ind` STRING,
    `cutb_rc_risk_rating` STRING,
    `cutb_rc_risk_source` STRING,
    `cutb_rc_risk_comments` STRING,
    `operational_site_factor` STRING,
    `safety_concern` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS lrm_replication")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.biogeoclimatic_variant (
    `bioz_zone_id` STRING,
    `bios_subzone_id` STRING,
    `biov_variant_id` STRING,
    `biov_variant_name` STRING,
    `biov_active_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.detailed_site_assessment (
    `sila_seq_nbr` DECIMAL(15,0),
    `dsas_stratum_name` STRING,
    `potr_purpose_of_treatment` STRING,
    `dsas_water_bodies` STRING,
    `dsas_fish_habitat` STRING,
    `dsas_wildlife_habitat` STRING,
    `dsas_community_watershed` STRING,
    `dsas_buffer_size` DECIMAL(3,0),
    `dsas_chemical` STRING,
    `dsas_active` DECIMAL(9,6),
    `dsas_apply_rate` DECIMAL(13,4),
    `dsas_pmp_pup_nbr` STRING,
    `dsas_referal_ind` STRING,
    `dsas_referal` STRING,
    `dsas_approval_ind` STRING,
    `dsas_pfz_width` DECIMAL(7,2),
    `pmpl_pmp_nbr` STRING,
    `dsas_actual_brush_stems` DECIMAL(7,2),
    `dsas_performance_quality_pct` DECIMAL(6,2),
    `dsas_product_id` STRING,
    `dsas_chemical_secondary` STRING,
    `dsas_product_id_secondary` STRING,
    `dsas_solution_volume_total` DECIMAL(11,2),
    `dsas_solution_volume` DECIMAL(11,2),
    `dsas_mix_load_location` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.planting_species (
    `plsp_seq_nbr` DECIMAL(15,0),
    `plun_seq_nbr` DECIMAL(15,0),
    `sisp_species_id` STRING,
    `plsp_number_trees` DECIMAL(10,0),
    `sisl_seed_lot_number` STRING,
    `sirk_request_key` STRING,
    `plsp_seed_weight` DECIMAL(12,4),
    `plsp_price_per_tree` DECIMAL(13,4),
    `plsp_comment` STRING,
    `ssfm_seq_nbr` DECIMAL(15,0),
    `sinu_code` STRING,
    `plsp_stock_type` STRING,
    `plsp_lift_date` TIMESTAMP,
    `plsp_trees_outside_zone` DECIMAL(7,0),
    `plsp_yrs_orig_cntnr` DECIMAL(5,0),
    `plsp_yrs_transplanted` DECIMAL(5,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `v_outside_transfer_limit` STRING,
    `cbst_comments` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.silviculture_company_activity (
    `sica_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `siab_base_code` STRING,
    `siat_technique_code` STRING,
    `siam_method_code` STRING,
    `sica_activity_name` STRING,
    `siao_objective_code` STRING,
    `siao_objective_code2` STRING,
    `siao_objective_code3` STRING,
    `sica_default_ind` STRING,
    `sica_sort_order` DECIMAL(10,0),
    `sica_key_ind` STRING,
    `sica_status_ind` STRING,
    `sica_date_relative` STRING,
    `sica_date_rel_start_end` STRING,
    `sica_cost_per_ha` DECIMAL(10,5),
    `sica_default_area_pct` DECIMAL(7,4),
    `sica_current_status_ind` STRING,
    `sica_liability_pct` DECIMAL(7,4),
    `sica_active_ind` STRING,
    `sica_pre_harvest_ind` STRING,
    `sica_silv_regime_comments_ind` STRING,
    `sica_system_code` STRING,
    `sica_layer_class` STRING,
    `sica_window` STRING,
    `sica_rotation_ind` STRING,
    `sica_inv_type_id` STRING,
    `sica_establ_method_ind` STRING,
    `sica_group` STRING,
    `sica_comment` STRING,
    `sica_cost_type` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `update_planned_act_flag` STRING,
    `sica_spatial_ind` DECIMAL(1,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.silviculture_stocking_status (
    `sist_seq_nbr` DECIMAL(15,0),
    `slay_layer` STRING,
    `srnk_rank` STRING,
    `sttp_stocking_type_id` STRING,
    `ssts_survey_source` STRING,
    `ssts_survey_date` TIMESTAMP,
    `ssts_stock_age` DECIMAL(38,10),
    `ssts_stock_age_plant` DECIMAL(38,10),
    `ssts_stock_height` DECIMAL(10,4),
    `ssts_crown_closure` DECIMAL(7,4),
    `ssts_reference_year` DECIMAL(4,0),
    `sicl_site_class` STRING,
    `ssts_site_index` DECIMAL(10,0),
    `ssts_density` DECIMAL(10,1),
    `ssts_well_spaced_density` DECIMAL(10,1),
    `ssts_free_growing_density` DECIMAL(10,1),
    `srty_reserve_type` STRING,
    `ssts_basal_area` DECIMAL(7,2),
    `ssts_cm_top_height` DECIMAL(10,4),
    `ssts_cm_leader` DECIMAL(10,4),
    `ssts_vigour_pct_good` DECIMAL(7,4),
    `ssts_vigour_pct_medium` DECIMAL(7,4),
    `ssts_vigour_pct_poor` DECIMAL(7,4),
    `ssts_comments` STRING,
    `ssts_total_coniferous` DECIMAL(10,0),
    `ssts_plantable_spots` DECIMAL(10,0),
    `ssts_prepable_slash_spots` DECIMAL(10,0),
    `ssts_prepable_brush_spots` DECIMAL(10,0),
    `ssts_prepable_duff_spots` DECIMAL(10,0),
    `ssts_non_productive_spots` DECIMAL(10,0),
    `ssts_formc_printed_ind` STRING,
    `ssts_formc_date` TIMESTAMP,
    `stst_stocking_status_id` STRING,
    `ssts_root_collar_diameter` DECIMAL(10,0),
    `sssc_source_code` STRING,
    `ssts_free_grow_density_pref` DECIMAL(10,1),
    `ssts_well_spaced_density_pref` DECIMAL(10,1),
    `ssts_free_grow_lcl_density` DECIMAL(10,1),
    `ssts_well_spaced_lcl_density` DECIMAL(10,1),
    `ssts_countable_conifers` DECIMAL(10,0),
    `ssts_reentry_year` DECIMAL(4,0),
    `ssts_cvr_pattern` STRING,
    `ssts_res_objective` STRING,
    `ssts_total_well_spaced` DECIMAL(10,1),
    `ssts_germinant_per_ha` DECIMAL(10,1),
    `ssts_greened_up_tree_per_ha` DECIMAL(10,1),
    `ssts_total_trees_pa_fg` DECIMAL(10,0),
    `ssts_total_trees_p_fg` DECIMAL(10,0),
    `ssts_total_trees_wpa_fg` DECIMAL(10,0),
    `ssts_total_trees_wp_fg` DECIMAL(10,0),
    `ssts_decid_p_fg` DECIMAL(10,0),
    `ssts_tpi_pa_fg` DECIMAL(10,1),
    `ssts_ppi_pa_fg` DECIMAL(10,1),
    `ssts_ppi_low_conf_limit_fg` DECIMAL(10,1),
    `ssts_ppi_sampling_error_fg` DECIMAL(10,1),
    `ssts_total_trees_pa_regen` DECIMAL(10,0),
    `ssts_total_trees_p_regen` DECIMAL(10,0),
    `ssts_total_trees_wpa_regen` DECIMAL(10,0),
    `ssts_total_trees_wp_regen` DECIMAL(10,0),
    `ssts_decid_p_regen` DECIMAL(10,0),
    `ssts_tpi_pa_regen` DECIMAL(10,1),
    `ssts_ppi_pa_regen` DECIMAL(10,1),
    `ssts_ppi_low_conf_limit_regen` DECIMAL(10,1),
    `ssts_ppi_sampling_error_regen` DECIMAL(10,1),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.silv_activity_cost (
    `sacc_seq_nbr` DECIMAL(15,0),
    `sila_seq_nbr` DECIMAL(15,0),
    `plun_seq_nbr` DECIMAL(15,0),
    `scac_seq_nbr` DECIMAL(15,0),
    `sacc_item_cost` DECIMAL(13,4),
    `sacc_comment` STRING,
    `sacc_series` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.licence_shape_evw (
    `objectid` DECIMAL(38,0),
    `transaction_id` STRING,
    `licn_seq_nbr` DECIMAL(16,0),
    `feature_len` DECIMAL(38,8),
    `feature_area` DECIMAL(38,8),
    `shape_len` DECIMAL(38,8),
    `shape_area` DECIMAL(38,8),
    `shape` STRING,
    `manu_seq_nbr` DECIMAL(15,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `sde_state_id` DECIMAL(38,10)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.tenure_type (
    `tent_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `tent_tenure_id` STRING,
    `tent_tenure_name` STRING,
    `tety_tenure_type` STRING,
    `tent_active_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.mark (
    `mark_seq_nbr` DECIMAL(15,0),
    `mark_mark_id` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `mark_mark_desc` STRING,
    `mark_mark_state` STRING,
    `mark_crown_granted_ind` STRING,
    `mark_aac_partition` STRING,
    `mark_apportionment` STRING,
    `mark_endemic_percent` DECIMAL(4,2),
    `mark_species_type` STRING,
    `mark_all_log_grades_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.cut_block (
    `cutb_seq_nbr` DECIMAL(15,0),
    `cutb_block_id` STRING,
    `cutb_block_number` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `cutb_devt_plan_status_code` STRING,
    `cutb_devt_plan_apprvl_date` TIMESTAMP,
    `cutb_silvi_plan_status_code` STRING,
    `cutb_silvi_plan_apprvl_date` TIMESTAMP,
    `cutb_block_memo` STRING,
    `cutb_woods_run_km_nbr` DECIMAL(15,6),
    `cutb_hauling_km_nbr` DECIMAL(15,6),
    `cutb_hlcptr_drop_cruise_ind` STRING,
    `cutb_logging_plan_status_code` STRING,
    `cutb_logging_plan_apprvl_date` TIMESTAMP,
    `opar_operating_area_id` STRING,
    `bioz_zone_id` STRING,
    `cutb_gross_ha_area` DECIMAL(11,6),
    `cutb_bcgs` STRING,
    `cutb_opening` STRING,
    `cutb_nts` STRING,
    `cutb_ext_ha_area` DECIMAL(11,6),
    `cutb_photos` STRING,
    `cutb_field_work_by` STRING,
    `cutb_field_work_date` TIMESTAMP,
    `cutb_site_class` STRING,
    `fdps_status_id` STRING,
    `cutb_site_index` DECIMAL(5,0),
    `cutb_cpi_slope_pct` DECIMAL(7,4),
    `cutb_cpi_road_access_km` DECIMAL(13,3),
    `cutb_cpi_road_block_km` DECIMAL(13,3),
    `cutb_cpi_road_num_landings` DECIMAL(10,0),
    `cutb_cpi_road_haul_km` DECIMAL(13,3),
    `sblk_supply_block_id` STRING,
    `cutb_formc_harvest_print_date` TIMESTAMP,
    `cutb_formc_regen_print_date` TIMESTAMP,
    `cutb_formc_freegrow_print_date` TIMESTAMP,
    `fund_funding_code` STRING,
    `cutb_highway_ind` STRING,
    `cutb_winter_road_ind` STRING,
    `finz_forest_inventory_zone_id` STRING,
    `cutb_user_mapsheet_id` STRING,
    `cutb_cell_number` STRING,
    `fjap_fdp_joint_approval` STRING,
    `pmod_modifier_id` STRING,
    `cutb_traverse_method_code` STRING,
    `cutb_critical_date_ind` STRING,
    `cutb_greenup_date` TIMESTAMP,
    `grns_greenup_source` STRING,
    `pmpo_operating_zone` STRING,
    `lsun_landscape_unit` STRING,
    `plan_seq_nbr` DECIMAL(15,0),
    `cutb_forma_print_date` TIMESTAMP,
    `cutb_block_state` STRING,
    `cutb_digi_ind` STRING,
    `sttp_stand_type` STRING,
    `ttac_timbertype_age_class` STRING,
    `ttsc_timbertype_stock_class` STRING,
    `tthc_timbertype_height_class` STRING,
    `cutb_site_plan_exempt_ind` STRING,
    `sssc_source_code` STRING,
    `cutb_vol_data_source` STRING,
    `cutb_vol_data_source_type` STRING,
    `cutb_heli_flight_dist` DECIMAL(10,0),
    `cutb_harvest_sequence` DECIMAL(6,0),
    `cutb_prov_forest_conflict` STRING,
    `treg_seq_nbr` DECIMAL(15,0),
    `bcat_category_code` STRING,
    `cutb_traverse_start_point` STRING,
    `cutb_traverse_end_point` STRING,
    `cutb_forma_printed` STRING,
    `cutb_system_id` STRING,
    `cutb_block_status` STRING,
    `cutb_latitude_bak` STRING,
    `cutb_longitude_bak` STRING,
    `cutb_damage_type` STRING,
    `cutb_opening_id` DECIMAL(10,0),
    `silp_use_block_num_ind` STRING,
    `cutb_location` STRING,
    `cutb_silv_act_harv_link` STRING,
    `cutb_file_id` STRING,
    `suop_subop_area_id` STRING,
    `cutb_selling_price_period` TIMESTAMP,
    `cutb_synch_status` STRING,
    `cutb_block_grouping_id` STRING,
    `pers_seq_nbr` DECIMAL(15,0),
    `ctor_seq_nbr` DECIMAL(15,0),
    `cutb_variant_id` STRING,
    `cutb_season_id` STRING,
    `cutb_volume` DECIMAL(15,6),
    `cloc_seq_nbr` DECIMAL(15,0),
    `cutb_parent_seq` DECIMAL(15,0),
    `cutb_prev_forma_print_date` TIMESTAMP,
    `sila_seq_nbr` DECIMAL(15,0),
    `cutb_cruise_lock_ind` STRING,
    `cutb_ref_dec` STRING,
    `cutb_reg_code` STRING,
    `cutb_apr_eff_date` TIMESTAMP,
    `siph_seq_nbr` DECIMAL(15,0),
    `cdat_seq_nbr` DECIMAL(38,10),
    `cutb_access_restriction` STRING,
    `cutb_planned_vol` DECIMAL(15,6),
    `cutb_row_ind` STRING,
    `cutb_tim_dev_date` TIMESTAMP,
    `cutb_ebm_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `documentkey` DECIMAL(9,0),
    `log_start_cascade_date_ind` STRING,
    `pricing_date` TIMESTAMP,
    `cutb_latitude_dd` DECIMAL(9,6),
    `cutb_longitude_dd` DECIMAL(9,6),
    `cutb_latitude` STRING,
    `cutb_longitude` STRING,
    `marked_for_del_ind` DECIMAL(1,0),
    `marked_for_del_by` STRING,
    `marked_for_del_on` TIMESTAMP,
    `cutb_archive_reason` STRING,
    `cutb_archive_date` TIMESTAMP,
    `cutb_cprp_protection_ind` STRING,
    `cutb_rc_risk_rating` STRING,
    `cutb_rc_risk_source` STRING,
    `cutb_rc_risk_comments` STRING,
    `operational_site_factor` STRING,
    `safety_concern` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.activity_class (
    `accl_seq_nbr` DECIMAL(15,0),
    `accl_description` STRING,
    `accl_object_type` STRING,
    `accl_display_order` DECIMAL(3,0),
    `divi_div_nbr` DECIMAL(2,0),
    `accl_key_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.division_code_lookup (
    `colu_lookup_type` STRING,
    `colu_lookup_id` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.division (
    `divi_div_nbr` DECIMAL(2,0),
    `divi_country_code` STRING,
    `divi_prov_state_code` STRING,
    `divi_division_name` STRING,
    `divi_startup_date` TIMESTAMP,
    `divi_end_date` TIMESTAMP,
    `divi_short_code` STRING,
    `divi_line1_addr` STRING,
    `divi_line2_addr` STRING,
    `divi_line3_addr` STRING,
    `divi_city_name` STRING,
    `divi_postal_code` STRING,
    `divi_intl_routing_code` STRING,
    `divi_phone_nbr` STRING,
    `divi_fax_nbr` STRING,
    `divi_stmpg_acctcd` STRING,
    `divi_stmpg_offset_acctcd` STRING,
    `divi_walker_entity_acctcd` STRING,
    `divi_gst_pay_acctcd` STRING,
    `divi_gst_recovery_acctd` STRING,
    `divi_operation_location_ind` STRING,
    `divi_client_location_code` STRING,
    `divi_abbreviation_code` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.activity_type (
    `actt_seq_nbr` DECIMAL(15,0),
    `accl_seq_nbr` DECIMAL(15,0),
    `actt_description` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `actt_default_ind` STRING,
    `actt_responsibility` STRING,
    `actt_display_order` DECIMAL(2,0),
    `actt_key_ind` STRING,
    `actt_status_ind` STRING,
    `actt_date_relative` STRING,
    `ctor_contractor_id` STRING,
    `actt_view_level` DECIMAL(2,0),
    `actt_system_ind` STRING,
    `ctor_seq_nbr` DECIMAL(15,0),
    `actt_active_ind` STRING,
    `actt_key_pair` STRING,
    `actt_harvs_ind` STRING,
    `actt_trvol_ind` STRING,
    `actt_indirect_cost_ind` STRING,
    `actt_default_cost` DECIMAL(9,2),
    `actt_cost_uom` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.silv_treatment_regime (
    `treg_seq_nbr` DECIMAL(15,0),
    `pers_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `treg_regime_id` STRING,
    `treg_regime_name` STRING,
    `treg_create_date` TIMESTAMP,
    `treg_active_ind` STRING,
    `treg_def_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.bcts_harvest_history (
    `bchh_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `bchh_billing_year` DECIMAL(4,0),
    `bchh_billing_month` DECIMAL(4,0),
    `bchh_billing_period` TIMESTAMP,
    `bchh_hdbs_tree_species` STRING,
    `bchh_forest_product_code` STRING,
    `bchh_log_grade` STRING,
    `bchh_billing_type_code` STRING,
    `bchh_volume_billed` DECIMAL(9,2),
    `bchh_pieces_billed` DECIMAL(10,0),
    `bchh_royalty_amount` DECIMAL(11,2),
    `bchh_reserve_stmpg_amt` DECIMAL(9,2),
    `bchh_bonus_stumpage_amt` DECIMAL(9,2),
    `bchh_silv_levy_amount` DECIMAL(9,2),
    `bchh_dev_levy_amount` DECIMAL(9,2),
    `bchh_update_timestamp` TIMESTAMP,
    `bchh_update_userid` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.commitments (
    `licn_seq_nbr` DECIMAL(15,0),
    `commit_seq_nbr` DECIMAL(15,0),
    `copa_partition` STRING,
    `copa_commit_appo` DECIMAL(15,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `manu_seq_nbr` DECIMAL(15,0),
    `copa_commit_lic_type` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.ecology_unit (
    `ecou_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `ecou_name` STRING,
    `bioz_zone_id` STRING,
    `bios_subzone_id` STRING,
    `biov_variant_id` STRING,
    `ecou_site_type_id` STRING,
    `ecou_additional_info` STRING,
    `ecou_ecosite_fit_code` STRING,
    `ecou_ha_area` DECIMAL(11,6),
    `ecou_digitised_ind` STRING,
    `mreg_edatopic_moisture_id` STRING,
    `nreg_edatopic_nutrient_id` STRING,
    `ecou_elevation_min` DECIMAL(5,0),
    `ecou_elevation_max` DECIMAL(5,0),
    `ecou_elevation_avg` DECIMAL(5,0),
    `ecou_aspect` STRING,
    `ecou_slope_min` DECIMAL(7,4),
    `ecou_slope_max` DECIMAL(7,4),
    `ecou_slope_avg` DECIMAL(7,4),
    `ecou_slope_position` STRING,
    `ecou_terrain` STRING,
    `ecou_parent_material` STRING,
    `ecou_drainage` STRING,
    `ecou_substrate_decaying_wood` DECIMAL(7,4),
    `ecou_substrate_bedrock` DECIMAL(7,4),
    `ecou_substrate_cobbles` DECIMAL(7,4),
    `ecou_substrate_mineral_soil` DECIMAL(7,4),
    `ecou_substrate_organic` DECIMAL(7,4),
    `ecou_substrate_water` DECIMAL(7,4),
    `ecou_organic_layer` STRING,
    `ecou_humus_l_depth_min` DECIMAL(10,4),
    `ecou_humus_l_depth_max` DECIMAL(10,4),
    `ecou_humus_f_depth_min` DECIMAL(10,4),
    `ecou_humus_f_depth_max` DECIMAL(10,4),
    `ecou_humus_h_depth_min` DECIMAL(10,4),
    `ecou_humus_h_depth_max` DECIMAL(10,4),
    `ecou_humus_form` STRING,
    `ecou_soil_order` STRING,
    `ecou_soil_type` STRING,
    `ecou_soil_depth_water` DECIMAL(10,4),
    `ecou_soil_depth_water_max` DECIMAL(10,4),
    `ecou_soil_depth_mottles` DECIMAL(10,4),
    `ecou_soil_depth_mottles_max` DECIMAL(10,4),
    `ecou_soil_depth_gleying` DECIMAL(10,4),
    `ecou_soil_depth_gleying_max` DECIMAL(10,4),
    `ecou_soil_depth_calcar` DECIMAL(10,4),
    `ecou_soil_depth_calcar_max` DECIMAL(10,4),
    `ecou_soil_depth_bedrock` DECIMAL(10,4),
    `ecou_soil_depth_bedrock_max` DECIMAL(10,4),
    `ecou_soil_depth_compact` DECIMAL(10,4),
    `ecou_soil_depth_compact_max` DECIMAL(10,4),
    `ecou_soil_rooting_depth` DECIMAL(10,4),
    `ecou_soil_seepage_ind` STRING,
    `ecou_soil_seepage_depth` DECIMAL(10,4),
    `ecou_soil_pit_depth` DECIMAL(10,4),
    `ecou_soil_acid_test` STRING,
    `ecou_soil_acid_test_depth` DECIMAL(10,4),
    `ecou_soil_acid_ph_test` DECIMAL(9,6),
    `ecou_soil_comments` STRING,
    `ecou_field_guide` STRING,
    `ecou_field_guide_year` DECIMAL(4,0),
    `ecou_surveyed_by` STRING,
    `ecou_survey_date` TIMESTAMP,
    `ecou_comments` STRING,
    `ecou_perched_ind` STRING,
    `ecou_soil_acid_ph` STRING,
    `bgrg_region_code` STRING,
    `ecou_soil_op` STRING,
    `ecou_coarse_fragment` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `documentkey` DECIMAL(9,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.sub_operating_area (
    `suop_subop_area_id` STRING,
    `suop_subop_area_name` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `opar_operating_area_id` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.licence_allocation (
    `licn_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `manu_seq_nbr` DECIMAL(15,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.licensee (
    `lsee_licensee_id` STRING,
    `lsee_licensee_name` STRING,
    `lsee_client_code` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.code_lookup (
    `colu_lookup_type` STRING,
    `colu_lookup_id` STRING,
    `colu_lookup_desc` STRING,
    `colu_user_defined_ind` STRING,
    `colu_display_ind` STRING,
    `colu_display_order` DECIMAL(10,0),
    `colu_comment` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `hq_display_ind` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.cut_permit (
    `perm_seq_nbr` DECIMAL(15,0),
    `regn_region_id` STRING,
    `perm_permit_id` STRING,
    `perm_revision_date` TIMESTAMP,
    `papr_point_of_apprsl_id` STRING,
    `lsee_licensee_id` STRING,
    `admin_dsct_district_name` STRING,
    `geo_dsct_district_name` STRING,
    `perm_mgmt_unit_code` STRING,
    `perm_tsb_name` STRING,
    `perm_expiry_date` TIMESTAMP,
    `perm_remote_oper_ind` STRING,
    `perm_basic_silvi_ind` STRING,
    `perm_road_mtce_ind` STRING,
    `perm_road_land_use_chrg` DECIMAL(15,2),
    `perm_cost_eff_date` TIMESTAMP,
    `perm_rate_eff_date` TIMESTAMP,
    `perm_water_transport_ind` STRING,
    `perm_primary_block_seq_nbr` DECIMAL(15,0),
    `perm_primary_mark_id` STRING,
    `perm_haul_ind` STRING,
    `tsar_tsa_nbr` DECIMAL(3,0),
    `sblk_supply_block_id` STRING,
    `sptc_support_centre_name` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `perm_damage_status` STRING,
    `perm_support_centre_dist` DECIMAL(6,2),
    `opty_opening_type_id` STRING,
    `pers_seq_nbr` DECIMAL(15,0),
    `perm_permit_state` STRING,
    `perm_logprod_external_ind` STRING,
    `perm_logprod_customer_id` STRING,
    `perm_cruise_res_source` STRING,
    `perm_jurisdiction` STRING,
    `perm_application_description` STRING,
    `perm_primary_species` STRING,
    `mkme_marking_method_code` STRING,
    `mkin_marking_instrument_code` STRING,
    `perm_prov_forest_conflict` STRING,
    `perm_cruise_based_ind` STRING,
    `perm_lsee_representative` DECIMAL(15,0),
    `perm_traverse_start_point` STRING,
    `perm_traverse_end_point` STRING,
    `perm_digi_ind` STRING,
    `perm_bdt_conversion` DECIMAL(15,7),
    `cpcl_permit_class` STRING,
    `perm_parent_permit` DECIMAL(15,0),
    `perm_spuc` STRING,
    `perm_salvage_ind` STRING,
    `perm_location` STRING,
    `perm_status` STRING,
    `ctor_seq_nbr` DECIMAL(15,0),
    `cloc_seq_nbr` DECIMAL(15,0),
    `perm_fst_seq_nbr` DECIMAL(15,0),
    `perm_bid_amount` DECIMAL(10,2),
    `perm_total_cost` DECIMAL(10,2),
    `perm_legal_description` STRING,
    `perm_gross_area` DECIMAL(15,7),
    `perm_high_lvl_pln` STRING,
    `perm_lcn_hvst_auth` STRING,
    `perm_lcn_reg` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.activity (
    `acti_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `actt_seq_nbr` DECIMAL(15,0),
    `ctor_seq_nbr` DECIMAL(15,0),
    `acti_status_ind` STRING,
    `acti_status_date` TIMESTAMP,
    `acti_comments` STRING,
    `acti_cost` DECIMAL(15,2),
    `acti_harv_seas_id` STRING,
    `acti_responsibility` STRING,
    `acti_area` DECIMAL(11,6),
    `acti_harvest_vol` DECIMAL(15,6),
    `acti_int_reason` STRING,
    `acti_target_date` TIMESTAMP,
    `acti_fdp_reason` STRING,
    `acti_target_cost` DECIMAL(15,2),
    `plan_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `cloc_seq_nbr` DECIMAL(15,0),
    `acti_digitized_ind` STRING,
    `acti_cost_uom` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `documentkey` DECIMAL(9,0),
    `accl_description` STRING,
    `acti_variable_cost_upset` DECIMAL(15,2),
    `acti_total_cost_upset` DECIMAL(15,2),
    `acti_mps70` DECIMAL(15,2)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.operating_area (
    `opar_operating_area_id` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `opar_operating_area_name` STRING,
    `ozon_operating_zone_id` STRING,
    `opar_seq_nbr` DECIMAL(15,0),
    `opar_m3_ha_factor` DECIMAL(15,6),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.permit_allocation (
    `peal_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `manu_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.biogeoclimatic_subzone (
    `bioz_zone_id` STRING,
    `bios_subzone_id` STRING,
    `bios_subzone_name` STRING,
    `bios_active_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.biogeoclimatic_zone (
    `bioz_zone_id` STRING,
    `bioz_zone_name` STRING,
    `bioz_active_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.standard_unit (
    `stun_seq_nbr` DECIMAL(15,0),
    `silp_seq_nbr` DECIMAL(15,0),
    `stun_id` STRING,
    `suty_type_id` STRING,
    `stun_description` STRING,
    `stun_gross_ha_area` DECIMAL(11,6),
    `stun_digitised_ind` STRING,
    `spss_seq_nbr` DECIMAL(15,0),
    `stun_critical_site_conditions` STRING,
    `stun_soil_compaction` STRING,
    `stun_soil_erosion` STRING,
    `stun_soil_displacement` STRING,
    `stun_depth_unfavourable_min` DECIMAL(13,4),
    `stun_depth_unfavourable_max` DECIMAL(13,4),
    `stun_type_unfavourable` STRING,
    `stun_sediment_risk` STRING,
    `stun_max_disturbance` DECIMAL(7,4),
    `stun_regen_date_early` DECIMAL(4,0),
    `stun_freegrow_date_early` DECIMAL(4,0),
    `stun_freegrow_date_late` DECIMAL(4,0),
    `stun_silvicultural_system_comm` STRING,
    `stun_temp_access_max_disturb` DECIMAL(7,4),
    `stun_ncbr_ha_area` DECIMAL(11,6),
    `stun_ncbr_digitised_ind` STRING,
    `stun_temp_access_bank_height` DECIMAL(13,4),
    `stun_temp_access_avg_bank_hgt` DECIMAL(13,4),
    `stun_temp_access_equipment` STRING,
    `stun_soil_puddling` STRING,
    `stun_soil_frost_heave` STRING,
    `stun_soil_windthrow` STRING,
    `stun_veg_comp_severity` STRING,
    `stty_original_standard_type` STRING,
    `stun_original_std_declare_date` TIMESTAMP,
    `stun_wet_low_density_ind` STRING,
    `stun_ms_decl_comment` STRING,
    `stun_formc_postharv_prn_date` TIMESTAMP,
    `stun_formc_regen_prn_date` TIMESTAMP,
    `stun_formc_freegrow_prn_date` TIMESTAMP,
    `stds_seq_nbr` DECIMAL(15,0),
    `stun_site_index` DECIMAL(5,0),
    `stun_other_perf_standards` STRING,
    `stun_pathway` STRING,
    `stun_si_species` STRING,
    `stun_brush_hazard` STRING,
    `stun_browse_hazard` STRING,
    `stun_si_source` STRING,
    `stun_non_mappable_area` DECIMAL(11,6),
    `stun_designation_code` STRING,
    `stun_declaration_area` DECIMAL(11,6),
    `stun_declaration_area_lock_ind` STRING,
    `stun_prev_postharv_prn_date` TIMESTAMP,
    `stun_prev_regen_prn_date` TIMESTAMP,
    `stun_prev_freegrow_prn_date` TIMESTAMP,
    `stun_objective_type` STRING,
    `stun_regen_obligation_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `stun_primary_obj` STRING,
    `stun_secondary_obj` STRING,
    `np_ha_area` DECIMAL(11,6)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.person (
    `pers_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `pers_first_name` STRING,
    `pers_last_name` STRING,
    `pers_display_name` STRING,
    `pers_person_number` STRING,
    `pers_phone_number` STRING,
    `pers_email` STRING,
    `pers_fax` STRING,
    `ctor_seq_nbr` DECIMAL(15,0),
    `tpct_category_id` STRING,
    `tpdp_department_id` STRING,
    `pers_comment` STRING,
    `pers_active_ind` STRING,
    `pers_inactive_date` TIMESTAMP,
    `pers_professional_number` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.brushing_target_species (
    `btrs_seq_nbr` DECIMAL(15,0),
    `sila_seq_nbr` DECIMAL(15,0),
    `sisp_species_id` STRING,
    `btrs_coverage_pct` DECIMAL(3,0),
    `btrs_height` DECIMAL(10,4),
    `btrs_broadleaf_tree_diameter` DECIMAL(13,4),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.cut_block_shape (
    `objectid` DECIMAL(38,10),
    `transaction_id` STRING,
    `cutb_seq_nbr` DECIMAL(16,0),
    `bufferdist` DECIMAL(38,8),
    `objectid_1` DECIMAL(10,0),
    `transactio` STRING,
    `objectid_2` DECIMAL(10,0),
    `hectares` DECIMAL(38,8),
    `feature_len` DECIMAL(38,8),
    `feature_area` DECIMAL(38,8),
    `shape_len` DECIMAL(38,8),
    `shape_area` DECIMAL(38,8),
    `shape` STRING,
    `licn_seq_nbr` DECIMAL(16,0),
    `manu_seq_nbr` DECIMAL(16,0),
    `mark_seq_nbr` DECIMAL(16,0),
    `perm_seq_nbr` DECIMAL(16,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.cut_block_shape_evw (
    `objectid` DECIMAL(38,0),
    `transaction_id` STRING,
    `cutb_seq_nbr` DECIMAL(16,0),
    `bufferdist` DECIMAL(38,8),
    `objectid_1` DECIMAL(10,0),
    `transactio` STRING,
    `objectid_2` DECIMAL(10,0),
    `hectares` DECIMAL(38,8),
    `feature_len` DECIMAL(38,8),
    `feature_area` DECIMAL(38,8),
    `shape_len` DECIMAL(38,8),
    `shape_area` DECIMAL(38,8),
    `shape` STRING,
    `licn_seq_nbr` DECIMAL(16,0),
    `manu_seq_nbr` DECIMAL(16,0),
    `mark_seq_nbr` DECIMAL(16,0),
    `perm_seq_nbr` DECIMAL(16,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `sde_state_id` DECIMAL(38,10)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.frst_cost_item (
    `csti_cost_item_id` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `csti_cost_item_description` STRING,
    `csti_cost_item_active_ind` STRING,
    `csti_cost_item_display_order` DECIMAL(4,0),
    `csti_cost_item_account_code` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.planting_unit (
    `plun_seq_nbr` DECIMAL(15,0),
    `sila_seq_nbr` DECIMAL(15,0),
    `plun_digitised_ind` STRING,
    `plun_ha_area` DECIMAL(11,6),
    `plun_quality_percent` DECIMAL(7,4),
    `plun_planting_cost` DECIMAL(15,5),
    `plun_planting_unit_id` DECIMAL(3,0),
    `plun_survival_pct` DECIMAL(7,4),
    `plun_last_survival_check` TIMESTAMP,
    `plun_contract_min_spacing` DECIMAL(7,2),
    `plun_contract_target_spacing` DECIMAL(7,2),
    `plun_contract_max_spacing` DECIMAL(7,2),
    `plun_planting_plan_cost` DECIMAL(15,5),
    `plun_row` DECIMAL(7,2),
    `plun_net_area` DECIMAL(11,6),
    `plun_drill` DECIMAL(6,2),
    `plun_min_stock_size` STRING,
    `plun_max_stock_size` STRING,
    `plun_plan_cost_per_unit` DECIMAL(15,5),
    `plun_cost_per_unit` DECIMAL(15,5),
    `plun_density` DECIMAL(7,2),
    `plun_lock_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `when_area_is_changed` STRING,
    `plsp_editing_trees_or_pct` STRING,
    `total_trees` DECIMAL(12,0),
    `plun_cost_per_tree` DECIMAL(15,5),
    `plun_planned_cost_per_tree` DECIMAL(15,5),
    `plun_cost_per_ha` DECIMAL(15,5),
    `plun_planned_cost_per_ha` DECIMAL(15,5)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.silviculture_activity (
    `sila_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `sica_seq_nbr` DECIMAL(15,0),
    `sila_status` STRING,
    `sila_gross_ha_area` DECIMAL(11,6),
    `sila_digitised_ind` STRING,
    `sila_start_date` TIMESTAMP,
    `sila_completion_date` TIMESTAMP,
    `fund_funding_code` STRING,
    `resp_responsibility_code` STRING,
    `sila_contractor_id` STRING,
    `sila_formb_printed` STRING,
    `sila_formb_date` TIMESTAMP,
    `sisy_silvicultural_system_id` STRING,
    `siva_variant_id` STRING,
    `siph_cut_phase_id` STRING,
    `sila_reserve_type` STRING,
    `sila_disturbance_code` STRING,
    `hame_method_id` STRING,
    `sila_harvest_volume` DECIMAL(15,6),
    `sila_cost_per_ha` DECIMAL(16,5),
    `sila_machine_hours` DECIMAL(7,2),
    `sila_plantable_spots` DECIMAL(10,1),
    `sila_soil_disturbance` DECIMAL(7,4),
    `sila_season` STRING,
    `sila_percent_paid` DECIMAL(7,4),
    `sila_brush_chemical` STRING,
    `sila_brush_active` DECIMAL(9,6),
    `sila_brush_target_species1` STRING,
    `sila_brush_target_species2` STRING,
    `sila_brush_deciduous` DECIMAL(10,0),
    `sila_brush_apply_rate` DECIMAL(13,4),
    `sila_brush_pmp_pup_nbr` STRING,
    `ctor_seq_nbr` DECIMAL(15,0),
    `sila_treatment_unit_id` STRING,
    `stst_stocking_status_id` STRING,
    `sila_total_stocking_pct` DECIMAL(5,2),
    `sila_project_num` STRING,
    `cloc_seq_nbr` DECIMAL(15,0),
    `sila_cost_per_ha_plan` DECIMAL(16,5),
    `sila_treated_area` DECIMAL(11,6),
    `pers_seq_nbr` DECIMAL(15,0),
    `oalt_object_id` STRING,
    `objt_seq_nbr` DECIMAL(15,0),
    `hrun_seq_nbr` DECIMAL(15,0),
    `sila_start_date_planned` TIMESTAMP,
    `sila_completion_date_planned` TIMESTAMP,
    `sila_net_area` DECIMAL(11,6),
    `surveyor_seq_nbr` DECIMAL(15,0),
    `recorder_seq_nbr` DECIMAL(15,0),
    `sila_activity_approval_status` STRING,
    `sila_priority` STRING,
    `sila_comments` STRING,
    `sila_cost_unit` STRING,
    `sila_budgeted_ind` STRING,
    `sila_prev_formb_date` TIMESTAMP,
    `sila_allocation_id` STRING,
    `sila_harvest_area_finalized` STRING,
    `sila_plan_lock_ind` STRING,
    `sila_act_lock_ind` STRING,
    `atu_id` STRING,
    `sila_retreatment_ind` STRING,
    `sila_sdd_submission` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `documentkey` DECIMAL(9,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.ctor_contractor_location (
    `cloc_seq_nbr` DECIMAL(15,0),
    `ctor_seq_nbr` DECIMAL(15,0),
    `cloc_address_line1` STRING,
    `cloc_address_line2` STRING,
    `cloc_address_line3` STRING,
    `cloc_city` STRING,
    `cloc_phone_number1` STRING,
    `cloc_phone_number2` STRING,
    `cloc_fax_number` STRING,
    `ctry_country_code` STRING,
    `prov_prov_state_code` STRING,
    `cloc_postal_code` STRING,
    `cloc_client_number` STRING,
    `cloc_client_location_code` STRING,
    `cloc_client_location_name` STRING,
    `cloc_locn_expired_ind` STRING,
    `cloc_returned_mail_date` TIMESTAMP,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.silviculture_stratum (
    `sist_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `sist_stratum_name` STRING,
    `stst_stocking_status_id` STRING,
    `sist_digitised_ind` STRING,
    `sist_ha_area` DECIMAL(11,6),
    `sist_green_up_date` TIMESTAMP,
    `sist_green_up_ind` STRING,
    `sist_free_grow_date` TIMESTAMP,
    `sist_free_grow_ind` STRING,
    `sist_regen_delay_met_date` TIMESTAMP,
    `sist_regen_delay_met_ind` STRING,
    `sila_seq_nbr` DECIMAL(15,0),
    `sist_post_harvest_date` TIMESTAMP,
    `sist_post_harvest_ind` STRING,
    `sist_nonmap_parent` DECIMAL(15,0),
    `sist_establishment_year` TIMESTAMP,
    `sist_crop_stems_per_ha` DECIMAL(5,0),
    `sist_total_stems_per_ha` DECIMAL(5,0),
    `sist_site_index` DECIMAL(2,0),
    `sist_pct_distribution` DECIMAL(3,0),
    `sist_pct_productive` DECIMAL(3,0),
    `sist_prefix` STRING,
    `sist_class` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `np_ha_area` DECIMAL(11,6),
    `dep_ind` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.block_allocation (
    `cutb_seq_nbr` DECIMAL(15,0),
    `blal_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `blal_gross_ha_area` DECIMAL(11,6),
    `blal_merch_ha_area` DECIMAL(11,6),
    `blal_cruise_m3_vol` DECIMAL(15,6),
    `blal_harvested_m3_vol` DECIMAL(15,6),
    `blal_harvested_ha_area` DECIMAL(11,6),
    `blal_firs_cutblock_id` STRING,
    `blal_firs_timbermark_id` STRING,
    `blal_estimated_area` DECIMAL(7,2),
    `manu_seq_nbr` DECIMAL(15,0),
    `blal_digi_ind` STRING,
    `blal_primary_ind` STRING,
    `blal_actual_partition_vol` DECIMAL(15,2),
    `blal_usr_cruise_m3_vol` DECIMAL(15,6),
    `blal_rw_vol` DECIMAL(15,6),
    `blal_rw_ha_area` DECIMAL(11,6),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `blal_data_source` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.management_unit (
    `manu_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `manu_id` STRING,
    `manu_name` STRING,
    `manu_comment` STRING,
    `manu_type_id` STRING,
    `manu_operating_zone` STRING,
    `manu_mgr_seq_nbr` DECIMAL(15,0),
    `manu_fst_seq_nbr` DECIMAL(15,0),
    `manu_digitize_ind` STRING,
    `manu_area` DECIMAL(11,6),
    `manu_net_area` DECIMAL(11,6),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `nat_res_area` STRING,
    `nat_res_region` STRING,
    `manu_aac_effect_date` TIMESTAMP,
    `manu_aac` DECIMAL(10,2),
    `non_bcts_aac_ind` DECIMAL(1,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.licence (
    `licn_seq_nbr` DECIMAL(15,0),
    `licn_licence_id` STRING,
    `licn_licence_desc` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `licn_crown_land` STRING,
    `licn_annual_allowable_cut` DECIMAL(9,0),
    `lsee_licensee_id` STRING,
    `tent_seq_nbr` DECIMAL(15,0),
    `licn_licence_state` STRING,
    `licn_permit_exists_ind` STRING,
    `licn_salvage_ind` STRING,
    `licn_category_id` STRING,
    `licn_field_team_id` STRING,
    `ctor_seq_nbr` DECIMAL(15,0),
    `cloc_seq_nbr` DECIMAL(15,0),
    `blaz_admin_zone_id` STRING,
    `licn_digi_ind` STRING,
    `licl_licence_class` STRING,
    `licn_parent_licence` DECIMAL(15,0),
    `licn_crown_granted_ind` STRING,
    `licn_client_loc_code` STRING,
    `licn_category_type` STRING,
    `licn_licence_to_cut_code` STRING,
    `linc_cert_level_id` STRING,
    `licn_mgr_seq_nbr` DECIMAL(15,0),
    `licn_fst_seq_nbr` DECIMAL(15,0),
    `licn_gross_area` DECIMAL(11,6),
    `licn_net_area` DECIMAL(11,6),
    `licn_comment` STRING,
    `licn_apportion_tenure_type` STRING,
    `team_seq_nbr` DECIMAL(15,0),
    `licn_hammermark` STRING,
    `licn_client_location_code` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `licn_archive_ind` STRING,
    `licn_archive_date` TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.silv_company_activity_cost (
    `scac_seq_nbr` DECIMAL(15,0),
    `sica_seq_nbr` DECIMAL(15,0),
    `csti_cost_item_id` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `scac_active_ind` STRING,
    `scac_display_order` DECIMAL(4,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.silviculture_prescription (
    `silp_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `silp_lterm_man_obj` STRING,
    `silp_wildlife` STRING,
    `silp_sensitive_areas` STRING,
    `silp_fisheries` STRING,
    `silp_watersheds` STRING,
    `silp_recreation` STRING,
    `silp_community_watershed_ind` STRING,
    `silp_bio_diversity` STRING,
    `silp_wildlife_tree_credit_ha` DECIMAL(7,2),
    `silp_visual_landscape` STRING,
    `silp_visual_assessment` STRING,
    `silp_cultural_heritage` STRING,
    `silp_range` STRING,
    `silp_range_tenure_holder` STRING,
    `silp_other_resources` STRING,
    `silp_trapper_registration` STRING,
    `silp_guide_registration` STRING,
    `silp_na_conditions` STRING,
    `silp_riparian_assessment` STRING,
    `silp_gully_manage_strategy` STRING,
    `silp_gully_assessment` STRING,
    `silp_forest_health` STRING,
    `silp_pest_assessment` STRING,
    `silp_coarse_woody_debris` STRING,
    `silp_archaeological_sites` STRING,
    `silp_archaeology_assessment` STRING,
    `silp_vegetation_management` STRING,
    `silp_livestock_used` STRING,
    `silp_slope_instability` STRING,
    `silp_slope_assessment` STRING,
    `silp_perm_access_max_pct` DECIMAL(7,4),
    `silp_temp_access_max_rehab` DECIMAL(10,0),
    `silp_temp_access_max_disturb` DECIMAL(7,4),
    `silp_temp_access_location` STRING,
    `silp_temp_access_bank_height` DECIMAL(13,4),
    `silp_temp_access_avg_bank_hgt` DECIMAL(13,4),
    `silp_temp_access_equipment` STRING,
    `silp_temp_access_ha_area` DECIMAL(11,6),
    `silp_soil_cons_comments` STRING,
    `silp_stocking_req_comments` STRING,
    `pers_seq_nbr` DECIMAL(15,0),
    `silp_sp_report_format` STRING,
    `silp_amendments_comments` STRING,
    `silp_temp_access_comments` STRING,
    `silp_riparian_comments` STRING,
    `silp_bio_diversity_calc_ind` STRING,
    `silp_rpf_certify_comments` STRING,
    `silp_admin_assessment_comments` STRING,
    `silp_use_block_num_ind` STRING,
    `silp_coarse_woody_debris_m3_ha` DECIMAL(7,2),
    `silp_bio_diversity_pct` DECIMAL(3,0),
    `silp_lichen_assessment` STRING,
    `silp_roadside_disturb_max_pct` DECIMAL(7,4),
    `silp_biodiv_perf_std_start_pct` DECIMAL(7,3),
    `silp_bio_divers_block_tgt_pct` DECIMAL(7,3),
    `silp_cwd_perf_std_pct` DECIMAL(7,3),
    `silp_cwd_block_tgt_pct` DECIMAL(7,3),
    `silp_perm_access_max_area` DECIMAL(11,6),
    `silp_additional_comments` STRING,
    `silp_pest_required` STRING,
    `silp_visual_required` STRING,
    `silp_vegetation_required` STRING,
    `silp_raiparian_required` STRING,
    `silp_fhealth_required` STRING,
    `silp_gully_required` STRING,
    `silp_archeosite_required` STRING,
    `silp_slope_required` STRING,
    `silp_lichen_required` STRING,
    `silp_forest_health_pest` STRING,
    `silp_biodiv_perf_std_end_pct` DECIMAL(7,3),
    `slip_updated_by` STRING,
    `slip_updated_date` TIMESTAMP,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `fspm_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_lrm_commitments (
    `licn_seq_nbr` DECIMAL(15,0),
    `commit_seq_nbr` DECIMAL(15,0),
    `copa_partition` STRING,
    `copa_commit_appo` DECIMAL(15,0),
    `v_copa_commit_m3_vol` DECIMAL(38,10),
    `v_remain_commit_m3_vol` DECIMAL(38,10),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `manu_seq_nbr` DECIMAL(15,0),
    `copa_commit_lic_type` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.su_type (
    `suty_type_id` STRING,
    `suty_type_desc` STRING,
    `suty_sort_order` DECIMAL(38,10),
    `suty_key_ind` STRING,
    `suty_default_crown_closure` DECIMAL(7,4),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.mark_allocation (
    `mark_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `manu_seq_nbr` DECIMAL(15,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `maal_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.ctor_contractor (
    `ctor_seq_nbr` DECIMAL(15,0),
    `ctor_name` STRING,
    `ctor_email_address` STRING,
    `ctor_status_code` STRING,
    `ctor_internal_code` STRING,
    `ctor_ems_ind` STRING,
    `ctor_wcb_nbr` DECIMAL(10,0),
    `ctor_gst` STRING,
    `ctor_wcb_clearance` TIMESTAMP,
    `ctor_insurance_liability` TIMESTAMP,
    `ctor_date_approved` TIMESTAMP,
    `ctor_expiration_date` TIMESTAMP,
    `ctor_approved_ind` STRING,
    `ctor_comment` STRING,
    `ctor_id` STRING,
    `ctor_password` STRING,
    `ctor_vendor_nbr` STRING,
    `ctor_post_ap_ind` STRING,
    `ctor_gst_ind` STRING,
    `ctor_legal_first_name` STRING,
    `ctor_legal_middle_name` STRING,
    `ctor_bcts_client_number` STRING,
    `ctor_bcts_ocg_client_number` STRING,
    `ctor_bcts_prime_mailing_locn` STRING,
    `ctor_bcts_total_balance` STRING,
    `ctor_birthdate` TIMESTAMP,
    `ctor_drivers_license` STRING,
    `ctor_sin` STRING,
    `ctor_client_other_id` STRING,
    `ctor_bcts_corp_regn_nmbr` STRING,
    `ctor_bcts_corp_reporting_ind` STRING,
    `ctor_risky_contractor` STRING,
    `ctor_rate_benefit_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.lrm_vt_commit_lic_type (
    `code` STRING,
    `description` STRING,
    `activeflag` DECIMAL(1,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.cut_block_silv_regime (
    `cbsr_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `treg_seq_nbr` DECIMAL(15,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_res_vt_fdtm_team (
    `colu_lookup_type` STRING,
    `colu_lookup_id` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `colu_lookup_desc` STRING,
    `colu_user_defined_ind` STRING,
    `colu_display_ind` STRING,
    `colu_display_order` DECIMAL(10,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.commitment_partition (
    `copa_seq_nbr` DECIMAL(15,0),
    `copa_partition` STRING,
    `copa_percent` DECIMAL(7,4),
    `mark_seq_nbr` DECIMAL(38,10),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `cutb_seq_nbr` DECIMAL(15,0),
    `copa_block_id` STRING,
    `copa_cruise_m3_vol` DECIMAL(9,0),
    `copa_partition_type` STRING,
    `copa_commit_m3_vol` DECIMAL(9,0),
    `copa_commit_part_percent` DECIMAL(3,0),
    `commit_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.block_seed_zone (
    `blsz_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `blsz_class_id` STRING,
    `blsz_species_id` STRING,
    `blsz_zone_id` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `silp_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.apportionment (
    `appo_seq_nbr` DECIMAL(15,0),
    `manu_seq_nbr` DECIMAL(15,0),
    `appo_tenure_type` STRING,
    `appo_date` TIMESTAMP,
    `appo_m3_vol` DECIMAL(15,6),
    `pers_seq_nbr` DECIMAL(15,0),
    `corp_user_id` STRING,
    `appo_comments` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `fiyr` DECIMAL(4,0),
    `dispo_agreement` STRING,
    `appo_end_date` TIMESTAMP,
    `dispo_recv_lic_nmb` STRING,
    `appo_m3_vol_prorated` DECIMAL(15,6),
    `non_bcts_tenure_holder` STRING,
    `fn_code` STRING,
    `resources_data_ind` DECIMAL(1,0),
    `formula` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_lrm_licence_shape (
    `objectid` DECIMAL(38,0),
    `manu_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(16,0),
    `shape` STRING,
    `sde_state_id` DECIMAL(38,10),
    `v_treefield` STRING,
    `shape_area` DECIMAL(38,8),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.block_admin_zone (
    `blaz_admin_zone_id` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `blaz_admin_zone_desc` STRING,
    `blaz_active_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_lrm_cut_block (
    `manu_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `cutb_block_id` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `cutb_gross_ha_area` DECIMAL(11,6),
    `c_cutb_latitude` STRING,
    `c_cutb_longitude` STRING,
    `cutb_opening` STRING,
    `cutb_photos` STRING,
    `sblk_supply_block_id` STRING,
    `opar_operating_area_id` STRING,
    `finz_forest_inventory_zone_id` STRING,
    `cutb_cell_number` STRING,
    `cutb_user_mapsheet_id` STRING,
    `cutb_greenup_date` TIMESTAMP,
    `grns_greenup_source` STRING,
    `pmpo_operating_zone` STRING,
    `lsun_landscape_unit` STRING,
    `cutb_prov_forest_conflict` STRING,
    `cutb_digi_ind` STRING,
    `cutb_block_number` STRING,
    `cutb_opening_id` DECIMAL(10,0),
    `min_elevation` DECIMAL(38,10),
    `max_elevation` DECIMAL(38,10),
    `cutb_latitude` STRING,
    `cutb_longitude` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `cutb_cpi_slope_pct` DECIMAL(7,4),
    `cutb_system_id` STRING,
    `cutb_block_state` STRING,
    `cutb_file_id` STRING,
    `cutb_row_ind` STRING,
    `suop_subop_area_id` STRING,
    `licn_licence_id` STRING,
    `generate_system_id` STRING,
    `cutb_location` STRING,
    `cutb_latitude_dd` DECIMAL(9,6),
    `cutb_longitude_dd` DECIMAL(9,6),
    `no_shape` DECIMAL(38,10),
    `cutb_archive_reason` STRING,
    `cutb_archive_date` TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

````

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### LRM FORESTVIEW VIEWS

# CELL ********************

# spark.sql("CREATE SCHEMA IF NOT EXISTS lrm_replication")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_scaling_history (
    `timber_mark` STRING,
    `scale_site` STRING,
    `scaling_period` TIMESTAMP,
    `scale_species_code` STRING,
    `scale_product_code` STRING,
    `scale_grade_code` STRING,
    `billing_type_code` STRING,
    `volume_scaled` DECIMAL(10,3),
    `total_amount` DECIMAL(15,2),
    `royalty_amount` DECIMAL(11,2),
    `reserve_stmpg_amt` DECIMAL(11,2),
    `bonus_stumpage_amt` DECIMAL(11,2),
    `silv_levy_amount` DECIMAL(11,2),
    `dev_levy_amount` DECIMAL(11,2),
    `entry_timestamp` TIMESTAMP,
    `entry_userid` STRING,
    `update_timestamp` TIMESTAMP,
    `update_userid` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_cp (
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `tenure` STRING,
    `licence_id` STRING,
    `permit_id` STRING,
    `perm_application_description` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.sv_sales_schedule (
    `divi_div_nbr` DECIMAL(2,0),
    `divi_short_code` STRING,
    `ozon_operating_zone_id` STRING,
    `field_team` STRING,
    `manu_id` STRING,
    `opar_operating_area_name` STRING,
    `licence_id` STRING,
    `licn_category_id` STRING,
    `linc_cert_level_id` STRING,
    `term` DECIMAL(38,10),
    `tenure_type` STRING,
    `licn_licence_state` STRING,
    `registrant` STRING,
    `perm_permit_id` STRING,
    `geographic_location` STRING,
    `mark_mark_id` STRING,
    `block_id` STRING,
    `block_full_name` STRING,
    `cutb_system_id` STRING,
    `cutb_block_state` STRING,
    `cutb_longitude` STRING,
    `cutb_latitude` STRING,
    `sttp_stand_type` STRING,
    `cutb_vol_data_source` STRING,
    `blal_merch_ha_area` DECIMAL(11,6),
    `blal_harvested_ha_area` DECIMAL(11,6),
    `blal_cruise_m3_vol` DECIMAL(15,6),
    `rw_vol` DECIMAL(15,6),
    `block_volume` DECIMAL(38,10),
    `blal_harvested_m3_vol` DECIMAL(15,6),
    `auction_status` STRING,
    `auction_date` TIMESTAMP,
    `auction_fiscal` DECIMAL(38,10),
    `auction_quarter` STRING,
    `auc_quart_month` STRING,
    `current_period_flag` STRING,
    `current_period_start` TIMESTAMP,
    `current_period_end` TIMESTAMP,
    `silviculture_system` STRING,
    `yarding_system` STRING,
    `harvest_considerations` STRING,
    `piece_size` DECIMAL(38,10),
    `species_composition` STRING,
    `hi_status` STRING,
    `hi_date` TIMESTAMP,
    `expire_status` STRING,
    `expire_date` TIMESTAMP,
    `rc_status` STRING,
    `rc_date` STRING,
    `dr_status` TIMESTAMP,
    `dr_date` TIMESTAMP,
    `dvs_status` STRING,
    `dvs_date` TIMESTAMP,
    `layout_status` STRING,
    `layout_date` TIMESTAMP,
    `dvc_status` STRING,
    `dvc_date` TIMESTAMP,
    `write_off_status` STRING,
    `write_off_date` TIMESTAMP,
    `licn_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(16,0),
    `objectid` DECIMAL(38,0),
    `shape` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_licence_activity_all (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `field_team_desc` STRING,
    `nav_name` STRING,
    `tenure` STRING,
    `licence_id` STRING,
    `licn_licence_state` STRING,
    `activity_class` STRING,
    `activity_type` STRING,
    `actt_key_ind` STRING,
    `accl_object_type` STRING,
    `acti_responsibility` STRING,
    `acti_status_ind` STRING,
    `acti_target_date` TIMESTAMP,
    `acti_target_cost` DECIMAL(15,2),
    `activity_date` TIMESTAMP,
    `acti_status_date` TIMESTAMP,
    `acti_cost` DECIMAL(15,2),
    `acti_comments` STRING,
    `licensee` STRING,
    `acti_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `createdby` STRING,
    `createdon` TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_activity_cost (
    `actc_seq_nbr` DECIMAL(15,0),
    `acti_seq_nbr` DECIMAL(15,0),
    `actc_item_cost` DECIMAL(10,4),
    `csti_cost_item_id` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `csti_cost_item_description` STRING,
    `csti_cost_item_account_code` STRING,
    `actc_series` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_apportionment (
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `management_type` STRING,
    `appo_date` TIMESTAMP,
    `apportionment_tenure` STRING,
    `appo_m3_vol` DECIMAL(15,6),
    `authorized_by` STRING,
    `entered_by` STRING,
    `apportionment_partition` STRING,
    `appp_partition_m3_vol` DECIMAL(15,6),
    `commitment_m3_vol` DECIMAL(38,10),
    `actual_m3_volume` DECIMAL(38,10),
    `variance` DECIMAL(38,10),
    `manu_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `appo_seq_nbr` DECIMAL(15,0),
    `fiscal_year` DECIMAL(38,10)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_appr_bridge_tab_cost (
    `divi_div_nbr` DECIMAL(2,0),
    `btab_road_name` STRING,
    `btab_crib_height_nbr` DECIMAL(4,1),
    `btab_span_length_nbr` DECIMAL(2,0),
    `optn_least_cost_option_code` STRING,
    `btab_identifier_desc` STRING,
    `btab_applicable_vol` DECIMAL(9,0),
    `btab_station_nbr` DECIMAL(38,10),
    `btab_crown_portion_pct` DECIMAL(7,4),
    `btab_appraisal_year` DECIMAL(4,0),
    `btab_comment` STRING,
    `btab_section_built_ind` STRING,
    `btab_start_metre_nbr` DECIMAL(13,4),
    `btab_end_metre_nbr` DECIMAL(13,4),
    `btab_bridge_type` STRING,
    `btab_inspection_date` TIMESTAMP,
    `btab_bridge_status` STRING,
    `proj_seq_nbr` DECIMAL(15,0),
    `prjv_version_nbr` DECIMAL(38,10),
    `pver_version_nbr` DECIMAL(3,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `btab_seq_nbr` DECIMAL(15,0),
    `btab_seq_nbr_lng` DECIMAL(10,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_appr_culvert_tab_cost (
    `divi_div_nbr` DECIMAL(2,0),
    `ctab_road_name` STRING,
    `ctab_station_nbr` DECIMAL(13,4),
    `ctab_end_station_nbr` DECIMAL(13,4),
    `rctp_construction_type_id` STRING,
    `ctab_identifier_desc` STRING,
    `optn_least_cost_option_code` STRING,
    `ctab_wooden_ind` STRING,
    `ctab_culvert_diameter_nbr` DECIMAL(5,0),
    `ctab_culvert_cnt` DECIMAL(2,0),
    `ctab_length_nbr` DECIMAL(3,0),
    `ctab_applicable_vol` DECIMAL(9,0),
    `ctab_crown_portion_pct` DECIMAL(7,4),
    `ctab_appraisal_year` DECIMAL(4,0),
    `ctab_comment` STRING,
    `culs_status_id` STRING,
    `proj_seq_nbr` DECIMAL(15,0),
    `prjv_version_nbr` DECIMAL(38,10),
    `pver_version_nbr` DECIMAL(3,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `ctab_seq_nbr` DECIMAL(15,0),
    `road_seq_nbr` DECIMAL(38,10),
    `ctab_seq_nbr_lng` DECIMAL(10,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_appr_internal_rate (
    `divi_div_nbr` DECIMAL(2,0),
    `intr_start_date` TIMESTAMP,
    `intr_end_date` TIMESTAMP,
    `intr_lumber_sp` DECIMAL(8,2),
    `intr_chip_sp` DECIMAL(8,2),
    `intr_stud_log_pct` DECIMAL(7,4),
    `intr_selling_price` DECIMAL(8,2),
    `intr_logging_cost` DECIMAL(8,2),
    `intr_manu_cost` DECIMAL(8,2),
    `intr_silv_cost` DECIMAL(8,2),
    `intr_operating_cost` DECIMAL(8,2),
    `intr_base_rate` DECIMAL(8,2),
    `intr_value_index` DECIMAL(8,2),
    `intr_mean_value_index` DECIMAL(8,2),
    `intr_indicated_rate` DECIMAL(8,2),
    `intr_stumpage_rate` DECIMAL(8,2),
    `mand_eff_date` TIMESTAMP,
    `adjd_eff_date` TIMESTAMP,
    `perm_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_appr_ministry_rate (
    `divi_div_nbr` DECIMAL(2,0),
    `minr_start_date` TIMESTAMP,
    `minr_end_date` TIMESTAMP,
    `minr_lumber_sp` DECIMAL(8,2),
    `minr_chip_sp` DECIMAL(8,2),
    `minr_stud_log_pct` DECIMAL(7,4),
    `minr_selling_price` DECIMAL(8,2),
    `minr_logging_cost` DECIMAL(8,2),
    `minr_manu_cost` DECIMAL(8,2),
    `minr_silv_cost` DECIMAL(8,2),
    `minr_operating_cost` DECIMAL(8,2),
    `minr_base_rate` DECIMAL(8,2),
    `minr_value_index` DECIMAL(8,2),
    `minr_mean_value_index` DECIMAL(8,2),
    `minr_indicated_rate` DECIMAL(8,2),
    `minr_stumpage_rate` DECIMAL(8,2),
    `mand_eff_date` TIMESTAMP,
    `adjd_eff_date` TIMESTAMP,
    `perm_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_appr_road_tab_cost (
    `divi_div_nbr` DECIMAL(2,0),
    `road_road_name` STRING,
    `rtab_road_name` STRING,
    `rtab_road_type` STRING,
    `rtab_start_metre_nbr` DECIMAL(13,4),
    `rtab_end_metre_nbr` DECIMAL(13,4),
    `rtab_length_nbr` DECIMAL(9,3),
    `rtab_soil_moisture_type` STRING,
    `rtab_rock_pct` DECIMAL(7,4),
    `rtab_biogeo_zone` STRING,
    `rtab_side_slope_pct` DECIMAL(7,4),
    `rtab_haul_distance_nbr` DECIMAL(7,1),
    `rtab_stabilizing_material_type` STRING,
    `pver_version_nbr` DECIMAL(3,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `rtab_seq_nbr` DECIMAL(15,0),
    `road_seq_nbr` DECIMAL(38,10),
    `rtab_seq_nbr_lng` DECIMAL(10,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_attachment (
    `bsto_table_name` STRING,
    `seq_nbr_type` STRING,
    `seq_nbr_value` DECIMAL(38,10),
    `bsto_file_name` STRING,
    `bsto_file_description` STRING,
    `bsto_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_block_area (
    `cutb_seq_nbr` DECIMAL(15,0),
    `total_area` DECIMAL(38,10),
    `gross_area` DECIMAL(38,10),
    `intres_area` DECIMAL(38,10),
    `extres_area` DECIMAL(38,10),
    `resrv_area` DECIMAL(38,10),
    `npnat_area` DECIMAL(38,10),
    `npunn_area` DECIMAL(38,10),
    `nar_area` DECIMAL(38,10)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_block_cruise (
    `blkc_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `blkc_system_nbr` DECIMAL(15,0),
    `blkc_plot_nbr` DECIMAL(15,0),
    `cruise_source` STRING,
    `species_id` STRING,
    `species` STRING,
    `gross_merch_m3` DECIMAL(15,7),
    `net_merch_m3` DECIMAL(15,7),
    `net_merch_per_ha` DECIMAL(15,7),
    `gross_vol_per_tree` DECIMAL(15,7),
    `net_vol_per_tree` DECIMAL(15,7),
    `decay` DECIMAL(15,7),
    `waste` DECIMAL(15,7),
    `breakage` DECIMAL(15,7),
    `gross_dwb` DECIMAL(15,7),
    `stems` DECIMAL(15,7),
    `avg_dbh` DECIMAL(15,7),
    `snags` DECIMAL(15,7),
    `distribution` DECIMAL(15,7),
    `dead_potential` DECIMAL(15,7),
    `avg_weight_merch_ht` DECIMAL(15,7),
    `basal_area` DECIMAL(15,7),
    `sampling_error` DECIMAL(15,7),
    `old_growth` DECIMAL(15,7),
    `second_growth` DECIMAL(15,7),
    `blowdown` DECIMAL(15,7),
    `blkc_area_ha` DECIMAL(15,7),
    `blkc_insect_damage_green_pct` DECIMAL(38,10),
    `blkc_insect_damage_red_pct` DECIMAL(38,10),
    `blkc_insect_damage_grey_pct` DECIMAL(38,10),
    `blkc_stud_pct` DECIMAL(15,7),
    `blkc_small_log_pct` DECIMAL(15,7),
    `blkc_large_log_pct` DECIMAL(15,7),
    `blkc_lrf_bdft_per_m3` DECIMAL(15,7),
    `blkc_m3_per_lineal_m` DECIMAL(15,7),
    `blkc_gross_m3_per_ha` DECIMAL(15,7),
    `blkc_pulp_m3_per_ha` DECIMAL(15,7),
    `blkc_sawlog_m3_per_ha` DECIMAL(15,7),
    `blkc_sawlog_m3_per_tree` DECIMAL(15,7),
    `blkc_pulp_net_m3_per_tree` DECIMAL(15,7),
    `blkc_burn_damage_pct` DECIMAL(38,10),
    `specie_pct` DECIMAL(38,10),
    `finz_forest_inventory_zone_id` STRING,
    `divi_div_nbr` DECIMAL(2,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_block_depletion_stage (
    `dpst_seq_nbr` DECIMAL(15,0),
    `acti_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `dpst_start_date` TIMESTAMP,
    `dpst_comp_date` TIMESTAMP,
    `dpst_area` DECIMAL(11,6),
    `dpst_depletion_vol` DECIMAL(15,6),
    `dpst_digi_ind` STRING,
    `dpst_phase_id` STRING,
    `dpst_depletion_vol_decid` DECIMAL(15,6),
    `dpst_depletion_vol_conif` DECIMAL(15,6)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_block_harvest_method (
    `cbhm_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `hame_method_id` STRING,
    `cbhm_area` DECIMAL(15,6),
    `cbhm_harvest_vol` DECIMAL(15,6),
    `cbhm_digi_ind` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_block_harvest_strategy (
    `cbst_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `stgy_strategy_id` STRING,
    `cbst_area` DECIMAL(15,6),
    `cbst_harvest_vol` DECIMAL(15,6),
    `cbst_digi_ind` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_block_nmar (
    `nmar_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `nmcl_class_id` STRING,
    `nmtp_type_id` STRING,
    `nmar_label` STRING,
    `divi_div_nbr` DECIMAL(2,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_block_old (
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `tenure` STRING,
    `licence_id` STRING,
    `permit_id` STRING,
    `mark_id` STRING,
    `block_id` STRING,
    `block_nbr` STRING,
    `ubi` STRING,
    `opening` STRING,
    `op_area` STRING,
    `supply_block` STRING,
    `photo` STRING,
    `latitude` STRING,
    `longitude` STRING,
    `prov_frst_conflict` STRING,
    `mapsheet_id` STRING,
    `landscape_unit` STRING,
    `sp_exempt` STRING,
    `stand_type` STRING,
    `age_class` STRING,
    `hgt_class` STRING,
    `stk_class` STRING,
    `site_index` DECIMAL(5,0),
    `source` STRING,
    `fdp_status` STRING,
    `funding_code` STRING,
    `cutb_block_memo` STRING,
    `gross_area` DECIMAL(11,6),
    `est_area` DECIMAL(7,2),
    `merch_area` DECIMAL(11,6),
    `harvest_area` DECIMAL(11,6),
    `remaining_area` DECIMAL(38,10),
    `cruise_vol` DECIMAL(15,6),
    `harvest_vol` DECIMAL(15,6),
    `remaining_vol` DECIMAL(38,10),
    `cutb_forma_print_date` TIMESTAMP,
    `cutb_forma_printed` STRING,
    `cutb_block_state` STRING,
    `pmod_modifier_id` STRING,
    `harvest_started` TIMESTAMP,
    `harvest_completed` TIMESTAMP,
    `cutb_location` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_block_tasks_issue (
    `tso_code` STRING,
    `nav_name` STRING,
    `licence_id` STRING,
    `permit_id` STRING,
    `mark_id` STRING,
    `block_id` STRING,
    `issue` STRING,
    `task_description` STRING,
    `responsibility` STRING,
    `target_date` TIMESTAMP,
    `completion_date` TIMESTAMP,
    `provincial_env_comments` STRING,
    `government_public_comments` STRING,
    `licencee_comments` STRING,
    `forester_comments` STRING,
    `recommended_requirements` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `tso_name` STRING,
    `isst_issue_id` STRING,
    `actp_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `field_team_desc` STRING,
    `ubi` STRING,
    `licn_licence_state` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_block_timber (
    `cutb_seq_nbr` DECIMAL(15,0),
    `timber_type` STRING,
    `timber_order` DECIMAL(4,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_coastal_cost_survey_bh (
    `bhct_id` STRING,
    `csud_soft_med_rock_cost_per_m` DECIMAL(15,2),
    `csud_hard_rock_cost_per_m` DECIMAL(15,2),
    `csud_soft_med_rock_const_m` DECIMAL(13,4),
    `csud_hard_rock_const_m` DECIMAL(13,4),
    `csud_ballast_haul_km` DECIMAL(13,4),
    `csur_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_coastal_cost_survey_roads (
    `tso_code` STRING,
    `construction_fiscal` DECIMAL(4,0),
    `manu_id` STRING,
    `manu_type_id` STRING,
    `manu_name` STRING,
    `road_name` STRING,
    `uri` STRING,
    `rcom_start_metre_nbr` DECIMAL(13,4),
    `rcom_end_metre_nbr` DECIMAL(13,4),
    `rcom_planned_date` TIMESTAMP,
    `rcom_completion_date` TIMESTAMP,
    `csur_start_metre_nbr` DECIMAL(13,4),
    `csur_end_metre_nbr` DECIMAL(13,4),
    `csur_report_date` TIMESTAMP,
    `csur_project_id` STRING,
    `csur_bid_type` STRING,
    `porg_point_of_origin_id` STRING,
    `csur_operation_specifics` STRING,
    `prepared_by` STRING,
    `csur_comment` STRING,
    `csur_seq_nbr` DECIMAL(15,0),
    `road_seq_nbr` DECIMAL(16,0),
    `csur_seq_nbr_lng` DECIMAL(10,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_commitments (
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `licence_id` STRING,
    `permit_id` STRING,
    `mark_id` STRING,
    `apportionment_type` STRING,
    `partition_type` STRING,
    `copa_percent` DECIMAL(7,4),
    `commitment_partition_area` DECIMAL(38,10),
    `pred_committed_volume` DECIMAL(38,10),
    `actual_partition_volume` DECIMAL(38,10),
    `licence_issued_date` TIMESTAMP,
    `fiscal_year` DECIMAL(38,10),
    `divi_div_nbr` DECIMAL(2,0),
    `manu_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `copa_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_commit_tracking_sale_info (
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `licence_id` STRING,
    `mark_id` STRING,
    `ctor_name` STRING,
    `licn_licence_desc` STRING,
    `apportionment_type` STRING,
    `partition_type` STRING,
    `fiscal_year` DECIMAL(38,10),
    `licence_issued_date` TIMESTAMP,
    `licence_state` STRING,
    `appraisal_vol` DECIMAL(38,10),
    `billed_volume` DECIMAL(38,10),
    `outstanding_volume` DECIMAL(38,10),
    `cut_cruise_ratio` DECIMAL(38,10),
    `licn_seq_nbr` DECIMAL(15,0),
    `manu_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `actual_commit_vol` DECIMAL(38,10)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_cost_survey_bridges (
    `bridge_station` DECIMAL(13,4),
    `bridge_name` STRING,
    `footings` STRING,
    `constcategory` STRING,
    `cribheight` STRING,
    `decklength` STRING,
    `deckmaterial` STRING,
    `deckwidth` STRING,
    `footingsmaterial` STRING,
    `loadcapacity` STRING,
    `loadrating` STRING,
    `maxdptpiledrn` STRING,
    `spanmaterial` STRING,
    `csus_comment` STRING,
    `csus_bridge_cost` DECIMAL(15,2),
    `csus_bridge_approach_work_cost` DECIMAL(15,2),
    `csus_bridge_barging_cost` DECIMAL(15,2),
    `total_cost` DECIMAL(38,10),
    `csur_last_update_mode` STRING,
    `csur_seq_nbr` DECIMAL(15,0),
    `csur_seq_nbr_lng` DECIMAL(10,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_detailed_site_assessment (
    `cutb_seq_nbr` DECIMAL(15,0),
    `sila_seq_nbr` DECIMAL(15,0),
    `dsas_chemical` STRING,
    `dsas_active` DECIMAL(9,6),
    `dsas_apply_rate` DECIMAL(13,4),
    `pmpl_pmp_nbr` STRING,
    `dsas_pfz_width` DECIMAL(7,2),
    `dsas_stratum_name` STRING,
    `potr_purpose_of_treatment` STRING,
    `dsas_buffer_size` DECIMAL(3,0),
    `dsas_approval_ind` STRING,
    `dsas_referal_ind` STRING,
    `dsas_water_bodies` STRING,
    `dsas_fish_habitat` STRING,
    `dsas_community_watershed` STRING,
    `dsas_wildlife_habitat` STRING,
    `dsa_stock_species` STRING,
    `dsa_site_series` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ecology (
    `cutb_seq_nbr` DECIMAL(15,0),
    `ecou_seq_nbr` DECIMAL(15,0),
    `eco_unit` STRING,
    `gross_area` DECIMAL(11,6),
    `site_series_complex` STRING,
    `bioz_zone_id` STRING,
    `bios_subzone_id` STRING,
    `biov_variant_id` STRING,
    `site_series_percent` STRING,
    `eco_prod_area` DECIMAL(38,10),
    `reserve_area` DECIMAL(38,10),
    `npnat` DECIMAL(38,10),
    `npunn` DECIMAL(38,10),
    `ncbr` DECIMAL(38,10),
    `divi_div_nbr` DECIMAL(2,0),
    `ecou_elevation_min` DECIMAL(5,0),
    `ecou_elevation_max` DECIMAL(5,0),
    `ecou_elevation_avg` DECIMAL(5,0),
    `ecou_aspect` STRING,
    `ecou_slope_min` DECIMAL(7,4),
    `ecou_slope_max` DECIMAL(7,4),
    `ecou_slope_avg` DECIMAL(7,4),
    `ecou_slope_position` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ecology_site_series (
    `ecou_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `ecou_name` STRING,
    `bioz_zone_id` STRING,
    `bios_subzone_id` STRING,
    `biov_variant_id` STRING,
    `bgrg_region_code` STRING,
    `biss_site_series_id` STRING,
    `euss_percent_cover` DECIMAL(7,4)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ems_action_plan (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `field_team` STRING,
    `licence_id` STRING,
    `entered_date` TIMESTAMP,
    `completion_date` TIMESTAMP,
    `target_completion_date` TIMESTAMP,
    `action_plan_id` STRING,
    `action_status` STRING,
    `assigned_to` STRING,
    `inspection_type` STRING,
    `inspection_sub_type` STRING,
    `project_id` STRING,
    `project_name` STRING,
    `emsa_seq_nbr` DECIMAL(15,0),
    `emsi_seq_nbr` DECIMAL(15,0),
    `eins_seq_nbr` DECIMAL(15,0),
    `emsp_seq_nbr` DECIMAL(38,10),
    `licn_seq_nbr` DECIMAL(15,0),
    `lems_seq_nbr` DECIMAL(38,10)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ems_action_plan_descr (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `licence_id` STRING,
    `item_nbr` DECIMAL(3,0),
    `responsibility` STRING,
    `cause_description` STRING,
    `action_status` STRING,
    `description` STRING,
    `action` STRING,
    `target_date` TIMESTAMP,
    `completion_date` TIMESTAMP,
    `emsa_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ems_contract (
    `divi_div_nbr` DECIMAL(15,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `contract_id` STRING,
    `ctor_name` STRING,
    `contract_coordinator` STRING,
    `contract_location` STRING,
    `start_date` TIMESTAMP,
    `start_status` STRING,
    `end_date` TIMESTAMP,
    `end_status` STRING,
    `viewing_date` TIMESTAMP,
    `internal_ind` STRING,
    `emsc_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ems_inspection (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `field_team` STRING,
    `class` STRING,
    `type` STRING,
    `sub_type` STRING,
    `inspection_date` TIMESTAMP,
    `projectid_licence` STRING,
    `project_name` STRING,
    `road_tenure` STRING,
    `log_permit_id` STRING,
    `agent` STRING,
    `contractor` STRING,
    `inspection_status` STRING,
    `supervisor` STRING,
    `geo_location` STRING,
    `data_source` STRING,
    `functional_area` STRING,
    `lems_epea_status` STRING,
    `contract_location` STRING,
    `emsp_seq_nbr` DECIMAL(38,10),
    `licn_seq_nbr` DECIMAL(38,10),
    `eins_seq_nbr` DECIMAL(15,0),
    `epea_seq_nbr` DECIMAL(38,10),
    `lems_seq_nbr` DECIMAL(38,10),
    `ctor_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ems_inspection_frequency (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `field_team` STRING,
    `projectid_licence` STRING,
    `road_permit` STRING,
    `log_dump_permit` STRING,
    `inspection_date` TIMESTAMP,
    `status` STRING,
    `inspector` STRING,
    `eifr_type` STRING,
    `eins_number` DECIMAL(3,0),
    `frequency` STRING,
    `data_source` STRING,
    `inspection_type` STRING,
    `functional_area` STRING,
    `project_name` STRING,
    `contract_location` STRING,
    `contractor` STRING,
    `licn_seq_nbr` DECIMAL(15,0),
    `emsc_seq_nbr` DECIMAL(15,0),
    `lems_seq_nbr` DECIMAL(10,0),
    `epea_seq_nbr` DECIMAL(15,0),
    `emsp_seq_nbr` DECIMAL(15,0),
    `eins_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ems_inspection_test_drill (
    `divi_div_nbr` DECIMAL(38,10),
    `tso_code` STRING,
    `tso_name` STRING,
    `field_team` STRING,
    `projectid_licence` STRING,
    `road_permit` STRING,
    `log_dump_permit` STRING,
    `inspection_date` TIMESTAMP,
    `status` STRING,
    `test_drill` STRING,
    `eins_type` STRING,
    `due_date` TIMESTAMP,
    `completion_date` TIMESTAMP,
    `data_source` STRING,
    `inspection_type` STRING,
    `functional_area` STRING,
    `project_name` STRING,
    `geo_location` STRING,
    `contractor` STRING,
    `licn_seq_nbr` DECIMAL(15,0),
    `emsc_seq_nbr` DECIMAL(15,0),
    `lems_seq_nbr` DECIMAL(10,0),
    `epea_seq_nbr` DECIMAL(15,0),
    `emsp_seq_nbr` DECIMAL(15,0),
    `eins_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ems_issue (
    `divi_div_nbr` DECIMAL(2,0),
    `tso` STRING,
    `business_area` STRING,
    `field_team` STRING,
    `issue_project_name` STRING,
    `issue_id` STRING,
    `entered_by` STRING,
    `entered_date` TIMESTAMP,
    `issue_source` STRING,
    `issue_type` STRING,
    `sub_type` STRING,
    `issue_event` STRING,
    `issue_supervisor` STRING,
    `issue_investigator` STRING,
    `issue_client` STRING,
    `rdpm_permit_id` STRING,
    `issue_status` STRING,
    `occurrence_date` TIMESTAMP,
    `reported_date` TIMESTAMP,
    `licn_licence_id` STRING,
    `funct_area` STRING,
    `funct_area_activity` STRING,
    `issue_description` STRING,
    `general_location` STRING,
    `detailed_description` STRING,
    `root_cause` STRING,
    `environmental_impact` STRING,
    `licn_seq_nbr` DECIMAL(15,0),
    `sdom_seq_nbr` DECIMAL(15,0),
    `emsi_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ems_issue_government (
    `govt_agency` STRING,
    `govt_date` TIMESTAMP,
    `govt_details` STRING,
    `govt_contact` STRING,
    `govt_determiniation` STRING,
    `emsi_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ems_issue_investigation (
    `investigation_date` TIMESTAMP,
    `investigation_details` STRING,
    `emsi_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ems_plan_vs_complete_insp (
    `divi_div_nbr` DECIMAL(38,10),
    `tso` STRING,
    `division` STRING,
    `project_licence_id` STRING,
    `licence_state` STRING,
    `ems_class` STRING,
    `ems_type` STRING,
    `ems_subtype` STRING,
    `contractor` STRING,
    `project_risk_ranking` STRING,
    `ems_risk_rating` STRING,
    `inspector` STRING,
    `inspection_type` STRING,
    `inspection_number` DECIMAL(3,0),
    `frequency` STRING,
    `calcemsinsp` DECIMAL(38,10),
    `compemsinsp` DECIMAL(38,10),
    `percemsinspcomp` DECIMAL(38,10),
    `data_source` STRING,
    `licn_inspection_type` STRING,
    `inspection_date` TIMESTAMP,
    `inspection_status` STRING,
    `functional_area` STRING,
    `field_team_id` STRING,
    `emsp_project_name` STRING,
    `emsc_ctor_location` STRING,
    `eifr_seq_nbr` DECIMAL(15,0),
    `eins_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(38,10),
    `emsc_seq_nbr` DECIMAL(38,10),
    `manu_seq_nbr` DECIMAL(38,10),
    `lems_seq_nbr` DECIMAL(38,10),
    `epea_seq_nbr` DECIMAL(38,10),
    `ctor_seq_nbr` DECIMAL(15,0),
    `emsp_seq_nbr` DECIMAL(38,10)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ems_plan_vs_compl_prework (
    `divi_div_nbr` DECIMAL(38,10),
    `tso` STRING,
    `business_area` STRING,
    `field_team` STRING,
    `project_licence_id` STRING,
    `licence_state` STRING,
    `licence_activity` STRING,
    `planned_date` TIMESTAMP,
    `planned_status` STRING,
    `done_date` TIMESTAMP,
    `done_status` STRING,
    `prework_present` STRING,
    `ems_class` STRING,
    `ems_type` STRING,
    `ems_subtype` STRING,
    `prework_date` TIMESTAMP,
    `prework_status` STRING,
    `data_source` STRING,
    `inspection_type` STRING,
    `inspection_date` TIMESTAMP,
    `inspection_status` STRING,
    `functional_area` STRING,
    `project_name` STRING,
    `project_location` STRING,
    `contractor` STRING,
    `licn_seq_nbr` DECIMAL(38,10),
    `emsc_seq_nbr` DECIMAL(38,10),
    `manu_seq_nbr` DECIMAL(38,10),
    `lems_seq_nbr` DECIMAL(38,10),
    `epea_seq_nbr` DECIMAL(38,10),
    `ctor_seq_nbr` DECIMAL(15,0),
    `emsp_seq_nbr` DECIMAL(38,10)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_ems_requirement (
    `divi_div_nbr` DECIMAL(38,10),
    `tso` STRING,
    `business_area` STRING,
    `field_team` STRING,
    `licence_project_id` STRING,
    `road_tenure` STRING,
    `log_dump_permit` STRING,
    `licence_inspection_date` TIMESTAMP,
    `licence_inspection_status` STRING,
    `requirement_type` STRING,
    `requirement_desc` STRING,
    `requirement_status` STRING,
    `data_source` STRING,
    `project_inspection_type` STRING,
    `project_inspection_date` TIMESTAMP,
    `project_inspection_status` STRING,
    `functional_area` STRING,
    `project_name` STRING,
    `project_location` STRING,
    `contractor` STRING,
    `eins_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `emsc_seq_nbr` DECIMAL(15,0),
    `lems_seq_nbr` DECIMAL(10,0),
    `epea_seq_nbr` DECIMAL(15,0),
    `emsp_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_fdu (
    `tso_code` STRING,
    `fsfd_name` STRING,
    `fspm_seq_nbr` DECIMAL(15,0),
    `fsfd_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_forest_comment (
    `seq_nbr_type` STRING,
    `seq_nbr_value` DECIMAL(15,0),
    `comm_comment_date` TIMESTAMP,
    `comm_comment` STRING,
    `comm_agency` STRING,
    `comm_agent` STRING,
    `comm_response_date` TIMESTAMP,
    `comm_respondant` STRING,
    `comm_response` STRING,
    `comm_action_required` STRING,
    `comm_action_item` STRING,
    `comm_comment_type` STRING,
    `comm_fdp_id` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_frpa_results_strategies (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `parent_plan_name` STRING,
    `parent_effective_date` TIMESTAMP,
    `parent_expiry_date` TIMESTAMP,
    `parent_plan_status` STRING,
    `plan_type` STRING,
    `plan_name` STRING,
    `plan_description` STRING,
    `plan_status` STRING,
    `plan_effective_date` TIMESTAMP,
    `plan_expiry_date` TIMESTAMP,
    `plan_approval_date` TIMESTAMP,
    `plan_comment` STRING,
    `fspm_submission_date` TIMESTAMP,
    `fspm_ts_num` STRING,
    `fdu_name` STRING,
    `fdu_area` DECIMAL(14,2),
    `fdu_selected` DECIMAL(1,0),
    `fdu_comments` STRING,
    `fdu_block_area` DECIMAL(14,2),
    `fdu_road_length` DECIMAL(14,2),
    `documentkey` DECIMAL(9,0),
    `sde_state_id` DECIMAL(38,10),
    `licence_id` STRING,
    `permit` STRING,
    `block_id` STRING,
    `ubi` STRING,
    `block_area` DECIMAL(11,6),
    `block_plan_status` STRING,
    `landscape_unit` STRING,
    `mapsheet` STRING,
    `fsob_id` STRING,
    `fsrs_id` STRING,
    `applies` STRING,
    `fsrs_description` STRING,
    `fsrs_comment` STRING,
    `actt_seq_nbr` DECIMAL(38,10),
    `asses_desc` STRING,
    `fsrl_comment` STRING,
    `fsrc_comment_rs` STRING,
    `cutb_seq_nbr` DECIMAL(15,0),
    `fdu_seq_nbr` DECIMAL(15,0),
    `plan_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_gis_actd (
    `divi_div_nbr` DECIMAL(2,0),
    `manu_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `blal_merch_ha_area` DECIMAL(11,6),
    `blal_harvested_ha_area` DECIMAL(11,6),
    `blal_cruise_m3_vol` DECIMAL(15,6),
    `blal_rw_vol` DECIMAL(15,6),
    `blal_harvested_m3_vol` DECIMAL(15,6),
    `auction_date_compare` TIMESTAMP,
    `auction_date` TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_gis_actd_dates (
    `divi_div_nbr` DECIMAL(2,0),
    `manu_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `blal_merch_ha_area` DECIMAL(11,6),
    `blal_harvested_ha_area` DECIMAL(11,6),
    `blal_cruise_m3_vol` DECIMAL(15,6),
    `blal_rw_vol` DECIMAL(15,6),
    `blal_harvested_m3_vol` DECIMAL(15,6),
    `actt_key_ind` STRING,
    `status_date` TIMESTAMP,
    `auction_date_compare` TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_gis_acts (
    `licn_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `auction_status` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_gis_acts_status (
    `licn_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `actt_key_ind` STRING,
    `status_ind` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_harvested_block_status (
    `tso_code` STRING,
    `nav_name` STRING,
    `licence_id` STRING,
    `permit_id` STRING,
    `mark_id` STRING,
    `block_id` STRING,
    `block_nbr` STRING,
    `op_area` STRING,
    `gross_area` DECIMAL(11,6),
    `acti_status_ind` STRING,
    `acti_status_date` TIMESTAMP,
    `block_rg_status` STRING,
    `block_fg_status` STRING,
    `block_gu_status` STRING,
    `block_status` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_harvest_history (
    `tso_code` STRING,
    `licn_licence_id` STRING,
    `mark_mark_id` STRING,
    `bchh_billing_period` TIMESTAMP,
    `bchh_hdbs_tree_species` STRING,
    `bchh_forest_product_code` STRING,
    `bchh_log_grade` STRING,
    `bchh_billing_type_code` STRING,
    `bchh_volume_billed` DECIMAL(9,2),
    `bchh_pieces_billed` DECIMAL(10,0),
    `bchh_royalty_amount` DECIMAL(11,2),
    `bchh_reserve_stmpg_amt` DECIMAL(9,2),
    `bchh_bonus_stumpage_amt` DECIMAL(9,2),
    `bchh_silv_levy_amount` DECIMAL(9,2),
    `bchh_dev_levy_amount` DECIMAL(9,2),
    `bchh_update_timestamp` TIMESTAMP,
    `licn_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_harvest_unit (
    `cbin_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `harvest_unit_id` STRING,
    `season` STRING,
    `method` STRING,
    `strategy` STRING,
    `total_area` DECIMAL(15,6),
    `total_vol` DECIMAL(15,6),
    `depleted_area` DECIMAL(38,10),
    `depleted_vol` DECIMAL(38,10),
    `digitized` STRING,
    `cbin_comments` STRING,
    `season_id` STRING,
    `method_id` STRING,
    `strategy_id` STRING,
    `silv_strategy_id` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_interior_cost_survey_culv (
    `culvert_station` DECIMAL(13,4),
    `diameter` STRING,
    `length` STRING,
    `ctype` STRING,
    `xrise` STRING,
    `csus_comment` STRING,
    `csus_culvert_material_cost` DECIMAL(15,2),
    `csus_culvert_install_cost` DECIMAL(15,2),
    `total_cost` DECIMAL(38,10),
    `csur_seq_nbr` DECIMAL(15,0),
    `csur_seq_nbr_lng` DECIMAL(10,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_interior_cost_survey_roads (
    `tso_code` STRING,
    `construction_fiscal` DECIMAL(4,0),
    `manu_id` STRING,
    `manu_type_id` STRING,
    `manu_name` STRING,
    `road_name` STRING,
    `uri` STRING,
    `rcom_start_metre_nbr` DECIMAL(13,4),
    `rcom_end_metre_nbr` DECIMAL(13,4),
    `rcom_planned_date` TIMESTAMP,
    `rcom_completion_date` TIMESTAMP,
    `csur_start_metre_nbr` DECIMAL(13,4),
    `csur_end_metre_nbr` DECIMAL(13,4),
    `csur_report_date` TIMESTAMP,
    `csur_project_id` STRING,
    `csur_road_type` STRING,
    `csur_moisture_code` STRING,
    `bioz_zone_id` STRING,
    `csur_side_slope_perc` DECIMAL(7,4),
    `csur_boulder_area_perc` DECIMAL(7,4),
    `prepared_by` STRING,
    `csur_comment` STRING,
    `csur_mt_hard_rock_perc` DECIMAL(7,4),
    `csur_mt_rippable_rock_perc` DECIMAL(7,4),
    `csur_mt_coarse_perc` DECIMAL(7,4),
    `csur_mt_fine_perc` DECIMAL(7,4),
    `csur_mt_organic_perc` DECIMAL(7,4),
    `total_pct` DECIMAL(38,10),
    `csur_as_code` STRING,
    `csur_as_length_km` DECIMAL(13,4),
    `csur_as_surface_width_m` DECIMAL(13,4),
    `csur_as_type` STRING,
    `csur_as_depth_m` DECIMAL(13,4),
    `csur_as_distance_to_source_km` DECIMAL(13,4),
    `csur_as_actual_cost` DECIMAL(15,2),
    `csur_as_ttt_transfer_cost` DECIMAL(15,2),
    `csur_as_other_transfer_cost` DECIMAL(15,2),
    `addl_stab_cost` DECIMAL(38,10),
    `addl_stab_cost_per_km` DECIMAL(38,10),
    `csur_sg_length_km` DECIMAL(13,4),
    `csur_sg_surface_width_m` DECIMAL(13,4),
    `csur_sg_actual_cost` DECIMAL(15,2),
    `csur_sg_ttt_transfer_cost` DECIMAL(15,2),
    `csur_sg_other_transfer_cost` DECIMAL(15,2),
    `sub_grade_cost` DECIMAL(38,10),
    `csur_sg_landing_cost` DECIMAL(15,2),
    `csur_sg_end_haul_cost` DECIMAL(15,2),
    `csur_sg_overland_cost` DECIMAL(15,2),
    `csur_sg_other_eng_cost` DECIMAL(15,2),
    `csur_end_haul_distance_m` DECIMAL(13,4),
    `csur_end_haul_volume_m3` DECIMAL(15,6),
    `csur_overland_distance_m` DECIMAL(13,4),
    `csur_overland_volume_m3` DECIMAL(15,6),
    `csur_seq_nbr` DECIMAL(15,0),
    `road_seq_nbr` DECIMAL(16,0),
    `csur_seq_nbr_lng` DECIMAL(10,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_mark (
    `licn_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `tenure` STRING,
    `licence_id` STRING,
    `permit_id` STRING,
    `mark_id` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `mark_cruise_volume_m3` DECIMAL(38,10),
    `hbs_volume_billed` DECIMAL(38,10),
    `mark_state` STRING,
    `mark_description` STRING,
    `hbs_update_timestamp` TIMESTAMP,
    `mark_rw_volume_m3` DECIMAL(38,10),
    `appraisal_volume` DECIMAL(38,10),
    `field_team_desc` STRING,
    `licn_licence_state` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_mark_activity (
    `mark_seq_nbr` DECIMAL(15,0),
    `acti_seq_nbr` DECIMAL(15,0),
    `activity_class` STRING,
    `activity_type` STRING,
    `activity_date` TIMESTAMP,
    `status_ind` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_non_frpa_results_strategies (
    `cmcm_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(4,0),
    `silp_seq_nbr` DECIMAL(38,10),
    `cmtg_content` STRING,
    `cmcm_text` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_steward (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `rdst_start_metre_nbr` DECIMAL(13,4),
    `rdst_end_metre_nbr` DECIMAL(13,4),
    `rdst_length` DECIMAL(38,10),
    `rdst_steward_name` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `rdst_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_structure (
    `division_number` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `rstr_object_type` STRING,
    `rstr_planned_date` TIMESTAMP,
    `rstr_install_date` TIMESTAMP,
    `rstr_replace_date` TIMESTAMP,
    `rstr_deactivate_date` TIMESTAMP,
    `rstr_class_type` STRING,
    `rstr_status_code` STRING,
    `rstr_status_date` TIMESTAMP,
    `rstr_at_metre_nbr` DECIMAL(13,4),
    `rstr_location_desc` STRING,
    `rstr_object_comments` STRING,
    `rstr_file_id` STRING,
    `rstr_latitude` STRING,
    `rstr_longitude` STRING,
    `rstr_lat_long_datatype` STRING,
    `rstr_drawings_on_file` STRING,
    `rstr_object_subtype` STRING,
    `rstr_structure_id` STRING,
    `rstr_budgeted_cost` DECIMAL(15,2),
    `rstr_actual_cost` DECIMAL(15,2),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `rstr_seq_nbr` DECIMAL(15,0),
    `rstr_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_structure_attr (
    `divi_div_nbr` DECIMAL(2,0),
    `road_road_name` STRING,
    `uri` STRING,
    `rstr_object_type` STRING,
    `rstr_at_metre_nbr` DECIMAL(13,4),
    `rstr_start_metre_nbr` DECIMAL(13,4),
    `sdef_attribute_type` STRING,
    `satr_data_desc` STRING,
    `satr_unit_code` STRING,
    `sdef_sort_nbr` DECIMAL(2,0),
    `sdef_option_ind` STRING,
    `sdef_abr_code_id` STRING,
    `sdef_cost_survey_ind` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `rstr_seq_nbr` DECIMAL(15,0),
    `satr_seq_nbr` DECIMAL(15,0),
    `satr_seq_nbr_lng` DECIMAL(20,0),
    `sdef_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_structure_culvert (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `rstr_object_type` STRING,
    `rstr_object_subtype` STRING,
    `rstr_at_metre_nbr` DECIMAL(13,4),
    `rstr_structure_id` STRING,
    `rstr_status_code` STRING,
    `rstr_class_type` STRING,
    `diam_diameter_width` DECIMAL(13,4),
    `diam_diameter_unit_code` STRING,
    `diam_length_lgth` DECIMAL(13,4),
    `diam_length_unit_code` STRING,
    `diam_flow_rate` DECIMAL(13,4),
    `diam_seq_nbr` DECIMAL(38,10),
    `road_seq_nbr` DECIMAL(16,0),
    `rstr_seq_nbr` DECIMAL(15,0),
    `rstr_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_structure_inspection (
    `rstr_seq_nbr` DECIMAL(15,0),
    `insp_seq_nbr` DECIMAL(15,0),
    `meth_method_type` STRING,
    `insp_planned_date` TIMESTAMP,
    `insp_inspection_date` TIMESTAMP,
    `insp_inspection_file_id` STRING,
    `insp_inspector_type` STRING,
    `performed_by` STRING,
    `insp_budgeted_cost` DECIMAL(15,2),
    `insp_actual_cost` DECIMAL(15,2),
    `variance` DECIMAL(38,10),
    `responsibility` STRING,
    `insp_condition_desc` STRING,
    `insp_inspection_memo` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `insp_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_structure_maintenance (
    `rstr_seq_nbr` DECIMAL(15,0),
    `main_seq_nbr` DECIMAL(15,0),
    `insp_seq_nbr` DECIMAL(15,0),
    `main_planned_date` TIMESTAMP,
    `main_completed_date` TIMESTAMP,
    `meth_method_type` STRING,
    `main_budgeted_cost` DECIMAL(15,2),
    `main_actual_cost` DECIMAL(15,2),
    `variance` DECIMAL(38,10),
    `ctor_name` STRING,
    `responsibility` STRING,
    `main_priority` STRING,
    `main_activity_memo` STRING,
    `main_activity_type` STRING,
    `main_method_type` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `main_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_use_permit (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_name` STRING,
    `road_uri` STRING,
    `ruse_start_metre_nbr` DECIMAL(13,4),
    `ruse_end_metre_nbr` DECIMAL(13,4),
    `ruse_length` DECIMAL(38,10),
    `rdpm_start_date` TIMESTAMP,
    `rdpm_expiry_date` TIMESTAMP,
    `rdpm_permit_type` STRING,
    `rdpm_permit_id` STRING,
    `ruse_permit_id` STRING,
    `ruse_primary_name` STRING,
    `ruse_secondary_name` STRING,
    `ruse_tenure_type` STRING,
    `ruse_amendment_nbr` STRING,
    `ruse_section_id` STRING,
    `ruse_poc` STRING,
    `ruse_memo` STRING,
    `ruse_gross_ha_area` DECIMAL(7,2),
    `ruse_right_of_way_width` DECIMAL(10,4),
    `ruse_cruise_m3_vol` DECIMAL(15,6),
    `ruse_harv_completed_status` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(15,0),
    `rdpm_seq_nbr` DECIMAL(15,0),
    `ruse_seq_nbr` DECIMAL(15,0),
    `ruse_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_sale_forecast_tka_temp (
    `field_team` STRING,
    `licn_licence_id` STRING,
    `cutb_block_id` STRING,
    `opar_operating_area_name` STRING,
    `blal_merch_ha_area` DECIMAL(11,6),
    `cruise_m3` DECIMAL(38,10),
    `cruise_m3_per_tree` DECIMAL(38,10),
    `cruise_label` STRING,
    `est_m3` DECIMAL(38,10),
    `est_m3_per_tree` DECIMAL(38,10),
    `est_label` STRING,
    `fc_m3` DECIMAL(38,10),
    `fc_m3_per_tree` DECIMAL(38,10),
    `fc_label` STRING,
    `ocular_m3` DECIMAL(38,10),
    `ocular_m3_per_tree` DECIMAL(38,10),
    `ocular_label` STRING,
    `comp_m3` DECIMAL(38,10),
    `comp_m3_per_tree` DECIMAL(38,10),
    `comp_label` STRING,
    `sisy_silvicultural_system_id` STRING,
    `num_system` DECIMAL(38,10),
    `status` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_silvicultural_system (
    `cutb_seq_nbr` DECIMAL(15,0),
    `silp_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `sisy_silvicultural_system_id` STRING,
    `siva_variant_id` STRING,
    `siph_cut_phase_id` STRING,
    `srty_reserve_type` STRING,
    `spss_area` DECIMAL(7,2),
    `spss_stand_structure_site_cond` STRING,
    `spss_leave_tree_species_comm` STRING,
    `spss_composition_goals` STRING,
    `spss_residual_basal_area` DECIMAL(7,2),
    `spss_residual_basal_density` DECIMAL(10,1),
    `spss_opening_size_ha_max` DECIMAL(7,2),
    `spss_opening_size_ha_min` DECIMAL(7,2),
    `spss_opening_size_ha_avg` DECIMAL(7,2)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_silviculture_amendment (
    `samm_amendment_nbr` DECIMAL(10,0),
    `samm_amendment_date` TIMESTAMP,
    `samm_licensee_appr_date` TIMESTAMP,
    `samm_dist_appr_date` TIMESTAMP,
    `samm_map_required` STRING,
    `samm_comment` STRING,
    `samm_amendment_type` STRING,
    `samm_status` STRING,
    `amendment_type_code` STRING,
    `amnd_description` STRING,
    `responsibility` STRING,
    `silp_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_gap_analysis (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `divi_division_name` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `uri` STRING,
    `road_road_name` STRING,
    `road_road_desc` STRING,
    `field_team_desc` STRING,
    `road_end_metre_nbr` DECIMAL(13,4),
    `poc` DECIMAL(13,4),
    `pot` DECIMAL(38,10),
    `prime_road_seq_nbr` DECIMAL(38,10),
    `rdst_seq_nbr` DECIMAL(15,0),
    `rdst_steward_name` STRING,
    `rcls_seq_nbr` DECIMAL(15,0),
    `rcls_class_type` STRING,
    `rcls_accounting_type` STRING,
    `rsta_seq_nbr` DECIMAL(15,0),
    `rsta_status_code` STRING,
    `rsta_road_state` STRING,
    `ruse_seq_nbr` DECIMAL(15,0),
    `rdpm_permit_id` STRING,
    `ruse_section_id` STRING,
    `rdpm_permit_type` STRING,
    `rdpm_start_date` TIMESTAMP,
    `rdpm_expiry_date` TIMESTAMP,
    `rdpm_amendment_nbr` STRING,
    `rcom_seq_nbr` DECIMAL(15,0),
    `const_method_type` STRING,
    `rcom_planned_date` TIMESTAMP,
    `rcom_completion_date` TIMESTAMP,
    `rcom_budgeted_cost` DECIMAL(15,2),
    `rcom_method` STRING,
    `deac_seq_nbr` DECIMAL(15,0),
    `deac_planned_date` TIMESTAMP,
    `deac_end_date` TIMESTAMP,
    `deac_method_type` STRING,
    `deac_level_type` STRING,
    `deac_budgeted_cost` DECIMAL(15,2)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_block (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `field_team_desc` STRING,
    `nav_name` STRING,
    `tenure` STRING,
    `licence_id` STRING,
    `permit_id` STRING,
    `mark_id` STRING,
    `block_id` STRING,
    `block_nbr` STRING,
    `ubi` STRING,
    `opening` STRING,
    `op_area` STRING,
    `opar_operating_area_name` STRING,
    `ozon_operating_zone_id` STRING,
    `supply_block` STRING,
    `ebm_indicator` STRING,
    `photo` STRING,
    `latitude` STRING,
    `longitude` STRING,
    `prov_frst_conflict` STRING,
    `mapsheet_id` STRING,
    `landscape_unit` STRING,
    `sp_exempt` STRING,
    `stand_type` STRING,
    `age_class` STRING,
    `hgt_class` STRING,
    `stk_class` STRING,
    `site_index` DECIMAL(38,10),
    `source` STRING,
    `fdp_status` STRING,
    `funding_code` STRING,
    `cutb_block_memo` STRING,
    `gross_area` DECIMAL(11,6),
    `est_area` DECIMAL(7,2),
    `merch_area` DECIMAL(11,6),
    `harvest_area` DECIMAL(11,6),
    `remaining_area` DECIMAL(38,10),
    `cruise_vol` DECIMAL(15,6),
    `data_source` STRING,
    `harvest_vol` DECIMAL(15,6),
    `remaining_vol` DECIMAL(38,10),
    `blal_usr_cruise_m3_vol` DECIMAL(15,6),
    `rw_area` DECIMAL(11,6),
    `blal_rw_vol` DECIMAL(15,6),
    `cutb_forma_print_date` TIMESTAMP,
    `cutb_forma_printed` STRING,
    `cutb_block_state` STRING,
    `pmod_modifier_id` STRING,
    `cutb_location` STRING,
    `suop_subop_area_id` STRING,
    `suop_subop_area_name` STRING,
    `cutb_opening_id` DECIMAL(10,0),
    `licn_licence_state` STRING,
    `seed_zone` STRING,
    `cutb_file_id` STRING,
    `min_elevation` DECIMAL(38,10),
    `max_elevation` DECIMAL(38,10),
    `bcat_category_code` STRING,
    `cutb_access_restriction` STRING,
    `regime_created_by` STRING,
    `treg_regime_id` STRING,
    `treg_regime_name` STRING,
    `treg_create_date` TIMESTAMP,
    `treg_active_ind` STRING,
    `treg_def_ind` STRING,
    `nav_id` STRING,
    `fiz` STRING,
    `hvs_status` STRING,
    `hvs_target_date` TIMESTAMP,
    `hvs_status_date` TIMESTAMP,
    `hvc_status` STRING,
    `hvc_target_date` TIMESTAMP,
    `hvc_status_date` TIMESTAMP,
    `manu_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `cutb_cprp_protection_ind` STRING,
    `cutb_rc_risk_rating` STRING,
    `cutb_rc_risk_source` STRING,
    `cutb_rc_risk_comments` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_block_activity_all (
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `tenure` STRING,
    `licence_id` STRING,
    `permit_id` STRING,
    `mark_id` STRING,
    `block_id` STRING,
    `block_nbr` STRING,
    `cutb_seq_nbr` DECIMAL(15,0),
    `acti_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `actt_seq_nbr` DECIMAL(15,0),
    `activity_class` STRING,
    `activity_type` STRING,
    `actt_key_ind` STRING,
    `activity_date` TIMESTAMP,
    `acti_status_ind` STRING,
    `accl_object_type` STRING,
    `acti_responsibility` STRING,
    `ctor_name` STRING,
    `acti_cost` DECIMAL(15,2),
    `acti_target_date` TIMESTAMP,
    `acti_target_cost` DECIMAL(15,2),
    `acti_comments` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `field_team_desc` STRING,
    `ubi` STRING,
    `licn_licence_state` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_silviculture_overlay_xref (
    `siox_seq_nbr` DECIMAL(15,0),
    `siox_ha_area` DECIMAL(11,6),
    `siox_digitised_ind` STRING,
    `stun_seq_nbr` DECIMAL(15,0),
    `sila_seq_nbr` DECIMAL(15,0),
    `plun_seq_nbr` DECIMAL(15,0),
    `sish_seq_nbr` DECIMAL(15,0),
    `cbhm_seq_nbr` DECIMAL(15,0),
    `siox_comments` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_silviculture_prescription (
    `prescription_prepared_by` STRING,
    `additional_sp_comments` STRING,
    `rfp_certification_comments` STRING,
    `silp_admin_assessment_comments` STRING,
    `amendments_comments` STRING,
    `silp_sp_report_format` STRING,
    `silp_perm_access_max_area` DECIMAL(11,6),
    `silp_perm_access_max_pct` DECIMAL(7,4),
    `max_roadside_disturbance` DECIMAL(7,4),
    `soil_conservation_comments` STRING,
    `silp_temp_access_max_rehab` DECIMAL(10,0),
    `silp_temp_access_comments` STRING,
    `silp_slope_instability` STRING,
    `silp_slope_required` STRING,
    `silp_slope_assessment` STRING,
    `forest_health_comments` STRING,
    `pest_specific_comments` STRING,
    `silp_pest_required` STRING,
    `silp_pest_assessment` STRING,
    `silp_vegetation_management` STRING,
    `silp_vegetation_required` STRING,
    `silp_livestock_used` STRING,
    `silp_riparian_comments` STRING,
    `silp_riparian_required` STRING,
    `silp_riparian_assessment` STRING,
    `silp_gully_manage_strategy` STRING,
    `silp_gully_required` STRING,
    `silp_gully_assessment` STRING,
    `silp_archaeological_sites` STRING,
    `silp_archeosite_required` STRING,
    `silp_archaeology_assessment` STRING,
    `silp_lterm_man_obj` STRING,
    `silp_wildlife` STRING,
    `silp_lichen_required` STRING,
    `silp_lichen_assessment` STRING,
    `silp_range` STRING,
    `silp_range_tenure_holder` STRING,
    `silp_fisheries` STRING,
    `silp_coarse_woody_debris` STRING,
    `performance_std_pct` DECIMAL(7,3),
    `block_target_pct` DECIMAL(7,3),
    `silp_coarse_woody_debris_m3_ha` DECIMAL(7,2),
    `silp_watersheds` STRING,
    `silp_community_watershed_ind` STRING,
    `silp_sensitive_areas` STRING,
    `silp_recreation` STRING,
    `silp_na_conditions` STRING,
    `silp_visual_landscape` STRING,
    `silp_visual_required` STRING,
    `silp_visual_assessment` STRING,
    `silp_cultural_heritage` STRING,
    `silp_bio_diversity` STRING,
    `silp_biodiv_perf_std_start_pct` DECIMAL(7,3),
    `silp_biodiv_perf_std_end_pct` DECIMAL(7,3),
    `silp_bio_divers_block_tgt_pct` DECIMAL(7,3),
    `silp_wildlife_tree_credit_ha` DECIMAL(7,2),
    `silp_bio_diversity_calc_ind` STRING,
    `silp_other_resources` STRING,
    `silp_trapper_registration` STRING,
    `silp_guide_registration` STRING,
    `cutb_seq_nbr` DECIMAL(15,0),
    `silp_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_silviculture_stratum_history (
    `divi_div_nbr` DECIMAL(2,0),
    `sish_seq_nbr` DECIMAL(15,0),
    `sila_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `sish_stratum_name` STRING,
    `sish_digitised_ind` STRING,
    `sish_ha_area` DECIMAL(11,6),
    `sish_green_up_date` TIMESTAMP,
    `sish_green_up_ind` STRING,
    `sish_free_grow_date` TIMESTAMP,
    `sish_free_grow_ind` STRING,
    `sish_regen_delay_met_date` TIMESTAMP,
    `sish_regen_delay_met_ind` STRING,
    `sila_comments` STRING,
    `stst_stocking_status_id` STRING,
    `sttp_stocking_type_id` STRING,
    `sish_mortality_pct` DECIMAL(5,2),
    `sish_post_harvest_date` TIMESTAMP,
    `sish_post_harvest_ind` STRING,
    `sish_plots` DECIMAL(4,0),
    `sish_establishment_year` TIMESTAMP,
    `sish_crop_stems_per_ha` DECIMAL(5,0),
    `sish_total_stems_per_ha` DECIMAL(5,0),
    `sish_site_index` DECIMAL(2,0),
    `sish_pct_distribution` DECIMAL(3,0),
    `sish_pct_productive` DECIMAL(3,0),
    `sish_prefix` STRING,
    `sish_class` STRING,
    `sica_activity_name` STRING,
    `sila_start_date` TIMESTAMP,
    `sila_completion_date` TIMESTAMP,
    `sila_status` STRING,
    `sila_gross_ha_area` DECIMAL(11,6),
    `stun_id` STRING,
    `sipr_project_id` STRING,
    `stun_seq_nbr` DECIMAL(15,0),
    `suha_ha_area` DECIMAL(11,6),
    `suha_digitised_ind` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_silv_project (
    `sipr_seq_nbr` DECIMAL(15,0),
    `ctor_seq_nbr` DECIMAL(15,0),
    `cloc_seq_nbr` DECIMAL(15,0),
    `pers_seq_nbr` DECIMAL(15,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `project_id` STRING,
    `contractor` STRING,
    `contract_coord` STRING,
    `start_date` TIMESTAMP,
    `start_status` STRING,
    `end_date` TIMESTAMP,
    `end_status` STRING,
    `viewing_date` TIMESTAMP,
    `unit_bid_code` STRING,
    `total_bid_price` DECIMAL(15,2),
    `total_overhead_cost` DECIMAL(15,2),
    `total_actual_cost` DECIMAL(38,10),
    `result_type` STRING,
    `sipr_contr_res_comment` STRING,
    `sipr_general_comment` STRING,
    `divi_div_nbr` DECIMAL(15,0),
    `done` DECIMAL(38,10),
    `planned` DECIMAL(38,10),
    `sipr_project_type` STRING,
    `sipr_crew_hire_code` STRING,
    `sipr_result_type` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_silv_project_block (
    `cutb_seq_nbr` DECIMAL(15,0),
    `sila_seq_nbr` DECIMAL(15,0),
    `sipr_seq_nbr` DECIMAL(15,0),
    `sipr_project_id` STRING,
    `divi_div_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_species_composition (
    `sist_seq_nbr` DECIMAL(15,0),
    `slay_layer` STRING,
    `sisp_species_id` STRING,
    `spco_percent` DECIMAL(7,4),
    `leading_species_ind` STRING,
    `spco_age` DECIMAL(4,1),
    `spco_height` DECIMAL(8,2),
    `spco_stems_per_ha` DECIMAL(5,0),
    `spco_increment` DECIMAL(15,7),
    `spco_well_spaced` DECIMAL(5,0),
    `spco_free_growing` DECIMAL(5,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_species_composition_hist (
    `sish_seq_nbr` DECIMAL(15,0),
    `slay_layer` STRING,
    `sisp_species_id` STRING,
    `spch_percent` DECIMAL(7,4),
    `spch_age` DECIMAL(4,1),
    `spch_height` DECIMAL(8,2),
    `spch_stems_per_ha` DECIMAL(5,0),
    `spch_increment` DECIMAL(15,7),
    `spch_well_spaced` DECIMAL(5,0),
    `spch_free_growing` DECIMAL(5,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_stocking_standard (
    `stun_seq_nbr` DECIMAL(15,0),
    `layt_layer_code` STRING,
    `stsd_post_spacing_min` DECIMAL(10,1),
    `stsd_post_spacing_max` DECIMAL(10,1),
    `stsd_max_coniferous` DECIMAL(10,1),
    `stsd_well_spaced_target` DECIMAL(10,1),
    `stsd_well_spaced_min` DECIMAL(10,1),
    `stsd_well_spaced_pref_min` DECIMAL(10,1),
    `stsd_well_spaced_min_horiz` DECIMAL(13,4),
    `stsd_min_prune_hgt_m` DECIMAL(10,4),
    `stsd_height_rel_to_comp_pct` DECIMAL(7,4),
    `stsd_height_rel_to_comp_cm` DECIMAL(10,4),
    `silp_stocking_req_comments` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_stratum_status (
    `cutb_seq_nbr` DECIMAL(15,0),
    `sist_seq_nbr` DECIMAL(15,0),
    `stratum_name` STRING,
    `area` DECIMAL(11,6),
    `sica_activity_name` STRING,
    `base` STRING,
    `technique` STRING,
    `method` STRING,
    `start_date` TIMESTAMP,
    `complete_date` TIMESTAMP,
    `current_status` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `sstatus` STRING,
    `sipr_seq_nbr` DECIMAL(15,0),
    `stun_seq_nbr` DECIMAL(15,0),
    `stun_id` STRING,
    `stype` STRING,
    `sist_green_up_date` TIMESTAMP,
    `sist_green_up_ind` STRING,
    `sist_regen_delay_met_date` TIMESTAMP,
    `sist_regen_delay_met_ind` STRING,
    `sist_post_harvest_date` TIMESTAMP,
    `sist_post_harvest_ind` STRING,
    `sist_free_grow_date` TIMESTAMP,
    `sist_free_grow_ind` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_su (
    `cutb_seq_nbr` DECIMAL(15,0),
    `stun_seq_nbr` DECIMAL(15,0),
    `stun_id` STRING,
    `su_regen_obligation` STRING,
    `gross_area` DECIMAL(11,6),
    `ncbr_area` DECIMAL(11,6),
    `nar_num` DECIMAL(38,10),
    `nar` STRING,
    `npnat_area` DECIMAL(38,10),
    `npunn_area` DECIMAL(38,10),
    `divi_div_nbr` DECIMAL(2,0),
    `stun_max_disturbance` DECIMAL(7,4),
    `regen_delay` TIMESTAMP,
    `stun_regen_date_early` DECIMAL(4,0),
    `early_ftg` TIMESTAMP,
    `stun_freegrow_date_early` DECIMAL(4,0),
    `late_ftg` TIMESTAMP,
    `stun_freegrow_date_late` DECIMAL(4,0),
    `silv_system_id` STRING,
    `siva_variant_id` STRING,
    `siph_cut_phase_id` STRING,
    `free_grow_date` TIMESTAMP,
    `free_grow_ind` STRING,
    `post_harv_date` TIMESTAMP,
    `post_harv_ind` STRING,
    `regen_date` TIMESTAMP,
    `regen_ind` STRING,
    `shsd_submission_date` TIMESTAMP,
    `shsd_submission_ind` STRING,
    `stun_formc_postharv_prn_date` TIMESTAMP,
    `stun_formc_regen_prn_date` TIMESTAMP,
    `stun_formc_freegrow_prn_date` TIMESTAMP,
    `suty_type_id` STRING,
    `stun_description` STRING,
    `stds_legal_id` STRING,
    `su_disturb_area` DECIMAL(38,10),
    `stun_objective_type` STRING,
    `stty_original_standard_type` STRING,
    `stun_original_std_declare_date` TIMESTAMP,
    `stun_declaration_area` DECIMAL(11,6),
    `stun_declaration_area_lock_ind` STRING,
    `stun_designation_code` STRING,
    `spss_leave_tree_species_comm` STRING,
    `stun_soil_compaction` STRING,
    `stun_soil_erosion` STRING,
    `stun_soil_displacement` STRING,
    `stun_type_unfavourable` STRING,
    `stun_sediment_risk` STRING,
    `stun_depth_unfavourable_max` DECIMAL(13,4),
    `stun_depth_unfavourable_min` DECIMAL(13,4),
    `silp_sp_report_format` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_su_eco_allocation (
    `ecou_seq_nbr` DECIMAL(15,0),
    `stun_seq_nbr` DECIMAL(15,0),
    `suea_ha_area` DECIMAL(11,6),
    `suea_digitised_ind` STRING,
    `cutb_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_su_layer (
    `stun_seq_nbr` DECIMAL(15,0),
    `stun_id` STRING,
    `layt_layer_code` STRING,
    `stsd_post_spacing_min` DECIMAL(10,1),
    `stsd_post_spacing_max` DECIMAL(10,1),
    `stsd_max_coniferous` DECIMAL(10,1),
    `stsd_residual_area` DECIMAL(10,4),
    `stsd_well_spaced_target` DECIMAL(10,1),
    `stsd_well_spaced_min` DECIMAL(10,1),
    `stsd_well_spaced_pref_min` DECIMAL(10,1),
    `stsd_well_spaced_min_horiz` DECIMAL(13,4),
    `stsd_height_rel_to_comp_pct` DECIMAL(7,4),
    `stsd_height_rel_to_comp_cm` DECIMAL(10,4),
    `stsd_min_prune_hgt_m` DECIMAL(10,4),
    `preferred_species` STRING,
    `accepted_species` STRING,
    `cutb_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_su_stratum_allocation (
    `sist_seq_nbr` DECIMAL(15,0),
    `stun_seq_nbr` DECIMAL(15,0),
    `susa_ha_area` DECIMAL(11,6),
    `susa_digitised_ind` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_third_party_interest (
    `cutb_seq_nbr` DECIMAL(15,0),
    `trdp_seq_nbr` DECIMAL(15,0),
    `paty_party_type_id` STRING,
    `trdp_identifier_code` STRING,
    `divi_div_nbr` DECIMAL(2,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_timber_inventory_dip (
    `business_area_region_category` STRING,
    `business_area_region` STRING,
    `business_area` STRING,
    `business_area_code` STRING,
    `fieldteam` STRING,
    `manu_id` STRING,
    `licence_id` STRING,
    `tenure_type` STRING,
    `perm_permit_id` STRING,
    `mark_mark_id` STRING,
    `block_id` STRING,
    `ubi` STRING,
    `block_nbr` STRING,
    `sub_operating_area` STRING,
    `licn_licence_state` STRING,
    `cutb_block_state` STRING,
    `deferred_at_report_date` STRING,
    `inventory_category` STRING,
    `merch_area` DECIMAL(11,6),
    `cruise_volume` DECIMAL(15,6),
    `rw_volume` DECIMAL(15,6),
    `rc_status` STRING,
    `rc_date` TIMESTAMP,
    `rc_fiscal` DECIMAL(38,10),
    `dr_status` STRING,
    `dr_date` TIMESTAMP,
    `dr_fiscal` DECIMAL(38,10),
    `dvs_status` STRING,
    `dvs_date` TIMESTAMP,
    `dvs_fiscal` DECIMAL(38,10),
    `dsc_status` STRING,
    `dsc_date` TIMESTAMP,
    `dvc_status` STRING,
    `dvc_date` TIMESTAMP,
    `dvc_fiscal` DECIMAL(38,10),
    `days_in_dip` DECIMAL(38,10),
    `woff_status` STRING,
    `woff_date` TIMESTAMP,
    `woff_fiscal` DECIMAL(38,10),
    `auc_status` STRING,
    `auc_date` TIMESTAMP,
    `hi_status` STRING,
    `hi_date` TIMESTAMP,
    `hvs_status` STRING,
    `hvs_date` TIMESTAMP,
    `hvc_status` STRING,
    `hvc_date` TIMESTAMP,
    `fg_met_status` STRING,
    `fg_date` TIMESTAMP,
    `def_change_of_op_plan_status` STRING,
    `def_change_of_op_plan` TIMESTAMP,
    `def_first_nations_status` STRING,
    `def_first_nations` TIMESTAMP,
    `def_loss_of_access_status` STRING,
    `def_loss_of_access` TIMESTAMP,
    `def_other_status` STRING,
    `def_other` TIMESTAMP,
    `def_planning_constraint_status` STRING,
    `def_planning_constraint` TIMESTAMP,
    `def_returned_to_bcts_status` STRING,
    `def_returned_to_bcts` TIMESTAMP,
    `def_stale_dated_fieldwork_status` STRING,
    `def_stale_dated_fieldwork` TIMESTAMP,
    `def_stakeholder_issue_status` STRING,
    `def_stakeholder_issue` TIMESTAMP,
    `def_environmental_stewardship_initiative_status` STRING,
    `def_environmental_stewardship_initiative` TIMESTAMP,
    `def_reactivated_status` STRING,
    `def_reactivated` TIMESTAMP,
    `old_growth_strategy_status` STRING,
    `old_growth_strategy` TIMESTAMP,
    `ogs_reactivated_forest_health_status` STRING,
    `ogs_reactivated_forest_health` TIMESTAMP,
    `ogs_reactivated_fn_proceed_status` STRING,
    `ogs_reactivated_fn_proceed` TIMESTAMP,
    `ogs_reactivated_field_verified_status` STRING,
    `ogs_reactivated_field_verified` TIMESTAMP,
    `ogs_reactivated_minor_status` STRING,
    `ogs_reactivated_minor` TIMESTAMP,
    `ogs_reactivated_road_status` STRING,
    `ogs_reactivated_road` TIMESTAMP,
    `ogs_reactivated_re_engineered_status` STRING,
    `ogs_reactivated_re_engineered` TIMESTAMP,
    `xxx_zzz_flag` STRING,
    `spatial_flag` STRING,
    `rc_flag` STRING,
    `dr_flag` STRING,
    `dvs_flag` STRING,
    `dsc_flag` STRING,
    `dvc_flag` STRING,
    `count_of_blocks` DECIMAL(38,10),
    `licn_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `objectid` DECIMAL(38,10)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_silv_stockstat_history (
    `sish_seq_nbr` DECIMAL(15,0),
    `slay_layer` STRING,
    `srnk_rank` STRING,
    `stst_stocking_status_id` STRING,
    `sttp_stocking_type_id` STRING,
    `sssh_survey_source` STRING,
    `sssh_survey_date` TIMESTAMP,
    `sssh_stock_age` DOUBLE,
    `sssh_stock_age_plant` DOUBLE,
    `sssh_stock_height` DECIMAL(10,4),
    `sssh_crown_closure` DECIMAL(7,4),
    `sssh_reference_year` DECIMAL(4,0),
    `sicl_site_class` STRING,
    `sssh_site_index` DECIMAL(10,0),
    `sssh_density` DECIMAL(10,1),
    `sssh_well_spaced_density` DECIMAL(10,1),
    `sssh_free_growing_density` DECIMAL(10,1),
    `srty_reserve_type` STRING,
    `sssh_basal_area` DECIMAL(7,2),
    `sssh_cm_top_height` DECIMAL(10,4),
    `sssh_cm_leader` DECIMAL(10,4),
    `sssh_vigour_pct_good` DECIMAL(7,4),
    `sssh_vigour_pct_medium` DECIMAL(7,4),
    `sssh_vigour_pct_poor` DECIMAL(7,4),
    `sssh_comments` STRING,
    `sssh_total_coniferous` DECIMAL(10,0),
    `sssh_plantable_spots` DECIMAL(10,0),
    `sssh_prepable_slash_spots` DECIMAL(10,0),
    `sssh_prepable_brush_spots` DECIMAL(10,0),
    `sssh_prepable_duff_spots` DECIMAL(10,0),
    `sssh_non_productive_spots` DECIMAL(10,0),
    `sssh_root_collar_diameter` DECIMAL(10,0),
    `sssc_source_code` STRING,
    `sssh_free_grow_density_pref` DECIMAL(10,1),
    `sssh_well_spaced_density_pref` DECIMAL(10,1),
    `sssh_free_grow_lcl_density` DECIMAL(10,1),
    `sssh_well_spaced_lcl_density` DECIMAL(10,1),
    `sssh_countable_conifers` DECIMAL(10,0),
    `sssh_reentry_year` DECIMAL(4,0),
    `sssh_cvr_pattern` STRING,
    `sssh_res_objective` STRING,
    `sssh_total_well_spaced` DECIMAL(10,1),
    `sssh_germinant_per_ha` DECIMAL(10,1),
    `sssh_greened_up_tree_per_ha` DECIMAL(10,1)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_silviculture_stocking_status (
    `sist_seq_nbr` DECIMAL(15,0),
    `slay_layer` STRING,
    `srnk_rank` STRING,
    `sttp_stocking_type_id` STRING,
    `ssts_survey_source` STRING,
    `ssts_survey_date` TIMESTAMP,
    `ssts_stock_age` DECIMAL(38,10),
    `ssts_stock_age_plant` DECIMAL(38,10),
    `ssts_stock_height` DECIMAL(10,4),
    `ssts_crown_closure` DECIMAL(7,4),
    `ssts_reference_year` DECIMAL(4,0),
    `sicl_site_class` STRING,
    `ssts_site_index` DECIMAL(10,0),
    `ssts_density` DECIMAL(10,1),
    `ssts_well_spaced_density` DECIMAL(10,1),
    `ssts_free_growing_density` DECIMAL(10,1),
    `srty_reserve_type` STRING,
    `ssts_basal_area` DECIMAL(7,2),
    `ssts_cm_top_height` DECIMAL(10,4),
    `ssts_cm_leader` DECIMAL(10,4),
    `ssts_vigour_pct_good` DECIMAL(7,4),
    `ssts_vigour_pct_medium` DECIMAL(7,4),
    `ssts_vigour_pct_poor` DECIMAL(7,4),
    `ssts_comments` STRING,
    `ssts_total_coniferous` DECIMAL(10,0),
    `ssts_plantable_spots` DECIMAL(10,0),
    `ssts_prepable_slash_spots` DECIMAL(10,0),
    `ssts_prepable_brush_spots` DECIMAL(10,0),
    `ssts_prepable_duff_spots` DECIMAL(10,0),
    `ssts_non_productive_spots` DECIMAL(10,0),
    `ssts_formc_printed_ind` STRING,
    `ssts_formc_date` TIMESTAMP,
    `stst_stocking_status_id` STRING,
    `ssts_root_collar_diameter` DECIMAL(10,0),
    `sssc_source_code` STRING,
    `ssts_free_grow_density_pref` DECIMAL(10,1),
    `ssts_well_spaced_density_pref` DECIMAL(10,1),
    `ssts_free_grow_lcl_density` DECIMAL(10,1),
    `ssts_well_spaced_lcl_density` DECIMAL(10,1),
    `ssts_countable_conifers` DECIMAL(10,0),
    `ssts_reentry_year` DECIMAL(4,0),
    `ssts_cvr_pattern` STRING,
    `ssts_res_objective` STRING,
    `ssts_total_well_spaced` DECIMAL(10,1),
    `species` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_planting (
    `cutb_seq_nbr` DECIMAL(15,0),
    `sila_seq_nbr` DECIMAL(15,0),
    `plun_seq_nbr` DECIMAL(15,0),
    `licence_id` STRING,
    `permit_id` STRING,
    `block_id` STRING,
    `plant_id` DECIMAL(3,0),
    `base` STRING,
    `technique` STRING,
    `method` STRING,
    `act_name` STRING,
    `fund_funding_code` STRING,
    `sila_gross_ha_area` DECIMAL(11,6),
    `start_date` TIMESTAMP,
    `complete_date` TIMESTAMP,
    `status` STRING,
    `plun_ha_area` DECIMAL(11,6),
    `contractor` STRING,
    `density` DECIMAL(7,2),
    `plant_species` STRING,
    `plant_species_and_pct` STRING,
    `comments` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `sipr_seq_nbr` DECIMAL(15,0),
    `sipr_project_id` STRING,
    `plun_quality_percent` DECIMAL(7,4),
    `trees` DECIMAL(38,10),
    `plun_contract_max_spacing` DECIMAL(7,2),
    `plun_contract_min_spacing` DECIMAL(7,2),
    `plun_contract_target_spacing` DECIMAL(7,2)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_silviculture_activity (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `field_team` STRING,
    `manu_id` STRING,
    `operating_zone` STRING,
    `tenure` STRING,
    `licence` STRING,
    `permit` STRING,
    `mark` STRING,
    `block_id` STRING,
    `block_nbr` STRING,
    `ubi` STRING,
    `block_state` STRING,
    `cutb_opening` STRING,
    `sp_exempt` STRING,
    `cruise_volume` DECIMAL(15,6),
    `nar_area` DECIMAL(38,10),
    `hvs_status` STRING,
    `hvs_date` TIMESTAMP,
    `hvs_fiscal` DECIMAL(38,10),
    `hvc_status` STRING,
    `hvc_date` TIMESTAMP,
    `hvc_fiscal` DECIMAL(38,10),
    `treatment_unit` STRING,
    `block_funding` STRING,
    `activity_funding` STRING,
    `base` STRING,
    `technique` STRING,
    `method` STRING,
    `activity` STRING,
    `status` STRING,
    `start_date` TIMESTAMP,
    `start_fiscal` DECIMAL(38,10),
    `complete_date` TIMESTAMP,
    `complete_fiscal` DECIMAL(38,10),
    `sila_season` STRING,
    `treatment_area` DECIMAL(38,10),
    `sila_gross_area_char` STRING,
    `cost_per_ha_plan` DECIMAL(38,10),
    `cost_per_ha` DECIMAL(38,10),
    `planned_total_cost` DECIMAL(38,10),
    `actual_total_cost` DECIMAL(38,10),
    `planned_service_line` STRING,
    `planned_sl_description` STRING,
    `items_planned_total_cost` DECIMAL(38,10),
    `actual_service_line` STRING,
    `actual_sl_description` STRING,
    `items_actual_total_cost` DECIMAL(38,10),
    `planting_unit` DECIMAL(3,0),
    `plun_digitised_ind` STRING,
    `plun_planned_total_cost` DECIMAL(15,5),
    `plun_item_planned_total_cost` DECIMAL(38,10),
    `plun_actual_total_cost` DECIMAL(15,5),
    `plun_item_actual_total_cost` DECIMAL(38,10),
    `plun_ha_area` DECIMAL(11,6),
    `plun_net_area` DECIMAL(11,6),
    `plun_density` DECIMAL(7,2),
    `total_trees` DECIMAL(12,0),
    `sica_key_ind` STRING,
    `objective1` STRING,
    `objective2` STRING,
    `objective3` STRING,
    `responsibility` STRING,
    `target_species` STRING,
    `apply_rate` DECIMAL(13,4),
    `pmp_nbr` STRING,
    `comments` STRING,
    `sila_formb_date` TIMESTAMP,
    `sila_formb_printed` STRING,
    `project` STRING,
    `project_start_date` TIMESTAMP,
    `project_end_date` TIMESTAMP,
    `contract` STRING,
    `contractor` STRING,
    `contract_start_date` TIMESTAMP,
    `contract_end_date` TIMESTAMP,
    `plun_modifiedby` STRING,
    `plun_modifiedon` TIMESTAMP,
    `plun_modifiedusing` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `primary_record` STRING,
    `emsp_seq_nbr` DECIMAL(15,0),
    `emsc_seq_nbr` DECIMAL(15,0),
    `plun_seq_nbr` DECIMAL(38,10),
    `sila_seq_nbr` DECIMAL(15,0),
    `sica_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `licn_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_pest_infestation (
    `slay_layer` STRING,
    `sist_seq_nbr` DECIMAL(15,0),
    `spst_pest_desc` STRING,
    `spst_pest_code` STRING,
    `pein_percent` DECIMAL(7,4)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_planting_species (
    `plun_seq_nbr` DECIMAL(15,0),
    `sisp_species_id` STRING,
    `plsp_number_trees` DECIMAL(10,0),
    `sisl_seed_lot_number` STRING,
    `sirk_request_key` STRING,
    `plsp_seed_weight` DECIMAL(12,4),
    `plsp_price_per_tree` DECIMAL(13,4),
    `cutb_seq_nbr` DECIMAL(15,0),
    `sirk_stock_type` STRING,
    `cc_percentage` DECIMAL(38,10),
    `plsp_trees_outside_zone` DECIMAL(7,0),
    `plsp_comment` STRING,
    `sirk_years_original_container` DECIMAL(38,10),
    `sirk_years_transplanted` DECIMAL(38,10),
    `sisl_seed_zone` STRING,
    `sisl_genetic_class` STRING,
    `sisl_genetic_worth` STRING,
    `sisl_elevation_min` DECIMAL(5,0),
    `sisl_elevation_max` DECIMAL(5,0),
    `sisl_elevation_mean` DECIMAL(5,0),
    `sisl_latitude_min` DECIMAL(38,10),
    `sisl_latitude_max` DECIMAL(38,10),
    `sisl_latitude_mean` DECIMAL(38,10),
    `sisl_longitude` DECIMAL(38,10),
    `sisl_number_trees` DECIMAL(10,0),
    `sisl_kilograms` DECIMAL(12,4),
    `sisl_owner_name` STRING,
    `sisl_comments` STRING,
    `sisl_viability_pct` DECIMAL(5,2),
    `ssuc_seed_use_code` STRING,
    `sisl_active_ind` STRING,
    `bioz_zone_id` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_qb_valuation (
    `tso_code` STRING,
    `nav_name` STRING,
    `tenure` STRING,
    `licence_id` STRING,
    `permit_id` STRING,
    `mark_id` STRING,
    `block_id` STRING,
    `block_nbr` STRING,
    `sppr_effective_date` TIMESTAMP,
    `hauling_source` STRING,
    `hauling_cost` DECIMAL(38,10),
    `silviculture_source` STRING,
    `silviculture_cost` DECIMAL(38,10),
    `stumpage_source` STRING,
    `stumpage_cost` DECIMAL(38,10),
    `indirect_source` STRING,
    `indirect_cost` DECIMAL(38,10),
    `roads_source` STRING,
    `roads_cost` DECIMAL(38,10),
    `tree_to_truck_source` STRING,
    `tree_to_truck_cost` DECIMAL(38,10),
    `item1_source` STRING,
    `item1` DECIMAL(38,10),
    `item2_source` STRING,
    `item2` DECIMAL(38,10),
    `item3_source` STRING,
    `item3` DECIMAL(38,10),
    `item4_source` STRING,
    `item4` DECIMAL(38,10),
    `item5_source` STRING,
    `item5` DECIMAL(38,10),
    `item6_source` STRING,
    `item6` DECIMAL(38,10),
    `item7_source` STRING,
    `item7` DECIMAL(38,10),
    `item8_source` STRING,
    `item8` DECIMAL(38,10),
    `item9_source` STRING,
    `item9` DECIMAL(38,10),
    `item10_source` STRING,
    `item10` DECIMAL(38,10),
    `mill_mill_id` STRING,
    `mill_id` STRING,
    `user_id` STRING,
    `estim_date` TIMESTAMP,
    `lcst_seq_nbr` DECIMAL(15,0),
    `lcst_item6_source` STRING,
    `block_avg_cost` DECIMAL(38,10),
    `coastal_appr_rate` DECIMAL(38,10),
    `profit_risk_pct` DECIMAL(38,10),
    `block_cruise_vol` DECIMAL(38,10),
    `appr_road_bridge_cost` DECIMAL(38,10),
    `appr_road_culvert_cost` DECIMAL(38,10),
    `appr_road_road_cost` DECIMAL(38,10),
    `appr_road_land_use_cost` DECIMAL(38,10),
    `appr_road_sp_op_cost` DECIMAL(38,10),
    `appr_road_eng_proj_cost` DECIMAL(38,10),
    `appr_road_cost` DECIMAL(38,10),
    `est_bonus_bid_coeff` DECIMAL(38,10),
    `road_cost_acti_construction` DECIMAL(38,10),
    `road_cost_acti_inspection` DECIMAL(38,10),
    `road_cost_acti_maintenance` DECIMAL(38,10),
    `road_cost_acti_deactivation` DECIMAL(38,10),
    `road_cost_acti_access_control` DECIMAL(38,10),
    `road_cost_acti_assessment` DECIMAL(38,10),
    `road_cost_stru_general` DECIMAL(38,10),
    `road_cost_stru_inspection` DECIMAL(38,10),
    `road_cost_stru_maintenance` DECIMAL(38,10),
    `road_cost_stru_replacement` DECIMAL(38,10),
    `road_cost_total` DECIMAL(38,10),
    `silviculture_cost_pl_total` DECIMAL(38,10),
    `silviculture_cost_su_total` DECIMAL(38,10),
    `silviculture_cost_br_total` DECIMAL(38,10),
    `silviculture_cost_sp_total` DECIMAL(38,10),
    `silviculture_cost_gen_total` DECIMAL(38,10),
    `silviculture_cost_total` DECIMAL(38,10),
    `indirect_cost_total` DECIMAL(38,10),
    `hauling_comment_cnt` DECIMAL(38,10),
    `stumpage_comment_cnt` DECIMAL(38,10),
    `indirect_comment_cnt` DECIMAL(38,10),
    `ttt_comment_cnt` DECIMAL(38,10),
    `silviculture_comment_cnt` DECIMAL(38,10),
    `roads_comment_cnt` DECIMAL(38,10),
    `item1_comment_cnt` DECIMAL(38,10),
    `item2_comment_cnt` DECIMAL(38,10),
    `item3_comment_cnt` DECIMAL(38,10),
    `item4_comment_cnt` DECIMAL(38,10),
    `item5_comment_cnt` DECIMAL(38,10),
    `item6_comment_cnt` DECIMAL(38,10),
    `item7_comment_cnt` DECIMAL(38,10),
    `item8_comment_cnt` DECIMAL(38,10),
    `item9_comment_cnt` DECIMAL(38,10),
    `item10_comment_cnt` DECIMAL(38,10),
    `licn_seq_nbr` DECIMAL(15,0),
    `perm_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_riparian_zone (
    `cutb_seq_nbr` DECIMAL(15,0),
    `silp_seq_nbr` DECIMAL(15,0),
    `divi_div_nbr` DECIMAL(2,0),
    `ripz_lake_id` STRING,
    `ripz_class` STRING,
    `rimz_zone_id` STRING,
    `rizd_buffer_width` DECIMAL(13,4),
    `rizd_basal_area_min` DECIMAL(10,2),
    `rizd_basal_area_max` DECIMAL(10,2),
    `rizd_density` DECIMAL(10,1),
    `rizd_harvesting_ind` STRING,
    `rizd_su_cross_reference` STRING,
    `rizd_comments` STRING,
    `rizd_area` DECIMAL(7,2),
    `rizd_digitised_ind` STRING,
    `rizd_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road (
    `road_seq_nbr` DECIMAL(16,0),
    `divi_div_nbr` DECIMAL(2,0),
    `division` STRING,
    `division_name` STRING,
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_name` STRING,
    `road_type` STRING,
    `road_desc` STRING,
    `uri` STRING,
    `rdst_steward_name` STRING,
    `state` STRING,
    `start_station` DECIMAL(13,4),
    `end_station` DECIMAL(13,4),
    `length` DECIMAL(38,10),
    `modified_date` TIMESTAMP,
    `modifiedby` STRING,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `station_source` STRING,
    `adj_valid` STRING,
    `spatial_title` STRING,
    `owner` STRING,
    `valid_mark` STRING,
    `within_alr` STRING,
    `road_seq_nbr_lng` DECIMAL(20,0),
    `field_team` STRING,
    `rcls_accounting_type` STRING,
    `modifiedon` TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_activity (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `road_road_desc` STRING,
    `uri` STRING,
    `rcom_start_metre_nbr` DECIMAL(13,4),
    `rcom_end_metre_nbr` DECIMAL(13,4),
    `rcom_planned_date` TIMESTAMP,
    `rcom_completion_date` TIMESTAMP,
    `rcom_constr_type` STRING,
    `rins_start_metre_nbr` DECIMAL(13,4),
    `rins_end_metre_nbr` DECIMAL(13,4),
    `rins_planned_date` TIMESTAMP,
    `rins_inspection_date` TIMESTAMP,
    `rins_condition_desc` STRING,
    `rins_insp_type` STRING,
    `mtce_start_metre_nbr` DECIMAL(13,4),
    `mtce_end_metre_nbr` DECIMAL(13,4),
    `mtce_planned_date` TIMESTAMP,
    `mtce_completion_date` TIMESTAMP,
    `mtce_method` STRING,
    `deac_start_metre_nbr` DECIMAL(13,4),
    `deac_end_metre_nbr` DECIMAL(13,4),
    `deac_planned_date` TIMESTAMP,
    `deac_start_date` TIMESTAMP,
    `deac_end_date` TIMESTAMP,
    `deac_level_type` STRING,
    `deac_access_type` STRING,
    `deac_method` STRING,
    `acon_start_metre_nbr` DECIMAL(13,4),
    `acon_end_metre_nbr` DECIMAL(13,4),
    `acon_planned_date` TIMESTAMP,
    `acon_start_date` TIMESTAMP,
    `acon_end_date` TIMESTAMP,
    `acon_access_type` STRING,
    `acon_method` STRING,
    `rass_start_metre_nbr` DECIMAL(13,4),
    `rass_end_metre_nbr` DECIMAL(13,4),
    `rass_planned_date` TIMESTAMP,
    `rass_completion_date` TIMESTAMP,
    `rass_type` STRING,
    `road_seq_nbr` DECIMAL(16,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_activity_cost_row (
    `divi_div_nbr` DECIMAL(2,0),
    `racm_activity_type` STRING,
    `csti_cost_item_id` STRING,
    `csti_cost_item_description` STRING,
    `raco_item_cost` DECIMAL(15,2),
    `raco_cost_type` STRING,
    `raco_cost_date` TIMESTAMP,
    `raco_comment` STRING,
    `raco_seq_nbr` DECIMAL(15,0),
    `racm_seq_nbr` DECIMAL(15,0),
    `ract_seq_id` STRING,
    `ract_seq_nbr` DECIMAL(38,10)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_activity_row (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `meth_activity_type` STRING,
    `meth_method_type` STRING,
    `pers_display_name` STRING,
    `ctor_name` STRING,
    `start_station` DECIMAL(13,4),
    `end_station` DECIMAL(13,4),
    `planned_date` TIMESTAMP,
    `start_date` TIMESTAMP,
    `completion_date` TIMESTAMP,
    `budgeted_cost` DECIMAL(15,2),
    `actual_cost` DECIMAL(15,2),
    `memo` STRING,
    `rstr_at_metre_nbr` DECIMAL(38,10),
    `structure_id` STRING,
    `structure_file_id` STRING,
    `inspect_frequency` DECIMAL(38,10),
    `ract_seq_id` STRING,
    `ract_seq_nbr` DECIMAL(15,0),
    `road_seq_nbr` DECIMAL(16,0),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_agri_land_reserve (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `field_team_desc` STRING,
    `manu_id` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `ralr_start_metre_nbr` DECIMAL(13,4),
    `ralr_end_metre_nbr` DECIMAL(13,4),
    `ralr_length` DECIMAL(38,10),
    `ralr_within_alr_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `ralr_seq_nbr` DECIMAL(15,0),
    `ralr_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_assessment (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `meth_activity_type` STRING,
    `rass_start_metre_nbr` DECIMAL(13,4),
    `rass_end_metre_nbr` DECIMAL(13,4),
    `rass_planned_date` TIMESTAMP,
    `rass_completion_date` TIMESTAMP,
    `meth_method_type` STRING,
    `rass_status` STRING,
    `rass_responsibility` STRING,
    `rass_contractor` STRING,
    `rass_activity_memo` STRING,
    `rass_budgeted_cost` DECIMAL(15,2),
    `rass_actual_cost` DECIMAL(15,2),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(15,0),
    `rass_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_class (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `rcls_start_metre_nbr` DECIMAL(13,4),
    `rcls_end_metre_nbr` DECIMAL(13,4),
    `rcls_length` DECIMAL(38,10),
    `rcls_class_type` STRING,
    `rcls_road_type` STRING,
    `rcls_accounting_type` STRING,
    `rcls_comments` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(15,0),
    `rcls_seq_nbr` DECIMAL(15,0),
    `rcls_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_completion (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `rcom_start_metre_nbr` DECIMAL(13,4),
    `rcom_end_metre_nbr` DECIMAL(13,4),
    `rcom_length` DECIMAL(38,10),
    `rcom_planned_date` TIMESTAMP,
    `rcom_completion_date` TIMESTAMP,
    `rcom_stage_type` STRING,
    `rcom_surface_type` STRING,
    `rcom_actual_cost` DECIMAL(15,2),
    `rcom_budgeted_cost` DECIMAL(15,2),
    `rcom_method` STRING,
    `rcom_option_type` STRING,
    `rcom_fdp_critical` STRING,
    `rcom_joint_approval` STRING,
    `rcom_construction_type` STRING,
    `rcom_contractor` STRING,
    `rcom_accounting_type` STRING,
    `meth_activity_type` STRING,
    `meth_method_type` STRING,
    `rcom_responsibility` STRING,
    `rcom_abr_reported_ind` STRING,
    `rcom_actual_start_date` TIMESTAMP,
    `rcom_actual_completion_date` TIMESTAMP,
    `rcom_grade_percent` DECIMAL(10,0),
    `ctor_name` STRING,
    `pers_display_name` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(15,0),
    `rcom_seq_nbr` DECIMAL(15,0),
    `rcom_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_cut_block (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `rcut_start_metre_nbr` DECIMAL(13,4),
    `rcut_end_metre_nbr` DECIMAL(13,4),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `rcut_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `rcut_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_deactivation (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `deac_start_metre_nbr` DECIMAL(13,4),
    `deac_end_metre_nbr` DECIMAL(13,4),
    `deac_length` DECIMAL(38,10),
    `deac_planned_date` TIMESTAMP,
    `deac_start_date` TIMESTAMP,
    `deac_end_date` TIMESTAMP,
    `meth_activity_type` STRING,
    `meth_method_type` STRING,
    `deac_level_type` STRING,
    `deac_access_type` STRING,
    `deac_activity_memo` STRING,
    `deac_method_option_type` STRING,
    `deac_method_attribute_desc` STRING,
    `deac_fdp_critical` STRING,
    `deac_contractor` STRING,
    `deac_responsibility` STRING,
    `ctor_seq_nbr` DECIMAL(15,0),
    `pers_seq_nbr` DECIMAL(15,0),
    `cloc_seq_nbr` DECIMAL(15,0),
    `deac_budgeted_cost` DECIMAL(15,2),
    `deac_actual_cost` DECIMAL(15,2),
    `deac_abr_reported_ind` STRING,
    `deac_actual_start_date` TIMESTAMP,
    `deac_actual_completion_date` TIMESTAMP,
    `deac_completion_date` TIMESTAMP,
    `ctor_name` STRING,
    `pers_display_name` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `deac_seq_nbr` DECIMAL(15,0),
    `deac_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_event_mapping (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `divi_division_name` STRING,
    `field_team_desc` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `road_road_name` STRING,
    `uri` STRING,
    `poc` DECIMAL(13,4),
    `pot` DECIMAL(38,10),
    `prime_road_seq_nbr` DECIMAL(38,10),
    `rdpm_permit_type` STRING,
    `rdpm_permit_id` STRING,
    `ruse_section_id` STRING,
    `rdst_steward_name` STRING,
    `rsta_status_code` STRING,
    `rsta_road_state` STRING,
    `rcls_class_type` STRING,
    `rcls_road_type` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_inspection (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `rins_start_metre_nbr` DECIMAL(13,4),
    `rins_end_metre_nbr` DECIMAL(13,4),
    `rins_length` DECIMAL(38,10),
    `rins_planned_date` TIMESTAMP,
    `rins_inspection_date` TIMESTAMP,
    `rins_condition_desc` STRING,
    `rins_performed_by_name` STRING,
    `rins_activity_memo` STRING,
    `rins_inspection_type` STRING,
    `rins_option_type` STRING,
    `rins_inspection_file_id` STRING,
    `meth_activity_type` STRING,
    `meth_method_type` STRING,
    `rins_responsibility` STRING,
    `inspected_by_person` STRING,
    `rins_inspector_type` STRING,
    `rins_budgeted_cost` DECIMAL(15,2),
    `rins_actual_cost` DECIMAL(15,2),
    `rins_actual_start_date` TIMESTAMP,
    `rins_actual_completion_date` TIMESTAMP,
    `rins_integrity_ok` STRING,
    `rins_drainage_ok` STRING,
    `rins_industrial_use_ok` STRING,
    `rins_revegetated` STRING,
    `ctor_name` STRING,
    `pers_display_name` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(15,0),
    `rins_seq_nbr` DECIMAL(15,0),
    `rins_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_linear_struct (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `rlst_start_metre_nbr` DECIMAL(13,4),
    `rlst_end_metre_nbr` DECIMAL(13,4),
    `abre_struct_class` STRING,
    `abrf_struct_type_fd` STRING,
    `abrw_struct_type_wall` STRING,
    `rlst_eng_ind` STRING,
    `rlst_offset_nbr` DECIMAL(13,4),
    `rlst_road_side` STRING,
    `rlst_reported_ind` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `rlst_seq_nbr` DECIMAL(15,0),
    `rlst_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_maintenance (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `meth_activity_type` STRING,
    `meth_method_type` STRING,
    `mtce_start_metre_nbr` DECIMAL(13,4),
    `mtce_end_metre_nbr` DECIMAL(13,4),
    `mtce_length` DECIMAL(38,10),
    `mtce_planned_date` TIMESTAMP,
    `mtce_completion_date` TIMESTAMP,
    `mtce_activity_memo` STRING,
    `mtce_method_option_type` STRING,
    `mtce_method_attribute_desc` STRING,
    `mtce_contractor` STRING,
    `rins_seq_nbr` DECIMAL(15,0),
    `mtce_responsibility` STRING,
    `ctor_name` STRING,
    `pers_display_name` STRING,
    `cloc_seq_nbr` DECIMAL(15,0),
    `mtce_budgeted_cost` DECIMAL(15,2),
    `mtce_actual_cost` DECIMAL(15,2),
    `mtce_actual_start_date` TIMESTAMP,
    `mtce_actual_completion_date` TIMESTAMP,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `mtce_seq_nbr` DECIMAL(15,0),
    `mtce_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_management_unit (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `road_name` STRING,
    `uri` STRING,
    `field_team_desc` STRING,
    `manu_id` STRING,
    `manu_name` STRING,
    `rmanu_start_metre_nbr` DECIMAL(13,4),
    `rmanu_end_metre_nbr` DECIMAL(13,4),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(15,0),
    `manu_seq_nbr` DECIMAL(15,0),
    `rmanu_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_mapsheet (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `rmap_start_metre_nbr` DECIMAL(13,4),
    `rmap_end_metre_nbr` DECIMAL(13,4),
    `maps_mapsheet_id` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `rmap_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_on_block (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `field_team_desc` STRING,
    `manu_id` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `ronb_start_metre_nbr` DECIMAL(13,4),
    `ronb_end_metre_nbr` DECIMAL(13,4),
    `ronb_length` DECIMAL(38,10),
    `licn_licence_id` STRING,
    `mark_mark_id` STRING,
    `cutb_block_id` STRING,
    `ronb_section_id` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `ronb_seq_nbr` DECIMAL(15,0),
    `road_seq_nbr` DECIMAL(16,0),
    `licn_seq_nbr` DECIMAL(15,0),
    `mark_seq_nbr` DECIMAL(15,0),
    `cutb_seq_nbr` DECIMAL(15,0),
    `ronb_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_op_area (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `ropa_start_metre_nbr` DECIMAL(13,4),
    `ropa_end_metre_nbr` DECIMAL(13,4),
    `ropa_length` DECIMAL(38,10),
    `ropa_operating_area` STRING,
    `ropa_working_circle` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `ropa_seq_nbr` DECIMAL(15,0),
    `ropa_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_organic_mat (
    `road_seq_nbr` DECIMAL(16,0),
    `divi_div_nbr` DECIMAL(2,0),
    `road_road_name` STRING,
    `uri` STRING,
    `romt_start_metre_nbr` DECIMAL(13,4),
    `romt_end_metre_nbr` DECIMAL(13,4),
    `abrm_feature_type` STRING,
    `romt_reported_ind` STRING,
    `romt_seq_nbr` DECIMAL(15,0),
    `romt_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_permit (
    `rdpm_seq_nbr` DECIMAL(15,0),
    `tso_code` STRING,
    `dsct_district_name` STRING,
    `lsee_licensee_id` STRING,
    `rdpm_permit_id` STRING,
    `rdpm_permit_type` STRING,
    `rdpm_expiry_date` TIMESTAMP,
    `rdpm_start_date` TIMESTAMP,
    `rdpm_primary_user` STRING,
    `rdpm_secondary_user` STRING,
    `rdpm_comments` STRING,
    `rdpm_amendment_nbr` STRING,
    `rdpm_timber_mark_id` STRING,
    `rdpm_parent_perm_id` STRING,
    `road_initial_road_length` DECIMAL(11,2),
    `road_current_road_length` DECIMAL(11,2),
    `road_amendment_reason` STRING,
    `rdpm_cascade_split_code` STRING,
    `rdpm_mgmt_unit_type` STRING,
    `rdpm_mgmt_unit_id` STRING,
    `rdpm_amendment_type` STRING,
    `rdpm_application_desc` STRING,
    `rdpm_deemed_owner_ind` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_activity_cost (
    `divi_div_nbr` DECIMAL(2,0),
    `racm_activity_type` STRING,
    `csti_cost_item_id` STRING,
    `csti_cost_item_description` STRING,
    `raco_item_cost` DECIMAL(15,2),
    `raco_cost_type` STRING,
    `raco_cost_date` TIMESTAMP,
    `raco_comment` STRING,
    `rcom_seq_nbr` DECIMAL(15,0),
    `rins_seq_nbr` DECIMAL(15,0),
    `mtce_seq_nbr` DECIMAL(15,0),
    `deac_seq_nbr` DECIMAL(15,0),
    `acon_seq_nbr` DECIMAL(15,0),
    `rass_seq_nbr` DECIMAL(15,0),
    `insp_seq_nbr` DECIMAL(15,0),
    `main_seq_nbr` DECIMAL(15,0),
    `repl_seq_nbr` DECIMAL(15,0),
    `rstr_seq_nbr` DECIMAL(15,2)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_prov_forest (
    `road_seq_nbr` DECIMAL(16,0),
    `divi_div_nbr` DECIMAL(2,0),
    `road_road_name` STRING,
    `uri` STRING,
    `rprf_start_metre_nbr` DECIMAL(13,4),
    `rprf_end_metre_nbr` DECIMAL(13,4),
    `rprf_length` DECIMAL(38,10),
    `rprf_conflict_code` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `rprf_seq_nbr` DECIMAL(15,0),
    `rprf_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_radio (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `rrad_start_metre_nbr` DECIMAL(13,4),
    `rrad_end_metre_nbr` DECIMAL(13,4),
    `rrad_channel_name` STRING,
    `rrad_transmit_nbr` DECIMAL(15,6),
    `rrad_receive_nbr` DECIMAL(15,6),
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `rrad_seq_nbr` DECIMAL(15,0),
    `rrad_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_risk (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `rrra_risk_rating` STRING,
    `rris_start_metre_nbr` DECIMAL(13,4),
    `rris_end_metre_nbr` DECIMAL(13,4),
    `rris_insp_freq` DECIMAL(3,0),
    `rris_adjusted_insp_freq` DECIMAL(3,0),
    `rris_memo` STRING,
    `rris_data_source` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `rris_seq_nbr` DECIMAL(15,0),
    `rris_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_section (
    `road_seq_nbr` DECIMAL(38,10),
    `poc` DECIMAL(13,4),
    `pot` DECIMAL(38,10)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_spatial_meta (
    `road_seq_nbr` DECIMAL(16,0),
    `divi_div_nbr` DECIMAL(2,0),
    `road_road_name` STRING,
    `uri` STRING,
    `rspm_start_metre_nbr` DECIMAL(13,4),
    `rspm_end_metre_nbr` DECIMAL(13,4),
    `rspm_length` DECIMAL(38,10),
    `abru_data_source` STRING,
    `rspm_source_date` TIMESTAMP,
    `gmlc_capture_method` STRING,
    `rspm_observation_date` TIMESTAMP,
    `abrh_horizontal_datum` STRING,
    `abrz_vertical_datum` STRING,
    `abrs_ccsm_code` STRING,
    `abro_coordinate_system` STRING,
    `rspm_data_accuracy` STRING,
    `rspm_horiz_accuracy` STRING,
    `rspm_vert_accuracy` STRING,
    `rspm_reported_ind` STRING,
    `rspm_description` STRING,
    `rspm_seq_nbr` DECIMAL(15,0),
    `rspm_seq_nbr_lng` DECIMAL(20,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_road_status (
    `divi_div_nbr` DECIMAL(2,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `field_team_desc` STRING,
    `road_road_name` STRING,
    `uri` STRING,
    `rsta_start_metre_nbr` DECIMAL(13,4),
    `rsta_end_metre_nbr` DECIMAL(13,4),
    `rsta_length` DECIMAL(38,10),
    `rsta_status_code` STRING,
    `rsta_status_changed_date` TIMESTAMP,
    `rsta_road_state` STRING,
    `modifiedby` STRING,
    `modifiedon` TIMESTAMP,
    `modifiedusing` STRING,
    `createdby` STRING,
    `createdon` TIMESTAMP,
    `createdusing` STRING,
    `road_seq_nbr` DECIMAL(16,0),
    `rsta_seq_nbr_lng` DECIMAL(20,0),
    `rsta_cprp_protection_ind` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_licence (
    `licn_seq_nbr` DECIMAL(15,0),
    `tent_seq_nbr` DECIMAL(15,0),
    `ctor_seq_nbr` DECIMAL(15,0),
    `cloc_seq_nbr` DECIMAL(15,0),
    `tso_code` STRING,
    `tso_name` STRING,
    `nav_name` STRING,
    `licence_id` STRING,
    `category` STRING,
    `tenure` STRING,
    `licensee` STRING,
    `registrant` STRING,
    `registrant_city` STRING,
    `field_team` STRING,
    `district_name` STRING,
    `divi_div_nbr` DECIMAL(2,0),
    `licn_category_id` STRING,
    `blaz_admin_zone_id` STRING,
    `blaz_admin_zone_desc` STRING,
    `licn_licence_state` STRING,
    `licn_licence_desc` STRING,
    `licn_licence_to_cut_code` STRING,
    `linc_cert_level_id` STRING,
    `licn_digi_ind` STRING,
    `licn_salvage_ind` STRING,
    `licn_apportion_tenure_type` STRING,
    `apportion_type` STRING,
    `partition_type` STRING,
    `commit_licence_type` STRING,
    `commit_volume` DECIMAL(38,10),
    `remain_commit_volume` DECIMAL(38,10),
    `bchh_billing_year` STRING,
    `manu_seq_nbr` DECIMAL(15,0)
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lrm_replication.v_block_spatial (
    `tso_code` STRING,
    `nav_name` STRING,
    `licence_id` STRING,
    `permit_id` STRING,
    `mark_id` STRING,
    `block_id` STRING,
    `cutb_seq_nbr` DECIMAL(15,0),
    `spatial_flag` STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
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
