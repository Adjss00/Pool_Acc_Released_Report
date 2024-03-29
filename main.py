from helpers.ObjectExtractor import ObjectExtractor
from controllers.AccReleased import AccReleased

if __name__ == "__main__":
    #* Access data to SF
    username = 'jesus.sanchez@engen.com.mx'
    password = '21558269Antonio#'
    security_token = 'WqRoCDbMwhMPZ62iWUcXnmbmg'

    #* helpers/ObjectExtractor
    extractor = ObjectExtractor(username, password, security_token)

    objects_to_extract = [
        ('Account', 'accounts_df', ['Id', 'Name','Region__c', 'Account_Type__c', 'Vertical_Focus__c', 'ParentId', 'ACC_tx_Account_Status__c', 'CreatedDate', 'OwnerId', 'ACC_tx_EXT_REF_ID__c']),
        ('User', 'users_df', ['Id', 'Name', 'IsActive', 'USU_ls_Region__c']),
        ('Event', 'events_df', ['Id', 'ActivityDate', 'AccountId', 'OwnerId', 'OwnerName__c']),
        ('Opportunity', 'opportunities_df', ['Id', 'AccountId', 'Name', 'StageName', 'CreatedDate', 'OwnerId', 'OPP_ls_Region__c', 'OPP_tx_EXT_REF_ID__c']),
    ]

    extracted_dataframes = extractor.extract_objects_to_dataframes(objects_to_extract)

    #* controllers/AccReleased
    data_processor = AccReleased(extracted_dataframes['accounts_df'], extracted_dataframes['users_df'], extracted_dataframes['opportunities_df'], extracted_dataframes['events_df'])
    data_processor.insert_top_column()
    data_processor.insert_owner_column()
    data_processor.filter_owner_active()
    data_processor.filter_owner_region()
    data_processor.insert_top_parent_column()
    data_processor.insert_top_parent()
    data_processor.map_accounts_name()
    data_processor.map_accounts_top_ref()
    data_processor.sort_and_group_events_df()
    data_processor.insert_id_meeting()
    data_processor.merge_activity_date()
    data_processor.calculate_days_difference()
    data_processor.filter_opportunities_by_stage()
    data_processor.map_top_parent_to_opportunities()
    data_processor.mark_latest_opportunity()
    data_processor.mark_latest_activity()
    data_processor.filter_latest_opportunities()
    data_processor.map_stage_to_accounts()
    #data_processor.calculate_days_difference_opps()
    data_processor.fill_missing_days_diff_citas()
    data_processor.add_cita_six_month_column()
    #data_processor.add_opps_six_month_column()
    #data_processor.add_released_column()
    data_processor.filter_vertical_focus()
    data_processor.filter_account_type()
    data_processor.filter_account_status()
    data_processor.filter_region()
    data_processor.filter_owner_names()

    #Print DF
    data_processor.imprimir_dfs()

    #Exportar archivo
    file_path = "out/Pool_Account_Released.xlsx"
    data_processor.export_to_excel(file_path)

    #Exportar Grupos
    file_path_group = "out/release"
    data_processor.export_released_pools_active(file_path_group)

    #Exportar Grupos
    file_path_inactive = "out/release_non_active"
    data_processor.export_released_pools_inactive(file_path_inactive)

