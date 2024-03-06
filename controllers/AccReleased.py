import pandas as pd
from datetime import datetime
import xlsxwriter

class AccReleased:
    def __init__(self, accounts_df, users_df, opportunities_df, events_df):
        self.accounts_df = accounts_df
        self.users_df = users_df
        self.opportunities_df = opportunities_df
        self.events_df = events_df
        self.nombre_df = None 

    def imprimir_dfs(self):
        print("Accounts DataFrame:")
        print(self.accounts_df)
        print("\nUsers DataFrame:")
        print(self.users_df)
        print("\nOpportunities DataFrame:")
        print(self.opportunities_df)
        print("\nEvents DataFrame:")
        print(self.events_df)


    def insert_top_column(self):
        # Crear un diccionario de mapeo entre 'Id' y 'Name' del DataFrame accounts_df
        id_to_name_mapping = self.accounts_df.set_index('Id')['Name'].to_dict()
        
        # Mapear los valores de 'ParentId' en accounts_df utilizando el diccionario de mapeo
        self.accounts_df['TOP'] = self.accounts_df['ParentId'].map(id_to_name_mapping)

    def insert_owner_column(self):
        mapping = self.users_df.set_index('Id')['Name'].to_dict()
        
        self.accounts_df['Owner Name'] = self.accounts_df['OwnerId'].map(mapping)

    def filter_owner_names(self):
        # Lista de nombres a filtrar
        owners_to_remove = ['Axel Martinez', 'Alejandro Arreola', 'Owner IT Calidad', 'Servicios Salesforce', 'Soporte Salesforce', 'Rodolfo Escalante']
        
        # Filtrar el DataFrame accounts_df
        self.accounts_df = self.accounts_df[~self.accounts_df['Owner Name'].isin(owners_to_remove)]

    def insert_top_parent_column(self):
        # Hacer una copia del DataFrame accounts_df
        self.accounts_df = self.accounts_df.copy()
        
        # Si la columna 'TOP' está vacía, colocar el 'Name' en la columna 'Top Parent'
        self.accounts_df.loc[self.accounts_df['TOP'].isna(), 'Top Parent'] = self.accounts_df['Name']
        
        # Si la columna 'TOP' no está vacía, colocar ese dato en la columna 'Top Parent'
        self.accounts_df['Top Parent'].fillna(self.accounts_df['TOP'], inplace=True)

    def insert_top_parent(self):
        # Hacer una copia del DataFrame accounts_df
        self.accounts_df = self.accounts_df.copy()
        
        # Si la columna 'TOP' está vacía, colocar el 'Name' en la columna 'Top Parent'
        self.accounts_df.loc[self.accounts_df['TOP'].isna(), 'Top Parent'] = self.accounts_df['Name']
        
        # Si la columna 'TOP' no está vacía, colocar ese dato en la columna 'Top Parent'
        self.accounts_df['Top Parent'].fillna(self.accounts_df['TOP'], inplace=True)
        
        # Ordenar el DataFrame por la columna 'Top Parent'
        self.accounts_df.sort_values(by='Top Parent', inplace=True)

    def map_accounts_name(self):
        # Crear un mapeo entre los valores de 'Id' y 'ACC_tx_EXT_REF_ID__c' del DataFrame de cuentas
        id_to_ext_ref_mapping = self.accounts_df.set_index('Id')['Name'].to_dict()

        # Mapear los valores de 'AccountId' en el DataFrame de eventos utilizando el mapeo
        self.events_df['Account Name'] = self.events_df['AccountId'].map(id_to_ext_ref_mapping)

    def map_accounts_ext_ref(self):
        # Crear un mapeo entre los valores de 'Id' y 'ACC_tx_EXT_REF_ID__c' del DataFrame de cuentas
        id_to_ext_ref_mapping = self.accounts_df.set_index('Id')['ACC_tx_EXT_REF_ID__c'].to_dict()

        # Mapear los valores de 'AccountId' en el DataFrame de eventos utilizando el mapeo
        self.events_df['Accounts EXT REF'] = self.events_df['AccountId'].map(id_to_ext_ref_mapping)

    def map_accounts_top_ref(self):
        # Crear un mapeo entre los valores de 'Id' y 'ACC_tx_EXT_REF_ID__c' del DataFrame de cuentas
        id_to_ext_ref_mapping = self.accounts_df.set_index('Id')['Top Parent'].to_dict()

        # Mapear los valores de 'AccountId' en el DataFrame de eventos utilizando el mapeo
        self.events_df['Top Parent'] = self.events_df['AccountId'].map(id_to_ext_ref_mapping)

    def sort_and_group_events_df(self):
        # Ordenar el DataFrame por 'Top Parent' de manera ascendente y luego por 'ActivityDate' de manera descendente
        sorted_events_df = self.events_df.sort_values(by=['Top Parent', 'ActivityDate'], ascending=[True, False])
        
        # Agrupar el DataFrame por 'Top Parent' y tomar el primer registro de cada grupo
        grouped_events_df = sorted_events_df.groupby('Top Parent').first().reset_index()
        
        return grouped_events_df

    def insert_id_meeting(self):
        # Obtener el DataFrame agrupado de eventos
        grouped_events_df = self.sort_and_group_events_df()
        
        # Fusionar el DataFrame de cuentas con el DataFrame agrupado de eventos en 'Top Parent'
        merged_df = pd.merge(self.accounts_df, grouped_events_df[['Top Parent', 'Id']], on='Top Parent', how='left')
        
        # Renombrar la columna 'Id' a 'IdMeeting'
        merged_df.rename(columns={'Id': 'IdMeeting'}, inplace=True)
        
        # Actualizar el DataFrame de cuentas con los valores fusionados
        self.accounts_df = merged_df

    def merge_activity_date(self):
        # Crear un diccionario donde la clave es el 'Id' y el valor es el 'ActivityDate' del DataFrame de eventos
        events_dict = self.events_df.set_index('Id')['ActivityDate'].to_dict()
        
        # Mapear los valores de 'Id_y' en el DataFrame de cuentas utilizando el diccionario de eventos
        self.accounts_df['ActivityDate'] = self.accounts_df['Id_y'].map(events_dict)
        
        return self.accounts_df
    
    def calculate_days_difference(self):
        # Convertir la columna 'ActivityDate' en objetos de fecha y hora si no lo está
        if not pd.api.types.is_datetime64_any_dtype(self.accounts_df['ActivityDate']):
            self.accounts_df['ActivityDate'] = pd.to_datetime(self.accounts_df['ActivityDate'], errors='coerce')
        
        # Obtener la fecha y hora actual
        now = datetime.now()
        
        # Calcular la diferencia en días entre la fecha de hoy y la columna 'ActivityDate'
        self.accounts_df['Days Diff Citas'] = (now - self.accounts_df['ActivityDate']).dt.days
        
        return self.accounts_df
    
    def filter_opportunities_by_stage(self):
        # Valores de StageName que deseas mantener
        desired_stages = ['Lead', 'Backlog with fundings', 'Backlog', 'In Credit', 'Opportunity Identified', 'Proposal Awarded', 'Proposal']
        
        # Filtrar el DataFrame de oportunidades por los valores deseados en StageName
        filtered_opportunities = self.opportunities_df[self.opportunities_df['StageName'].isin(desired_stages)]
        
        # Actualizar el DataFrame de oportunidades con el resultado filtrado
        self.opportunities_df = filtered_opportunities
    
    def map_top_parent_to_opportunities(self):
        # Crear un diccionario con clave: Id_x y valor: Top Parent del DataFrame de cuentas
        accounts_dict = dict(zip(self.accounts_df['Id_x'], self.accounts_df['Top Parent']))
        
        # Mapear el valor de Top Parent del diccionario de cuentas al DataFrame de oportunidades
        self.opportunities_df['Top Parent Acc'] = self.opportunities_df['AccountId'].map(accounts_dict)

    def mark_latest_opportunity(self):
        # Agrupar por Top Parent Acc y encontrar la fecha más reciente en cada grupo
        latest_dates = self.opportunities_df.groupby('Top Parent Acc')['CreatedDate'].max().reset_index()

        # Fusionar la información de las fechas más recientes con el DataFrame de oportunidades
        self.opportunities_df = pd.merge(self.opportunities_df, latest_dates, on=['Top Parent Acc', 'CreatedDate'], how='left')

        # Crear una nueva columna llamada 'Last Created Date' e insertar "1" si es la fecha más reciente, de lo contrario "0"
        self.opportunities_df['Last Created Date'] = 0
        self.opportunities_df.loc[self.opportunities_df.groupby('Top Parent Acc')['CreatedDate'].idxmax(), 'Last Created Date'] = 1

    def filter_latest_opportunities(self):
        # Filtrar el DataFrame de oportunidades por 'Last Created Date' igual a 1
        filtered_opportunities = self.opportunities_df[self.opportunities_df['Last Created Date'] == 1]

        # Crear un diccionario con clave: Top Parent Acc y valor: CreatedDate del DataFrame filtrado de oportunidades
        opps_dict = dict(zip(filtered_opportunities['Top Parent Acc'], filtered_opportunities['CreatedDate']))

        # Comparar la clave del diccionario con la columna Top Parent de accounts y añadir la columna Created Date Opp
        self.accounts_df['Created Date Opp'] = self.accounts_df['Top Parent'].map(opps_dict)

    def map_stage_to_accounts(self):
        # Paso 1: Filtrar opportunities_df por Last Created Date igual a 1
        latest_opportunities = self.opportunities_df[self.opportunities_df['Last Created Date'] == 1]
        
        # Paso 2: Crear un diccionario con clave: Top Parent Acc y valor: StageName
        top_parent_stage_dict = dict(zip(latest_opportunities['Top Parent Acc'], latest_opportunities['StageName']))

        # Paso 3: Mapear el valor de StageName del diccionario al DataFrame accounts_df
        self.accounts_df['Stage Opp'] = self.accounts_df['Top Parent'].map(top_parent_stage_dict)

        # Paso 4: Rellenar los valores NaN con una cadena vacía en la columna Stage Opp
        self.accounts_df['Stage Opp'].fillna('', inplace=True)

    def calculate_days_difference_opps(self):
        # Obtener la fecha y hora actuales en formato UTC
        today_utc = datetime.utcnow()

        try:
            # Convertir la columna 'Created Date Opp' al formato de fecha si está presente
            if 'Created Date Opp' in self.accounts_df.columns:
                # Verificar si las fechas están en el formato correcto antes de la conversión
                if self.accounts_df['Created Date Opp'].dtype == 'object':
                    self.accounts_df['Created Date Opp'] = pd.to_datetime(self.accounts_df['Created Date Opp'], errors='coerce')
            
            # Calcular la diferencia entre la fecha y hora actuales y 'Created Date Opp' si está presente, de lo contrario, dejarlo vacío
            if 'Created Date Opp' in self.accounts_df.columns:
                # Convertir las fechas a un formato que no incluya la zona horaria
                self.accounts_df['Created Date Opp'] = pd.to_datetime(self.accounts_df['Created Date Opp']).dt.tz_localize(None)

                # Calcular la diferencia de días
                self.accounts_df['Days Diff Opps'] = (today_utc - self.accounts_df['Created Date Opp']).dt.days.fillna('')
        except Exception as e:
            print("Error:", e)

    def add_cita_six_month_column(self):
        # Añadir una nueva columna 'Cita_Six_Month' basada en la condición de 'Days Diff Citas'
        self.accounts_df['Citas < 6?'] = self.accounts_df['Days Diff Citas'].apply(lambda x: 'No' if x > 182.5 else 'Sí')

    def add_opps_six_month_column(self):
        # Convertir la columna 'Days Diff Opps' a números si es posible
        self.accounts_df['Days Diff Opps'] = pd.to_numeric(self.accounts_df['Days Diff Opps'], errors='coerce')
        
        # Añadir una nueva columna 'Opps_Six_Month' basada en la condición de 'Days Diff Opps'
        self.accounts_df['Opps < 6?'] = self.accounts_df.apply(lambda row: 'Sí' if row['Days Diff Opps'] < 182.5 else 'No' if pd.notnull(row['Days Diff Opps']) else '', axis=1)

    def fill_missing_days_diff_citas(self):
        try:
            # Verificar si la columna 'Days Diff Citas' está presente en el DataFrame
            if 'Days Diff Citas' in self.accounts_df.columns:
                # Rellenar los valores vacíos con 365
                self.accounts_df['Days Diff Citas'].fillna(365, inplace=True)
        except Exception as e:
            print("Error:", e)

    def mark_latest_activity(self):
        # Agrupar por 'Top Parent' y encontrar la fecha más reciente en cada grupo
        latest_dates = self.events_df.groupby('Top Parent')['ActivityDate'].max().reset_index()

        # Fusionar la información de las fechas más recientes con el DataFrame de eventos
        self.events_df = pd.merge(self.events_df, latest_dates, on=['Top Parent', 'ActivityDate'], how='left')

        # Crear una nueva columna llamada 'Last Activity Date' e insertar "1" si es la fecha más reciente, de lo contrario "0"
        self.events_df['Last Activity Date'] = 0
        self.events_df.loc[self.events_df.groupby('Top Parent')['ActivityDate'].idxmax(), 'Last Activity Date'] = 1


    def add_released_column(self):
        # Añadir una nueva columna 'Released?' basada en las condiciones especificadas
        self.accounts_df['Released?'] = ''
        self.accounts_df.loc[(self.accounts_df['Citas < 6?'] == 'No') & (self.accounts_df['Opps < 6?'] == 'Sí'), 'Released?'] = 'No'
        self.accounts_df.loc[(self.accounts_df['Citas < 6?'] == 'No') & (self.accounts_df['Opps < 6?'] == 'No'), 'Released?'] = 'Sí'
        self.accounts_df.loc[(self.accounts_df['Citas < 6?'] == 'Sí') & (self.accounts_df['Opps < 6?'] == 'Sí'), 'Released?'] = 'No'
        self.accounts_df.loc[(self.accounts_df['Citas < 6?'] == 'Sí') & (self.accounts_df['Opps < 6?'] == 'No'), 'Released?'] = 'No'
        self.accounts_df.loc[(self.accounts_df['Citas < 6?'] == 'Sí') & (self.accounts_df['Opps < 6?'] == ''), 'Released?'] = 'No'
        self.accounts_df.loc[(self.accounts_df['Citas < 6?'] == 'No') & (self.accounts_df['Opps < 6?'] == ''), 'Released?'] = 'Sí'
        
    def export_to_excel(self, file_path):
        # Crear un ExcelWriter para escribir en el archivo de Excel
        writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
        
        # Exportar el DataFrame accounts_df a una hoja llamada 'Accounts'
        self.accounts_df.to_excel(writer, sheet_name='Accounts', index=False)
        
        # Exportar el DataFrame events_df a una hoja llamada 'Events'
        self.events_df.to_excel(writer, sheet_name='Events', index=False)

        self.opportunities_df.to_excel(writer, sheet_name='Opps', index = False)
        
        # Guardar y cerrar el archivo de Excel
        writer.close()
