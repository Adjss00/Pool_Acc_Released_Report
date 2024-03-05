from simple_salesforce import Salesforce
import pandas as pd

class ObjectExtractor:
    def __init__(self, username, password, security_token, domain='login'):
        # Inicializa el objeto ObjectExtractor con los datos de inicio de sesión de Salesforce
        self.username = username
        self.password = password
        self.security_token = security_token
        self.domain = domain
        # Crea una instancia de Salesforce para interactuar con la API de Salesforce
        self.sf = Salesforce(username=self.username, password=self.password, security_token=self.security_token, domain=self.domain)

    def extract_objects_to_dataframes(self, objects_to_extract):
        # Método para extraer objetos de Salesforce a DataFrames de Pandas
        dataframes = {}
        # Itera sobre la lista de objetos a extraer
        for obj_name, df_name, columns_to_extract in objects_to_extract:
            # Obtiene los metadatos del objeto específico
            metadata_objeto = self.sf.__getattr__(obj_name).describe()

            # Extrae solo los nombres de columna solicitados
            if columns_to_extract:
                nombres_campos = [campo['name'] for campo in metadata_objeto['fields'] if campo['name'] in columns_to_extract]
            else:
                nombres_campos = [campo['name'] for campo in metadata_objeto['fields']]  # Extrae todos si no hay una selección explícita

            # Construye la cadena de consulta basada en las columnas extraídas
            query_string = "SELECT {} FROM {}".format(", ".join(nombres_campos), obj_name)

            # Realiza la consulta y recupera los datos
            registros = self.sf.query_all(query_string)

            # Extrae datos de los registros
            registros_data = []
            for registro in registros['records']:
                registro_data = {}
                for campo, valor in registro.items():
                    if campo != 'attributes': 
                        registro_data[campo] = valor
                registros_data.append(registro_data)

            # Crea un DataFrame de Pandas y lo asigna al nombre especificado
            dataframes[df_name] = pd.DataFrame(registros_data)
            print(f'Dataframe [{df_name}] generado...')
        return dataframes