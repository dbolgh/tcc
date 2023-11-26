import logging
import azure.functions as func


app = func.FunctionApp()

@app.schedule(schedule="*/3 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    import io
    import zipfile
    import pandas as pd
    from sqlalchemy import create_engine
    import psycopg2
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    import time
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
    from io import BytesIO
    import json 
    import re

    def get_response(url):

        response = requests.get(url)
        print('Status code: ', response.status_code)
        return response


    def get_links(response, url):
        soup = BeautifulSoup(response.text)

        list_of_final_links = []

        links_list = soup.find_all('a', href=True)
        for item in links_list:
            link_str = item.get_text()
            if "zip" in link_str:

                list_of_final_links.append(url + link_str)

        return list_of_final_links


    def download_extract_zip(response, df_to_append, type_of_report):
        
        if response.status_code == 200:
            # Open the zip file in memory
            print("Opening zip file")
            with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zip_ref:
                # List the files in the zip archive
                print("File unzip successful.")
                print(f"retrieving list of files")            
                file_list = zip_ref.namelist()
                file_list_final = [file_name for file_name in file_list if type_of_report in file_name]
                print(f"the files to be downloaded are: {file_list_final}")
                # Iterate over each Excel file in the zip archive

                df_to_append = pd.DataFrame()

                for file_name in file_list_final:
                    print(f"Reading file {file_name}")
                    if (file_name.endswith('.csv') or file_name.endswith('.xls')):
                        # Read the Excel file into a DataFrame
                        print(f"Creating dataframe for file {file_name}")
                        with zip_ref.open(file_name) as excel_file:
                            df = pd.DataFrame(excel_file)
                            df['decoded'] = df[0].apply(lambda x: x.decode('1252'))
                            df_final = df[['decoded']]
                            df_final = df_final['decoded'].str.split(';', expand=True)#.explode('decoded').reset_index(drop=True)
                            if type_of_report == "DRE":
                                col = 14
                            else:
                                col = 13
                            df_final[col] = df_final[col].apply(lambda x: 'S' if 'S' in x else 'N')
                            df_final.columns = df_final.iloc[0]
                            df_final = df_final.rename(columns={"S":"ST_CONTA_FIXA"})
                            df_final = df_final.drop(df_final.index[0]).reset_index(drop=True)
                            df_final['file_origin'] = file_name
                            df_final['DT_REFER'] = pd.to_datetime(df_final['DT_REFER'])
                            # df_final['VL_CONTA'] = df_final['VL_CONTA'].astype('float')

                            print(df_final.info())
                            print(f"DataFrame for file {file_name} completed.")
                            df_to_append = pd.concat([df_to_append, df_final], ignore_index=1)

                return df_to_append

    def create_dim_tables(df, list_of_cols, id_col_name):

            df = (df[list_of_cols]
                .drop_duplicates()
                .reset_index(drop=True)
                .reset_index()
                .rename(columns={"index": id_col_name}))
            return df

    def create_fact_table(df, df_dim, cols_to_merge, cols_to_drop):

        df_fact = df.merge(df_dim, on=cols_to_merge, how='left').drop(columns=cols_to_drop, axis=1)

        return df_fact



    def compare_tables(df, df_to_compare, cols_to_compare):    

        df_merged = df.merge(df_to_compare, on=cols_to_compare, how='outer', indicator=True)

        df_difference = df_merged[df_merged['_merge'] == 'right_only']

        df_difference_final = df_difference.drop('_merge', axis=1)

        return df_difference_final

    def create_pg_engine(engine_credentials):

        engine = create_engine(f'postgresql://{engine_credentials["username"]}:{engine_credentials["pswd"]}@{engine_credentials["host"]}:{engine_credentials["port"]}/{engine_credentials["db_name"]}')

        return engine


    def save_dataframe_to_postgres(engine, df, table_name, schema_name):

        df.to_sql(table_name, engine, schema=schema_name, if_exists="append", index=0)

        return None

    def read_data_from_postgres(engine, table_name, schema_name):

        query = f"""
                select * from postgres.{schema_name}.{table_name}
                """

        df = pd.read_sql(query, engine)

        return df

    def run_query(engine):
        
        query = """
                create table postgres.financial_data.fact_bpa (
                    VL_CONTA float,
                    id_empresa int4,
                    id_grupo_dfp int2,
                    id_ordem_exec int2,
                    id_plano_contas int4,
                    id_versao int2,
                    id_datas int2
                )
                """
        
        keepalive_kwargs = {
        "keepalives": 1,
        "keepalives_idle": 60,
        "keepalives_interval": 10,
        "keepalives_count": 5
        }

        conn = psycopg2.connect(engine, **keepalive_kwargs)
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()

        return None


    def create_fact_table_run(type_of_df):

        list_of_comps = [
            'BCO BRADESCO S.A.',
            'BCO BRASIL S.A.',
            'BRF S.A.',
            'CSN MINERAÇÃO S.A.',
            'GOL LINHAS AEREAS INTELIGENTES S.A.',
            'ITAU UNIBANCO HOLDING S.A.',
            'LOCALIZA RENT A CAR S.A.',
            'PETROLEO BRASILEIRO S.A. PETROBRAS',
            'RAIZEN ENERGIA S.A.',
            'VALE S.A.',
        ]


        import os
        list_of_files = os.listdir(r"C:\Users\danil\Documents\tcc\files")

        df0 = pd.DataFrame()

        df1 = read_file_from_blob(type_of_df, 'update_file')
        df1 = df1[df1['DENOM_CIA'].isin(list_of_comps)]

        df0 = pd.concat([df0, df1], ignore_index=1)

        dict_creds = {
        'username': 'admin_user',
        'pswd': 'Ju160189',
        'port': 5432,
        'db_name': 'postgres',
        'host': 'tcc-fia-server.postgres.database.azure.com'
        }

        engine = create_pg_engine(dict_creds)

        df_dim_empresas = read_data_from_postgres(engine, 'dim_empresas', 'financial_data')[['id_empresa', 'CNPJ_CIA']]
        df_dim_grupo_dfp = read_data_from_postgres(engine, 'dim_grupo_dfp', 'financial_data')
        df_dim_ordem_exerc = read_data_from_postgres(engine, 'dim_ordem_exerc', 'financial_data')
        df_dim_plano_contas = read_data_from_postgres(engine, 'dim_plano_contas', 'financial_data')[['CD_CONTA', 'DS_CONTA', 'ST_CONTA_FIXA', 'id_plano_contas']]
        df_dim_versao = read_data_from_postgres(engine, 'dim_versao', 'financial_data')
        df_dim_datas = read_data_from_postgres(engine, 'dim_datas', 'financial_data')
        print(df_dim_datas)


        df_fact = create_fact_table(df0, df_dim_empresas, 'CNPJ_CIA', ['CNPJ_CIA', 'DENOM_CIA', 'CD_CVM'])
        df_fact = create_fact_table(df_fact, df_dim_grupo_dfp, 'GRUPO_DFP', ['GRUPO_DFP'])
        df_fact = create_fact_table(df_fact, df_dim_ordem_exerc, 'ORDEM_EXERC', ['ORDEM_EXERC'])
        df_fact = create_fact_table(df_fact, df_dim_plano_contas, ['CD_CONTA', 'DS_CONTA', 'ST_CONTA_FIXA'], ['CD_CONTA', 'DS_CONTA', 'ST_CONTA_FIXA'])
        df_fact = create_fact_table(df_fact, df_dim_versao, 'VERSAO', ['VERSAO'])
        if type_of_df == 'DRE':
            df_fact = create_fact_table(df_fact, df_dim_datas, ['DT_REFER', 'DT_FIM_EXERC', 'DT_INI_EXERC'], ['DT_REFER', 'DT_FIM_EXERC', 'DT_INI_EXERC'])
        else:
            df_fact = create_fact_table(df_fact, df_dim_datas, ['DT_REFER', 'DT_FIM_EXERC'], ['DT_REFER', 'DT_FIM_EXERC'])
        df_fact = df_fact.drop(columns=['MOEDA', 'ESCALA_MOEDA', 'file_origin'])

        # df_fact['id_empresa'] = df_fact['id_empresa'].astype('int16')
        df_fact['id_grupo_dfp'] = df_fact['id_grupo_dfp'].astype('int8')
        df_fact['id_ordem_exec'] = df_fact['id_ordem_exec'].astype('int8')
        # df_fact['id_plano_contas'] = df_fact['id_plano_contas'].astype('int16')
        # df_fact['id_datas'] = df_fact['id_datas'].astype('int16')
        df_fact['id_versao'] = df_fact['id_versao'].astype('int8')
        df_fact['VL_CONTA'] = df_fact['VL_CONTA'].astype('float64')

        print(df_fact.info())

        query = f"""truncate table financial_data.fact_{type_of_df.lower()}"""

        run_query(engine, query)

        save_dataframe_to_postgres(engine, df_fact, f'fact_{type_of_df.lower()}', 'financial_data')



    def create_dims():

        list_of_comps = [
            'BCO BRADESCO S.A.',
            'BCO BRASIL S.A.',
            'BRF S.A.',
            'CSN MINERAÇÃO S.A.',
            'GOL LINHAS AEREAS INTELIGENTES S.A.',
            'ITAU UNIBANCO HOLDING S.A.',
            'LOCALIZA RENT A CAR S.A.',
            'PETROLEO BRASILEIRO S.A. PETROBRAS',
            'RAIZEN ENERGIA S.A.',
            'VALE S.A.',
        ]


        dict_of_tables_bpa = {
                'dim_empresas':
                    {'list_of_cols':["CNPJ_CIA", "DENOM_CIA", "CD_CVM"], 
                    'id_col_name':'id_empresa'}, 

                'dim_plano_contas':
                    {'list_of_cols':["CD_CONTA", "DS_CONTA", "ST_CONTA_FIXA"], 
                    'id_col_name':'id_plano_contas'}, 

                'dim_grupo_dfp':
                    {'list_of_cols':["GRUPO_DFP"], 
                    'id_col_name':'id_grupo_dfp'}, 

                'dim_ordem_exerc':
                    {'list_of_cols':["ORDEM_EXERC"], 
                    'id_col_name':'id_ordem_exec'}, 

                'dim_datas':
                    {'list_of_cols':["DT_REFER", "DT_INI_EXERC", "DT_FIM_EXERC"], 
                    'id_col_name':'id_datas'}, 

                'dim_versao':
                    {'list_of_cols':["VERSAO"], 
                    'id_col_name':'id_versao'}      
                    }
        
        dict_creds = {
        'username': 'admin_user',
        'pswd': 'Ju160189',
        'port': 5432,
        'db_name': 'postgres',
        'host': 'tcc-fia-server.postgres.database.azure.com'
        }

        import os
        list_of_files = os.listdir(r"C:\Users\danil\Documents\tcc\files")

        df0 = pd.DataFrame()

        for item in list_of_files:
            df1 = pd.read_parquet(os.path.join(r"C:\Users\danil\Documents\tcc\files", item))
            df1 = df1[df1['DENOM_CIA'].isin(list_of_comps)]

            df0 = pd.concat([df0, df1], ignore_index=1)

        print(df0)

        engine = create_pg_engine(dict_creds)
        
        for key in dict_of_tables_bpa.keys():
            
            # print(key)
            # print(dict_of_tables_bpa[key]['list_of_cols'])
            # print(dict_of_tables_bpa[key]['id_col_name'])

            df_dim = create_dim_tables(df0, dict_of_tables_bpa[key]['list_of_cols'], dict_of_tables_bpa[key]['id_col_name'])
            print(df_dim)
            save_dataframe_to_postgres(engine, df_dim, key, 'financial_data')
            
    def update_dim_tables():
        
        dict_of_tables_bpp = {
                'dim_empresas':
                    {'list_of_cols':["CNPJ_CIA", "DENOM_CIA", "CD_CVM"], 
                    'id_col_name':'id_empresa'}, 

                'dim_plano_contas':
                    {'list_of_cols':["CD_CONTA", "DS_CONTA", "ST_CONTA_FIXA"], 
                    'id_col_name':'id_plano_contas'}, 

                'dim_grupo_dfp':
                    {'list_of_cols':["GRUPO_DFP"], 
                    'id_col_name':'id_grupo_dfp'}, 

                'dim_ordem_exerc':
                    {'list_of_cols':["ORDEM_EXERC"], 
                    'id_col_name':'id_ordem_exec'}, 

                'dim_datas':
                    {'list_of_cols':["DT_REFER", "DT_INI_EXERC"], 
                    'id_col_name':'id_datas'}, 

                'dim_versao':
                    {'list_of_cols':["VERSAO"], 
                    'id_col_name':'id_versao'}      
                    }
        
        dict_creds = {
        'username': 'admin_user',
        'pswd': 'Ju160189',
        'port': 5432,
        'db_name': 'postgres',
        'host': 'tcc-fia-server.postgres.database.azure.com'
        }

        df0 = pd.read_parquet(r"C:\Users\danil\Documents\tcc\files\bpa_dfp.parquet")

        engine = create_pg_engine(dict_creds)
        
        for key in dict_of_tables_bpp.keys():

            df_atual = read_data_from_postgres(engine, key, 'financial_data')

            df_dim = create_dim_tables(df0, dict_of_tables_bpp[key]['list_of_cols'], dict_of_tables_bpp[key]['id_col_name'])

            df_compared = compare_tables(df_atual[dict_of_tables_bpp[key]['list_of_cols']], df_dim[dict_of_tables_bpp[key]['list_of_cols']], dict_of_tables_bpp[key]['list_of_cols'])

            id_max = df_atual[dict_of_tables_bpp[key]['id_col_name']].max() + 1
            print(id_max)

            df_compared = df_compared.reset_index()

            # df_compared['index'] = df_compared['index'] + id_max

            df_compared = df_compared.rename(columns={'index':dict_of_tables_bpp[key]['id_col_name']})

            print(df_compared)

            save_dataframe_to_postgres(engine, df_compared, key, 'financial_data')


    def save_df_to_blob(df, type_of_df, folder):

        connection_string = "DefaultEndpointsProtocol=https;AccountName=tccfiablob;AccountKey=Lgmkj8iU3vmZHPCG04jTbVX19ItFb7zjTJmtDLUo0KYMQqBSWbiXAMsdjxi7+JzfLUBe3nw0l7wU+AStHZDmig==;EndpointSuffix=core.windows.net"
        container_name = "tccfiacontainer"
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        data_buffer = BytesIO()
        df.to_parquet(data_buffer, index=False)
        df_data = data_buffer.getvalue()

        # Get a BlobClient for your container
        blob_client = blob_service_client.get_container_client(container_name)

        # Specify the blob name (replace "your_blob_name.csv" with your desired blob name)
        blob_name = f"{folder}/dfp_{type_of_df.lower()}.parquet"

        # Upload the CSV data to the blob
        blob_client.upload_blob(name=blob_name, data=df_data, overwrite=True)

        print(f"DataFrame successfully uploaded to Azure Blob Storage. Blob Name: {blob_name}")



    def read_file_from_blob(type_of_df, folder):

        connection_string = "DefaultEndpointsProtocol=https;AccountName=tccfiablob;AccountKey=Lgmkj8iU3vmZHPCG04jTbVX19ItFb7zjTJmtDLUo0KYMQqBSWbiXAMsdjxi7+JzfLUBe3nw0l7wU+AStHZDmig==;EndpointSuffix=core.windows.net"
        container_name = "tccfiacontainer"
        blob_name = f"dfp_{type_of_df.lower()}.parquet"

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        blob_data = blob_client.download_blob()
        blob_stream = BytesIO(blob_data.readall())

        df = pd.read_parquet(blob_stream)

        return df



    def save_file_to_blob(file, filename):
        
        connection_string = "DefaultEndpointsProtocol=https;AccountName=tccfiablob;AccountKey=Lgmkj8iU3vmZHPCG04jTbVX19ItFb7zjTJmtDLUo0KYMQqBSWbiXAMsdjxi7+JzfLUBe3nw0l7wU+AStHZDmig==;EndpointSuffix=core.windows.net"
        container_name = "tccfiacontainer"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        blob_client = blob_service_client.get_container_client(container_name)

        blob_name = filename

        blob_client.upload_blob(name=blob_name, data=file, overwrite=True)




    def compare_update_file():

        if read_file_from_blob() == json.loads(get_files_metadata()):
            print(True)
            return True
        return False



    def read_file_from_blob():

        connection_string = "DefaultEndpointsProtocol=https;AccountName=tccfiablob;AccountKey=Lgmkj8iU3vmZHPCG04jTbVX19ItFb7zjTJmtDLUo0KYMQqBSWbiXAMsdjxi7+JzfLUBe3nw0l7wU+AStHZDmig==;EndpointSuffix=core.windows.net"
        container_name = "tccfiacontainer"
        blob_name = "update_info.json"

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        blob_data = blob_client.download_blob()
        json_text = blob_data.readall()

        json_data = json.loads(json_text)

        return json_data


    def get_files_metadata():

        url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"

        response = requests.get(url)
        soup = BeautifulSoup(response.text)
        files_text = soup.text.split("Index of /dados/CIA_ABERTA/DOC/DFP/DADOS/../")[1:]
        list_to_search = [files_text[0].split(r"M")][0]

        pat_name = r'dfp.*zip'
        pat_time = r'\s\d.*:\d{2}'

        list_names = []
        list_times = []

        for item in list_to_search:
            # if item[0] is not None or item[0] != "":
            try:
                name = re.findall(pat_name, item)
                list_names.append(name[0])
                time = re.findall(pat_time, item)    
                list_times.append(time[0])

            except:
                pass

        import json

        df_final = dict(zip(list_names, list_times))
        final_file = json.dumps(df_final)
        return final_file



        
    def create_files_metadata():

        json_file = get_files_metadata()
        save_file_to_blob(json_file, 'update_info.json')

    def create_files_metadata_2():

        json_file = get_files_metadata()
        save_file_to_blob(json_file, 'update_info2.json')


    def main_2():

        df = read_file_from_blob("DRE")
        print(df)


    def main(type_of_df):
        
        url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"

        response = get_response(url)
        list_of_links = get_links(response, url)

        df0 = pd.DataFrame()

        for item in list_of_links:

            print(item)

            response_zip = get_response(item)

            for item in [type_of_df]:

                df = download_extract_zip(response_zip, df0, item)

                df0 = pd.concat([df0, df], ignore_index=1)
                print(df0)
        
        save_df_to_blob(df0, type_of_df, 'update_file')


    if __name__ == 'function_app':
        if not compare_update_file():
            for item in ['DRE', 'BPA', 'BPP']:
                main(item)
                create_fact_table_run(item)
        print("update Complete.")

    logging.info('Python timer trigger function executed.')