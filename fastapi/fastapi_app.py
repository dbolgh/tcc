# main.py

from fastapi import FastAPI, HTTPException, Query
import psycopg2
from psycopg2 import sql
from pydantic import BaseModel
from typing import List
from sqlalchemy import create_engine
import pandas as pd
import json

app = FastAPI()

# Database configuration
cred_dict = {
    "host": '<my-db-endpoint>',
    "port": 5432,
    "user": "<my-user>",
    "password": '<my-password>',
    "database": '<my-db>'
}

engine = create_engine(f'postgresql://{cred_dict["user"]}:{cred_dict["password"]}@{cred_dict["host"]}:{cred_dict["port"]}/{cred_dict["database"]}')

# FastAPI routes
@app.get("/financial-data/")
async def read_financial_data(nome_conta, company_name, min_year=None,   max_year=None):

    dict_of_simplified_account_name = {
        'pl': 'Patrimônio Líquido Consolidado',
        'ativo': 'Ativo Total',
        'passivo': 'Passivo Total'
    }

    nome_conta = dict_of_simplified_account_name[nome_conta]
    try:
        # Define the SQL query to select data from the final_financial_data table
        query = f"""
            SELECT nome_empresa, nome_simp_empresa, nome_conta, ano_ref, qtr, valor, id_grupo_dfp 
            FROM postgres.financial_data.fin_financial_data 
            WHERE nome_conta = '{nome_conta}';
        """
        print(query)
        # Execute the query with the provided nome_conta parameter
        df = pd.read_sql(query, engine, dtype={'ano_ref': int}).sort_values(by='ano_ref')
        
        print(df)

        if min_year is None and max_year is None:
            df_filtered = df[(df['nome_conta'] == nome_conta) & (df['nome_simp_empresa'] == company_name)]

        elif max_year is not None and min_year is None:
            min_year = 2010            
            print(range(int(min_year), int(max_year)))
            df_filtered = df[(df['nome_conta'] == nome_conta) & (df['nome_simp_empresa'] == company_name) & (df['ano_ref'].isin(range(int(min_year), int(max_year) + 1)))]

        elif min_year is not None and max_year is None:
            max_year = 2023            
            print(range(int(min_year), int(max_year)))
            df_filtered = df[(df['nome_conta'] == nome_conta) & (df['nome_simp_empresa'] == company_name) & (df['ano_ref'].isin(range(int(min_year), int(max_year) + 1)))]


        else:
            print(range(int(min_year), int(max_year)))
            df_filtered = df[(df['nome_conta'] == nome_conta) & (df['nome_simp_empresa'] == company_name) & (df['ano_ref'].isin(range(int(min_year), int(max_year) + 1)))]


        final_dict = {int(df_filtered['ano_ref'].iloc[i]): df_filtered['valor'].iloc[i] for i in range(len(df_filtered))}     

        print(final_dict)   

        return json.dumps(final_dict)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
