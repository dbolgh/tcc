# app.py
import dash
from sqlalchemy import create_engine
from dash import dash_table, html, dcc, Input, Output
from dash.exceptions import PreventUpdate
import pandas as pd
import dash_bootstrap_components as dbc
from datetime import date

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

cred_dict = {
    "host": '<my-db-endpoint>',
    "port": 5432,
    "user": "<my_user>",
    "password": '<my-password>',
    "database": '<my-db>'
}

'postgresql://username:password@localhost:5432/mydatabase'

engine = create_engine(f'postgresql://{cred_dict["user"]}:{cred_dict["password"]}@{cred_dict["host"]}:{cred_dict["port"]}/{cred_dict["database"]}')

query = """
        select * from postgres.financial_data.final_financial_data
        """

df = pd.read_sql(query, engine)

list_of_companies = list(sorted(df["nome_empresa"].unique()))
list_of_accounts = list(sorted(df["nome_conta"].unique()))
min_year = int(df["ano_ref"].min())
max_year = int(df["ano_ref"].max())


app.layout = html.Div([
    html.Header([
        html.Div("Dashboard Financeiro"),
    ], style={'display': 'grid', 'grid-template-columns':'1fr'}),
    html.Div([
        html.Div([html.Div("Escolha A Empresa"), dcc.Dropdown(list_of_companies, id='dropdown-company-name')], style={"padding":'15px'}),
        html.Div([html.Div("Escolha A Conta Contábil"), dcc.Dropdown(list_of_accounts, id='dropdown-type-account')], style={"padding":'15px'}),
        html.Div([html.Div("Escolha O Período"), 
        dcc.RangeSlider(
            id='my-range-slider',
            marks={i: str(i) for i in range(min_year, max_year)},
            step=1, 
            # style={"padding":'15px'}
            # value=[-5, 5]
            )], style={"padding":'15px'})       
    ], style={'display': 'grid', 'grid-template-columns':'1fr 1fr 1fr', 'padding':'50px'} ),
    html.Div([
        html.Div("Graph Placeholder", id='graph-placeholder'),
        html.Div("table Placeholder", id='table-company-data'),
    ], style={'display': 'grid', 'grid-template-columns':'1fr 1fr', 'padding':'10px'} )
])


# @app.callback(
#     Output('graph-placeholder', 'children'),
#     Input('my-range-slider', 'value'),
# )
# def update_table(slider_value):
#     if slider_value is None or slider_value == '':
#         raise PreventUpdate
#     return f"{slider_value[0]}, {slider_value[1]}"

@app.callback(
    Output('table-company-data', 'children'),
    Output('graph-placeholder', 'children'),
    Input('dropdown-company-name', 'value'),
    Input('dropdown-type-account', 'value'),
    Input('my-range-slider', 'value')
)
def update_table(filter_value, account_value,slider_value):
    if (filter_value is None or filter_value == '') or (account_value is None or account_value == '') or (slider_value is None or slider_value == ''):
        return "No Data to Display.", "" 
    else:
        print(range(slider_value[0], slider_value[1]+1))
        # Filter the DataFrame based on the input value
        filtered_df = (df[(df['nome_empresa'] == filter_value) & (df['nome_conta'] == account_value) & (df['ano_ref'].isin([i for i in range(slider_value[0], slider_value[1]+1)]))]
                       .sort_values(by=['ano_ref', 'qtr'], ascending=True)
                       [['nome_empresa', 'nome_conta', 'ano_ref', 'valor']])
        
        print(filtered_df)
        return dash_table.DataTable(
                        id='my-datatable',
                        columns=[{'name': col, 'id': col} for col in filtered_df.columns],
                        data=filtered_df.to_dict('records')
                    ), dcc.Graph(
                        figure = {'data': [
                                        {'x': filtered_df['ano_ref'], 'y': filtered_df['valor'], 'type': 'bar', 'name': 'Line'}
                                            ],
                                            'layout': {
                                                'title': f'{account_value}',
                                            }
                                        })

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)