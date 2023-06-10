import os
import io
import pandas as pd
import ast

def process_covid_data(**context):
    covid_data_world = context['ti'].xcom_pull(key='covid_data_world')
    # Leitura do DataFrame a partir do XCom 'covid_data_world'
    df = pd.read_csv(io.StringIO(covid_data_world))  
    # Filtrar apenas dados do COVID-19 nos EUA

    df = df[df['Country_code'] == 'US']
    # Renomear colunas
    df = df.rename(columns={
        'Date_reported': 'data_notificacao',
        'Cumulative_cases': 'total_acumulado_mundo',
    })
    # Remover colunas
    colunas_remover = ['Country_code', 'Country', 'WHO_region', 'New_cases', 'New_deaths', 'Cumulative_deaths']
    df = df.drop(columns=colunas_remover)
    # Armazenar o DataFrame modificado no XCom 'covid_data_world_modify'
    context['ti'].xcom_push(key='covid_data_world_modify', value=df)    


def process_covid_eua_data(**context):
    covid_data_eua = context['ti'].xcom_pull(key='covid_data_eua')
    # Converter a string para um dicionário Python
    data_dict = ast.literal_eval(covid_data_eua)
    # Criar o DataFrame a partir do dicionário
    df = pd.DataFrame(data_dict)
    df1 = pd.DataFrame(data_dict)


    # Renomear colunas
    df = df.rename(columns={
        'date_updated': 'data_notificacao',
        'tot_cases': 'total_acumulado_eua',
    })
    df1 = df1.rename(columns={
        'date_updated': 'data_notificacao',
        'state': 'estado',
        'tot_cases': 'total_acumulado_eua',
    })
    print('df1.columns')

    print(df1.columns)

    df1['total_acumulado_eua'] = df1['total_acumulado_eua'].astype(float).fillna(0)
    df['total_acumulado_eua'] = df['total_acumulado_eua'].astype(float).fillna(0)


    # Remover colunas
    colunas_remover = ['state','start_date','end_date','new_cases','tot_deaths','new_deaths','new_historic_cases','new_historic_deaths']
    df = df.drop(columns=colunas_remover)

    colunas_remover1 = ['start_date','end_date','new_cases','tot_deaths','new_deaths','new_historic_cases','new_historic_deaths']
    df1 = df1.drop(columns=colunas_remover1)

    # Agrupar por data e somar os total_acumulado_eua para as mesmas datas
    df = df.groupby('data_notificacao').sum().reset_index()
    # Mapear as siglas dos estados para os nomes dos estados
    estado_mapping = {
        "ND": "North Dakota",
        "NV": "Nevada",
        "OH": "Ohio",
        "GU": "Guam",
        "NY": "New York",
        "HI": "Hawaii",
        "IN": "Indiana",
        "NE": "Nebraska",
        "WV": "West Virginia",
        "FL": "Florida",
        "ME": "Maine",
        "AR": "Arkansas",
        "CT": "Connecticut",
        "SD": "South Dakota",
        "WY": "Wyoming",
        "LA": "Louisiana",
        "MT": "Montana",
        "FSM": "Federated States of Micronesia",
        "RMI": "Marshall Islands",
        "NJ": "New Jersey",
        "MI": "Michigan",
        "MP": "Northern Mariana Islands",
        "UT": "Utah",
        "SC": "South Carolina",
        "VI": "Virgin Islands",
        "DE": "Delaware",
        "PW": "Palau",
        "CA": "California",
        "NH": "New Hampshire",
        "OR": "Oregon",
        "TX": "Texas",
        "PR": "Puerto Rico",
        "KY": "Kentucky",
        "NM": "New Mexico",
        "MS": "Mississippi",
        "DC": "District of Columbia",
        "MO": "Missouri",
        "NC": "North Carolina",
        "WI": "Wisconsin",
        "RI": "Rhode Island",
        "OK": "Oklahoma",
        "ID": "Idaho",
        "GA": "Georgia",
        "MN": "Minnesota",
        "PA": "Pennsylvania",
        "MD": "Maryland",
        "AK": "Alaska",
        "IL": "Illinois",
        "TN": "Tennessee",
        "WA": "Washington",
        "MA": "Massachusetts",
        "AL": "Alabama",
        "IA": "Iowa",
        "VT": "Vermont",
        "CO": "Colorado",
        "NYC": "New York City",
        "VA": "Virginia",
        "AS": "American Samoa",
        "AZ": "Arizona",
        "KS": "Kansas"
    }
    df1['estado'] = df1['estado'].map(estado_mapping)
    print(df)
    print(df1)
    # Armazenar o DataFrame modificado no XCom 'covid_data_eua_modify'
    context['ti'].xcom_push(key='covid_data_eua_modify', value=df)
    context['ti'].xcom_push(key='covid_data_eua_modify_2', value=df1)
    
