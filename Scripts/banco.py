import psycopg2
import pandas as pd
from datetime import datetime
import pytz

def insert_data_to_postgres(**context):
    # Conexão com o banco de dados
    conn = psycopg2.connect(host='xxx.xx.x.x', port=5434, database='engenharia', user='postgres', password='xxxxx')
    cursor = conn.cursor()
    print('Conectou')

    # Obtendo os dados processados
    data_world = context['ti'].xcom_pull(key='covid_data_world_modify', task_ids='process_covid_data')
    data_eua = context['ti'].xcom_pull(key='covid_data_eua_modify', task_ids='process_covid_eua_data')
    data_eua_2 = context['ti'].xcom_pull(key='covid_data_eua_modify_2', task_ids='process_covid_eua_data')
    print(data_world)
    print(data_eua)

    # Inserção dos dados da tabela dadosOMS
    data_oms = [(row['data_notificacao'], int(row['total_acumulado_mundo'])) for _, row in data_world.iterrows()]
    query_oms = "INSERT INTO dadosOMS (data_notificacao, total_acumulado_mundo) VALUES (%s, %s)"
    cursor.executemany(query_oms, data_oms)

    # Inserção dos dados da tabela dadosCDC
    data_cdc = [(row['data_notificacao'], int(str(row['total_acumulado_eua']).replace(',', '').split('.')[0])) for _, row in data_eua.iterrows()]
    query_cdc = "INSERT INTO dadosCDC (data_notificacao, total_acumulado_eua) VALUES (%s, %s)"
    cursor.executemany(query_cdc, data_cdc)

    # Inserção dos dados da tabela dadosCDC2
    data_cdc_2 = [(row['data_notificacao'], int(str(row['total_acumulado_eua']).replace(',', '').split('.')[0]), row['estado']) for _, row in data_eua_2.iterrows()]
    query_cdc_2 = "INSERT INTO dadosCDC2 (data_notificacao, total_acumulado_eua, estado) VALUES (%s, %s, %s)"
    cursor.executemany(query_cdc_2, data_cdc_2)

    print("PIPELINE FINALIZADA SEU DATAWAREHOUSE ESTÁ PRONTO PARA USO")

    # Confirmação das alterações no banco de dados
    conn.commit()

    # Fechamento da conexão
    cursor.close()
    conn.close()