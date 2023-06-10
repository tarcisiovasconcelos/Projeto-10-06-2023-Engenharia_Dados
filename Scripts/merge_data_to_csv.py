from datetime import datetime
import pytz
import psycopg2
import pandas as pd
import os


def merge_data_to_csv(**context):
    # Conexão com o banco de dados
    conn = psycopg2.connect(host='xxx.xx.x.x', port=5434, database='engenharia', user='postgres', password='xxxxx')
    cursor = conn.cursor()

    # Consulta para obter a união das tabelas dadosOMS e dadosCDC
    query_union = """
    SELECT o.data_notificacao, o.total_acumulado_mundo, c.total_acumulado_eua
    FROM dadosOMS o
    INNER JOIN dadosCDC c ON o.data_notificacao = c.data_notificacao
    """
    cursor.execute(query_union)
    result = cursor.fetchall()

    # Criação de um DataFrame com o resultado da consulta
    df_result = pd.DataFrame(result, columns=['data_notificacao', 'total_acumulado_mundo', 'total_acumulado_eua'])

    # Imprimir o DataFrame resultante
    print(df_result)

    # Consulta adicional para obter dados da tabela dadosCDC2
    query_extra = """
    SELECT c2.data_notificacao, c2.estado, c2.total_acumulado_eua
    FROM dadosCDC2 c2
    INNER JOIN dadosOMS o ON c2.data_notificacao = o.data_notificacao
    """
    cursor.execute(query_extra)
    result_extra = cursor.fetchall()

    # Criação de um DataFrame com o resultado da consulta adicional
    df_extra = pd.DataFrame(result_extra, columns=['data_notificacao', 'estado', 'total_acumulado_eua'])

    # Imprimir o DataFrame adicional
    print(df_extra)

    # Imprimir a data e hora no horário da Argentina
    tz_ar = pytz.timezone('America/Argentina/Buenos_Aires')
    now_ar = datetime.now(tz_ar)
    print("Data e hora no horário da Argentina:", now_ar)

    # Fechamento da conexão
    cursor.close()
    conn.close()

    data_dir = os.path.join(os.path.expanduser("~"), "airflow", "data")
    file_path = os.path.join(data_dir, "merged_data.csv")
    file_path_extra = os.path.join(data_dir, "csv_extra.csv")

    # Salva o DataFrame resultante como arquivo CSV
    df_result.to_csv(file_path, index=False)

    # Salva o DataFrame adicional como arquivo CSV
    df_extra.to_csv(file_path_extra, index=False)
