import os
import requests

def fetch_covid_data(**context):
    url_world = 'https://covid19.who.int/WHO-COVID-19-global-data.csv'

    try:
        response = requests.get(url_world)
        response.raise_for_status()  # Verifica se a resposta foi bem-sucedida
    except requests.exceptions.HTTPError as err:
        print("Erro HTTP: {}".format(err))
        # Aqui você pode adicionar tratamento adicional de erro, se necessário
    except requests.exceptions.RequestException as err:
        print("Erro na solicitação: {}".format(err))
        # Aqui você pode adicionar tratamento adicional de erro, se necessário

    data = response.content

    context['ti'].xcom_push(key='covid_data_world', value=data.decode('utf-8'))

def fetch_covid_eua_data(**context):
    url_eua = 'https://data.cdc.gov/resource/pwn4-m3yp.json?$limit=1000000'


    try:
        response = requests.get(url_eua)
        response.raise_for_status()  # Verifica se a resposta foi bem-sucedida
    except requests.exceptions.HTTPError as err:
        print(f"Erro HTTP: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Erro na solicitação: {err}")

    data = response.content
	

    context['ti'].xcom_push(key='covid_data_eua', value=data.decode('utf-8'))
    


