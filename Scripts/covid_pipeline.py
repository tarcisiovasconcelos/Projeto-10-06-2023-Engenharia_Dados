from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from fetch_data import fetch_covid_data, fetch_covid_eua_data
from process_data import process_covid_data, process_covid_eua_data
from banco import insert_data_to_postgres
from merge_data_to_csv import merge_data_to_csv

default_args = {
    'start_date': datetime(2023, 6, 6),
    'catchup': False
}
#A pipe line chama cada script, lembrando que o fetch e o process possuem cada um duas tasks(funções)
with DAG('covid_pipeline', default_args=default_args, schedule_interval='@daily') as dag:
    fetch_covid_data_task = PythonOperator(
        task_id='fetch_covid_data',
        python_callable=fetch_covid_data
    )

    process_covid_data_task = PythonOperator(
        task_id='process_covid_data',
        python_callable=process_covid_data
    )

    fetch_covid_eua_data_task = PythonOperator(
        task_id='fetch_covid_eua_data',
        python_callable=fetch_covid_eua_data
    )

    process_covid_eua_data_task = PythonOperator(
        task_id='process_covid_eua_data',
        python_callable=process_covid_eua_data
    )

    insert_data_task = PythonOperator(
        task_id='insert_data_task',
        python_callable=insert_data_to_postgres
    )

    merge_data_to_csv_task = PythonOperator(
        task_id='merge_data_to_csv',
        python_callable=merge_data_to_csv
    )

    fetch_covid_data_task >> process_covid_data_task >> insert_data_task >> merge_data_to_csv_task
    fetch_covid_eua_data_task >> process_covid_eua_data_task >> insert_data_task >> merge_data_to_csv_task
