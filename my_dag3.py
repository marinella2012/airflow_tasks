import datetime
import json
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.settings import engine
from airflow.models import Variable
import os


args = {
    'owner': 'marinella',  # Информация о владельце DAG
    'start_date': datetime.datetime(2021, 8, 23),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': datetime.timedelta(minutes=1),
    'depends_on_past': False,
    'provide_context': True    # Передаем ли мы контекст
}

# глобальные переменные
folder = os.path.expanduser('~')+'/data_dags/'
json_name_file = 'data_json.json'
csv_name_file = 'data_csv.csv'
key_api = Variable.get("KEY_API")
сity = 'Nizhny Novgorod'
moscow_timezone = 3


def extract_data(**kwargs):
    ti = kwargs['ti']
    # Запрос погоды
    response = requests.get(
            'http://api.worldweatheronline.com/premium/v1/weather.ashx',
            params={
                'q': '{}'.format(сity),
                'format': 'json',
                'FX': 'no',
                'num_of_days': 1,
                'key': key_api,
                'includelocation': 'no'
            },
            headers={
                'Authorization': key_api
            }
        )

    if response.status_code == 200:
        # Разбор ответа сервера
        json_data = response.json()
        print(json.dumps(json_data, indent=2))
        with open(folder + json_name_file, 'w') as f:
            json.dump(json_data, f, indent=2)
            f.close()


def transform_data(**kwargs):
    ti = kwargs['ti']

    with open(folder+json_name_file, 'r') as jd:
        json_data = json.load(jd)
        print(json_data)
        jd.close()

    value_list = []

    start_utc = datetime.datetime.utcnow()
    start_moscow = start_utc + datetime.timedelta(hours=moscow_timezone)

    city = json_data['data']['request'][0]['query']
    observation_time = json_data['data']['current_condition'][0]['observation_time']
    temp = json_data['data']['current_condition'][0]['temp_C']
    humidity = json_data['data']['current_condition'][0]['humidity']

    # Время наблюденя из json'а
    value_list.append(pd.to_datetime(observation_time).strftime('%Y-%m-%d %H:%M:%S'))
    res_df = pd.DataFrame(value_list, columns=['observation_time'])
    # Время запроса (по Москве)
    res_df["date_from_msk"] = start_moscow
    res_df["date_from_msk"] = pd.to_datetime(res_df["date_from_msk"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    # Время запроса (по UTC)
    res_df["date_from_utc"] = start_utc
    res_df["date_from_utc"] = pd.to_datetime(res_df["date_from_utc"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    # Город наблюдения
    res_df["city_observation"] = city
    # Температура
    res_df["city_temp_c"] = temp
    # Влажность
    res_df["city_humidity"] = humidity

    ti.xcom_push(key='weather_info', value=res_df.to_json())


def load_data(**kwargs):
    ti = kwargs['ti']
    res_df = pd.read_json(ti.xcom_pull(key='weather_info'))
    res_df.to_sql('world_weather_1', con=engine, schema='public', if_exists='append') # взяла свободное имя таблицы для отладки
    print(res_df)

# В контексте DAG'а зададим набор task'ок
# Объект-инстанс Operator'а - это и есть task
with DAG(
        dag_id='weather_worldweatheronline_api',
        schedule_interval='@hourly',
        default_args=args,
) as dag:

    extract_data = PythonOperator(task_id='extract_data', provide_context=True, python_callable=extract_data)

    transform_data = PythonOperator(task_id='transform_data', provide_context=True, python_callable=transform_data)

    load_data = PythonOperator(task_id='load_data', provide_context=True, python_callable=load_data)

    extract_data >> transform_data >> load_data
