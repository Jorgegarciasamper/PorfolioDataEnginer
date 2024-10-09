"""
*******************************************************************
Author = @jorge -- https://github.com/jorgegarciasamper           *
Date = '09/10/2024'                                               *
Description = Parallel data mining from multiple football leagues *
*******************************************************************
"""


import json
from datetime import datetime, timedelta
import os
import logging
import airflow
from airflow.models import Variable
from airflow import models
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
import snowflake.connector as sf
import pandas as pd
import time
import random
import os
from utils import get_data,data_processing,extract_league_data
from datetime import datetime




default_arguments = {   'owner': 'Jordi',
                        'email': 'jgarcia.samper@knowmadmood.com',
                        'retries':1 ,
                        'retry_delay':timedelta(minutes=5)}


with DAG('FOOTBAL_LEAGUES',
         default_args=default_arguments,
         description='Extracting Data Footbal League' ,
         start_date = datetime(2022, 9, 21),
         schedule_interval = '*/10 * * * *',
         tags=['tabla_espn'],
         catchup=False) as dag :


        params_info = Variable.get("feature_info", deserialize_json=True)
        df = pd.read_csv('/usr/local/airflow/df_ligas.csv')
         # Convertir el DataFrame a un diccionario
        leagues_dict = pd.Series(df['URL'].values, index=df['LIGA']).to_dict()

        df_team = pd.read_csv('/usr/local/airflow/team_table.csv')

          # Crear tareas de extracción por liga
        extract_tasks = {}
        for league, url in leagues_dict.items():
            extract_task = PythonOperator(
                task_id=f'extract_{league.lower()}_data',
                python_callable=extract_league_data,
                op_kwargs={'league': league, 'url': url},
            )
            extract_tasks[league] = extract_task

        # Tarea de carga en la etapa de Snowflake
        upload_stage = SnowflakeOperator(
            task_id='upload_data_stage',
            sql='./queries/upload_stage.sql',
            snowflake_conn_id='demo_snow_new_1',
            warehouse=params_info["DWH"],
            database=params_info["DB"],
            role=params_info["ROLE"],
            params=params_info
        )

        # Tarea para ingerir datos en la tabla de Snowflake
        ingest_table = SnowflakeOperator(
            task_id='ingest_table',
            sql='./queries/upload_table.sql',
            snowflake_conn_id='demo_snow_new_1',
            warehouse=params_info["DWH"],
            database=params_info["DB"],
            role=params_info["ROLE"],
            params=params_info
        )

        # Configurar dependencias
        for task in extract_tasks.values():
            task >> upload_stage

        upload_stage >> ingest_table