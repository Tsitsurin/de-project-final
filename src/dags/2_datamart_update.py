from typing import Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.decorators import dag

import os
import pandas as pd
import pendulum
import vertica_python

import logging

date = '2022-10-05'

VERTICA_CONN = {
        'host': 'vertica.tgcloudenv.ru',
        'port': '5433',
        'user': 'stv2023121121',
        'password': 'ck13ILubHgAPlNo'
}

def datamart_update(date:str):

    sql_file_path = os.path.join(os.path.dirname(__file__), 'datamart_update.sql')

    with open(sql_file_path, 'r') as file:
        sql_vertica_datamart = file.read().replace('_date_',date)

    try:
        vertica_conn = vertica_python.connect(**VERTICA_CONN)
        cur = vertica_conn.cursor()
        cur.execute(sql_vertica_datamart)
        vertica_conn.commit()
    finally:
        logging.info(f"datamart global_metrics has been updated")
        if cur:
            cur.close()
        if vertica_conn:
            vertica_conn.close()

@dag(schedule_interval=None, start_date=pendulum.parse('2022-01-01'), tags=['final', 'project', '2'])
def datamart_update_dag():
    start_task = DummyOperator(task_id='start_task')
    end_task = DummyOperator(task_id='end_task')

    update_datamart = PythonOperator(
        task_id="datamart_update", python_callable=datamart_update, 
        op_kwargs={'date': date,},
        )
    
    start_task >> update_datamart >> end_task
    

dag = datamart_update_dag()