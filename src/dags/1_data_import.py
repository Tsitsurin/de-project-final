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
import psycopg2
import datetime

from airflow.utils.task_group import TaskGroup

import logging

date = '2022-10-05'

#date = '{{ds}}'
#if date is None:
    #date = datetime.datetime.now().date().strftime("%Y-%m-%d")

output_path_csv = "/lessons/data"

# Параметры подключения к PostgreSQL
POSTGRES_CONN = { 
    'host': 'rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net',
    'port': '6432',
    'database': 'db1',
    'user': 'student',
    'password': 'de_student_112022',
}

VERTICA_CONN = {
        'host': 'vertica.tgcloudenv.ru',
        'port': '5433',
        'user': 'stv2023121121',
        'password': 'ck13ILubHgAPlNo'
}


def extract_transactions(date, output_path_csv):
    path_transactions = f"{output_path_csv}/transactions_{date}.csv"
    with psycopg2.connect(**POSTGRES_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(                
                """
                    SELECT
                        operation_id,
                        account_number_from,
                        account_number_to,
                        currency_code,
                        country,
                        status,
                        transaction_type,
                        amount,
                        transaction_dt
                    FROM
                        public.transactions
                    WHERE 
                        transaction_dt::date = %(date)s;
                """,
                {"date": date},
            )
            transactions_df = pd.DataFrame(cur.fetchall())
            transactions_df.to_csv(
                path_or_buf=path_transactions, 
                sep = ';', 
                header = False,
                encoding='utf-8',
                index=False
                )
            logging.info(f"transactions_{date}.csv has been created")


def extract_currencies(date, output_path_csv):
    path_currencies = f"{output_path_csv}/currencies_{date}.csv"
    with psycopg2.connect(**POSTGRES_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(                
                """
                    SELECT
                        date_update,
                        currency_code,
                        currency_code_with,
                        currency_with_div
                    FROM
                        public.currencies
                    WHERE 
                        date_update = %(date)s;
                """,
                {"date": date},
            )
            currencies_df = pd.DataFrame(cur.fetchall())
            currencies_df.to_csv(
                path_or_buf=path_currencies, 
                sep = ';', 
                header = False,
                encoding='utf-8',
                index=False
                )
            logging.info(f"transactions_{date}.csv has been created")



def load_currencies(date:str, output_path_csv:str):
    path_currencies = f"{output_path_csv}/currencies_{date}.csv"
    sql_vertica_currencies = f"""
        COPY STV2023121121__STAGING.currencies (
            date_update, 
            currency_code, 
            currency_code_with, 
            currency_with_div)
        FROM LOCAL '{path_currencies}'
        DELIMITER ';'
        REJECTED DATA AS TABLE STV2023121121__STAGING.currencies_date_projection; 
    """
    try:
        vertica_conn = vertica_python.connect(**VERTICA_CONN)
        cur = vertica_conn.cursor()
        cur.execute(sql_vertica_currencies)
        vertica_conn.commit()
    finally:
        logging.info(f"table currencies has been loaded to stg")
        if cur:
            cur.close()
        if vertica_conn:
            vertica_conn.close()


def load_transactions(date:str, output_path_csv:str):
    path_transactions = f"{output_path_csv}/transactions_{date}.csv"
    sql_vertica_transactions = f"""
        COPY STV2023121121__STAGING.transactions (
            operation_id,
            account_number_from,
            account_number_to,
            currency_code,
            country,
            status,
            transaction_type,
            amount,
            transaction_dt
            )
        FROM LOCAL '{path_transactions}'
        DELIMITER ';'
        REJECTED DATA AS TABLE STV2023121121__STAGING.transactions_date_projection; 
    """
    try:
        vertica_conn = vertica_python.connect(**VERTICA_CONN)
        cur = vertica_conn.cursor()
        cur.execute(sql_vertica_transactions)
        vertica_conn.commit()
    finally:
        logging.info(f"table transactions has been loaded to stg")
        if cur:
            cur.close()
        if vertica_conn:
            vertica_conn.close()

def remove_temp_files(date, output_path_csv):
    path_currencies = f"{output_path_csv}/currencies_{date}.csv"
    path_transactions = f"{output_path_csv}/transactions_{date}.csv"
    os.remove(path_currencies)
    logging.info(f"currencies for {date} has been removed")
    os.remove(path_transactions)
    logging.info(f"transactions file for {date} has been removed")


@dag(schedule_interval=None, start_date=pendulum.parse('2022-01-01'),tags=['final', 'project', '1'])
def data_import_dag():
    start_task = DummyOperator(task_id='start_task')
    end_task = DummyOperator(task_id='end_task')

    with TaskGroup("extract_data") as extract_data:
        extract_transtactions_to_csv = PythonOperator(
            task_id='extract_transtactions_to_csv',
            python_callable=extract_transactions,
            op_kwargs={
                "date": date, "output_path_csv": output_path_csv
            },
        )

        extract_currencies_to_csv = PythonOperator(
            task_id='extract_currencies_to_csv',
            python_callable=extract_currencies,
            op_kwargs={
                "date": date, "output_path_csv": output_path_csv
            },
        )

    with TaskGroup("load_data_stg") as load_data_stg:

        load_transtactions_to_stg = PythonOperator(
            task_id='load_transtactions_to_stg',
            python_callable=load_transactions,
            op_kwargs={
                "date": date, "output_path_csv": output_path_csv
            },
        )

        load_currencies_to_stg = PythonOperator(
            task_id='load_currencies_to_stg',
            python_callable=load_currencies,
            op_kwargs={
                "date": date, "output_path_csv": output_path_csv
            },
        )

    remove_files = PythonOperator(
        task_id="remove_temp_files", python_callable=remove_temp_files, 
        op_kwargs={
            'date': date, 'output_path_csv': '/lessons/data'
            },
        )
    

    start_task >> extract_data >> load_data_stg >> remove_files >> end_task
    

dag = data_import_dag()