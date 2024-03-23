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
    sql_vertica_datamart = f"""
        MERGE INTO STV2023121121__DWH.global_metrics AS tgt USING
        (WITH t1 AS
            (-- main table with all data
        SELECT o.*,
                c.currency_with_div,
                (o.amount * c.currency_with_div) AS amount_usd
            FROM STV2023121121__STAGING.transactions o
            LEFT JOIN STV2023121121__STAGING.currencies c ON o.currency_code = c.currency_code
            WHERE c.currency_code_with = 420
                AND o.transaction_dt::date = c.date_update::date
                AND o.status = 'done'
                AND o.transaction_dt::date = '{date}' ) SELECT cast(transaction_dt AS date) AS date_update,
                                                                currency_code AS currency_from,
                                                                sum(amount_usd) AS amount_total,
                                                                count(amount_usd) AS cnt_transactions,
                                                                round((count(amount_usd)/count(DISTINCT (account_number_from))),3) AS avg_transactions_per_account,
                                                                count(DISTINCT (account_number_from)) AS cnt_accounts_make_transactions
        FROM t1
        GROUP BY date_update,
                    currency_code
        ORDER BY date_update ASC) src ON tgt.date_update = src.date_update
        AND tgt.currency_from = src.currency_from WHEN MATCHED THEN
        UPDATE
        SET amount_total = src.amount_total,
            cnt_transactions = src.cnt_transactions,
            avg_transactions_per_account = src.avg_transactions_per_account,
            cnt_accounts_make_transactions = src.cnt_accounts_make_transactions WHEN NOT MATCHED THEN
        INSERT (date_update,
                currency_from,
                amount_total,
                cnt_transactions,
                avg_transactions_per_account,
                cnt_accounts_make_transactions)
        VALUES (src.date_update, src.currency_from, src.amount_total, src.cnt_transactions, src.avg_transactions_per_account, src.cnt_accounts_make_transactions);

    """
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