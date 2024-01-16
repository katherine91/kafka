import pandas as pd
import os
from datetime import datetime, timedelta
import math
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import cx_Oracle
import hashlib
from kafka import KafkaProducer
import json
import sys
import imaplib
import getpass
import email
import logging

from airflow.models import Variable
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.utils.dates import days_ago
from airflow.utils.edgemodifier import Label


default_args = {
    'owner': '',
    'depends_on_past': False,
    'start_date': datetime(2022,7,8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

def send_to_kafka():
    kafkaBrokers = Variable.get('kafka_brokers')
    certLocation = Variable.get('cert_location')
    keyLocation = Variable.get('key_location')
    ca_cert = Variable.get('ca_cert')

    checks_topic = Variable.get('checks_topic')

    producer = KafkaProducer(bootstrap_servers=kafkaBrokers,
                security_protocol='SSL',
                ssl_check_hostname=False,
                ssl_cafile=ca_cert,
                ssl_certfile=certLocation,
                ssl_keyfile=keyLocation,
                ssl_password=Variable.get('ssl_password'),
                acks=1,
                value_serializer=lambda m: json.dumps(m).encode('utf-8')
                )
    logging.info("----Kafka Producer created----")


    df = pd.read_csv(checks_path, delimiter=';', header=None, skiprows=[0])
    for line in df.itertuples():
        data = {
                "code_order": line[1],
                "store_id": line[2],
                "store_name": line[3],
                "date_receipt": line[4],
                "type_payment": line[5],
                "sum_payment": line[6],
                "PhoneHASHED": line[7]
            }

        producer.send(checks_topic, data)

    logging.info("----Checks data sent----")

    producer.flush()

checks_path = Variable.get('checks_path')


with DAG(
    dag_id='kafka_producer',
    description='Send data to ...',
    tags=['Kafka'],
    default_args=default_args,
    schedule_interval='30 8 * * *',
    catchup=False,
) as dag:

    a = DummyOperator(
            task_id='start'
        )

    send_to_kafka = PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_to_kafka,
        dag = dag
    )

    a >> Label("Send data to Kafka") >> send_to_kafka
