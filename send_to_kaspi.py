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
    'owner': 'maguser',
    'depends_on_past': False,
    'start_date': datetime(2022,7,8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}


def fetch_oracle_pass():
    mail = imaplib.IMAP4_SSL(Variable.get('imap_host'))
    mail.login(Variable.get('imap_user'), Variable.get('imap_password'))
    mail.list()
    mail.select('Inbox')
    result, data = mail.uid('search', None, '(SUBJECT "New Password for MAG_LOGIC on REPS")')
    latest = data[0].split()[-1]
    password = ''
    result, email_data = mail.uid('fetch', latest, '(RFC822)')
    raw_email = email_data[0][1]
    raw_email_string = raw_email.decode('utf-8')
    email_message = email.message_from_string(raw_email_string)

    for part in email_message.walk():
        if part.get_content_type() == "text/plain":
            password = part.get_payload(decode=True).decode('utf-8').strip()

    return password


def sqlQuery(querystr, conn):
    df = pd.read_sql(querystr, conn)
    return df


def prepare_data(ds):
    #adb_conn = PostgresHook(postgres_conn_id='adb').get_conn()
    adb_conn = psycopg2.connect(f"postgresql://ykhan:{Variable.get('yk_p')}@172.16.10.22:5432/adb", connect_timeout=5)
    # ora_conn = OracleHook(oracle_conn_id='reps').get_conn()
    # oracle_pass = fetch_oracle_pass()
    ora_conn = create_engine(f"oracle+cx_oracle://b_user:{Variable.get('reps_pwd')}@reps.magnum.local:1521/REPS")
    query="""
        select rtl.code_retail_order, fact.source_store_id store_id, ds.store_name, fact.date_time_order, rtl.type_payment, rtl.sum_payment, lty.code_client from ods.rtl_retail_order_payment_link rtl
        left join dwh_data.fact_order_new fact on rtl.code_retail_order = fact.source_order_id
        left join dwh_stgll.clients_loyalty lty on rtl.code_retail_order = lty.code_order
        left join dwh_data.dim_store ds on fact.source_store_id = ds.source_store_id and ds.dateend > now()
        where rtl.date_retail_order = '{}'::date--current_date-1
        and rtl.code_shop in ( select distinct(source_store_id) from dwh_data.dim_store where source_location_id in (1235, 1236) )
        """

    query2="""
        select prod.source_order_id, ds.source_store_id store_id, ds.store_name, prod.source_product_id, prod.quantity, prod.code_unit, prod.price_with_vat,
            prod.sum_discount, prod.sum_order_wares_vat
            from dwh_data.fact_product_sales prod
        inner join 
        ( select distinct(rtl.code_retail_order) from ods.rtl_retail_order_payment_link rtl
        left join dwh_data.fact_order_new fact on rtl.code_retail_order = fact.source_order_id
        left join dwh_stgll.clients_loyalty lty on rtl.code_retail_order = lty.code_order
        where rtl.date_retail_order = '{}'::date--current_date-1
            and rtl.code_shop in ( select distinct(source_store_id) from dwh_data.dim_store where source_location_id in (1235, 1236) ) ) checks
            on prod.source_order_id=checks.code_retail_order
        left join dwh_data.dim_store ds on prod.source_store_id = ds.source_store_id and ds.dateend > now()
        """

    df = sqlQuery(query.format(ds), adb_conn)
    df2 = sqlQuery(query2.format(ds), adb_conn)

    units = pd.read_sql('select * from spr.unit_dimension', ora_conn)
    df2 = df2.merge(units[['code_unit', 'name_unit']], how='left', left_on='code_unit', right_on='code_unit')

    prodsQuery="""
        select dwh_product_id, code_wares, name_wares, dpg.name_group_level2 l2, dpg.name_group_level3 l3, dpg.name_group_level4 l4, dpg.name_group_level5 l5, name_brand, name_country
        from dwh_data.dim_product_list a inner join
        ( select code_wares as code_wares2, max(dwh_product_id) as dwh_product_id2 from dwh_data.dim_product_list dpl
        group by code_wares ) b
        on a.dwh_product_id = b.dwh_product_id2
        left join dwh_data.dim_prod_group dpg on dpg.source_prod_id = a.code_wares and now() < dpg.dateend
        """
    prods = pd.read_sql(prodsQuery, adb_conn)

    cards = pd.read_sql('select * from dwh_stgll.card', adb_conn)
    clients = pd.read_sql('select * from dwh_stgll.client', adb_conn)
    devices = pd.read_sql('select * from dwh_stgll.device', adb_conn)
    cards['s_cardnum'] = cards['card_number'].astype(str)+cards['cvs'].astype(str)

    devices = devices.groupby('client_id').agg({'device_id':pd.Series.nunique})
    devices = devices.reset_index()

    clients = clients.merge(devices, how='left', left_on='id', right_on='client_id')

    cards = cards.merge(clients, how='left', left_on='client_id', right_on='id')
    cards.loc[(cards['phone'].str[0]!='7') | (cards['phone'].str.len()<10),'phone'] = np.nan
    registered = cards[~cards['phone'].isna()]
    registeredDev = registered[~registered['device_id'].isna()]

    cards['card_number'] = cards['card_number'].astype(str)+cards['cvs']
    cardsINFO = cards[['client_id_x', 'card_number']].merge(clients[['id', 'first_name', 'last_name', 'patronymic', 'gender', 'dt_birthday', 'phone']], how='left', left_on='client_id_x', right_on='id')
    cardsINFO.loc[(cardsINFO['phone'].str[0]!='7') | (cardsINFO['phone'].str.len()<10),'phone'] = np.nan
    registered_clients = df[df['code_client'].isin(cardsINFO[~cardsINFO['phone'].isna()]['card_number'])]['code_client'].unique()
    noncashchecks = df[df['type_payment']=='NC']['code_retail_order'].unique()

    dfFinal = df[(df['code_retail_order'].isin(noncashchecks)) | (df['code_client'].isin(registered_clients))]
    df2Final = df2[df2['source_order_id'].isin(dfFinal['code_retail_order'].unique())]
    users = cardsINFO[cardsINFO['card_number'].isin(dfFinal['code_client'].unique())]
    users = users[~users['phone'].isna()]
    prodsGr = prods

    products = prodsGr[prodsGr['code_wares'].isin(df2Final['source_product_id'].unique())]
    wares = set(np.append(products['code_wares'].unique(),[]))
    products = prodsGr[prodsGr['code_wares'].isin(wares)]

    users['PhoneHASHED'] = [hashlib.sha256(x.encode('utf-8')).hexdigest() for x in users['phone']]

    dfFinal = dfFinal.merge(users[['card_number', 'PhoneHASHED']], how='left', left_on='code_client', right_on='card_number')
    dfFinal = dfFinal.rename(columns={'code_retail_order':'code_order', 'date_time_order':'date_receipt'})
    df2Final = df2Final.rename(columns={'source_order_id':'code_order', 'source_product_id':'code_wares'})
    products = products.rename(columns={'l2':'napravlenie', 'l3':'otdel', 'l4':'gruppa', 'l5':'podgruppa'})

    dfFinal.iloc[:,[0,1,2,3,4,5,8]].to_csv(checks_path, index=False, sep=';')
    # df2Final.iloc[:,[0,1,2,3,4,6,7,5,8]].to_csv(checks_with_wares_path, index=False, sep=';')
    df2Final.to_csv(checks_with_wares_path, index=False, sep=';')
    products.iloc[:,1:].to_csv(wares_path, index=False, sep=';')
    users.iloc[:,[3,4,5,6,7,9]].to_csv(clients_path, index=False, sep=';')


def send_to_kafka():
    kafkaBrokers = Variable.get('kafka_brokers')
    certLocation = Variable.get('cert_location')
    keyLocation = Variable.get('key_location')
    ca_cert = Variable.get('ca_cert')

    wares_topic = Variable.get('wares_topic')
    clients_topic = Variable.get('clients_topic')
    checks_topic = Variable.get('checks_topic')
    checks_with_wares_topic = Variable.get('checks_with_wares_topic')

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


    df = pd.read_csv(checks_with_wares_path, delimiter=';', header=None, skiprows=[0])
    for line in df.itertuples():
        data = {
                "code_order": line[1],
                "store_id": line[2],
                "store_name": line[3],
                "code_wares": line[4],
                "quantity": line[5],
                "code_unit": line[6],
                "price_with_vat": line[7],
                "sum_discount": line[8],
                "sum_order_wares_vat": line[9],
                "name_unit": line[10]
            }

        producer.send(checks_with_wares_topic, data)

    logging.info("----ChecksWithWares data sent----")

    #try:
    df = pd.read_csv(clients_path, delimiter=';', header=None, skiprows=[0])
    for line in df.itertuples():
        data = {
            "first_name": line[1],
            "last_name": line[2],
            "patronymic": line[3],
            "gender": line[4],
            "dt_birthday": line[5],
            "PhoneHASHED": line[6]
        }

        producer.send(clients_topic, data)

    logging.info("----Clients data sent----")
    #except pd.errors.EmptyDataError:
    #    print('No data to parse from clients')


    df = pd.read_csv(wares_path, delimiter=';', header=None, skiprows=[0])
    for line in df.itertuples():
        data = {
            "code_wares": line[1],
            "name_wares": line[2],
            "napravlenie": line[3],
            "otdel": line[4],
            "gruppa": line[5],
            "podgruppa": line[6],
            "name_brand": line[7],
            "name_country": line[8]
            }

        producer.send(wares_topic, data)

    logging.info("----Wares data sent----")

    producer.flush()

clients_path = Variable.get('clients_path')
wares_path = Variable.get('wares_path')
checks_path = Variable.get('checks_path')
checks_with_wares_path = Variable.get('checks_with_wares_path')


with DAG(
    dag_id='magnum_to_kaspi_kafka',
    description='Send data to Kaspi',
    tags=['Kafka'],
    default_args=default_args,
    schedule_interval='30 8 * * *',
    template_searchpath='/home/maguser/airflow/include',
    catchup=False,
) as dag:

    a = DummyOperator(
            task_id='start'
        )

    prepare_data = PythonOperator(
        task_id='prepare_data',
        python_callable=prepare_data,
        dag = dag
    )

    send_to_kafka = PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_to_kafka,
        dag = dag
    )

    a >> Label("Prepare checks/clients/wares data") >> prepare_data >> Label("Send data to Kafka") >> send_to_kafka
