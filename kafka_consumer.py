from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable



def run():
    from kafka import KafkaConsumer
    from json import (loads, dumps)
    from sqlalchemy import create_engine
    import pandas as pd
    from psycopg2 import errors
    from psycopg2.errorcodes import UNIQUE_VIOLATION
    from sqlalchemy.dialects.postgresql import insert

    def upsert_dn(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]

        insert_stmt = insert(table.table).values(data)
        upsert_stmt = insert_stmt.on_conflict_do_nothing()
        conn.execute(upsert_stmt)

    consumer = KafkaConsumer(
        #topic,
        bootstrap_servers='',
        auto_offset_reset='earliest',
        security_protocol='SSL',
        ssl_check_hostname=False,
        ssl_cafile='/ca-cert',
        ssl_certfile='/certificate.pem',
        ssl_keyfile='/key.pem',
        ssl_password='',
        consumer_timeout_ms=60000,
        #max_poll_records=1,
        max_poll_interval_ms=600000,
        group_id='MAGNUM_GROUP_DWH3',
        client_id='TEST_MAGNUM_CLIENT',
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        enable_auto_commit=False
    )
    if consumer:
        print('Connected to Kafka broker')
    
    topic = 'PAYMENTS'
    consumer.subscribe([topic])
    engine = create_engine(f"postgresql://", pool_pre_ping=True)
    col_name = ['address','trade_point_rfo_code','tm_terminal_id','rrn','tran_date','reg_date','payment_type','payment_channel']
    sql_query = """select """

    while True:
        records = []
        try:
            i = 0
            for msg in consumer:
                try:
                    if not msg:
                        print("empty message")
                        break
                    json_object = dumps(msg.value, indent=4)
                    val = loads(json_object)
                    address = val.get('ADDRESS')
                    trade_point_rfo_code = val.get('TRADE_POINT_RFO_CODE')
                    tm_terminal_id = val.get('TM_TERMINAL_ID')
                    rrn = val.get('RRN')
                    tran_date = val.get('TRAN_DATE')
                    reg_date = val.get('REG_DATE')
                    payment_type = val.get('PAYMENT_TYPE')
                    payment_channel = val.get('PAYMENT_CHANNEL')


                    recs = (address,trade_point_rfo_code,tm_terminal_id,rrn,tran_date,reg_date,payment_type,payment_channel)
                    records.append(recs)
                    i = i + 1
                    if i == 1:
                        print("offset inside for loop" + str(msg.offset))
                    if i % 1000 == 0:
                        consumer.commit()
                        print("offset inside for loop" + str(msg.offset))

                    if i % 50000 == 0:
                        try:
                            df = pd.DataFrame(records, columns = col_name)
                            df = df.sort_values('reg_date', ascending=False)
                            df = df.drop_duplicates(['unique_hash'])
                            existing = pd.read_sql(sql_query, engine)
                            mask = ~df.unique_hash.isin(existing.unique_hash)
                            df.loc[mask].to_sql('table', con=engine, schema='schema', if_exists='append', index=False, method='multi', chunksize=100_000)
                            records = []
                            print("Successfuly inserted 50 000 rows")
                        except Exception as e:
                            print("Failed inserting into table {i}")
                            print(e)
                            break

                except Exception as einfor:
                    print(einfor)
                    print("offset inside for loop" + str(msg.offset))
                    print(json_object)
                    #i = 0
                    continue

            df = pd.DataFrame(records, columns = col_name)
            if df.shape[0] == 0:
                break
            else:
                try:
                    df = df.sort_values('reg_date', ascending=False)
                    df = df.drop_duplicates(['unique_hash'])
                    existing = pd.read_sql(sql_query, engine)
                    mask = ~df.unique_hash.isin(existing.unique_hash)
                    df.loc[mask].to_sql('table', con=engine, schema='schema', if_exists='append', index=False, method='multi', chunksize=100_000)
                    records = []
                    print("Successfuly inserted last rows")
                except Exception as e:
                    print("Failed inserting into table {i}")
                    print(e)
                    break

        except Exception as e:
            print("Exception: ")
            print(e)
            break
        finally:
            consumer.close()
            print("Consumer closed")
            engine.dispose()
            print("Engine closed")

default_args = {
    'owner': '',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
}


with DAG(
    dag_id='kafka_consumer',
    description='Analytical portal jobs',
    default_args=default_args,
    start_date=datetime(2022,11,21),
    schedule_interval='30 7 * * *',
    tags=['Analytics portal'],
    catchup=False,
) as dag:

    a = DummyOperator(
            task_id='start'
        )

    consume_data_task= PythonOperator(
        task_id="consume_data",
        python_callable=run,
    )

    a >> consume_data_task
