from airflow import DAG
#from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

    
    
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
        bootstrap_servers='194.187.246.240:9093',
        auto_offset_reset='earliest',
        security_protocol='SSL',
        ssl_check_hostname=False,
        ssl_cafile='/home/maguser/airflow/config/ca-cert',
        ssl_certfile='/home/maguser/airflow/config/S0145_MAGNUM_certificate.pem',
        ssl_keyfile='/home/maguser/airflow/config/S0145_MAGNUM_key.pem',
        ssl_password='9trxj9gbpwaprud84trhx934',
        consumer_timeout_ms=60000,
        #max_poll_records=1,
        max_poll_interval_ms=600000,
        group_id='MAGNUM_GROUP_N',
        client_id='TEST_MAGNUM_CLIENT',
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        enable_auto_commit=False
    )
    if consumer:
        print('Connected to Kafka broker')
    
    #tp = TopicPartition(topic,0)
    #consumer.assign([tp])
    #consumer.seek_to_beginning(tp)
    #lastOffset = consumer.end_offsets([tp])[tp]
    topic = 'OUT.S0145.PAYMENTS'
    consumer.subscribe([topic])
    login='rs'
    password='Qq123456#'
    engine = create_engine(f"postgresql://{login}:{password}@10.10.2.13:5432/portal", pool_pre_ping=True)
    col_name = ['address','trade_point_rfo_code','tm_terminal_id','rrn','tran_date','reg_date','payment_type','payment_channel',\
                        'kbs_debit','card_num','tran_amt','payment_amt','commission_amount','commission_rate','pay_comm_amt','pay_comm_rate','ind_str','unique_hash','user_hash']
    #src = PostgresHook(postgres_conn_id='adb')

    while True:
        records = []
        #consumer.poll(1.0)
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
                    kbs_debit = val.get('KBS_DEBIT')
                    card_num = val.get('CARD_NUM')
                    tran_amt = val.get('TRAN_AMT')
                    payment_amt = val.get('PAYMENT_AMT')
                    commission_amount = val.get('COMMISSION_AMOUNT')
                    commission_rate = val.get('COMMISSION_RATE')
                    pay_comm_amt = val.get('PAY_COMM_AMT')
                    pay_comm_rate = val.get('PAY_COMM_RATE')
                    ind_str = str(msg.offset) + "_" + msg.topic + "_" + str(msg.partition)
                    unique_hash = val.get('UNIQUE_HASH')
                    user_hash = val.get('USER_HASH')

                    recs = (address,trade_point_rfo_code,tm_terminal_id,rrn,tran_date,reg_date,payment_type,payment_channel,\
                            kbs_debit,card_num,tran_amt,payment_amt,commission_amount,commission_rate,pay_comm_amt,pay_comm_rate,ind_str,unique_hash, user_hash)
                    #print(recs)
                    records.append(recs)
                    if i % 1000 == 0:
                        print(i)
                        df = pd.DataFrame(records, columns = col_name)
                        try:
                            with engine.connect() as conn:
                                df.to_sql('payments_kaspi', con=conn, schema='rs', if_exists='append', index=False, method=upsert_dn, chunksize=100000)
                        except errors.lookup(UNIQUE_VIOLATION) as e:
                            print(e)
                        records = []
                        consumer.commit()
                        print("rows = " + str(df.shape[0]) + "; colums = " + str(df.shape[1]))
                        print("consumer commit mannually")
                    #print(recs)
                    i = i + 1
                except Exception as einfor:
                    print(einfor)
                    print("offset inside for loop" + str(msg.offset))
                    #print(json_object)
                    continue
                #if msg.offset == lastOffset - 1:
                #    break
            i = i - 1
            df = pd.DataFrame(records, columns = col_name)
            if df.shape[0] == 0:
                break
            if i % 1000 != 0 and df.shape[0] != 0:
                print(i)
                try:
                    with engine.connect() as conn:
                        df.to_sql('payments_kaspi', con=conn, schema='rs', if_exists='append', index=False, method=upsert_dn, chunksize=100000)
                except errors.lookup(UNIQUE_VIOLATION) as e:
                    print(e)
                records = []
                consumer.commit()
                print("rows = " + str(df.shape[0]) + "; colums = " + str(df.shape[1]))
                print("consumer commit mannually")

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
    'owner': 'maguser',
    'depends_on_past': False,
    'email': ['khan.y@magnum.kz'],
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 5,
    #'retry_delay': timedelta(seconds=60),
}

with DAG(
    dag_id='kaspi_payments',
    description='Analytical portal jobs',
    default_args=default_args,
    start_date=datetime(2022,9,4),
    schedule_interval='0 8 * * *',
    tags=['Analytics portal'],
    # template_searchpath='/home/maguser/airflow/include',
) as dag:

    a = DummyOperator(
            task_id='start'
        )

    read_kafka_topic = PythonOperator(
        task_id='read_kafka_topic',
        python_callable=run,
    )

    #success_email_body = f"""
    #Добрый день! <br><br>
    #Процесс забора данных каспи успешно завершен в {datetime.now()}.
    #"""

    #send_mail = EmailOperator(
    #    task_id="send_mail", 
    #    to=['khan.y@magnum.kz'],
    #    subject='Airflow Success: receive kaspi',
    #    html_content=success_email_body,
    #    dag=dag)

    a >> read_kafka_topic #>> send_mail