import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaException
from cryptography.fernet import Fernet
import os
import json
import time

CPU_THRESHOLD = 88.3
MEM_THRESHOLD = 88.33
NET_IN_THRESHOLD = 4625.85
DISK_IO_THRESHOLD = 3277.57

KAFKA_BATCH_SIZE = 15
FLUSH_INTERVAL = 5

INSERT_QUERY = "INSERT INTO timescalegrafanaservermetrics (server_id, ts, cpu_pct, mem_pct, net_in, net_out, disk_io) VALUES %s"

DB_NAME = os.environ.get("POSTGRES_DB", "postgres")
DB_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "password123")
DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_PORT = os.environ.get("POSTGRES_PORT", 5432)

try:
    KAFKA_FERNET_KEY = os.environ.get("KAFKA_FERNET_KEY")
    fernet = Fernet(KAFKA_FERNET_KEY)
except Exception as e:
    print(f"Error initializing Fernet: {e}\n")
    raise Exception("Failed to intialize Fernet Key\n")

conf = {
    'bootstrap.servers' : os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    'group.id' : 'grp1',
    'auto.offset.reset' : 'earliest',
    'enable.auto.commit' : False
}

def on_assign(consumer, partitions):
    print(f"Partitions assigned: \n")
    for p in partitions:
        print(f"Partition: {p.partition}, Offset: {p.offset}")
    consumer.assign(partitions)

def on_revoke(consumer, partitions):
    print(f"Partitions revoked: \n")
    for p in partitions:
        print(f"Partition: {p.partition}, Offset: {p.offset}")
    consumer.commit(asynchronous=False)
    
try:
    consumer = Consumer(conf)
    consumer.subscribe(['server-telemetry'], on_assign=on_assign, on_revoke=on_revoke)
except KafkaException as e:
    print(f"Error initializing Kafka consumer: {e}\n")
    raise Exception("Failed to initialize Kafka consumer\n")

def get_db_connection():
    while True:
        try:
            return psycopg2.connect(
                dbname = DB_NAME,
                user = DB_USER,
                password = DB_PASSWORD,
                host = DB_HOST,
                port = DB_PORT
            )
        except Exception as e:
            print(f"Error connecting to PostgreSQL database: {e}\nRetrying in 2 seconds\n")
            time.sleep(2)

conn = get_db_connection()
cursor = conn.cursor()



def publish_data(batch):

    if not batch:
        return

    try:

        execute_values(cursor, INSERT_QUERY, batch, page_size = len(batch))
        conn.commit()
        consumer.commit(asynchronous = False)
        print(f"Batch of size : {len(batch)} has been published to PostgreSQL, committed to Kafka and cleared from memory.\n")

    except Exception as e:
        print(f"Error publishing data to PostgreSQL: {e}\n")
        conn.rollback()
    finally:
        batch.clear()

def process_data():

    batch = []
    LAST_FLUSH_TIME = time.time()
    try:
        while True:
            msg = consumer.poll(1)

            if time.time() - LAST_FLUSH_TIME >= FLUSH_INTERVAL:
                publish_data(batch)
                LAST_FLUSH_TIME = time.time()

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error : {msg.error()}")
                continue

            data = json.loads(fernet.decrypt(msg.value()).decode("utf-8"))
            key = (data['server_id'], data['ts'])

            server_id, ts = key
            cpu_pct = data['cpu_pct']
            mem_pct = data['mem_pct']
            net_in = data['net_in']
            net_out = data['net_out']
            disk_io = data['disk_io']
            batch.append((server_id, ts, cpu_pct, mem_pct, net_in, net_out, disk_io))

            if len(batch) >= KAFKA_BATCH_SIZE:
                publish_data(batch)
                LAST_FLUSH_TIME = time.time()


    except KeyboardInterrupt as e:
        print(f"Process interrupted by user : {e}\n")
    except KafkaException as e:
        print(f"Kafka error: {e}\n")
    finally:
        publish_data(batch)
        consumer.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    process_data()