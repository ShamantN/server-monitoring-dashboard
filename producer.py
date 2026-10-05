from confluent_kafka import Producer
from cryptography.fernet import Fernet
import time
import json
import os
import psutil
import random
from datetime import datetime, timezone

SERVER_ID = os.environ.get("SERVER_ID", "NODE-1")

KAFKA_FERNET_KEY = os.environ.get("KAFKA_FERNET_KEY")
fernet = Fernet(KAFKA_FERNET_KEY)

conf = {
    "bootstrap.servers" : os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "acks" : "all",
    "client.id" : "producer-1",
    "retries" : 3
}

# db_conn = psycopg2.connect(
#     dbname = "postgres",
#     user = "postgres",
#     password = 'password123',
#     host = "localhost",
#     port = 5432
# )
# db_conn.autocommit = True
# db_cursor = db_conn.cursor()

producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        raise Exception(f"Message delivery failed: {err}")
    else:
        print(f"Message Delivered to topic : {msg.topic()} Partition : {msg.partition()} Offset : {msg.offset()}\n")

def publish_data():

    base_cpu = psutil.cpu_percent(interval=None)
    prev_net = psutil.net_io_counters()
    prev_disk = psutil.disk_io_counters()

    while True:
        
        time.sleep(1)

        cpu_pct = psutil.cpu_percent()
        mem_pct = psutil.virtual_memory().percent
        cur_net = psutil.net_io_counters()
        cur_disk = psutil.disk_io_counters()

        net_in = cur_net.bytes_recv - prev_net.bytes_recv
        net_out = cur_net.bytes_sent - prev_net.bytes_sent
        disk_io = (cur_disk.read_bytes + cur_disk.write_bytes) - (prev_disk.read_bytes + prev_disk.write_bytes)

        key = SERVER_ID.encode("utf-8")

        ts = time.strftime("%H:%M:%S")
        dts = datetime.now(timezone.utc).isoformat()

        # db_cursor.execute(
        #     "INSERT INTO timescaleGrafanaServerMetrics (recorded_at, servere_id, cpu_pct, mem_pct, net_in, net_out, disk_io) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        #     (dts, SERVER_ID, cpu_pct, mem_pct, net_in, net_out, disk_io)
        # )

        prev_net = cur_net
        prev_disk = cur_disk

        # SERVER_ID = "NODE-" + str(random.randint(1,9))
        # key = SERVER_ID.encode("utf-8")
        data = fernet.encrypt(json.dumps({'server_id' : SERVER_ID, 'ts' : dts,  'cpu_pct' : cpu_pct, 'mem_pct' : mem_pct, 'net_in' : net_in, 'net_out' : net_out, 'disk_io' : disk_io}).encode("utf-8"))


        producer.produce(topic = "server-telemetry",key = key, value = data, on_delivery = delivery_report)

        producer.poll(0)


    # with open("dataset.csv","r") as f:
    #     reader = csv.DictReader(f)
        
    #     for row in reader:
    #         key = fernet.encrypt(row["server_id"].encode("utf-8"))

    #         cpu_data = fernet.encrypt(json.dumps({"ts" : row["ts"], "server_id" : row["server_id"], "cpu_pct" : row["cpu_pct"]}).encode("utf-8"))
    #         mem_data = fernet.encrypt(json.dumps({"ts" : row["ts"], "server_id" : row["server_id"], "mem_pct" : row["mem_pct"]}).encode("utf-8"))
    #         net_data = fernet.encrypt(json.dumps({"ts" : row["ts"], "server_id" : row["server_id"], "net_in" : row["net_in"], "net_out" : row["net_out"]}).encode("utf-8"))
    #         disk_data = fernet.encrypt(json.dumps({"ts" : row["ts"], "server_id" : row["server_id"], "disk_io" : row["disk_io"]}).encode("utf-8"))

    #         producer.produce("topic-cpu", key = key, value = cpu_data, on_delivery=delivery_report)
    #         producer.produce("topic-mem", key = key, value = mem_data, on_delivery=delivery_report)
    #         producer.produce("topic-net", key = key, value = net_data, on_delivery=delivery_report)
    #         producer.produce("topic-disk", key = key, value = disk_data, on_delivery=delivery_report)

    #         producer.poll(0)
    #         time.sleep(0.001)

    # producer.flush()

if __name__ == "__main__":
    publish_data()
