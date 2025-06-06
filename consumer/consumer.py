from kafka import KafkaConsumer
from db import dbconnection
import json
from datetime import datetime
import time
import threading



BATCH_SIZE = 5
WAIT_DURATION = 50
topics = ('north', 'south', 'east', 'west', 'central', 'northwest', 'southwest', 'other')


def start_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='kafka:29092',
        auto_offset_reset='latest',
        group_id='central2'
    )


    data_list = []
    last_received_time = time.time()
    lock = threading.Lock()

    def flush_on_timeout():
        nonlocal data_list, last_received_time
        while True:
            with lock:
                if data_list and (time.time() - last_received_time >= WAIT_DURATION):
                    dbconnection(data_list)
                    data_list = []

    threading.Thread(target=flush_on_timeout, daemon=True).start()


    for message in consumer:
        message = json.loads(message.value.decode('utf-8'))
        message['date_time'] = datetime.strptime(message['date_time'], "%d@%m!%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        message['topic_name'] = topic_name
        # print(message)
        with lock:
            data_list.append(message)
            if len(data_list) % BATCH_SIZE == 0:
                dbconnection(data_list)
                data_list= []

for topic in topics:
    threading.Thread(target=start_consumer, args= (topic, ),  daemon=True).start()

while True:
    time.sleep(1)