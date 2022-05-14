from kafka import KafkaProducer, KafkaConsumer
from time import sleep

import threading
import random
import json
import uuid


# KAFKA_HOST = "kafka-cluster-kafka-brokers:9092"
KAFKA_HOST = "kafka-cluster-kafka-bootstrap:9092"


def generate_fake_message():
    return {
        "CPU": round(random.uniform(0, 100), 2),
        "memory": round(random.uniform(0, 100), 2),
        "disk": round(random.uniform(0, 100), 2),
    }


def start_producing():

    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)

    while True:
        message_id = str(uuid.uuid4())
        message = {"request_id": message_id, "data": generate_fake_message()}
        # message = {'request_id': message_id, 'data': json.loads(generate_fake_message())}

        print(message)

        producer.send("app_messages", json.dumps(message).encode("utf-8"))
        producer.flush()

        print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(message_id))
        sleep(2)


# def start_consuming():
#     print("Start Consuming")

#     consumer = KafkaConsumer("app_messages", bootstrap_servers=KAFKA_HOST)

#     print("line 43")

#     for msg in consumer:
#         print("line 46")
#         message = json.loads(msg.value)
#         # if 'prediction' in message:
#         request_id = message["request_id"]
#         print(
#             "\033[1;32;40m ** CONSUMER: Received prediction {} for request id {}".format(
#                 message["prediction"], request_id
#             )
#         )

# threads = []
# t = threading.Thread(target=start_producing)
# t2 = threading.Thread(target=start_consuming)
# threads.append(t)
# threads.append(t2)
# t.start()
# t2.start()

if __name__ == "__main__":
    start_producing()
