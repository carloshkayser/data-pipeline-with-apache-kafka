from kafka import KafkaProducer
from time import sleep

import threading
import random
import json
import uuid
import csv
import os


# KAFKA_HOST = "kafka-cluster-kafka-brokers:9092"
KAFKA_HOST = os.getenv("KAFKA_CLUSTER", "kafka-cluster-kafka-bootstrap:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "to_predict")


def start_producing():

    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)

    while True:
        # message_id = str(uuid.uuid4())
        # message = {"request_id": message_id, "data": get_next_row()}
        # message = {'request_id': message_id, 'data': json.loads(generate_fake_message())}

        message = get_next_row()

        print(message)

        producer.send(KAFKA_TOPIC, json.dumps(message).encode("utf-8"))
        producer.flush()

        print(
            "\033[1;31;40m -- PRODUCER: Sent message with id {}\x1b[0m".format(
                message["id"]
            )
        )

        sleep(2)


if __name__ == "__main__":

    csvfile = open("test.csv")
    dataset = csv.DictReader(csvfile)

    get_next_row = lambda _: next(dataset)

    start_producing()

    csvfile.close()
