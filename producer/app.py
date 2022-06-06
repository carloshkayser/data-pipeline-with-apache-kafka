from kafka import KafkaProducer
from time import sleep

import pandas as pd
import json
import os


# KAFKA_HOST = "kafka-cluster-kafka-brokers:9092"
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka-cluster-kafka-bootstrap:9092")
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

    print("KAFKA_TOPIC", KAFKA_TOPIC)
    print("KAFKA_HOST", KAFKA_HOST)

    file_path = "../dataset/click-through-rate-prediction/test.gz"
    df = pd.read_csv(file_path)
    df = df.dropna()

    row_iterator = df.iterrows()

    def get_next_row():
        _, row = next(row_iterator)
        return row.to_dict()

    print("Starting producing")
    start_producing()
