from kafka import KafkaProducer
from time import sleep

import pandas as pd
import decimal
import json
import os


# KAFKA_HOST = "kafka-cluster-kafka-brokers:9092"
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka-cluster-kafka-bootstrap:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "to_predict")

# https://stackoverflow.com/a/3885198/7412570
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def start_producing():

    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)

    while True:
        # message_id = str(uuid.uuid4())
        # message = {"request_id": message_id, "data": get_next_row()}
        # message = {'request_id': message_id, 'data': json.loads(generate_fake_message())}

        message = get_next_row()
        message["id"] = decimal.Decimal(message["id"])

        message_json = json.dumps(message, cls=DecimalEncoder).encode("utf-8")
        print(message_json)

        producer.send(KAFKA_TOPIC, message_json)
        producer.flush()

        print(
            "\033[1;31;40m -- PRODUCER: Sent message with id {}\x1b[0m".format(
                message["id"]
            )
        )

        sleep(1)


if __name__ == "__main__":

    print("KAFKA_TOPIC", KAFKA_TOPIC)
    print("KAFKA_HOST", KAFKA_HOST)

    # # read csv file without pandas
    # with open("test.gz", "r") as f:
    #     line = f.readline()
    #     while line:
    #         # check if there is nan or null values
    #         if "nan" in line or "null" in line:
    #             line = f.readline()
    #             continue

    #         print(line)

    #         line = f.readline()

    file_path = "test.gz"
    df = pd.read_csv(file_path)
    df = df.dropna()

    row_iterator = df.iterrows()

    def get_next_row():
        _, row = next(row_iterator)
        return row.to_dict()

    print("Starting producing")
    start_producing()
