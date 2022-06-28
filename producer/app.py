from kafka import KafkaProducer
from time import sleep

import pandas as pd
import decimal
import json
import os


# KAFKA_HOST = "kafka-cluster-kafka-brokers:9092"
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka-cluster-kafka-bootstrap:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "to_predict")
WAIT_TIME = float(os.getenv("WAIT_TIME", "0.100"))  # seconds

# https://stackoverflow.com/a/3885198/7412570
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def start_producing(reader):

    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)

    for chunk in reader:

        # chunk is a pandas.DataFrame
        # dropna() to remove rows with NaN
        chunk = chunk.dropna()

        for row in chunk.iterrows():
            # transform in dict
            message = row[1].to_dict()
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

            sleep(WAIT_TIME)


if __name__ == "__main__":

    print("KAFKA_TOPIC", KAFKA_TOPIC)
    print("KAFKA_HOST", KAFKA_HOST)

    file_path = "test.gz"

    def get_next_chunk(file_path: str) -> pd.DataFrame:
        with pd.read_csv(file_path, chunksize=10000) as reader:
            for chunk in reader:
                yield chunk

    reader = get_next_chunk(file_path)

    print("Starting producing")
    start_producing(reader)
