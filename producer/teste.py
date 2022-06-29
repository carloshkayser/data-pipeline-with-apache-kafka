# %%
# from kafka import KafkaProducer
from time import sleep

import pandas as pd
import decimal
import json
import os

# https://stackoverflow.com/a/3885198/7412570
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

# %%
file_path = "../dataset/click-through-rate-prediction/test.gz"

def get_next_row(file_path: str):
    with pd.read_csv(file_path, chunksize=10000) as reader:
        for chunk in reader:
            chunk = chunk.dropna()
            yield chunk

reader = get_next_row(file_path)
type(reader)

# %%
obj = next(reader)
type(obj)

# %%
obj

# %%
for row in obj.iterrows():
    # transform in dict
    row = row[1].to_dict()

    print(row)
    print(row["id"])
    break

# %%
