FROM python:3.9.10-slim

RUN pip install kafka-python==2.0.2 pandas

COPY ./dataset/click-through-rate-prediction/test.gz .

COPY ./producer . 

CMD [ "python3", "app.py" ]