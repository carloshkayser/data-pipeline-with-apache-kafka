FROM python:3.9.10-slim

RUN pip install kafka-python==2.0.2

COPY ../datasets/click-through-rate-prediction/test.csv .

COPY . . 

CMD [ "python3", "app.py" ]