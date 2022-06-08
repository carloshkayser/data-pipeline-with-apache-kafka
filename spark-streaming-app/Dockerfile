ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3

# FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}
FROM gcr.io/spark-operator/spark:v3.1.1

COPY --from=py3 / /

ARG PYSPARK_VERSION=3.2.0
RUN pip install pyspark==${PYSPARK_VERSION}

RUN pip install \
    sparkmonitor==1.1.1

RUN mkdir -p /home/app/checkpointLocation

COPY . /home/app

CMD [ "python3", "app.py" ]