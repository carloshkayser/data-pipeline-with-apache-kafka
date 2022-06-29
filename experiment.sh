#!/bin/bash

# scale a kubectl deployment
for i in {1..40}
do
    kubectl scale deployment fake-kafka-producer -n demo --replicas=$i

    # sleep 30 seconds
    sleep 30
done

for i in {40..1}
do
    kubectl scale deployment fake-kafka-producer -n demo --replicas=$i

    # sleep 30 seconds
    sleep 30
done