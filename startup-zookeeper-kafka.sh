#!/bin/bash

# To run this script, you need to have the following installed:
# brew install zooKeeper kafka

zookeeper-server-start /home/linuxbrew/.linuxbrew/etc/kafka/zookeeper.properties &

kafka-server-start /home/linuxbrew/.linuxbrew/etc/kafka/server.properties