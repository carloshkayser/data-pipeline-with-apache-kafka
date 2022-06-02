# data-pipelines-with-apache-kafka

**Prerequisites**

- Python 3.10
- Java JDK 11
- Poetry 1.1.13
- Minikube v1.25.2
- Kubectl v1.23.5




sudo apt install default-jdk
- [Helm](https://helm.sh/docs/intro/install/)
- []()
- []()
- []()

## Setup

### Apache Kafka Operator

```sh
# create namespace
kubectl create namespace demo

# Add Strimzi repository
helm repo add strimzi https://strimzi.io/charts
helm repo update

# Install Strimzi Operator
helm install kafka strimzi/strimzi-kafka-operator \
  --namespace demo --version 0.28.0

kubectl apply -n demo -f kafka/kafka-jbod.yaml

kubectl get pods -w
```

### Apache Spark Operator

```sh
# Add Spark operator
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm repo update

# Install operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark --create-namespace \
  --set sparkJobNamespace=default \
  --set image.tag=v1beta2-1.3.0-3.1.1 \
  --set enableMetrics=true

# 

```














### producer

```sh
eval $(minikube docker-env)
docker build -f ./producer/Dockerfile -t carloshkayser/fake-kafka-producer:latest .
kubectl apply -n demo -f producer/deployment.yaml
```










```sh

# start zookeeper server
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

# start kafka server
zookeeper-server-start /home/linuxbrew/.linuxbrew/etc/kafka/zookeeper.properties

# create kafka topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic to_predict

# initialize producer console
kafka-console-producer --broker-list localhost:9092 --topic to_predict

# initialize consumer console
kafka-console-consumer --bootstrap-server localhost:9092 --topic to_predict --from-beginning
```