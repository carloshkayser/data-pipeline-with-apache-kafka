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
kubectl create namespace kafka

# Add Strimzi repository
helm repo add strimzi https://strimzi.io/charts
helm repo update

# Install Strimzi Operator
helm install kafka strimzi/strimzi-kafka-operator \
  --namespace kafka --version 0.28.0

kubectl apply -n kafka -f kafka-jbod.yaml
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
