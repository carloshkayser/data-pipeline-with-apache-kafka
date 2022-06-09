# Producer

This application sends messages to a topic in Apache Kafka

```sh
eval $(minikube docker-env)

cd ..
docker build -f ./producer/Dockerfile -t carloshkayser/fake-kafka-producer:latest .
kubectl apply -n demo -f producer/deployment.yaml
```