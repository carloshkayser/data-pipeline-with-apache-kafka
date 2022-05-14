

eval $(minikube docker-env)

docker build -t carloshkayser/fake-kafka-producer:latest .

k apply -f deployment.yaml