

eval $(minikube docker-env)

docker build -t carloshkayser/fake-kafka-producer:latest .

kubectl apply -f deployment.yaml