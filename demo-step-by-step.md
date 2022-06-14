```sh
minikube status

kubectl get nodes

kubectl get pods -n demo

# show producer python code

kubectl logs --tail 100 -f fake-kafka-producer-

# show Kafka cluster YAML file

kubectl get kafkatopics.kafka.strimzi.io -n demo

kubectl get all -n demo

kafka-console-consumer \
  --bootstrap-server $(minikube service kafka-cluster-kafka-external-bootstrap --url -n demo) \
  --topic to_predict

# show jupyter notebook
```
