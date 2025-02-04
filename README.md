# Data Pipelines with Apache Kafka and Spark

A demonstration project showing how to build real-time data pipelines using Apache Kafka and Spark Structured Streaming on Kubernetes. This project implements a click-through rate (CTR) prediction system that processes streaming data in real-time.

## Prerequisites

- Python 3.10
- Java JDK 11 (`sudo apt install default-jdk`)
- Poetry 1.1.13 (Python dependency management)
- Minikube v1.25.2 (Local Kubernetes cluster)
- Kubectl v1.23.5 (Kubernetes CLI)
- [Helm](https://helm.sh/docs/intro/install/) (Kubernetes package manager)

## Architecture Overview

This project implements a streaming data pipeline with the following components:

1. **Data Producer**: Python application that simulates click-through events
2. **Apache Kafka**: Message broker for reliable data streaming
3. **Apache Spark**: Real-time data processing using Structured Streaming
4. **PostgreSQL**: Storage for processed predictions

### Data Flow
1. Producer generates synthetic click-through data
2. Events are published to Kafka topic "to_predict"
3. Spark Streaming consumes events and makes predictions
4. Results are stored in PostgreSQL for analysis

## Setup Instructions

### 1. Start Minikube Cluster

Start a Minikube cluster with sufficient resources:

```sh
minikube start --memory=16g --cpus=8
```

### 2. Deploy Apache Kafka

We use the Strimzi operator to manage Kafka on Kubernetes:

```sh
# Create namespace
kubectl create namespace demo

# Add Strimzi repository
helm repo add strimzi https://strimzi.io/charts
helm repo update

# Install Strimzi Operator
helm install kafka strimzi/strimzi-kafka-operator \
  --namespace demo --version 0.28.0

# Deploy Kafka cluster
kubectl apply -n demo -f kafka/kafka-jbod.yaml

# Watch the pods being created
kubectl get pods -w -n demo
```

Get the Kafka bootstrap server address:

```sh
minikube service kafka-cluster-kafka-external-bootstrap --url -n demo
# Or
echo $(minikube ip):$(kubectl get svc kafka-cluster-kafka-external-bootstrap -n demo -o jsonpath='{.spec.ports[].nodePort}')
```

### 3. Deploy Producer Application

The producer application generates sample click-through data:

```sh
# Configure Docker to use Minikube's Docker daemon
eval $(minikube docker-env)

# Build producer image
docker build -f ./producer/Dockerfile -t carloshkayser/fake-kafka-producer:latest .

# Deploy producer
kubectl apply -n demo -f producer/deployment.yaml

# Watch the producer pod
kubectl get pods -w -n demo
```

Verify data production:
```sh
kafka-console-consumer --bootstrap-server <kafka-bootstrap-servers> --topic to_predict
```

### 4. Deploy Apache Spark Operator

Set up Spark for Kubernetes:

```sh
# Add Spark operator repository
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm repo update

# Install operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --set sparkJobNamespace=spark-apps \
  --set enableWebhook=true \
  --set enableMetrics=true

# Check operator status
helm status spark-operator -n spark-operator
```

### 5. PostgreSQL Setup

Create the predictions table:

```sql
CREATE TABLE predictions (
    id DECIMAL(38, 0),
    probability DOUBLE PRECISION,
    processed_at TIMESTAMP
);
```

## Development Environment

### Local Development Setup

```sh
# Install dependencies
poetry install

# Start Jupyter Lab for notebooks
poetry run jupyter-lab
```

### Running Tests

```sh
poetry run pytest
```

## Project Structure

```
.
├── dataset/                      # Training and test datasets
│   └── click-through-rate-prediction/
├── kafka/                        # Kafka deployment configs
│   ├── kafka-jbod.yaml          # Kafka cluster definition
│   └── kafka-topics.yaml        # Topic definitions
├── producer/                     # Kafka producer application
│   ├── app.py                   # Producer main script
│   ├── Dockerfile              
│   ├── deployment.yaml
│   └── pyproject.toml
├── spark-ml-training/           # ML model training
│   ├── notebooks/
│   │   └── Spark ML Training.ipynb
│   └── pyproject.toml
├── spark-streaming-app/         # Streaming application
│   ├── app.py                   # Main streaming app
│   ├── Dockerfile
│   ├── deployment.yaml
│   ├── rbac.yaml
│   └── pyproject.toml
├── scripts/                     # Utility scripts
│   ├── startup-zookeeper-kafka.sh
│   └── experiment.sh
└── README.md
```

## Configuration

### Environment Variables

Producer Application:
- `KAFKA_HOST`: Kafka bootstrap servers (default: "localhost:9092")
- `KAFKA_TOPIC`: Topic to produce to (default: "to_predict")
- `WAIT_TIME`: Delay between messages (default: 0.1)

Spark Streaming:
- `POSTGRES_HOST`: PostgreSQL host
- `POSTGRES_DB`: Database name
- `POSTGRES_USER`: Database user
- `POSTGRES_PASSWORD`: Database password

## Monitoring

### Kafka Topics

Monitor topic lag and throughput:
```sh
kubectl exec -it kafka-cluster-kafka-0 -n demo -- bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group spark-streaming
```

### Spark Metrics

Access Spark UI:
```sh
kubectl port-forward <spark-driver-pod> 4040:4040 -n demo
```

## Troubleshooting

Common issues and solutions:

1. **Kafka Connection Issues**
   - Verify Kafka service is running: `kubectl get pods -n demo`
   - Check Kafka logs: `kubectl logs kafka-cluster-kafka-0 -n demo`

2. **Spark Job Failures**
   - Check driver logs: `kubectl logs <spark-driver-pod> -n demo`
   - Verify RBAC permissions: `kubectl describe rolebinding -n demo`

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Strimzi Operator Guide](https://strimzi.io/docs/operators/latest/overview.html)

## License

This project is open source and available under the MIT license.
