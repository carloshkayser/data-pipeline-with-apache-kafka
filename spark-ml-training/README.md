
```sh
poetry install
poetry run jupyter-lab
```

https://www.kaggle.com/competitions/avazu-ctr-prediction
https://www.kaggle.com/competitions/online-advertising-challenge-fall-2020
  







# Running on Kubernetes

```sh
minikube start
```

```sh
kubectl create namespace spark

helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator

helm repo update

helm install spark-operator spark-operator/spark-operator \
  --namespace spark --create-namespace

# --set image.tag=v1beta2-1.3.0-3.1.1 \
```

```sh
```