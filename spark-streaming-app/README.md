
eval $(minikube docker-env)

docker build -t carloshkayser/spark-streaming-app:latest .

docker run -it -v $(pwd):/app carloshkayser/spark-streaming-app:latest bash




```sh
poetry run jupyter-lab
```