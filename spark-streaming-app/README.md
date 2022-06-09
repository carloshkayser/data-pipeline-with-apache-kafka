# Streaming Application

1. **Create spark docker image**
    ```sh
    eval $(minikube docker-env)
    
    $SPARK_HOME/bin/docker-image-tool.sh \
      -m -t v3.2.1 \
      -p $SPARK_HOME/kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
    
bin/docker-image-tool.sh \
  -m -t v3.1.3 \
  -p kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
    ```

2. **Submit application**
    ```sh
    kubectl create serviceaccount spark

    kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
    
    # On minikube
    $SPARK_HOME/bin/spark-submit \
      --master k8s://https://$(minikube ip):8443 \
      --deploy-mode cluster \
      --name spark-pi \
      --class org.apache.spark.examples.SparkPi \
      --conf spark.executor.instances=2 \
      --conf spark.kubernetes.docker.image.pullPolicy=Never \
      --conf spark.kubernetes.namespace=default \
      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
      --conf spark.kubernetes.container.image=spark:v3.2.1 \
      --conf spark.kubernetes.file.upload.path=file:///$SPARK_HOME/examples/jars/spark-examples_2.12-3.2.1.jar \
      local:///$SPARK_HOME/examples/jars/spark-examples_2.12-3.2.1.jar
    
# On minikube
$SPARK_HOME/bin/spark-submit \
  --master k8s://https://$(minikube ip):8443 \
  --deploy-mode cluster \
  --name spark-pi \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=2 \
  --conf spark.kubernetes.docker.image.pullPolicy=Never \
  --conf spark.kubernetes.namespace=default \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=spark:v3.1.3 \
  --conf spark.kubernetes.file.upload.path=file:///$SPARK_HOME/examples/jars/spark-examples_2.12-3.1.3.jar \
  local:///$SPARK_HOME/examples/jars/spark-examples_2.12-3.1.3.jar
    ```

3. 



spark-submit --master k8s://https://ip_master_k8s:port_https_api  
  --deploy-mode cluster 
  --name spark-pi 
  --class org.apache.spark.examples.SparkPi 
  --conf spark.executor.instances=3 
  --conf spark.kubernetes.container.image=your_image 
  --conf spark.kubernetes.authenticate.submission.caCertFile=selfsigned_certificate.pem 
  --conf spark.kubernetes.authenticate.submission.oauthToken=spark-token-XXX hdfs://ip_master_hdfs/my_jar



# Install operator
helm install spark-operator spark-operator/spark-operator \
  --namespace default \
  --set sparkJobNamespace=default \
  --set enableWebhook=true \
  --set enableMetrics=true







---

eval $(minikube docker-env)

docker build -t carloshkayser/spark-streaming-app:latest .

docker run -it -v $(pwd):/app carloshkayser/spark-streaming-app:latest bash




```sh
poetry run jupyter-lab
```




```sh
eval $(minikube docker-env)
$SPARK_HOME/bin/docker-image-tool.sh -m -t v3.2.1 build
```

```sh
$SPARK_HOME/bin/spark-submit \
  --deploy-mode cluster \
  --name spark-pi
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=2 \
  --executor-memory 512m \
  --conf spark.kubernetes.container.image=spark:v3.2.1 \
  local:///$SPARK_HOME/examples/jars/spark-examples_2.12-3.2.1.jar
```