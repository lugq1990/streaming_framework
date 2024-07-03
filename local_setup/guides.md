## Guide in Windows

Based on the docker to init the environ, create `kafka`, `flink`, `hdfs` etc.


### Docker steps

- Install kafka
  
  <!-- [docker kafka install](https://hub.docker.com/r/bitnami/kafka)
  ```shell
  docker pull bitnami/kafka
  ``` -->

  With the docker compose to setup the kafka

  ```shell
  cd  D:\code_related\github_code\my_work\streaming_framework\local_setup\docker_related 
  docker-compose up -d
  ```