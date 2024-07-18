## Mac guide


### Start the server

- **start kafka cluster**
  
  [hands with kafka](https://kafka.apache.org/quickstart)
  ```shell
  bin/zookeeper-server-start.sh config/zookeeper.properties

  # start kafka broker
  bin/kafka-server-start.sh config/server.properties

  # create topic for read
  bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic transaction

  # create topic for write
  bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic transaction_output

  # describe topic
  bin/kafka-topics.sh --describe --topic transaction --bootstrap-server localhost:9092

  # ------bellow is just to test the topic-----
  
  # write to topic
  bin/kafka-console-producer.sh --topic transaction --bootstrap-server localhost:9092

  # read from topic
  bin/kafka-console-consumer.sh --topic transaction --from-beginning --bootstrap-server localhost:9092

  # read for write topic
  bin/kafka-console-consumer.sh --topic transaction_output --from-beginning --bootstrap-server localhost:9092


  # clean topic
  bin/kafka-topics.sh --delete --topic test --bootstrap-server localhost:9092
  bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic test

  # for output topic
bin/kafka-topics.sh --create --topic testout --bootstrap-server localhost:9092

  ```

