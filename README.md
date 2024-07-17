# streaming_framework
A config file based streaming framework based on flink and spark streaming

## Design pattern

![Design process pattern](./local_setup/materials/streaming_framwork.png)

### Problem statement

Solve real time processing for each appliction, no need to write the streaming code with spark streaming and flink, provide a framework that could be used repeatly, extract the core of `ETL` with configured based.

### Goal

Reduce the code side, and robust for each application, support both `Spark` and `Flink`, the reason to support 2 types of framework is currently most of bathed jobs is spark based, for flink is that less latency and high-throughout of streaming.

Both of frameworks support `SQL` logic.

Core of this framework define source, sink, with related transformations for the data stream.

User could just provide a config file that will define which platform to use with `spark` and `flink`, the framework will do the rest.

### Design

Config file based, one is for each application, the other is for the framework setting, so for each appilication could set some configs related to each apps.

- Application config -> business logic implement, with app name, queries, sources, sinks, transformations
- Framework config -> setup running environment for each apps, should support isolated for resource manager.

### Application setting

2 Types of logics are supported, but for each apps should be use one type of logic will be best practice!

Both `Spark Streaming` and `Flink` support these 2 types:

- **Data stream**
- **SQL based**
  

### Framework setting

Based on each framework logic that could config for all projects, that some of them could be overwritten based on each application.

Some of configs like `task manager resource`, `parallem`, `taskslots` etcs.

But most of them should be configured as default value, like `statebackend`, `recovery`, `high availability` etc.

#### Spark streaming

For spark streaming is batched based, some of configs should be configed in app side.

- batch_interval
  - a default value is 5s, could be overwriten in biz_config
  - should be balanced for latency and throughout
- checkpoint
  - a root path for app to store and high-avaiable
- resource config
  - numExecutors
  - executorCores
  - driverMemory
- deplay
  - master
  - deployMode

```json
{
    "appName": "MySparkStreamingApp",
    "batchInterval": 5,
    "checkpointDirectory": "hdfs://path/to/checkpoint/dir",
    "executorMemory": "2g",
    "numExecutors": 4,
    "executorCores": 2,
    "driverMemory": "1g",
    "master": "local[*]",
    "deployMode": "client"
}
```


#### Flink

Sample config, for statebackend should be a list of supported: memory, filesystem, rucksdb. But should support a default value, each app could have each own.

- stateBackend
  - list of supported: memory, filesystem, rucksdb
  - a root folder if is a path, each app will have it's own backend folder, with each time running will create it
- checkpoint
  - default interval with 6000s
  - default root path in HDFS to store
  - each app will have each own path
  - when to restore will use the latest one
- savepointPath
  - a root of full apps, each will have it's own path
  - when the app start, should save the job id to somewhere, then user could retrieve it, and see it in web, also could support savepoint to stop and start apps.
- highAvailability
  - based on zookeeper to avoid single point failure
  - stored in hdfs


```json
{
    "appName": "MyFlinkApp",
    "savepointPath": "file:///path/to/savepoint",
    "taskManagerMemory": "1024m",
    "numTaskSlots": 4,
    "parallelism": 2,
    "jobManagerMemory": "2048m",
    "restartStrategy": {
        "type": "fixed-delay",
        "attempts": 3,
        "delay": "10 s"
    },
    "checkpoint":{
        "path": "file:///path/to/checkpoint",
        "checkpointInterval": 6000
    },
    "stateBackend": {
        "type": "filesystem",
        "path": "file:///path/to/state/backend"
    },
    "highAvailability": {
        "mode": "zookeeper",
        "zookeeperQuorum": "localhost:2181",
        "storagePath": "file:///path/to/ha/storage"
    }
}

```


### implement

Each of application could have it's own config to implement biz logic, from source, transformation, sink.

For each component support `DAG` and `Sequeence` logic, as each of component will have one id, will construct DAG object to represent logic.

As currently is streaming based, but for source and sink are kafka supported best! To ensure `extractly-once` logic!

- Source
  - File
  - Kafka
  - Hive
  - HDFS
- Transform
  - Data stream
    - map
    - filter
    - reduce
    - select
    - limit
    - drop
    - *
  - SQL
    - query based
    - support watermark for pylink in sql side if user provide need, constructed in framework code side
    - agg based with sql
- Sink
  - File
  - Kafka
  - Hive
  - HDFS

### feedback

Already 2 projects have use this framework and will expand it to more projects.

### real use case for projects

- Transaction filtering, alerting related
- Real time dashboard refreshing

