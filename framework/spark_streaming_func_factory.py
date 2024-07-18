from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import expr, struct, to_json
import json
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, to_timestamp
from pyspark.sql.types import *
from utils import *
import tempfile
from abc import ABC
from kafka import KafkaConsumer
import pandas as pd
import json




class DataSink(ABC):
    def __init__(self, config) -> None:
        self.config = config
        super().__init__()
        
    def sink(self, df, params):
        pass
    
    
class DataSource(ABC):
    def __init__(self, config) -> None:
        self.config = config
        super().__init__()
        
    def read(self, config):
        pass


class DataTransformation(ABC):
    def __init__(self, config) -> None:
        self.config = config
        super().__init__()
        
    def transfapply_transformationsorm(self, df):
        pass
    

def map_function(params):
    func = eval(params['function'])
    return func

def reduce_by_key_function(params):
    func = eval(params['function'])
    return func

def filter_function(params):
    column = params['column']
    condition = params['condition']
    return lambda x: eval(f"x['{column}'] {condition}")

def map_values_function(params):
    func = eval(params['function'])
    return func

def flat_map_values_function(params):
    func = eval(params['function'])
    return func


class DataTransformFactory(DataTransformation):
    def __init__(self, config):
        self.config = config

        # Map transformation types to methods
        self.transformation_map = {
            "filter": self._filter,
            "select": self._select,
            "withColumn": self._withColumn,
            "drop": self._drop,
            "limit": self._limit,
            "flatMap": self._flatMap,
            "repartition": self._repartition,
            "reduceByKey": self._reduceByKey,
            "groupByKey": self._groupByKey,
            "mapValues": self._mapValues,
            "flatMapValues": self._flatMapValues,
            "reduceByKeyAndWindow": self._reduceByKeyAndWindow,
            "countByValue": self._countByValue,
            "window": self._window,
            "countByWindow": self._countByWindow,
            "reduceByWindow": self._reduceByWindow,
            "join": self._join,
            "leftOuterJoin": self._leftOuterJoin,
            "rightOuterJoin": self._rightOuterJoin,
            "cogroup": self._cogroup,
            "updateStateByKey": self._updateStateByKey
        }
        
        
    def _limit(self, df, params):
        return df.limit(params['num'])
    
    def _filter(self, df, params):
        return df.filter(f"{params['column']} {params['condition']}")

    def _select(self, df, params):
        return df.select(*params['columns'])

    def _withColumn(self, df, params):
        return df.withColumn(params['column'], expr(params['expression']))

    def _drop(self, df, params):
        return df.drop(*params['columns'])

    def _flatMap(self, dstream, params):
        return dstream.flatMap(flat_map_values_function)

    def _repartition(self, dstream, params):
        return dstream.repartition(params['numPartitions'])

    def _reduceByKey(self, dstream, params):
        return dstream.reduceByKey(reduce_by_key_function)

    def _groupByKey(self, dstream, params):
        return dstream.groupByKey()

    def _mapValues(self, dstream, params):
        return dstream.mapValues(map_values_function)

    def _flatMapValues(self, dstream, params):
        return dstream.flatMapValues(flat_map_values_function)

    def _reduceByKeyAndWindow(self, dstream, params):
        windowDuration = params['windowDuration']
        slideDuration = params['slideDuration']
        return dstream.reduceByKeyAndWindow(reduce_by_key_function, windowDuration, slideDuration)

    def _countByValue(self, dstream, params):
        return dstream.countByValue()

    def _window(self, dstream, params):
        windowDuration = params['windowDuration']
        slideDuration = params['slideDuration']
        return dstream.window(windowDuration, slideDuration)

    def _countByWindow(self, dstream, params):
        windowDuration = params['windowDuration']
        slideDuration = params['slideDuration']
        return dstream.countByWindow(windowDuration, slideDuration)

    def _reduceByWindow(self, dstream, params):
        windowDuration = params['windowDuration']
        slideDuration = params['slideDuration']
        return dstream.reduceByWindow(reduce_by_key_function, windowDuration, slideDuration)

    def _join(self, dstream, params):
        other_stream = self._get_other_stream(params)
        return dstream.join(other_stream)

    def _leftOuterJoin(self, dstream, params):
        other_stream = self._get_other_stream(params)
        return dstream.leftOuterJoin(other_stream)

    def _rightOuterJoin(self, dstream, params):
        other_stream = self._get_other_stream(params)
        return dstream.rightOuterJoin(other_stream)

    def _cogroup(self, dstream, params):
        other_stream = self._get_other_stream(params)
        return dstream.cogroup(other_stream)

    def _updateStateByKey(self, dstream, params):
        def updateFunction(new_values, running_count):
            return sum(new_values) + (running_count or 0)
        return dstream.updateStateByKey(updateFunction)

    def _print(self, dstream):
        return dstream.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

    def _saveAsTextFiles(self, dstream, params):
        dstream.saveAsTextFiles(params['path'], params.get('suffix', ''))

    def _saveAsObjectFiles(self, dstream, params):
        dstream.saveAsObjectFiles(params['path'], params.get('suffix', ''))

    def _foreachRDD(self, dstream, params):
        dstream.foreachRDD(lambda rdd: eval(params['function']))

    def transform(self, dstream):
        """Core process logic here, get the streaming df, apply the transformations and return the result,
        currently is sequence of operations, should support with DAG.
        
        For DAG, should provide an id for each component, then could connect each other

        Args:
            df (dstream): created by ssc

        Raises:
            ValueError: _description_
        """
        transformations = self.config['transformations']

        for transformation in transformations:
            trans_type = transformation['type']
            params = transformation['params']
            if trans_type in self.transformation_map:
                dstream = self.transformation_map[trans_type](dstream, params)
            else:
                raise ValueError(f"Unsupported transformation type: {trans_type}")

        return dstream
    
    

class DataSourceFactory(DataSource):
    def __init__(self, config) -> None:
        self.config = config['source']
        self.read_config = config['source']['read_config']
        
        self.source_factory = {
            'console': self.read_console,
            'hdfs': self.read_hdfs,
            'file': self.read_file,
            'kafka': self.read_kafka,
        }
        
        self.spark = get_spark_session()
         
    def read_kafka(self):
        bootstrap_servers = self.read_config['bootstrap_servers']
        input_topic = self.read_config['input_topic']
        startingOffsets = self.read_config.get('startingOffsets',  'earliest')
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", input_topic) \
            .option("startingOffsets", startingOffsets) \
            .load()
        
        # first infer schema, change from pyspark to pykafka to infer schema
        # schema = _infer_df_schema_for_kafka(df=df, spark=self.spark)
        schema = DataUtil._get_spark_schema(bootstrap_servers=bootstrap_servers, input_topic=input_topic)
        print('-'* 100)
        print("Read schema:\n")
        print(schema)
        
        df = df.selectExpr("CAST(value AS STRING)")
    
        # code here should be fine, but from the data gen should change.
        df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")
        # df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        
        df.printSchema()  

        return df
      
            
    def read_file(self):
        file_path = self.read_config['file_path']
        file_type = self.read_config.get('file_type', 'csv')
        infer_schema = self.read_config.get('infer_schema', 'true')
        
        return self.spark \
           .readStream \
           .format(file_type) \
           .option("header", infer_schema) \
           .load(file_path)
           
    def read_hdfs(self):
        file_path  = self.read_config['file_path']
        file_type  = self.read_config.get('file_type', 'csv')
        
        return self.spark \
           .readStream \
           .format(file_type) \
           .load(file_path)
           
    def read_console(self):
        return self.spark \
           .readStream \
           .format("console") \
           .load()
           
    def read(self):
        """Core function to read data from a source

        Args:
            config (Dict):  _description_

        Returns:
            _type_: _description_
        """
        source_type = self.config['type']
                
        df = self.source_factory[source_type]()
        return df
        
  
class DataSinkFactory(DataSink):
    #TODO: currently is only for spark, for flink should be similar
    def __init__(self, config):
        self.config = config['sink']
        self.sink_config = self.config['sink_config']
        self.sink_factary = {
            'console': self.sink_to_console,
            'file': self.sink_to_file,
            'kafka': self.sink_to_kafka,
        }
        
    def sink_to_console(self, df): 
        mode = self.sink_config.get('mode', 'append')
        query = df.writeStream \
            .outputMode(mode) \
            .format("console") \
            .start()
            
        query.awaitTermination()
    
    
    def sink_to_file(self, df):
        """Support both local file and hdfs"""
        file_path = self.sink_config['file_path']
        file_type = self.sink_config.get('file_type', 'csv')
        infer_schema = self.sink_config.get('infer_schema', 'true')
        
        query = df.write.format(file_type).option("header", infer_schema).save(file_path)
        query.awaitTermination()


    def sink_to_kafka(self, df):
        """Write data to kafka, supported with selected cols to dump.
        key_col must be provided, as the kafka only support with key-value pairs.

        Args:
            df (_type_): _description_
            params (_type_): _description_
            default_split_key (str, optional): _description_. Defaults to ','.

        Returns:
            _type_: _description_
        """
        selected_cols = self.sink_config.get('selected_cols', None)
        key_col = self.sink_config.get('key_col')
        default_split_key = self.sink_config.get('default_split_key', ',')
        
        bootstrap_servers = self.sink_config['bootstrap_servers']
        topic = self.sink_config['sink_topic']

        
        if selected_cols and not isinstance(selected_cols, list) and isinstance(selected_cols, str):
            # try to convert to list
            selected_cols = selected_cols.split(default_split_key)
        
        if not selected_cols:
            # then just dump full cols
            selected_cols = df.columns
            # selected_cols = [f'CAST({col} AS STRING)' for col in cols]
       
        # todo: here should be converted to func.
        value_expr = to_json(struct([col(c) for c in selected_cols])).alias("value")
        selected_df = df.select(value_expr)
        # kafka_df = df.select(col("amount").cast("string").alias("value"), col("transaction_id").cast("string").alias("key"))
        print('value_expr: ', value_expr)
        
        query = selected_df \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", topic) \
            .option("checkpointLocation", tempfile.mkdtemp()) \
            .start()
        query.awaitTermination()
            
    def sink(self, df):
        sink_type = self.config['sink_type']
        sink_params = self.config.get('params', '')
        
        if sink_type not in self.sink_factary:
            raise Exception(f'Sink type {sink_type} is not supported')
        
        return self.sink_factary[sink_type](df)
            

if __name__ == '__main__':
    # Example usage:
    
    # config = load_config('transform_to_console.json')
    config = load_config('spark_sample_transform.json')
    print('*' * 100)
    print(config)
    print('*' * 100)
    
    df = DataSourceFactory(config=config).read()
    df = DataTransformFactory(config).transform(df)
    df = DataSinkFactory(config).sink(df)
    
    # query = df.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", "false")  \
    #     .start()
    # query.awaitTermination()
    
    