from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import expr, struct, to_json
import json
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType;
from utils import check_config_transform
from utils import load_config, get_spark_session, get_streaming_context
import tempfile


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


class SparkStreamingProcessor:
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

    def apply_transformations(self, dstream):
        """Core process logic here, get the streaming df, apply the transformations and return the result,
        currently is sequence of operations, should support with DAG.
        
        For DAG, should provide an id for each component, then could connect each other

        Args:
            df (dstream): created by ssc

        Raises:
            ValueError: _description_
        """
        transformations = self.config['transformations']
        output_config = self.config['output']

        for transformation in transformations:
            trans_type = transformation['type']
            params = transformation['params']
            if trans_type in self.transformation_map:
                dstream = self.transformation_map[trans_type](dstream, params)
            else:
                raise ValueError(f"Unsupported transformation type: {trans_type}")

        # sink data
        dstream = fileSinkFactory().sink(df=dstream, params=output_config)
        return dstream


class fileSinkFactory:
    def __init__(self):
        self.sink_factary = {
            'log_to_console': self.log_to_console,
            'write_to_hdfs': self.write_to_hdfs,
            'write_to_file': self.write_to_file,
            'write_to_kafka': self.write_to_kafka,
        }
        
    @staticmethod   
    def log_to_console(df, params):
        if not params:
            mode = 'append'
        return df.writeStream \
            .outputMode(mode) \
            .format("console") \
            .start()
    
    @staticmethod
    def write_to_file(df, params):
        return df.write.format("csv").option("header", "true").save(params['path'])

    @staticmethod
    def write_to_hdfs(df, params):
        return df.write.format("parquet").save(params['path'])

    @staticmethod
    def write_to_kafka(df, params, default_split_key=','):
        """Write data to kafka, supported with selected cols to dump.
        key_col must be provided, as the kafka only support with key-value pairs.

        Args:
            df (_type_): _description_
            params (_type_): _description_
            default_split_key (str, optional): _description_. Defaults to ','.

        Returns:
            _type_: _description_
        """
        selected_cols = params.get('selected_cols', None)
        key_col = params.get('key_col')
        if not key_col:
            raise ValueError("write to kafka must provide the key_col!")
        
        if selected_cols and not isinstance(selected_cols, list) and isinstance(selected_cols, str):
            # try to convert to list
            selected_cols = selected_cols.split(default_split_key)
        
        if not selected_cols:
            # then just dump full cols
            selected_cols = df.columns
            # selected_cols = [f'CAST({col} AS STRING)' for col in cols]
       
        # convert to key value 
        value_expr = to_json(struct([col(c) for c in selected_cols])).alias("value")
        selected_df = df.select(value_expr, col(key_col).cast("string").alias("key"))
        # kafka_df = df.select(col("amount").cast("string").alias("value"), col("transaction_id").cast("string").alias("key"))
        print('value_expr: ', value_expr)
        
        return selected_df \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", params['bootstrapServers']) \
            .option("topic", params['topic']) \
            .option("checkpointLocation", tempfile.mkdtemp()) \
            .start()
            
    def sink(self, df, params):
        sink_type = params['type']
        sink_params = params.get('params', '')
        if sink_type not in self.sink_factary:
            raise Exception(f'Sink type {sink_type} is not supported')
        return self.sink_factary[sink_type](df, sink_params)
            
    
def _infer_df_schema_for_kafka(df, spark, is_kafka=True):
    """ this should be called only when the read the kafka data, otherwise will infer full data schema

    Args:
        df (_type_): _description_
        spark (_type_): _description_
        is_kafka (bool, optional): _description_. Defaults to True.

    Returns:
        _type_: _description_
    """
    if is_kafka:
        sample_json = df.selectExpr("CAST(value AS STRING)").take(1)[0][0]
    else:
        # if ono need to cast the data type to string
        sample_json = df.take(1)[0][0]
        
    schema = spark.read.format(spark.sparkContext.param([sample_json])) \
         .schema()
    return schema
    

if __name__ == '__main__':
    # Example usage:
    
    # config = load_config('transform_to_console.json')
    config = load_config('sample_transform.json')

    # Initialize the Spark session
    spark = get_spark_session()
    ssc = get_streaming_context()
    # Set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
        
    # Read the Kafka stream
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transaction") \
        .option("startingOffsets", "earliest") \
        .load()

    # Select the value column and cast it to string
    transactions_df = kafka_df.selectExpr("CAST(value AS STRING)")
    
    # here should be changed to a more generic way
    # schema = StructType([
    #     StructField("transaction_id", StringType(), True),
    #     StructField("amount", FloatType(), True),
    #     StructField("customer_id", StringType(), True),
    #     StructField("transaction_type", StringType(), True),
    #     StructField("timestamp", StringType(), True),  # We'll convert this to TimestampType later
    #     StructField("description", StringType(), True),
    #     StructField("account_number", StringType(), True),
    #     StructField("merchant", StringType(), True)
    # ])


    # Parse the JSON data and apply the schema
    # here let the spark to infer the schema dymanically 

    schema = _infer_df_schema_for_kafka(df=transactions_df, spark=spark)
         
    parsed_df = transactions_df \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Convert the timestamp string to TimestampType
    parsed_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))


    processor = SparkStreamingProcessor(config)
    dstream = processor.apply_transformations(parsed_df)

    dstream.awaitTermination()
    