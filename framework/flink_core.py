import json
import os

from pyflink.table.expressions import col
from pyflink.table.udf import udf
from pyflink.common.typeinfo import Types
from pyflink.table.types import DataTypes
from utils import load_user_config, DataUtil
from abc import ABC
from uuid import uuid4

from utils import DataUtil, convert_flink_table_data_type_to_sql_type, get_flink_t_env


# config = load_user_config('spark_trans.json')
# print('*' * 100)
# print("Get config: ", config)
# print('*' * 100)
# source_config = config['source']['read_config']
# input_topic = source_config['input_topic']
# bootstrap_servers = source_config['bootstrap_servers']

# schema = DataUtil._infer_kafka_data_schema(input_topic=input_topic, bootstrap_servers=bootstrap_servers)


class FlinkDataSink(ABC):
    def __init__(self, t_env, config) -> None:
        self.config = config
        self.t_env = t_env
        super().__init__()
    
    def sink(self, table):
        pass
    
    
class FlinkDataSource(ABC):
    def __init__(self, config, t_env) -> None:
        self.config = config
        self.t_env = t_env
        super().__init__()
        
    def read(self):
        pass


class FlinkDataTransformation(ABC):
    def __init__(self, config, t_env) -> None:
        self.config = config
        self.t_env = t_env
        super().__init__()
        
    def transform(self, table):
        pass
    
    def execute_queries(self, table):
        pass


class FlinkDataSourceFactory(FlinkDataSource):
    def __init__(self, config, t_env) -> None:
        super().__init__(config, t_env)
        self.source_factory = {
            'kafka': self.create_kafka_source_table,
            'file': self.create_file_source_table
        }
        # each app will have each own source_table_name that will be used for later step.
        self.source_table_name = 'source_table_{}'.format(uuid4().hex)
        # todo: for each type of the source should provide a inference logic to get the schema.
        
    def create_file_source_table(self):
        pass
    
    def create_kafka_source_table(self):
        source_config = self.config['source']['read_config']
        input_topic = source_config['input_topic']
        bootstrap_servers = source_config['bootstrap_servers']
        
        schema = DataUtil._infer_kafka_data_schema(input_topic=input_topic, bootstrap_servers=bootstrap_servers)
        print("Fink schema from kafka: ", schema)
    
        flink_sql = f"""
            CREATE TABLE {self.source_table_name} (
            {schema}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{source_config['input_topic']}',
                'properties.bootstrap.servers' = '{source_config['bootstrap_servers']}',
                'properties.group.id' = '{source_config['group_id']}',
                'format' = 'json',
                'scan.startup.mode' = 'earliest-offset',
                'json.ignore-parse-errors' = 'true'
            )
        """
        print("Source SQL: ", flink_sql)
        
        self.t_env.execute_sql(flink_sql)
        
    def read(self):
        """Based on different of input_type, return is the created table_name could be used for later step.

        Raises:
            ValueError: _description_

        Returns:
            _type_: _description_
        """
        source_type = self.config['source']['type']
        if source_type not in self.source_factory:
            raise ValueError('Invalid source type: {}'.format(source_type))
        
        print("Start to read source: {}".format(source_type))
        
        create_source_table_func = self.source_factory[source_type]
        create_source_table_func()
            
        print("Create source table: {}".format(self.source_table_name))

        # just return table obj
        table = self.t_env.from_path(self.source_table_name)
        return table
    

class FlinkDataSinkFactory(FlinkDataSink):
    def __init__(self, t_env, config, table) -> None:
        super().__init__(t_env, config)
        self.table = table
        self.sink_config = config['sink']['sink_config']
        self.sink_factory = {
            'console': self.create_console_sink_table,
            'file': self.create_file_sink_table,
            'kafka': self.create_kafka_sink_table
        }
        # based on created table object to get the schmea.
        self.schema = FlinkDataSinkFactory.get_table_schema(table=table)
        print("Sink schema: {}".format(self.schema))
        self.sink_table = 'sink_table_{}'.format(uuid4().hex)
        
    def create_file_sink_table(self):
        pass
          
    def create_console_sink_table(self):
        console_query = f"""
            CREATE TABLE {self.sink_table} (
                {self.schema}
            ) WITH (
                'connector' = 'print'
            )
        """
        print('Sink SQL:', console_query)
        self.t_env.execute_sql(console_query)
        
    def create_kafka_sink_table(self):
        sink_config = self.sink_config
        # todo: should infer schema from table obj with dynamic schema
        self.t_env.execute_sql(f"""
            CREATE TABLE {self.sink_table} (
                {self.schema}
            ) WITH (
                'connector' = 'print',
                'topic' = '{sink_config['sink_topic']}',
                'properties.bootstrap.servers' = '{sink_config['bootstrap_servers']}',
                'format' = 'json',
                'sink.partitioner' = 'round-robin'
            )
        """)
        
    def sink(self):
        """Sink func based on config, input is table obj that could be called 

        Args:
            table (_type_): _description_

        Returns:
            _type_: _description_
        """
        print("[SINK] sink started")
        sink_type = self.config['sink']['sink_type']
        
        if not sink_type in self.sink_factory:
            raise ValueError("No such sink type: {}".format(sink_type))
          
        # 1. create sink table, 2. execute insert based on table.
        print("Get sink type: {}".format(sink_type))
        
        create_sink_table_func = self.sink_factory[sink_type]
        create_sink_table_func()   
                      
        self.table.execute_insert(self.sink_table).wait()

    @staticmethod
    def get_table_schema(table):
        """Extracted table scheme that could be used to create the sink table"""
        schema = table.get_schema()
        field_names = schema.get_field_names()
        field_data_types = schema.get_field_data_types()
        
        sql_fields = []
        print('-' * 100)
        print('table schema: {}'.format(schema))
        print('-' * 100)
        for name, data_type in zip(field_names, field_data_types):
            print(data_type)
            sql_type = convert_flink_table_data_type_to_sql_type(data_type)
            sql_fields.append(f"{name} {sql_type}")
        
        return ", ".join(sql_fields)



class FlinkDataTransformFactory(FlinkDataTransformation):
    def __init__(self, config, t_env, table) -> None:
        super().__init__(config, t_env)
        self.table = table
         
    def execute_queries(self):
        """loop for each query and register as temp table, and do query, return a table

        Args:
            table (_type_): _description_

        Returns:
            _type_: _description_
        """
        table = self.table
        queries = self.config.get('queries', [])
        for query_config in queries:
            table_name = query_config["table_name"]
            query = query_config["query"]
            
            # create temp view and do the query
            self.t_env.create_temporary_view(table_name, table)
            
            table = self.t_env.sql_query(query)
        return table
  
    def apply_transformations(self):
        table = self.table
        
        transformations = self.config.get('transformations', [])
        for transform in transformations:
            if transform['type'] == 'filter':
                table = table.filter(f"{transform['params']['column']} {transform['params']['condition']}")
            elif transform['type'] == 'select':
                table = table.select(','.join(transform['params']['columns']))
            elif transform['type'] == 'withColumn':
                table = table.add_columns(f"{transform['params']['expression']} as {transform['params']['column']}")
            elif transform['type'] == 'drop':
                table = table.drop_columns(*transform['params']['columns'])
            elif transform['type'] == 'limit':
                table = table.limit(transform['params']['num'])
        return table


if __name__ == "__main__":
    t_env = get_flink_t_env()
    
    table = FlinkDataSourceFactory(t_env=t_env, config=config).read()
    # table.execute().wait()
    
    table = FlinkDataTransformFactory(t_env=t_env, config=config, table=table).execute_queries()
    
    FlinkDataSinkFactory(t_env=t_env, config=config, table=table).sink()
    