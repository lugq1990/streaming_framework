from abc import ABC, abstractmethod
from spark_core import SparkDataSourceFactory, SparkDataTransformFactory, SparkDataSinkFactory
from flink_core import FlinkDataSourceFactory, FlinkDataTransformFactory, FlinkDataSinkFactory
from utils import get_spark_session, get_flink_t_env, load_user_config
import os


class StreamFramwork(ABC):
    def __init__(self, config) -> None:
        self.config = config
    
    def run(self):
        pass
    
    

class FlinkStreamFramework(StreamFramwork):
    def __init__(self, config):
        super().__init__(config)
        self.t_env = get_flink_t_env()
    
    def run(self):
        print("Start to do pipeline processing for Flink!")
        table = FlinkDataSourceFactory(t_env=self.t_env, config=self.config).read()
        
        table = FlinkDataTransformFactory(t_env=self.t_env, config=self.config, table=table).execute_queries()
        
        FlinkDataSinkFactory(t_env=self.t_env, config=self.config, table=table).sink()
        
        print("Finished Spark pipeline!")
        
        

class SparkStreamFramework(StreamFramwork):
    def __init__(self, config):
        super().__init__(config)
        self.spark = get_spark_session(config=config)
        
    def run(self):
        print("Start to do pipeline processing for Spark!")
        df = SparkDataSourceFactory(config=self.config, spark=self.spark).read()
        
        df = SparkDataTransformFactory(config=self.config, spark=self.spark).execute_queries(df)
        
        df = SparkDataSinkFactory(config=self.config, spark=self.spark).sink(df)
        
        print("Finished Flink pipeline!")
        
        
if __name__ == "__main__":
    config = load_user_config('spark_trans.json')
    
    framework = SparkStreamFramework(config)
    framework.run()