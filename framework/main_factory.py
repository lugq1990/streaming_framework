"""Main entry point of streaming framework, main framework supported:
- Spark
- flink

User could select engine to execute streaming job.

NOTE:
Regarding to SQL side, user should provide valid SQL to execute, if run error with sql side, then engine will stop and raise 
Runtime error for query!


User is responsible to define user code logic and if needed with some configuration for application,
if user provide some config, then will overwrite it from default.

For each application will have it's own config file, then main func will load it and coordinate with resource manager
to get resource to execute in isolated env.

By default, resource manager is based on framework side with spark and flink.

"""
from abc import ABC, abstractmethod
from spark_core import SparkJobManager
from flink_core import FlinkTableJobManager
from utils import get_spark_session, get_flink_t_env, load_user_config
import os
from argparse import ArgumentParser
from datetime import datetime


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
        job_id = FlinkTableJobManager(t_env=self.t_env, config=self.config).run()
        print("get job_id: {}".format(job_id))
    
        return job_id
        
        

class SparkStreamFramework(StreamFramwork):
    def __init__(self, config):
        super().__init__(config)
        self.spark = get_spark_session(config=config)
        
    def run(self):
        print("Start to do pipeline processing for Spark!")
        
        df = SparkJobManager(config=self.config, spark=self.spark).run()
       
        print("Finished Flink pipeline!")
        
        
if __name__ == "__main__":
    
    parser = ArgumentParser()
    
    parser.add_argument('--config_name', type=str, default='project_trans.json' ,help='The config file name.')
    
    args = parser.parse_args()
    config_name = args.config_name
    
    config = load_user_config(config_name)
    
    run_engine = config.get('run_engine', 'spark').lower()
    
    if run_engine == 'spark':
        framework = SparkStreamFramework(config=config)
    elif run_engine == 'flink':
        framework = FlinkStreamFramework(config=config)
    else:
        raise Exception('The run engine should be spark or flink!')
    
    framework.run()
    