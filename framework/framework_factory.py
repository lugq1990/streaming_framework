from abc import ABC, abstractmethod
from utils import get_spark_session, get_streaming_context


class StreamingFramework(ABC):
    """Main class for spark and flink to follow"""
    def __init__(self) -> None:
        super().__init__()
        
    @abstractmethod
    def init(self) -> None:
       ...
       
    @abstractmethod
    def read_data(self) -> None:
       ...
       
    @abstractmethod
    def write_data(self) -> None:
       ...
       
    @abstractmethod
    def process_data(self) -> None:
       ...
       
    def run(self):
        self.init()
        input_data = self.read_data()
        output = self.process_data(input_data)
        self.write_data(output)
    

class SparkStreamingFramework(StreamingFramework):
    def init(self) -> None:
        self.spark = get_spark_session()
        self.streaming = get_streaming_context(self.spark)
    
    def read_data(self) -> None:
        return super().read_data()