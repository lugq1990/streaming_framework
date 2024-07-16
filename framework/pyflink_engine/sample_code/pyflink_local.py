"""

# this is workable
./bin/flink run \
      --python /Users/guangqianglu/Documents/codes/my_codes/streaming_framework/framework/pyflink_engine/sample_code/pyflink_local.py


"""
import sys
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration

print(sys.executable)
# Create an execution environment
env = StreamExecutionEnvironment.get_execution_environment()

# Set the local Flink cluster configuration
# config = Configuration()
# config.set_string('pipeline.jars', 'file:///Users/guangqianglu/Downloads/flink-connector-kafka-1.17.2.jar')
# config.set_string('execution.target', 'local')
# config.set_integer('execution.parallelism.default', 1)

# env.configure(config)

# Define your data processing job here
env.from_collection([1, 2, 3, 4, 5]).map(lambda x: x * x).print()

# Execute the job
env.execute("local pyflink job")
