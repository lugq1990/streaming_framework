"""Used to get kafka schema for user to write their sqls, will send out a mail for schema info"""
from utils import DataUtil
import json
from argparse import ArgumentParser



if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-m", "--mail", default=None)
    parser.add_argument("-topic", "--topic_name", default='test')
    parser.add_argument("-bs", "--bootstrap_servers", default="localhost:9092")
    parser.add_argument("-aor", "--auto_offset_reset", default='earliest')
    
    parser = parser.parse_args()
    
    topic_name = parser.topic_name
    mail = parser.mail
    bootstrap_servers = parser.bootstrap_servers
    auto_offset_reset = parser.auto_offset_reset
    
    schema = DataUtil._infer_kafka_data_schema(input_topic=topic_name, bootstrap_servers=bootstrap_servers, auto_offset_reset=auto_offset_reset)
    schema_str = json.dumps(schema, indent=4)
    
    print(schema_str)
    


