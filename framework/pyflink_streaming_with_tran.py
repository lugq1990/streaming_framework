import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, Kafka, Json

class TransformationEngine:
    def __init__(self, config):
        self.config = config

    def _filter(self, table, transformation):
        return table.filter(f"{transformation['column']} {transformation['condition']}")

    def _select(self, table, transformation):
        return table.select(', '.join(transformation['columns']))

    def _withColumn(self, table, transformation):
        return table.add_columns(f"{transformation['expression']} as {transformation['column']}")

    def _drop(self, table, transformation):
        columns_to_keep = [col for col in table.get_schema().get_field_names() if col not in transformation['columns']]
        return table.select(', '.join(columns_to_keep))

    def _groupBy(self, table, transformation):
        group_cols = ', '.join(transformation['columns'])
        agg_exprs = ', '.join([f"{value}({key}) as {key}" for key, value in transformation['aggregations'].items()])
        return table.group_by(group_cols).select(f"{group_cols}, {agg_exprs}")

    def _orderBy(self, table, transformation):
        order_cols = ', '.join([f"{col} {'ASC' if transformation.get('ascending', True) else 'DESC'}" for col in transformation['columns']])
        return table.order_by(order_cols)

    def _limit(self, table, transformation):
        return table.limit(transformation['num'])

    def apply_transformation(self, table, transformation):
        transformation_type = transformation['type']
        method = getattr(self, f"_{transformation_type}", None)
        if method is not None:
            return method(table, transformation)
        else:
            raise ValueError(f"Unknown transformation type: {transformation_type}")

    def apply_transformations(self, table):
        for transformation in self.config['transformations']:
            table = self.apply_transformation(table, transformation)
        return table

# Initialize the streaming environment
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# Define the schema for the transaction data
schema = DataTypes.ROW([
    DataTypes.FIELD("transaction_id", DataTypes.STRING()),
    DataTypes.FIELD("amount", DataTypes.FLOAT()),
    DataTypes.FIELD("customer_id", DataTypes.STRING()),
    DataTypes.FIELD("transaction_type", DataTypes.STRING()),
    DataTypes.FIELD("timestamp", DataTypes.STRING()),
    DataTypes.FIELD("description", DataTypes.STRING()),
    DataTypes.FIELD("account_number", DataTypes.STRING()),
    DataTypes.FIELD("merchant", DataTypes.STRING())
])

# Read the Kafka stream
t_env.connect(
    Kafka()
    .version("universal")
    .topic("bank_transactions")
    .start_from_latest()
    .property("zookeeper.connect", "localhost:2181")
    .property("bootstrap.servers", "localhost:9092")
).with_format(
    Json()
    .json_schema(schema)
    .fail_on_missing_field(True)
).with_schema(
    Schema()
    .field("transaction_id", DataTypes.STRING())
    .field("amount", DataTypes.FLOAT())
    .field("customer_id", DataTypes.STRING())
    .field("transaction_type", DataTypes.STRING())
    .field("timestamp", DataTypes.STRING())
    .field("description", DataTypes.STRING())
    .field("account_number", DataTypes.STRING())
    .field("merchant", DataTypes.STRING())
).in_append_mode().create_temporary_table("kafka_source")

# Load the transformation configuration
with open('transformations.json', 'r') as file:
    config = json.load(file)

# Create a table from the Kafka source
transaction_table = t_env.from_path("kafka_source")

# Create an instance of the TransformationEngine and apply transformations
transformation_engine = TransformationEngine(config)
transformed_table = transformation_engine.apply_transformations(transaction_table)

# Define the output sink (for example, print to console)
t_env.connect(
    Kafka()
    .version("universal")
    .topic("transformed_transactions")
    .property("zookeeper.connect", "localhost:2181")
    .property("bootstrap.servers", "localhost:9092")
).with_format(
    Json()
).with_schema(
    Schema()
    .field("transaction_id", DataTypes.STRING())
    .field("amount", DataTypes.FLOAT())
    .field("customer_id", DataTypes.STRING())
    .field("timestamp", DataTypes.STRING())
    .field("amount_usd", DataTypes.FLOAT())
    .field("total_amount", DataTypes.FLOAT())
    .field("transaction_count", DataTypes.BIGINT())
).in_append_mode().create_temporary_table("kafka_sink")

# Write the transformed table to the output sink
transformed_table.execute_insert("kafka_sink").wait()
