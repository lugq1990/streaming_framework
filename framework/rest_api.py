from flask import Flask, request, jsonify
from pyflink.table import TableEnvironment
from pyflink.table.expressions import col
from pyflink.common import JobID
import json
import os

from utils import get_flink_t_env, load_user_config
from flink_core import FlinkTableJobManager

# Flask app setup
app = Flask(__name__)


t_env = get_flink_t_env()
# todo: user should provide config file name, then load dynamic
user_config = load_user_config('project_trans.json')
flink_manager = FlinkTableJobManager(t_env=t_env, config=user_config)


@app.route('/get_job_id', methods=['GET'])
def get_job_id():
    job_id = flink_manager.get_job_id()
    return jsonify({'job_id': job_id})


@app.route('/trigger_savepoint', methods=['POST'])
def trigger_savepoint():
    data = request.json
    result = flink_manager.trigger_savepoint()
    return jsonify({'result': result})


@app.route('/stop_job_with_savepoint', methods=['POST'])
def stop_job_with_savepoint():
    data = request.json
    result = flink_manager.stop_job_with_savepoint()
    return jsonify({'result': result})


@app.route('/resume_job_from_savepoint', methods=['POST'])
def resume_job_from_savepoint():
    data = request.json
    result = flink_manager.resume_job_from_savepoint()
    return jsonify({'result': result})


if __name__ == '__main__':
    app.run(debug=True)
    
    