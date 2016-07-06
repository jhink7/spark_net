from flask import Blueprint

main = Blueprint('main', __name__)

from network_filter import NetworkFilter
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request, jsonify

@main.route("/traffic/block", methods=["POST"])
def should_block():
    features = request.get_json()['features']
    return jsonify({'block': network_filter.block(features) })


def create_app(spark_context):
    global network_filter

    network_filter = NetworkFilter(spark_context)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app