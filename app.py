from flask import Blueprint, abort, make_response
from time import time

main = Blueprint('main', __name__)

from network_filter import NetworkFilter
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request, jsonify

@main.errorhandler(400)
def not_found(error):
    return make_response(jsonify({'error': 'Bad Request'}), 400)

@main.errorhandler(500)
def not_found(error):
    return make_response(jsonify({'error': 'Server Error'}), 500)


@main.route("/traffic/block", methods=["POST"])
def should_block():
    try:
        if request is None or request.data is None:
            abort(400)

        post_data = request.get_json(force=True)

        if 'features' in post_data:
            features = post_data['features']

            return jsonify({'block': network_filter.block(features) })
        else:
            abort(400)
    except Exception as ex:
        if "Bad Request" in str(ex):
            abort(400)
        else:
            logger.error(str(ex))
            abort(500)

@main.route("/traffic/reload", methods=["POST"])
def reload_retrain():
    try:
        post_data = request.get_json()

        if 'path' in post_data:
            path = post_data['path']
            logger.info(path)

            t0 = time()
            network_filter.reload_and_retrain(path)
            train_time = time() - t0

            return jsonify({'retrained': True, 'trainingTime': train_time })
        else:
            abort(400)
    except Exception as ex:
        if "Bad Request" in str(ex):
            abort(400)
        else:
            logger.error(str(ex))
            abort(500)


def create_app(spark_context):
    global network_filter

    network_filter = NetworkFilter(spark_context)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app