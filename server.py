
import cherrypy as cp
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf


def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("network-classifier-server")
    # pass additional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['network_filter.py', 'app.py'])

    # quiet logs
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

    return sc


def start_server(app):
    # enable WSGI lgging
    app_logged = TransLogger(app)
    cp.tree.graft(app_logged, '/')

    # Start the cherrypy server
    cp.engine.start()
    cp.engine.block()


if __name__ == "__main__":
    # init the spark context
    sc = init_spark_context()
    # init our flask app
    app = create_app(sc)
    # start up the cherrypy web server fronting the flask app
    start_server(app)