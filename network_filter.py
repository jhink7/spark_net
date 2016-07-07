import os
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint
import numpy as np


import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_entry(line):
    segs = line.split(",")
    # take only the columns that are statistically significant
    # predetermined offline
    filtered = segs[0:1] + segs[4:6] + segs[7:19] + segs[20:41]
    attack = 1.0
    if segs[41]=='normal.':
        attack = 0.0
    return LabeledPoint(attack, np.array([float(x) for x in filtered]))

class NetworkFilter:

    def __load_data(self, data_path):
        logger.info("Loading training data...")
        raw_data = self.sc.textFile(data_path)
        data = raw_data.map(parse_entry)
        return data

    def __train_model(self):
        logger.info("Training the Logit model...")
        model = LogisticRegressionWithLBFGS.train(self.train_data)
        logger.info("Logit model built!")
        return model

    def block(self, features):
        return self.model.predict(features) == 1

    def reload_and_retrain(self, data_path):
        try:
            # Load intial training data
            data = self.__load_data(data_path)
            # Train the intial model
            model = self.__train_model()

            self.train_data = data
            self.model = model
        except Exception:
            logger.error("Error reloading data")
            raise Exception("Error reloading data")


    def __init__(self, sc):
        # Init filter with injected spark context
        logger.info("Starting up the Network Filter: ")
        self.sc = sc
        # Load intial training data
        self.train_data = self.__load_data('data/corrected.gz')
        # Train the intial model
        self.model = self.__train_model()