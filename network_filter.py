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

    def __train_model(self):
        logger.info("Training the Logit model...")
        self.model = LogisticRegressionWithLBFGS.train(self.train_data)
        logger.info("Logit model built!")

    def block(self, features):
        return self.model.predict(features) == 1

    def __init__(self, sc):
        # Init filter with injected spark context
        logger.info("Starting up the Network Filter: ")
        self.sc = sc
        # Load training data
        logger.info("Loading training data...")
        raw_data = self.sc.textFile('data/corrected.gz')
        self.train_data = raw_data.map(parse_entry)
        # Train the model
        self.__train_model()