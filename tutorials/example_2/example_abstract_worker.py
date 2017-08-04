'''
Created on Nov 9, 2015

@author: wldavis
@author: mletter
'''
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import hybrid.manager
import random


class ExampleAbstractWorker(hybrid.worker.worker.abstract_worker):
    def __init__(self, **kwargs):
        hybrid.worker.worker.abstract_worker.__init__(self, **kwargs)
        self.set_uses_model(False)

    def process_observation_core(self, doc, **kwargs):
        """
    this method is called second in the pipeline after model_and_modeling_worker is called
    :param doc: this is the json doc that is coming through the pipline
    :param kwargs:
    :return: doc
    """
        print "starting pipline"

        # grab a field from before the first step of the pipline
        # data_vector = doc.getMetaData("queryID")
        data_vector = random.randint(1, 100)
        #
        # print "print our data_vector recieved"
        # print(data_vector)

        # grab some stuff from the first step of the pipline
        # model_and_modeling_worker_result = doc.getMetaData("model_and_modeling_worker")
        # print("get the model_and_modeling_worker written values ::", model_and_modeling_worker_result)
        # target_class = model_and_modeling_worker_result["target_class"]

        print "print some meta data from the first step in the pipline with queryID %s" % data_vector
        # print "target_class:", target_class
        if (int(data_vector) % 2) == 0:
            self.addMetaData(doc, "feedback", False)
        else:
            self.addMetaData(doc, "feedback", True)
        print "adding meta data"
        return doc
