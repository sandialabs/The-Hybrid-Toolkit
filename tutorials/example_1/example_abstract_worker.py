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
        print "starting step 2 of the pipline"

        # grab a field from before the first step of the pipline
        data_vector = doc.getMetaData("data_vector")

        print "print our data_vector recieved"
        print(data_vector)

        # grab some stuff from the first step of the pipline
        model_and_modeling_worker_result = doc.getMetaData("model_and_modeling_worker")
        print("get the model_and_modeling_worker written values ::", model_and_modeling_worker_result)
        target_class = model_and_modeling_worker_result["target_class"]

        print "print some meta data from the first step in the pipline"
        print "target_class:", target_class

        self.addMetaData(doc, "last_bit_of_meta_data", "finishing pipline")
        print "adding meta data"
        return doc
