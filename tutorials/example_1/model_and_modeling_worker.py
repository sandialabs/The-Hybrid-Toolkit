"""
Created on May 15, 2015

@author: Matthew Letter mletter@sandia.gov
"""
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import cPickle as pickle

import hybrid.manager
import numpy
from hybrid.model import model
from hybrid.worker.worker import modeling_worker
from sklearn import svm

# Evaluation doc limit
__doc_limit = 100
guesses = 0


class ExampleModel(model):
    def __init__(self, uuid, **kwargs):
        # Call super class init first
        model.__init__(self, uuid, **kwargs)

        gamma = kwargs.get("gamma")
        if (gamma is None):
            gamma = 0.001

        # Handle to storage for model parameters
        # Set various bits of meta data, these are defaults and can be changed later
        self._parameters.setMetaData("model_type", u"example_model")
        self._parameters.setMetaData("model_desc", u"A description of the example_model")
        self._parameters.setMetaData("gamma", gamma)
        self._parameters.setMetaData("clf", {})
        self._parameters.setMetaData("feature_vectors", [])
        self._parameters.setMetaData("truth_vectors", [])
        # self._hyperparameter_list.append("pattern")

    def classify(self):
        pass

    def update(self):
        # Check my model data
        observation_vectors = self._model_data.getMetaData("feature_vectors")
        truth_vectors = self._model_data.getMetaData("truth_vectors")

        params = self._parameters
        print "Updating model with ", observation_vectors, truth_vectors
        params.validateMeta()

        observation_vectors = numpy.array(observation_vectors).astype(numpy.float)
        truth_vectors = numpy.array(truth_vectors).astype(numpy.int)
        print "Updating model with ", observation_vectors, truth_vectors
        # Evaluation mode loads several model artifacts from storage and sets them as inputs
        clf = svm.SVC(gamma=0.001)
        clf.fit(observation_vectors, truth_vectors)
        params.setMetaData("clf", pickle.dumps(clf))
        self.finalize()

    def update_param(self, field, value):
        self._parameters.setMetaData(field, value)
        return self.update()

    @staticmethod
    def loadFromJSON(json_data):
        jsontype = json_data["_jsontype"]
        if not (jsontype == "model_and_modeling_worker.ExampleModel"):
            return None
        return ExampleModel(json_data["name"], **json_data)


class ExampleWorker(modeling_worker):
    def __init__(self, **kwargs):
        """
    this is the first worker called in our pipline
    """
        if (kwargs.get("default_name") == None):
            kwargs["default_name"] = "example_worker"
        if (kwargs.get("model_type") == None):
            print("Making an exampleModel")
            kwargs["model_type"] = ExampleModel
        # Call super class init
        modeling_worker.__init__(self, **kwargs)

        if (kwargs.get("pattern") == None):
            pattern = []
        self._model_type = kwargs.get("model_type")
        self._model = kwargs.get("model")
        self._model_observations_manager_name = kwargs.get("model_observations_manager_name")
        self._model_observations_query_info = kwargs.get("model_observations_query_info")
        model = self.loadModel()

    @staticmethod
    def loadFromJSON(json_data):
        jsontype = json_data["_jsontype"]
        if not (jsontype == "model_and_modeling_worker.ExampleWorker"):
            return None

        return ExampleWorker(**json_data)

    def update_model(self):
        print("Updating the model from worker!!!!!")
        old_model = self._model
        if (self._model_observations_query_info == None):
            print("query info is None")
            model_observations_manager = old_model.getModelObservationsManager()
            print("manager is", model_observations_manager)
            # If the model observations manager isn't ready (unstable or whatever) return False
            if model_observations_manager.isValid() is False:
                print("observation manager is value? ::", model_observations_manager.isValid())
                # return False
            if not model_observations_manager.isUpdated():
                model_observations_manager.update()
            model_observations = model_observations_manager.getRows()
        else:
            # else  use the old style of update
            model_documents_view = view.create_view_from_query_info(self._model_observations_query_info)
            model_observations = model_documents_view.rows()  # should be a []
        print("Updating the model from worker!!!!!", len(model_observations))
        feature_vectors = []
        truth_vectors = []
        for training_data in model_observations:
            feature_vectors.append([x.encode('UTF8') for x in training_data.getMetaData('data_vector')])
            truth_vectors.append(iris_map.index(training_data.getMetaData('class')))

        print "training data:::::", feature_vectors, truth_vectors

        model_data = hybrid.data_blob.create("svm_model_data")
        model_data.setMetaData("feature_vectors", feature_vectors)
        model_data.setMetaData("truth_vectors", truth_vectors)

        model = self._model_type(old_model._parameters_uuid)
        model._parameters = old_model._parameters

        model.setDBType(self._model_db_type)
        model.setDBHost(self._model_db_host)
        model.setDBName(self._model_db_name)

        model.setModelData(model_data)
        return model.update()

    def process_observation_core(self, doc, **kwargs):
        """
    called when a new document is added to the example_db

    :param doc: this is the json going through the pipline,
    :param kwargs:
    :return: doc
    """
        pattern_guess = doc.getMetaData("data_vector")

        print "\n\n\nstarting step 1 of the pipline since we have seen a new addition to example_db with pattern ", pattern_guess
        print("getting gamma for svm")
        # found at the example.json tag
        gamma = self._model.getMetaData("gamma")

        print "gamma = ", gamma

        # write some stuff into our json pipeline can be found under the model_and_modeling_worker tag in example_db
        # my_guesses += 1

        # red and white are just some tags we can look at in step 2 of our pipline
        target_class = doc.getMetaData("class")

        print "target class = ", target_class
        print "vector = ", pattern_guess
        # this will write out the tags into the model_and_modeling_worker tag
        self.setMetaData(doc, "target_class", target_class)
        self.setMetaData(doc, "input_vector", pattern_guess)

        model = self.loadModel()
        model_data = hybrid.data_blob.create("lr_eval_data")
        model_data.setMetaData("observation_vector", pattern_guess)
        model.setModelData(model_data)

        print "in classifier"
        observation_vectors = [x.encode('UTF8') for x in pattern_guess]
        observation_vectors = numpy.array(observation_vectors).astype(numpy.float)

        print "numpy vector1", observation_vectors

        clf = pickle.loads(self._model.getMetaData('clf').encode('UTF8'))
        results_array = clf.predict([observation_vectors])
        print "done classifying ", results_array

        # model.classify_vector(model_data)
        # print "classified"
        # results = model.getMetaData ("clf_results")
        # print "setting results",results
        self.setMetaData(doc, "classified_as", iris_map[results_array[0]])
        # print "results set done \n\n\n"
        return doc


iris_map = ["setosa", "versicolor", "virginica"]
