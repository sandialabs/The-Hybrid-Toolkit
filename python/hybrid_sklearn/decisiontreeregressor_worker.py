"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import hybrid
from hybrid.worker.worker import modeling_worker
from hybrid_sklearn import sklearn_utils
from hybrid import view, data_blob

import numpy as np

class decision_tree_regressor_worker(modeling_worker):
  def __init__(self,**kwargs):
      # Call super class init first
      if (kwargs.get("default_name") == None):
          kwargs["default_name"] = "decision_tree_regressor"
      if (kwargs.get("model_type") == None):
          kwargs["model_type"]=sklearn_utils.decision_tree_regressor_model

      modeling_worker.__init__(self, **kwargs)

      self._model_type = kwargs.get("model_type")
      self._model = kwargs.get("model")
      self._model_observations_query_info = kwargs.get("model_observations_query_info")
      self._model_observations_manager_name = kwargs.get("model_observations_manager_name")

      self._feature_vector_paths = kwargs.get("feature_vectors")
      self._truth_vector_paths = kwargs.get("truth_vectors")

  @staticmethod
  def loadFromJSON(json_data):
      jsontype = json_data["_jsontype"]
      if not (jsontype == "hybrid_sklearn.decisiontreeregressor_worker.decision_tree_regressor_worker"):
          return None

      return decision_tree_regressor_worker(**json_data)

  def get_feature_vectors(self,observations):
    model = self._model

    feature_vectors = self._feature_vector_paths
    observation_vector_dict = hybrid.utils.create_observation_data(observations, feature_vectors)
    observation_array = sklearn_utils.observations_to_sklearn (observation_vector_dict, feature_vectors)
    return observation_array

  def get_truth_vectors (self, observations):
    model = self._model
    truth_name_list = []

    truth_vectors = self._truth_vector_paths
    truth_vector_dict = hybrid.utils.create_observation_data(observations, truth_vectors)
    truth_array = sklearn_utils.observations_to_sklearn (truth_vector_dict, truth_vectors)
    return np.ravel (truth_array)

  def update_model(self):
    old_model = self._model

    model_observations = None
    if (self._model_observations_query_info == None):
      model_observations_manager = old_model.getModelObservationsManager()

      if not model_observations_manager.isValid():
        return False

      if not model_observations_manager.isUpdated ():
        model_observations_manager.update ()
      model_observations = model_observations_manager.getRows ()
    else:
      # else  use the old style of update
      model_documents_view = view.create_view_from_query_info(self._model_observations_query_info)
      model_observations = model_documents_view.rows ()

    feature_vectors = self.get_feature_vectors(model_observations)
    truth_vectors = self.get_truth_vectors(model_observations)

    model_data = hybrid.data_blob.create ("dtr_model_data")
    model_data.setMetaData ("observation_vectors", feature_vectors)
    model_data.setMetaData ("truth_vectors", truth_vectors)

    model = self._model_type (old_model._parameters_uuid)
    model._parameters = old_model._parameters

    model.setDBType (self._model_db_type)
    model.setDBHost (self._model_db_host)
    model.setDBName (self._model_db_name)

    model.setModelData (model_data)
    return model.update ()

  def process_observations_core(self, observations, **kwargs):
    model = self.loadModel ()

    feature_vectors = self.get_feature_vectors(observations)

    model_data = hybrid.data_blob.create ("dtr_eval_data")
    model_data.setMetaData ("observation_vectors", feature_vectors)
    model.setModelData (model_data)

    model.project_and_store (model_data, None)
    results = model.getMetaData ("dtr_results")

    for observation_index in range (len (observations)):
      print "writing out result for index " + str(observation_index) + " as " + str(results[observation_index])
      self.setMetaData (observations[observation_index], "dtr_class", results[observation_index])

    return observations
