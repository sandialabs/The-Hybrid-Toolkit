"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import pickle

from hybrid import logger
from hybrid.model import model
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB
from sklearn.naive_bayes import MultinomialNB
from sklearn.tree import DecisionTreeRegressor

logger = logger.logger()

def observations_to_sklearn (observations, field_definitions):
  skobs = []
  for observation in observations:
    skob = []
    for col_name in field_definitions:
      try:
        skob.append (float(observation[col_name]))
      except ValueError:
        skob.append (observation[col_name])
    skobs.append (skob)
  return skobs

class gaussian_nb_model(model):
  ''' Gaussian Naive Bayes Model '''

  def __init__(self, uuid, **kwargs):
    # Call super class init first
    model.__init__(self, uuid, **kwargs)

    self._feature_vector_paths = kwargs.get("feature_vectors")
    self._truth_vector_paths = kwargs.get("truth_vectors")

    # Handle to storage for model parameters
    params = self._parameters

    # Set various bits of meta data, these are defaults and can be changed later
    params.setMetaData("model_type", u"gaussian_nb_model")
    params.setMetaData("model_desc", u"SciKits Gaussian Naive Bayes implementation")

    # Validator information
    params.setMetaData("db_views", [])

  @staticmethod
  def loadFromJSON(json_data):
    jsontype = json_data["_jsontype"]
    if not (jsontype == "hybrid_sklearn.sklearn_utils.gaussian_nb_model"):
        return None

    return gaussian_nb_model(json_data["name"], **json_data)

  def update(self):
    # Handle to storage for model parameters
    params = self._parameters

    # Make sure all my meta data is ready to go
    params.validateMeta()

    observation_vectors = []
    truth_vectors = []

    # Make sure my model data is ready to go
    self._model_data.validate()
    self._model_data.validateViews(self.getMetaData("db_views"))

    # Check my model data
    observation_vectors = self._model_data.getMetaData("observation_vectors")

    truth_vectors = self._model_data.getMetaData("truth_vectors")

    params.setMetaData("db_views", [])

    # Houston we are go
    gnb = GaussianNB ()
    gnb.fit (observation_vectors, truth_vectors)
    params.setBinaryData ("gnb_model", "application/pickle", pickle.dumps (gnb))

    self.finalize ()

  def project_and_store(self,model_data,model_data_db):
    observation_vectors = self._model_data.getMetaData("observation_vectors")

    params = self._parameters

    # Make sure all my meta data is ready to go
    params.validateMeta()

    gnb = pickle.loads (params.getBinaryData ("gnb_model"))

    results = gnb.predict (observation_vectors)
    params.setMetaData ("gnb_results", results)

class multinomial_nb_model(model):
  ''' Multinomial Naive Bayes Model '''

  def __init__(self, uuid, **kwargs):
    # Call super class init first
    model.__init__(self, uuid, **kwargs)

    # Handle to storage for model parameters
    params = self._parameters

    # Set various bits of meta data, these are defaults and can be changed later
    params.setMetaData("model_type", u"multinomial_nb_model")
    params.setMetaData("model_desc", u"SciKits Multinomial Naive Bayes implementation")

    params.setMetaData("alpha", kwargs.get("alpha"))
    params.setMetaData("fit_prior", kwargs.get("fit_prior"))
    params.setMetaData("class_prior", kwargs.get("class_prior"))

    # Validator information
    params.setMetaData("db_views", [])

  @staticmethod
  def loadFromJSON(json_data):
    jsontype = json_data["_jsontype"]
    if not (jsontype == "hybrid_sklearn.sklearn_utils.multinomial_nb_model"):
        return None

    return multinomial_nb_model(json_data["name"], **json_data)

  def update(self):
    # Handle to storage for model parameters
    params = self._parameters

    # Make sure all my meta data is ready to go
    params.validateMeta()

    observation_vectors = []
    truth_vectors = []

    # Make sure my model data is ready to go
    self._model_data.validate()
    self._model_data.validateViews(self.getMetaData("db_views"))

    # Check my model data
    observation_vectors = self._model_data.getMetaData("observation_vectors")

    truth_vectors = self._model_data.getMetaData("truth_vectors")

    params.setMetaData("db_views", [])

    # Houston we are go
    mnb = MultinomialNB ()

    mnb.alpha = self.getMetaData ("alpha")
    mnb.fit_prior = self.getMetaData ("fit_prior")
    class_prior = self.getMetaData ("class_prior")
    if (class_prior != None):
      mnb.class_prior = class_prior

    mnb.fit (observation_vectors, truth_vectors)
    params.setBinaryData ("mnb_model", "application/pickle", pickle.dumps (mnb))

    self.finalize ()

  def project_and_store(self,model_data,model_data_db):
    observation_vectors = self._model_data.getMetaData("observation_vectors")

    params = self._parameters

    # Make sure all my meta data is ready to go
    params.validateMeta()

    mnb = pickle.loads (params.getBinaryData ("mnb_model"))

    results = mnb.predict (observation_vectors)
    params.setMetaData ("mnb_results", results)

class logistic_regression_model(model):
  ''' Logistic Regression Model '''

  def __init__(self, uuid, **kwargs):
    # Call super class init first
    print "lr_model kwargs " + str(kwargs)

    model.__init__(self, uuid, **kwargs)

    self._feature_vector_paths = kwargs.get("feature_vectors")
    self._truth_vector_paths = kwargs.get("truth_vectors")

    # Handle to storage for model parameters
    params = self._parameters

    # Set various bits of meta data, these are defaults and can be changed later
    params.setMetaData("model_type", u"logistic_regression_model")
    params.setMetaData("model_desc", u"SciKits Logistic Regression implementation")

    params.setMetaData("penalty", kwargs.get("penalty"))
    params.setMetaData("dual", kwargs.get("dual"))
    params.setMetaData("C", kwargs.get("C"))
    params.setMetaData("fit_intercept", kwargs.get("fit_intercept"))
    params.setMetaData("intercept_scaling", kwargs.get("intercept_scaling"))
    params.setMetaData("class_weight", kwargs.get("class_weight"))
    params.setMetaData("max_iter", kwargs.get("max_iter"))
    params.setMetaData("random_state", kwargs.get("random_state"))
    params.setMetaData("solver", kwargs.get("solver"))
    params.setMetaData("tol", kwargs.get("tol"))
    params.setMetaData("multi_class", kwargs.get("multi_class"))
    params.setMetaData("verbose", kwargs.get("verbose"))

    # Validation fields
    # params.addRequiredBinaryFields([
            # "lr_coef_json", 
            # "lr_intercept_json"])

    # Validator information
    params.setMetaData("db_views", [])

  @staticmethod
  def loadFromJSON(json_data):
    jsontype = json_data["_jsontype"]
    if not (jsontype == "hybrid_sklearn.sklearn_utils.logistic_regression_model"):
        return None

    return logistic_regression_model(json_data["name"], **json_data)

  def update(self):
    # Handle to storage for model parameters
    params = self._parameters

    print "Starting to update Logistic Regression!"

    # Make sure all my meta data is ready to go
    params.validateMeta()

    observation_vectors = []
    truth_vectors = []

    # Make sure my model data is ready to go
    self._model_data.validate()
    self._model_data.validateViews(self.getMetaData("db_views"))

    # Check my model data
    observation_vectors = self._model_data.getMetaData("observation_vectors")

    truth_vectors = self._model_data.getMetaData("truth_vectors")

    params.setMetaData("db_views", [])

    # Houston we are go
    lr = LogisticRegression ()

    lr.penalty = params.getMetaData ("penalty")
    lr.dual = params.getMetaData ("dual")
    lr.C = params.getMetaData ("C")
    lr.fit_intercept = params.getMetaData ("fit_intercept")
    lr.intercept_scaling = params.getMetaData ("intercept_scaling")
    class_weight = params.getMetaData ("class_weight")
    if (class_weight != None):
      lr.class_weight = class_weight
    lr.max_iter = params.getMetaData ("max_iter")
    lr.random_state = params.getMetaData ("random_state")
    lr.solver = params.getMetaData ("solver")
    tol = params.getMetaData ("tol")
    if (tol != None):
      lr.tol = tol
    lr.multi_class = params.getMetaData ("multi_class")
    lr.verbose = params.getMetaData ("verbose")

    # Evaluation mode loads several model artifacts from storage and sets them as inputs
    lr.fit (observation_vectors, truth_vectors)
    params.setBinaryData ("lr_model", "application/pickle", pickle.dumps (lr))

    self.finalize ()

  def project_and_store(self,model_data,model_data_db):
    observation_vectors = self._model_data.getMetaData("observation_vectors")

    params = self._parameters

    # Make sure all my meta data is ready to go
    params.validateMeta()

    lr = pickle.loads (params.getBinaryData ("lr_model"))

    results = lr.predict (observation_vectors)
    probs = lr.predict_proba (observation_vectors)
    params.setMetaData ("lr_results", results)
    params.setMetaData ("lr_probs", probs)
    params.setMetaData ("lr_classes", lr.classes_)

class decision_tree_regressor_model(model):
  ''' Decision Tree Regressor Model '''

  def __init__(self, uuid, **kwargs):
    # Call super class init first
    model.__init__(self, uuid, **kwargs)

    self._feature_vector_paths = kwargs.get("feature_vectors")
    self._truth_vector_paths = kwargs.get("truth_vectors")

    # Handle to storage for model parameters
    params = self._parameters

    # Set various bits of meta data, these are defaults and can be changed later
    params.setMetaData("model_type", u"decision_tree_regressor_model")
    params.setMetaData("model_desc", u"SciKits Decision Tree Regressor implementation")

    params.setMetaData("criterion", kwargs.get("criterion"))
    params.setMetaData("splitter", kwargs.get("splitter"))
    params.setMetaData("max_features", kwargs.get("max_features"))
    params.setMetaData("max_depth", kwargs.get("max_depth"))
    params.setMetaData("min_samples_split", kwargs.get("min_samples_split"))
    params.setMetaData("min_samples_leaf", kwargs.get("min_samples_leaf"))
    params.setMetaData("max_weight_fraction_leaf", kwargs.get("max_weight_fraction_leaf"))
    params.setMetaData("max_leaf_nodes", kwargs.get("max_leaf_nodes"))
    params.setMetaData("random_state", kwargs.get("random_state"))
    params.setMetaData("presort", kwargs.get("presort"))

    # Validation fields
    # params.addRequiredBinaryFields([
            # "lr_coef_json", 
            # "lr_intercept_json"])

    # Validator information
    params.setMetaData("db_views", [])

  @staticmethod
  def loadFromJSON(json_data):
    jsontype = json_data["_jsontype"]
    if not (jsontype == "hybrid_sklearn.sklearn_utils.decision_tree_regressor_model"):
        return None

    return decision_tree_regressor_model(json_data["name"], **json_data)

  def update(self):
    # Handle to storage for model parameters
    params = self._parameters

    print "Starting to update Decision tree regressor!"

    # Make sure all my meta data is ready to go
    params.validateMeta()

    observation_vectors = []
    truth_vectors = []

    # Make sure my model data is ready to go
    self._model_data.validate()
    self._model_data.validateViews(self.getMetaData("db_views"))

    # Check my model data
    observation_vectors = self._model_data.getMetaData("observation_vectors")

    truth_vectors = self._model_data.getMetaData("truth_vectors")

    params.setMetaData("db_views", [])

    # Houston we are go
    dtr = DecisionTreeRegressor ()

    dtr.criterion = params.getMetaData ("criterion")
    dtr.splitter = params.getMetaData ("splitter")
    max_features = params.getMetaData ("max_features")
    if (max_features != None):
        dtr.max_features = max_features
    max_depth = params.getMetaData ("max_depth")
    if (max_depth != None):
        dtr.max_depth = max_depth
    dtr.min_samples_split = params.getMetaData ("min_samples_split")
    dtr.min_samples_leaf = params.getMetaData ("min_samples_leaf")
    max_leaf_nodes = params.getMetaData ("max_leaf_nodes")
    if (max_leaf_nodes != None):
        dtr.max_leaf_nodes = max_leaf_nodes
    dtr.random_state = params.getMetaData ("random_state")
    dtr.presort = params.getMetaData ("presort")

    # Evaluation mode loads several model artifacts from storage and sets them as inputs
    dtr.fit (observation_vectors, truth_vectors)

    params.setBinaryData ("dtr_model", "application/pickle", pickle.dumps(dtr))

    self.finalize ()

  def project_and_store(self,model_data,model_data_db):
    observation_vectors = self._model_data.getMetaData("observation_vectors")

    params = self._parameters

    # Make sure all my meta data is ready to go
    params.validateMeta()

    dtr = pickle.loads (params.getBinaryData ("dtr_model"))

    results = dtr.predict (observation_vectors)
    params.setMetaData ("dtr_results", results)
    
