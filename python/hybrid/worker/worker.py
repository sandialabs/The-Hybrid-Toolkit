"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
from hybrid import logger, db, model, utils
import uuid
import copy
import logging

logger = logging.getLogger(__name__)


class abstract_worker():
    def __init__(self, **kwargs):
        """ Create instance of the db management model
            Input:
                kwargs: various parameters
        """
        # Call super class init first

        # TODO fix up this code to use more defaults

        default_name = kwargs.get("default_name", "abstract_worker")

        self._name = kwargs.get("name", default_name)

        self._uuid = kwargs.get("uuid", uuid.uuid4())

        self._log_level = kwargs.get("log_level", 0)

        self._overwrite = kwargs.get("overwrite", False)

        self._reprocess = kwargs.get("reprocess", False)

        #        self._kwargs = kwargs

        self._uses_model = False

        self._type = "abstract"

        previous_worker_nest_name = kwargs.get("previous_worker_nest_name")
        if previous_worker_nest_name is None:
            previous_worker_nest_name = ""
        self._previous_worker_nest_name = previous_worker_nest_name

        self._nest_metadata = True
        nest_metadata = kwargs.get("nest_metadata")

        if not nest_metadata:
            self._nest_metadata = False
            self._current_nest_name = ""
        else:
            if previous_worker_nest_name == "":
                self._current_nest_name = self._name
            else:
                self._current_nest_name = previous_worker_nest_name + ":" + self._name

        if "overwrite" in kwargs:
            overwrite = kwargs["overwrite"]
        else:
            overwrite = False

        self._overwrite = overwrite

        self._version = 0
        self._data_version = 0

        self._worker_data_dependencies = kwargs.get("worker_data_dependencies")
        if self._worker_data_dependencies is None:
            self._worker_data_dependencies = {}

    def getKwargs(self):
        return self._kwargs

    def uses_model(self):
        return self._uses_model

    def batch_init(self, batch_context, **kwargs):
        return

    def batch_finalize(self, batch_context, **kwargs):
        return

    def set_uses_model(self, bool_uses_model):
        self._uses_model = bool_uses_model

    def set_worker_dependency(self, worker, version_list):
        self._worker_data_dependencies[worker.get_name()] = version_list

    def check_data_versions(self, observation):
        if len(self._worker_data_dependencies) == 0:
            return True
        for worker_path, version_list in self._worker_data_dependencies.iteritems():

            version_string = worker_path + ":" + "worker_data_version"
            bad_previous_data_string = worker_path + ":" + "worker_bad_data"

            if observation.hasMetaData(version_string):
                worker_version_in_observation = observation.getMetaData(version_string)
                if worker_version_in_observation is None:
                    logger.error(worker_path + " version is missing " + observation.getMetaData("_dataBlobID"))
                    return False
                if not (worker_version_in_observation in version_list):
                    logger.error(
                        worker_path + " version is incorrect in observation " + observation.getMetaData("_dataBlobID"))
                    return False
                if observation.hasMetaData(bad_previous_data_string):
                    logger.error(worker_path + " dependency data is bad" + observation.getMetaData("_dataBlobID"))
                    return False
            else:
                return False
        return True

    def get_name(self):
        return self._name

    def get_version(self):
        return self._version

    def get_data_version(self):
        return self._data_version

    def process_observation_core(self, observation, **kwargs):
        """ Task is sent in purely for reference. The worker can choose to act directly on an observation
            and remain agnostic towards database issues
        """
        raise NotImplementedError("This method should be overloaded by the subclass.")

    def process_observations_core(self, observations, **kwargs):
        for observation in observations:
            self.process_observation_core(observation, **kwargs)

        return observations

    def needs_processing(self, observation, **kwargs):
        # Do not reprocess the observation if the current worker version is valid
        if not self._reprocess:
            try:
                observation_dict = self.getMetaDataDict(observation)
                if "worker_data_version" in observation_dict:
                    worker_data_version = observation_dict["worker_data_version"]
                    if worker_data_version == self._data_version:
                        logger.info("Skipping", observation.getMetaData("_dataBlobID"), " for worker ", self.get_name(),
                                    " -- already completed.")
                        return False
            except:
                pass

        return True

    def process_observation(self, observation, **kwargs):

        if not (self.needs_processing(observation)):
            return observation, None

        self.initializeObservation(observation)
        if not self.check_data_versions(observation):
            self.setMetaData(observation, "worker_bad_data", True)
            completeObservation = None
            incompleteObservation = observation
        else:
            self.process_observation_core(observation, **kwargs)
            completeObservation = observation
            incompleteObservation = None

        return completeObservation, incompleteObservation

    def process_observations(self, observations, **kwargs):
        complete_observations = []
        incomplete_observations = []
        processable_observations = []
        observations_to_process = []
        previously_processed_observations = []

        for observation in observations:
            if not self.check_data_versions(observation):
                self.setMetaData(observation, "worker_bad_data", True)
                incomplete_observations.append(observation)
                continue
            processable_observations.append(observation)

        for observation in processable_observations:
            if self.needs_processing(observation):
                self.initializeObservation(observation)
                observations_to_process.append(observation)
            else:
                previously_processed_observations.append(observation)

        self.process_observations_core(observations_to_process, **kwargs)

        # One final check, as workers may call other workers whose dependencies weren't fully fleshed out
        for observation in observations_to_process:
            if self.hasMetaData(observation, "worker_bad_data"):
                incomplete_observations.append(observation)
            else:
                complete_observations.append(observation)

        for observation in previously_processed_observations:
            complete_observations.append(observation)

        # Add processing date
        datetime_dict = utils.getCurrentDateTimeDict()

        for observation in complete_observations:
            self.setMetaData(observation, "processed_datetime", datetime_dict)

        for observation in incomplete_observations:
            self.setMetaData(observation, "processed_datetime", datetime_dict)

        return complete_observations, incomplete_observations

    def getMetaDataDict(self, doc):
        if self._nest_metadata:
            return doc.getMetaData(self._current_nest_name)
        else:
            return doc.getMetaDataDict()

    def removeMetaDataDict(self, doc):
        if self._nest_metadata:
            doc.deleteMetaData(self._current_nest_name)
        else:
            doc.deleteMetaData(self._name)

    def getCompleteMetaDataFieldString(self, field, **kwargs):
        if not kwargs.get("nest_metadata"):
            nest_metadata = False
        else:
            nest_metadata = self._nest_metadata

        if nest_metadata:
            complete_field = self._current_nest_name + ":" + field
        else:
            complete_field = field

        return complete_field

    def getMetaData(self, doc, field, **kwargs):
        complete_field = self.getCompleteMetaDataFieldString(field, **kwargs)

        return doc.getMetaDataSafe(complete_field)

    def hasMetaData(self, doc, field, **kwargs):
        complete_field = self.getCompleteMetaDataFieldString(field, **kwargs)

        return doc.hasMetaData(complete_field)

    def addMetaData(self, doc, field, value, **kwargs):

        if kwargs.get("overwrite") is None:
            overwrite = False
        elif not kwargs.get("overwrite"):
            overwrite = False
        elif kwargs.get("overwrite"):
            overwrite = True
        else:
            overwrite = self._overwrite

        complete_field = self.getCompleteMetaDataFieldString(field, **kwargs)

        if overwrite:
            return doc.setMetaData(complete_field, value)
        else:
            return doc.addMetaData(complete_field, value)

    def setMetaData(self, doc, field, value, **kwargs):
        complete_field = self.getCompleteMetaDataFieldString(field, **kwargs)

        return doc.setMetaData(complete_field, value)

    def deleteMetaData(self, doc, field, **kwargs):
        complete_field = self.getCompleteMetaDataFieldString(field, **kwargs)

        doc.deleteMetaData(complete_field)

    def setOverwriteMetaData(self, overwrite):
        self._overwrite = overwrite

    def initializeObservation(self, observation):
        self.setMetaData(observation, "worker_data_version", self._data_version)

    def initializeObservations(self, observations):
        for observation in observations:
            self.setMetaData(observation, "worker_data_version", self._data_version)

    def getLockInfo(self, doc_id):
        lockInfo = {"name": self._name, "process_id": id(self)}

        if doc_id is None:
            lockInfo["doc_id"] = "_all"
        else:
            lockInfo["doc_id"] = doc_id

        return lockInfo


class modeling_worker(abstract_worker):
    def __init__(self, **kwargs):
        """ Create instance of the db management model
            Input:
                kwargs: various parameters
        """
        # Call super class init first
        if kwargs["default_name"] is None:
            kwargs["default_name"] = "modeling_worker"

        abstract_worker.__init__(self, **kwargs)

        self.set_uses_model(True)
        self._model_type = kwargs.get("model_type")

        if not (kwargs.get("model") is None):
            self.set_model(kwargs["model"])
        else:
            return None

        self._model_db_type = kwargs.get("model_db_type")
        self._model_db_host = kwargs.get("model_db_host")
        self._model_db_name = kwargs.get("model_db_name")

        self._validate_model_on_model_update = kwargs.get("validate_model_on_model_update")

        if self.get_model_db() is None:
            print "Error! No DB for model storage!!!"
            return None

    def get_model_db(self):
        model_db_type = self._model_db_type
        model_db_host = self._model_db_host
        model_db_name = self._model_db_name

        model_db = db.init(model_db_type, host=model_db_host, database=model_db_name,
                           push_views=False, create=False, log_level=logging.DEBUG)

        return model_db

    #     def get_model_observations_manager(self):
    #         worker_model = self.get_model()
    #         model_observations_manager = worker_model.getModelObservationsManager()
    #         return model_observations_manager

    def set_model(self, model):
        self._model = model

    def get_model(self):
        return self._model

    def set_model_class(self, model_class):
        self._model_class = model_class

    def get_model_class(self):
        return self._model_class

    def update_models(self, **kwargs):
        raise NotImplementedError("This method should be overloaded by the subclass.")

    def check_model_observations_current(self):
        worker_model = self.get_model()
        return worker_model.getModelObservationsCurrent()

    def check_hyperparameters(self, **kwargs):
        # Making a deepcopy so that no non-picklable data is stored in the model    
        model = copy.deepcopy(self.get_model())

        model.loadFromDB()
        print "Checking hyperparams from worker perspective"

        if model is None:
            print "Model doesn't exist, so hyperparams are considered 'different.'"
            return False

        if not (model.compareHyperParameters(self.get_model())):
            print "hyperparams are different"
            return False

        print "done checking hyperparams everything ok"
        return True

    def check_model(self, **kwargs):

        # Making a deepcopy so that no non-picklable data is stored in the model    
        model = copy.deepcopy(self.get_model())

        model.loadFromDB()

        if (model is None) or model.getMetaData("valid") == False:
            return False

        if not (model.compareModelMetaData(self.get_model())):
            return False

        return True

    def loadModel(self):
        if self._model_type is None:
            print "Must specify model type"
            return None

        loaded_model = model.loadFromCache(self._model_type, self._model._parameters_uuid)
        loaded_model.setDBName(self._model_db_name)
        loaded_model.setDBHost(self._model_db_host)
        loaded_model.setDBType(self._model_db_type)

        if not loaded_model.loadFromDB():
            return None
        # model_updated_correctly = self.update_model()
        #             if not(model_updated_correctly):
        #                 return None

        return loaded_model

# def updateModelAndFinalize(self,model,model_observations_manager):
#         if not(model.update()):
#             return False
#         print "Saving datetime===="
#         print model_observations_manager.getUpdateDatetimeDict()
#         model.setMetaData("model_observations_manager_last_update_datetime_dict",model_observations_manager.getUpdateDatetimeDict())
#         model.setMetaData("valid",True)
#         model.storeToDB()
#         
#         return True
