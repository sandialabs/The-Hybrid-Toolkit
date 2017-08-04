"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
from hybrid.worker.worker import modeling_worker

from hybrid.model import model
from hybrid import utils
from hybrid import encoding
import time


class attribute_index(model):
    def __init__(self, uuid, **kwargs):
        """ Create instance of the db management model
            Input:
                kwargs: various parameters
        """

        # Call super class init first
        model.__init__(self, uuid, **kwargs)

        # Handle to storage for model parameters
        params = self._parameters

        # Set various bits of meta data, these are defaults and can be changed later
        params.setMetaData("model_type", u"attribute_index")
        params.setMetaData("model_desc", u"The most awesome attribute_index model ever")
        params.setMetaData("value_index", {})
        params.setMetaData("processed_count", 0)
        params.setMetaData("count_threshold", 0)

        attributes_to_index = kwargs.get("attributes_to_index")
        if attributes_to_index is None:
            attributes_to_index = []

        values_to_index = kwargs.get("values_to_index")
        if values_to_index is None:
            values_to_index = []

        count_threshold = kwargs.get("count_threshold")
        if (count_threshold == None):
            count_threshold = 0

        params.setMetaData("attributes_to_index", attributes_to_index)
        params.setMetaData("values_to_index", values_to_index)
        params.setMetaData("count_threshold", count_threshold)

        self._hyperparameter_list.append("attributes_to_index")
        self._hyperparameter_list.append("values_to_index")
        self._hyperparameter_list.append("count_threshold")

    def update(self):
        # utils.debuglog("/tmp/stuff", "!!update_index model.getParameters=%s" % (str(self.getParameters())))
        self.finalize()

        return True

    @staticmethod
    def loadFromJSON(json_data):
        jsontype = json_data["_jsontype"]
        if not (jsontype == "hybrid.worker.attribute_indexer.attribute_index"):
            return None

        name = json_data["name"]
        return attribute_index(name, **json_data)


class attribute_indexer(modeling_worker):
    def __init__(self, **kwargs):
        ''' Create instance of the db management model
        Input:
            kwargs: various parameters
        '''
        # Call super class init first
        if (kwargs.get("default_name") == None):
            kwargs["default_name"] = "attribute_indexer"
        if (kwargs.get("model_type") == None):
            kwargs["model_type"] = attribute_index
        if (kwargs.get("index_uuid") == None):
            index_uuid = True
        else:
            index_uuid = kwargs.get("index_uuid")
        if (kwargs.get("update_index") == None):
            update_index = False
        else:
            update_index = kwargs.get("update_index")

        modeling_worker.__init__(self, **kwargs)

        self._model = kwargs.get("model")

        self._model_observations_query_info = kwargs.get("model_observations_query_info")

        self._value_index = {}
        self._processed_count = 0
        self._index_uuid = index_uuid
        self._update_index = update_index

        self.loadModel()

    @staticmethod
    def loadFromJSON(json_data):
        jsontype = json_data["_jsontype"]
        if not (jsontype == "hybrid.worker.attribute_indexer.attribute_indexer"):
            return None

        return attribute_indexer(**json_data)

    def update_value_count(self, value, observation, old_value_index):
        if self._index_uuid:
            old_dict = self._value_index.get(value)
            if (old_dict == None):
                old_dict = {}
            old_dict[observation.getMetaData("_dataBlobID")] = 1
            self._value_index[value] = old_dict
        else:
            if not (self._value_index.has_key(value)):
                stats = {}
                stats["count"] = 0
                stats["first_datetime"] = observation.getMetaData("creation_datetime")
                stats["first_datetime_str"] = utils.getDateTimeFromDict(observation.getMetaData("creation_datetime"))
                stats["first_id"] = observation.getMetaData("_dataBlobID")
            else:
                stats = self._value_index.get(value)

            stats["count"] += 1
            stats["last_datetime"] = observation.getMetaData("creation_datetime")
            stats["last_datetime_str"] = utils.getDateTimeFromDict(observation.getMetaData("creation_datetime"))
            stats["last_id"] = observation.getMetaData("_dataBlobID")
            self._value_index[value] = stats

            if (old_value_index.has_key(value)):
                old_count = old_value_index[value]["count"]
            else:
                old_count = 0

            count_threshold = self.get_model().getMetaData("count_threshold")
            if ((count_threshold > 0) and ((old_count + stats[
                "count"]) < count_threshold)):  # Logic can be changed to treat all items in one batch the same. This treats each item with respect to others that have come before in the same batch
                rare_attributes = self.getMetaData(observation, "rare_attributes")
                if rare_attributes == None:
                    rare_attributes = {}
                if (old_count == 0):
                    rare_attributes[value] = stats
                else:
                    rare_attributes[value] = old_value_index[value]

                self.addMetaData(observation, "rare_attributes", rare_attributes)

    def update_index(self):
        old_value_index = self._value_index
        processed_count = self._processed_count

        #        model_db = self.get_model_db()


        model = self.get_model()

        # utils.debuglog("/tmp/stuff", "!!update_index model=%s" % (str(model)))

        #         lockInfo=self.getLockInfo(model.getMetaData("_dataBlobID"))
        #
        #         if model_db.getType() == "mongodb":
        #             model_db.setLock()
        #         else:
        #             model_db.setLock(lockInfo,model.getMetaData("_dataBlobID"))

        new_model_value_index = model.getParameters().getMetaData("value_index")
        model_processed_count = model.getParameters().getMetaData("processed_count")

        for value_to_index, old_observation_info in old_value_index.iteritems():
            if self._index_uuid:
                model_id_dict = new_model_value_index.get(value_to_index)
                if (model_id_dict == None):
                    model_id_dict = {}
                new_model_value_index[value_to_index] = utils.dict_merge(model_id_dict, old_observation_info)
            else:

                stats = new_model_value_index.get(value_to_index)
                stats = new_model_value_index.get(value_to_index)
                if (stats == None):
                    stats = old_observation_info
                else:
                    # If the new first_datetime is older, then replace
                    new_first_datetime = stats.get("first_datetime")
                    datecompare = utils.compareDateTimeDict(old_observation_info.get("first_datetime"),
                                                            new_first_datetime)
                    if (datecompare == -1):
                        stats["first_datetime"] = old_observation_info.get("first_datetime")
                        stats["first_datetime_str"] = old_observation_info.get("first_datetime_str")
                        stats["first_id"] = old_observation_info.get("first_id")
                    # If the new last_datetime is newer, then replace
                    new_last_datetime = stats.get("last_datetime")
                    datecompare = utils.compareDateTimeDict(old_observation_info.get("last_datetime"),
                                                            new_last_datetime)
                    if (datecompare == 1):
                        stats["last_datetime"] = old_observation_info.get("last_datetime")
                        stats["last_datetime_str"] = old_observation_info.get("last_datetime_str")
                        stats["last_id"] = old_observation_info.get("last_id")
                    stats["count"] += old_observation_info["count"]

                # Save the new stats
                new_model_value_index[value_to_index] = stats

        model_processed_count += processed_count

        model.getParameters().setMetaData("processed_count", model_processed_count)

        model.setMetaData("valid", True)
        #         model.storeToDB(lockInfo)

        #         if model_db.getType() == "mongodb":
        #             model_db.removeLock()
        #         else:
        #             model_db.removeLock(lockInfo,model.getMetaData("_dataBlobID"))

        self._value_index = {}
        self._processed_count = 0

    def update_model(self):
        old_model = self._model
        model_observations_manager = old_model.getModelObservationsManager()
        # print "model_observations_manager",model_observations_manager

        # If the model observations manager isn't ready (unstable or whatever) return False
        if not (model_observations_manager.isValid()):
            return False

        # Get the valid rows (before something changes :/ )
        model_observations = model_observations_manager.getRows()

        model_observations = self.process_observations(model_observations, update_index=True)

        return old_model.update()

    def process_observations_core(self, observations, **kwargs):
        update_index = self._update_index

        if len(observations) == 0:
            return observations

        attributes_to_index = self.get_model().getMetaData("attributes_to_index")
        values_to_index = self.get_model().getMetaData("values_to_index")
        loaded_model_from_db = self.loadModel()
        old_value_index = loaded_model_from_db.getMetaData("value_index")

        for observation in observations:
            for attribute in attributes_to_index:
                if (observation.hasMetaData(attribute)):
                    value = observation.getMetaData(attribute)
                    if (isinstance(value, dict)):
                        for value_iterkey in value.iterkeys():
                            key = attribute + ":" + value_iterkey
                            self.update_value_count(key, observation, old_value_index)
                    if (isinstance(value, list)):
                        for item in value:
                            key = attribute + ":" + encoding.convertToUnicode(item)
                            self.update_value_count(key, observation, old_value_index)
                    else:
                        key = attribute + ":" + encoding.convertToUnicode(value)
                        self.update_value_count(key, observation, old_value_index)

            for value in values_to_index:
                if (observation.hasMetaData(value)):
                    self.update_value_count(value, observation, old_value_index)

        self._processed_count += len(observations)
        if (update_index == True):
            self.update_index()

        return observations
