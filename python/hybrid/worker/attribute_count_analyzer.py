"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import sys

from hybrid import utils
from hybrid.model import model
from hybrid.worker import attribute_indexer
from hybrid.worker.worker import modeling_worker


class attribute_count_analysis(model):
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
        params.setMetaData("model_type", u"attribute_count_analysis")
        params.setMetaData("model_desc", u"The most awesome attribute_count_analysis model ever")

        params.setMetaData("attribute_index_name", kwargs.get("attribute_index_name"))
        params.setMetaData("attribute_index_db_type", kwargs.get("attribute_index_db_type"))
        params.setMetaData("attribute_index_db_host", kwargs.get("attribute_index_db_host"))
        params.setMetaData("attribute_index_db_name", kwargs.get("attribute_index_db_name"))

        antecedent_key_list = kwargs.get("antecedent_key_list")
        if antecedent_key_list is None:
            antecedent_key_list = []
        antecedent_value_list = kwargs.get("antecedent_value_list")
        if antecedent_value_list is None:
            antecedent_value_list = []
        consequent_key_list = kwargs.get("consequent_key_list")
        if consequent_key_list is None:
            consequent_key_list = []
        consequent_value_list = kwargs.get("consequent_value_list")
        if consequent_value_list is None:
            consequent_value_list = []

        params.setMetaData("antecedent_key_list", antecedent_key_list)
        params.setMetaData("antecedent_value_list", antecedent_value_list)
        params.setMetaData("consequent_key_list", consequent_key_list)
        params.setMetaData("consequent_value_list", consequent_value_list)

        #         report_antecedent_count = kwargs.get("report_antecedent_count")
        #         if (report_antecedent_count==None):
        #             report_antecedent_count = False
        report_tp = kwargs.get("report_tp")
        if report_tp is None:
            report_tp = False
        report_fp = kwargs.get("report_fp")
        if report_fp is None:
            report_fp = False
        report_fn = kwargs.get("report_fn")
        if report_fn is None:
            report_fn = False
        report_precision = kwargs.get("report_precision")
        if report_precision is None:
            report_precision = False
        report_recall = kwargs.get("report_recall")
        if report_recall is None:
            report_recall = False
        report_f_measure = kwargs.get("report_f_measure")
        if report_f_measure is None:
            report_f_measure = False

        # params.setMetaData("report_antecedent_count", report_antecedent_count)
        params.setMetaData("report_tp", report_tp)
        params.setMetaData("report_fp", report_fp)
        params.setMetaData("report_fn", report_fn)
        params.setMetaData("report_precision", report_precision)
        params.setMetaData("report_recall", report_recall)
        params.setMetaData("report_f_measure", report_f_measure)

        self._hyperparameter_list.append("attribute_index_name")
        self._hyperparameter_list.append("attribute_index_db_type")
        self._hyperparameter_list.append("attribute_index_db_host")
        self._hyperparameter_list.append("attribute_index_db_name")
        self._hyperparameter_list.append("antecedent_key_list")
        self._hyperparameter_list.append("antecedent_value_list")
        self._hyperparameter_list.append("consequent_key_list")
        self._hyperparameter_list.append("consequent_value_list")
        self._hyperparameter_list.append("report_tp")
        self._hyperparameter_list.append("report_fp")
        self._hyperparameter_list.append("report_fn")
        self._hyperparameter_list.append("report_precision")
        self._hyperparameter_list.append("report_recall")
        self._hyperparameter_list.append("report_f_measure")

        params.setMetaData("value_lists", {})
        params.setMetaData("tp_lists", {})
        params.setMetaData("fp_lists", {})
        params.setMetaData("fn_lists", {})
        params.setMetaData("stats_dicts", {})

    def determineValuesToIndex(self):
        params = self._parameters

        attribute_index_model = attribute_indexer.attribute_index(params.getMetaData("attribute_index_name"))
        attribute_index_model.setDBType(params.getMetaData("attribute_index_db_type"))
        attribute_index_model.setDBHost(params.getMetaData("attribute_index_db_host"))
        attribute_index_model.setDBName(params.getMetaData("attribute_index_db_name"))
        attribute_index_model.loadFromDB()

        self._value_index = attribute_index_model.getMetaData("value_index")
        value_index = self._value_index

        antecedent_values_to_analyze = []
        for antecedent_value in params.getMetaData("antecedent_value_list"):
            if value_index.has_key(antecedent_value):
                antecedent_values_to_analyze.append(antecedent_value)
        for antecedent_key in params.getMetaData("antecedent_key_list"):
            for indexed_value in value_index.iterkeys():
                if indexed_value.startswith(antecedent_key):
                    antecedent_values_to_analyze.append(indexed_value)

        consequent_values_to_analyze = []
        for consequent_value in params.getMetaData("consequent_value_list"):
            if value_index.has_key(consequent_value):
                consequent_values_to_analyze.append(consequent_value)
        for consequent_key in params.getMetaData("consequent_key_list"):
            for indexed_value in value_index.iterkeys():
                if indexed_value.startswith(consequent_key):
                    consequent_values_to_analyze.append(indexed_value)

        params.setMetaData("antecedent_values_to_analyze", antecedent_values_to_analyze)
        params.setMetaData("consequent_values_to_analyze", consequent_values_to_analyze)
        params.setMetaData("merged_values_to_analyze", antecedent_values_to_analyze + list(
            set(consequent_values_to_analyze) - set(antecedent_values_to_analyze)))

    def calculate_stats(self):
        params = self._parameters
        value_lists = self.getParameters().getMetaData("value_lists")

        tp_lists = params.getMetaData("tp_lists")
        fp_lists = params.getMetaData("fp_lists")
        fn_lists = params.getMetaData("fn_lists")
        stats_dicts = params.getMetaData("stats_dicts")

        value_index = self._value_index

        calculate_tp = False
        calculate_fp = False
        calculate_tn = False
        calculate_fn = False
        calculate_precision = False
        calculate_recall = False
        calculate_f_measure = False

        report_tp = params.getMetaData("report_tp")
        report_fp = params.getMetaData("report_fp")
        report_fn = params.getMetaData("report_fn")
        report_precision = params.getMetaData("report_precision")
        report_recall = params.getMetaData("report_recall")
        report_f_measure = params.getMetaData("report_f_measure")

        if report_tp or report_precision or report_f_measure:
            calculate_tp = True
        if report_fp or report_precision or report_f_measure:
            calculate_fp = True
        if report_fn or report_recall or report_f_measure:
            calculate_fn = True
        if report_precision or report_f_measure:
            calculate_precision = True
        if report_recall or report_f_measure:
            calculate_recall = True
        if report_f_measure:
            calculate_f_measure = True

        for value_to_analyze in params.getMetaData("merged_values_to_analyze"):
            if value_index.has_key(value_to_analyze):
                id_list = value_index.get(value_to_analyze)
                value_lists[value_to_analyze] = id_list
                #                value_counts[value_to_analyze]=len(id_list)

        for antecedent_value in params.getMetaData("antecedent_values_to_analyze"):
            for consequent_value in params.getMetaData("consequent_values_to_analyze"):
                stats_dict = {}
                antecedent_id_dict = value_index.get(antecedent_value)
                consequent_id_dict = value_index.get(consequent_value)

                if calculate_tp:
                    # True Positives
                    tp_list = utils.dict_intersection([antecedent_id_dict, consequent_id_dict])
                    tp_lists[antecedent_value + "&" + consequent_value] = tp_list
                    tp_count = len(tp_list)
                    stats_dict["tp"] = tp_count

                if calculate_fp:
                    # False Positives
                    fp_list = utils.dict_difference([antecedent_id_dict, consequent_id_dict])
                    fp_lists[antecedent_value + "\\" + consequent_value] = fp_list
                    fp_count = len(fp_list)
                    stats_dict["fp"] = fp_count

                if calculate_fn:
                    # False Negatives
                    fn_list = utils.dict_difference([consequent_id_dict, antecedent_id_dict])
                    fn_lists[consequent_value + "\\" + antecedent_value] = fn_list
                    fn_count = len(fn_list)
                    stats_dict["fn"] = fn_count

                if calculate_precision:
                    precision = tp_count / (1.0 * tp_count + fp_count)
                    stats_dict["precision"] = precision

                if calculate_recall:
                    recall = tp_count / (1.0 * tp_count + fn_count)
                    stats_dict["recall"] = recall

                if calculate_f_measure:
                    if (precision + recall) > 0:
                        f_measure = 2.0 * (precision * recall) / (precision + recall)
                    else:
                        f_measure = 0
                    stats_dict["f_measure"] = f_measure

                stats_dicts[antecedent_value + "/" + consequent_value] = stats_dict
                # Leaving offf True Negatives for now (specificity)

    def update(self):
        self.determineValuesToIndex()
        self.calculate_stats()

        self.finalize()

        return True


class attribute_count_analyzer(modeling_worker):
    def __init__(self, **kwargs):
        ''' Create instance of the db management model
        Input:
            kwargs: various parameters
        '''
        # Call super class init first
        if kwargs.get("default_name") is None:
            kwargs["default_name"] = "attribute_count_analyzer"
        if kwargs.get("model_type") is None:
            kwargs["model_type"] = attribute_count_analysis

        modeling_worker.__init__(self, **kwargs)

        self._report_aggregation_max = kwargs.get("report_aggregation_max")
        self._report_aggregation_min = kwargs.get("report_aggregation_max")
        self._report_aggregation_avg = kwargs.get("report_aggregation_max")

        self._model = kwargs.get("model")

        self.loadModel()

    def update_model(self):
        old_model = self._model
        model_observations_manager = old_model.getModelObservationsManager()

        # If the model observations manager isn't ready (unstable or whatever) return False
        if not (model_observations_manager.isValid()):
            return False

        return old_model.update()

    def process_observations_core(self, observations, **kwargs):

        model = self.loadModel()
        params = model.getParameters()

        report_tp = params.getMetaData("report_tp")
        report_fp = params.getMetaData("report_fp")
        report_fn = params.getMetaData("report_fn")
        report_precision = params.getMetaData("report_precision")
        report_recall = params.getMetaData("report_recall")
        report_f_measure = params.getMetaData("report_f_measure")

        report_aggregation_max = self._report_aggregation_max
        report_aggregation_min = self._report_aggregation_min
        report_aggregation_avg = self._report_aggregation_avg

        #        tp_lists = params.getMetaData("tp_lists")
        #        fp_lists = params.getMetaData("fp_lists")
        #        fn_lists = params.getMetaData("fn_lists")

        model_stats_dicts = params.getMetaData(
            "stats_dicts")  # The stats dictionary in the model. Contains tp, precision, etc., for every antecedent/consequent combo found in the model data

        antecedent_key_list = model.getMetaData("antecedent_key_list")  # Antecedent keys delineated in model creation
        antecedent_value_list = model.getMetaData(
            "antecedent_value_list")  # Antecedent values delineated in model creation

        antecedent_values_to_analyze = model.getMetaData(
            "antecedent_values_to_analyze")  # Antecedent values present in the model set
        consequent_values_to_analyze = model.getMetaData(
            "consequent_values_to_analyze")  # Consequent values present in the model set

        antecedent_keys_and_consequent_values_for_stat_aggregation = []  # Antecedent keys and consequent values for which to aggregate stats. Some observations will satisfy multiple antecedent values.

        # Create a list of antecedent keys and consequent values for which to aggregate stats:
        # Loop over the antecedents in the delineated antecedent keys
        for antecedent_key in antecedent_key_list:
            # Loop over the consequent values present in the model set
            for consequent_value in consequent_values_to_analyze:
                antecedent_keys_and_consequent_values_for_stat_aggregation.append(
                    antecedent_key + "/" + consequent_value)

        # Loop over the antecedents in the delineated antecedent values
        for antecedent_value in antecedent_value_list:
            antecedent_key_list = antecedent_value.rsplit(":", 1)
            antecedent_key = antecedent_key_list[0]
            for consequent_value in consequent_values_to_analyze:
                antecedent_keys_and_consequent_values_for_stat_aggregation.append(
                    antecedent_key + "/" + consequent_value)

        # Loop over all observations handed to the worker
        for observation in observations:
            stats_to_aggregate_dict = {}
            observation_aggregated_stats = {}  # {antecedent_key/consequent_value, {max_stat0:max_stat0_value,min_stat0:min_stat0_value,max_stat1:max_stat1_value...}} for an observation

            # Loop over all antecedent values in our model. If they exist in the observation,
            # create statistics for that key
            for antecedent_value in antecedent_values_to_analyze:
                if not (observation.hasMetaDataValue(antecedent_value)):
                    continue

                # Get the base key for this antecedent value, for aggregation
                antecedent_key_list = antecedent_value.rsplit(":", 1)
                antecedent_key = antecedent_key_list[0]

                # Loop over the consequent values found in the model data
                for consequent_value in consequent_values_to_analyze:

                    # The individual statistics will be recorded under a key name
                    # comprised of the antecedent value and consequent value
                    stats_key = antecedent_value + "/" + consequent_value
                    aggregation_key = antecedent_key + "/" + consequent_value
                    stats_dict = model_stats_dicts.get(
                        stats_key)  # Get the stats for the specific antecedent/consequent pair we are reporting

                    # Store the individual antecedent/consequent stats into the observation
                    if report_tp:
                        tp = stats_dict.get("tp")
                        tp_field_name = "tp:" + stats_key
                        self.setMetaData(observation, tp_field_name, tp)
                    if report_fp:
                        fp = stats_dict.get("fp")
                        fp_field_name = "fp:" + stats_key
                        self.setMetaData(observation, fp_field_name, fp)
                    if report_fn:
                        fn = stats_dict.get("fn")
                        fn_field_name = "fn:" + stats_key
                        self.setMetaData(observation, fn_field_name, fn)
                    if report_precision:
                        precision = stats_dict.get("precision")
                        precision_field_name = "precision:" + stats_key
                        self.setMetaData(observation, precision_field_name, precision)
                    if report_recall:
                        recall = stats_dict.get("recall")
                        recall_field_name = "recall:" + stats_key
                        self.setMetaData(observation, recall_field_name, recall)
                    if report_f_measure:
                        f_measure = stats_dict.get("f_measure")
                        f_measure_field_name = "f_measure:" + stats_key
                        self.setMetaData(observation, f_measure_field_name, f_measure)

                    stats_to_aggregate_list = stats_to_aggregate_dict.get(aggregation_key)
                    if stats_to_aggregate_list is None:
                        stats_to_aggregate_list = []
                        stats_to_aggregate_dict[antecedent_key + "/" + consequent_value] = stats_to_aggregate_list
                    stats_to_aggregate_list.append(stats_dict)

                    #                     stats_to_aggregate_list = antecedent_keys_and_consequent_values_for_stat_aggregation.get(antecedent_key+"/"+consequent_value)
                    #                     stats_to_aggregate_list.append(stats_dict)
                    # Since we are going to need to aggregate this stat, let's create and aggregation
                    # dictionary for it, with some default values.
                    # Get the current aggregation dictionary for the antecedent key/consequent value pair in the observation
                    current_aggregated_stat_dict = observation_aggregated_stats.get(
                        antecedent_key + "/" + consequent_value)
                    # If the aggregation dictionary is empty, create one.
                    if current_aggregated_stat_dict is None:
                        current_aggregated_stat_dict = {}

                        # Fill in default aggregate values
                        if report_aggregation_max:
                            if report_tp or report_precision or report_recall or report_f_measure:
                                current_aggregated_stat_dict["max_tp"] = -1
                            if report_fp or report_precision or report_f_measure:
                                current_aggregated_stat_dict["max_fp"] = -1
                            if report_fn or report_recall or report_f_measure:
                                current_aggregated_stat_dict["max_fn"] = -1
                            if report_precision or report_f_measure:
                                current_aggregated_stat_dict["max_precision"] = -1
                            if report_recall or report_f_measure:
                                current_aggregated_stat_dict["max_recall"] = -1
                            if report_f_measure:
                                current_aggregated_stat_dict["max_f_measure"] = -1
                        if report_aggregation_min:
                            if report_tp or report_precision or report_recall or report_f_measure:
                                current_aggregated_stat_dict["min_tp"] = sys.maxint
                            if report_fp or report_precision or report_f_measure:
                                current_aggregated_stat_dict["min_fp"] = sys.maxint
                            if report_fn or report_recall or report_f_measure:
                                current_aggregated_stat_dict["min_fn"] = sys.maxint
                            if report_precision or report_f_measure:
                                current_aggregated_stat_dict["min_precision"] = sys.maxint
                            if report_recall or report_f_measure:
                                current_aggregated_stat_dict["min_recall"] = sys.maxint
                            if report_f_measure:
                                current_aggregated_stat_dict["min_f_measure"] = sys.maxint
                        if report_aggregation_avg:
                            if report_tp or report_precision or report_recall or report_f_measure:
                                current_aggregated_stat_dict["avg_tp"] = 0
                            if report_fp or report_precision or report_f_measure:
                                current_aggregated_stat_dict["avg_fp"] = 0
                            if report_fn or report_recall or report_f_measure:
                                current_aggregated_stat_dict["avg_fn"] = 0
                            if report_precision or report_f_measure:
                                current_aggregated_stat_dict["avg_precision"] = 0
                            if report_recall or report_f_measure:
                                current_aggregated_stat_dict["avg_recall"] = 0
                            if report_f_measure:
                                current_aggregated_stat_dict["avg_f_measure"] = 0

                        observation_aggregated_stats[
                            antecedent_key + "/" + consequent_value] = current_aggregated_stat_dict

            # At this point, we have finished looking through all the antecedent values to analyze.
            # Time to aggregate all stats for this observation, if required            
            if report_aggregation_max or report_aggregation_min or report_aggregation_avg:
                for stats_to_aggregate_key, stats_to_aggregate_list in stats_to_aggregate_dict.iteritems():

                    current_aggregated_stat_dict = observation_aggregated_stats.get(
                        stats_to_aggregate_key)  # get the aggregated stat dictionary for the current key
                    if current_aggregated_stat_dict is None:
                        continue
                    num_entries = len(stats_to_aggregate_list)
                    count = 0

                    for stats_dict in stats_to_aggregate_list:  # for every stat dictionary for a particular key in an observation
                        for stat_name, stat_value in stats_dict.iteritems():  # get the stat (tp, fp, precision, etc.) and the value
                            if report_aggregation_max:
                                current_max = current_aggregated_stat_dict["max_" + stat_name]
                                if stat_value > current_max:
                                    current_aggregated_stat_dict["max_" + stat_name] = stat_value
                            if report_aggregation_min:
                                current_min = current_aggregated_stat_dict["min_" + stat_name]
                                if stat_value < current_min:
                                    current_aggregated_stat_dict["min_" + stat_name] = stat_value
                            if report_aggregation_avg:
                                current_aggregated_stat_dict["avg_" + stat_name] += stat_value

                        if count == (num_entries - 1):
                            for temp_stat_name in stats_dict.iterkeys():  # get the stat (tp, fp, precision, etc.) and the value
                                current_aggregated_stat_dict["avg_" + temp_stat_name] /= num_entries

                        count += 1
                    field_name = "aggregated_stats:" + stats_to_aggregate_key
                    self.setMetaData(observation, field_name, current_aggregated_stat_dict)

        return observations
