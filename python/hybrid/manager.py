"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import datetime
import gc
import logging
import sys
import time

import hybrid

import utils

logger = logging.getLogger(__name__)


def taskEvaluateDocuments(workers, task, **kwargs):
    """
    TASK
    task   : task (equivalent to items on threading.queue previously)
    """
    #    sys.stdout = open(str(os.getpid()) + ".out", "w")
    #    sys.stderr = open(str(os.getpid()) + ".err", "w")

    print "taskEvalauate documents"
    input_db_type = kwargs.get("input_db_type")
    input_db_host = kwargs.get("input_db_host")
    input_db_name = kwargs.get("input_db_name")

    input_db = hybrid.db.init(input_db_type, host=input_db_host, database=input_db_name, create=False)

    output_db_type = kwargs.get("output_db_type")
    output_db_host = kwargs.get("output_db_host")
    output_db_name = kwargs.get("output_db_name")

    output_db = hybrid.db.init(output_db_type, host=output_db_host, database=output_db_name, create=False)

    same_db = True
    if not (input_db_type == output_db_type):
        same_db = False
    elif not (input_db_host == output_db_host):
        same_db = False
    elif not (input_db_name == output_db_name):
        same_db = False

    output_tag_list = kwargs.get("output_tag_list")
    if (output_tag_list == None):
        output_tag_list = []

    observations = []
    uuids = task["uuids"]
    for document_index in range(0, len(uuids)):
        uuid = uuids[document_index]

        try:
            doc = input_db.loadDataBlob(uuid, include_binary=True)
            if not (doc == None):
                observations.append(doc)
        except:
            logger.info("Skipping uuid %s for evaluation from db name= %s" % (uuid, input_db.getDBName))
            continue

    for worker in workers:
        # Open a connection to the database and couch logger
        try:
            # Register cleanup command
            # evaluation.registerCleanupCommand(evaluation.cleanup)

            batch_context = {}
            worker.batch_init(batch_context=batch_context, **kwargs)
            if worker.uses_model() == True:
                model = worker.get_model()
                loaded = False
                reloaded = False
                while not (loaded):
                    try:
                        loaded = model.loadFromDB()
                        if (reloaded):
                            logger.info("Loaded model successfully after reloading!!!!!]]]]]]]")
                    except Exception, e:
                        logger.exception("Gonna try this again hopefully...")
                        reloaded = True

            observations, incomplete_observations = worker.process_observations(observations, **kwargs)
            worker.batch_finalize(batch_context=batch_context, **kwargs)

            # If the input and output databases aren't the same, go ahead and store the datablob to the output_db
            if not same_db:
                for observation in observations:
                    output_db.storeDataBlob(observation, ignore_conflict=True)
                for observation in incomplete_observations:
                    output_db.storeDataBlob(observation, ignore_conflict=True)

            # Modify the input database to mark the current observations as having been processed.
            # This is done by adding the output tags.
            #
            # If everything was fine with the processing, the output tag gets a "complete" value.
            # A value of "incomplete" signifies that something was wrong, such as a missing data dependency.
            logger.info("====Storing output tags for documents====")
            for observation in observations:
                if same_db:
                    doc = observation
                else:
                    doc = input_db.loadDataBlob(observation.getMetaData("_dataBlobID"), include_binary=True)
                for j in range(0, len(output_tag_list)):
                    doc.setMetaData(output_tag_list[j], "complete")
                    doc.setMetaData(output_tag_list[j] + "_datetime",
                                    datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"))
                               # logger.info("storing tag " + output_tag_list[j] + " = complete on doc " + doc.getDataBlobUUID() + " in database " + str(input_db)
                input_db.storeDataBlob(doc, ignore_conflict=True)
            for observation in incomplete_observations:
                if same_db:
                    doc = observation
                else:
                    doc = input_db.loadDataBlob(observation.getMetaData("_dataBlobID"), include_binary=True)
                for j in range(0, len(output_tag_list)):
                    doc.setMetaData(output_tag_list[j], "incomplete")
                input_db.storeDataBlob(doc, ignore_conflict=True)
            logger.info("====Done storing output tags for documents====")

            # Check if something bad happened (like the models were changed)
            # if (len(observations) + len(incomplete_observations) == 0):
            # return the task, the master process_observations will add it back into
            # multiprocess pool's task queue
            #    return task

        except:
            logger.exception("manager having issues")
        #            logger.error("(Exception):, %s"%(str(e)) )
        #            logger.error( "LDA on this range [%s to %s] num_docs=%d"%(start_key, end_key, num_docs))
        # sys.exit(1)
    return None


class manager:
    def __init__(self, **kwargs):
        """ Create instance of lda model
            Input:
            kwargs: various lda data parameters, blah, blah, blah
        """
        # Default values are set to None if not entered explicitly
        self._workers = kwargs["workers"]
        self._kwargs = kwargs
        self._input_tag_list = kwargs.get("input_tag_list")
        self._output_tag_list = kwargs.get("output_tag_list")
        self._query = kwargs.get("query")
        self._debug = kwargs.get("debug")
        self._worker_threads = kwargs.get("worker_threads")
        self._max_worker_threads = kwargs.get("max_worker_threads")
        self._adaptive_threads = kwargs.get("adaptive_threads")
        uuid_list_string = kwargs.get("uuids")
        self._mp_type = kwargs.get("mp_type", "mp_pool")

        self._log_deamon = kwargs
        self._static = kwargs.get("static")

        self._input_db = kwargs.get("input_db")
        self._output_db = kwargs.get("output_db")
        self._observation_limit = kwargs.get("observation_limit")

        # Converting a few select Nones to sensible default values
        if (self._static == None):
            self._static = False
        if ((self._worker_threads == None) or (self._debug == True)):
            self._worker_threads = 1
        if ((self._max_worker_threads == None)):
            self._max_worker_threads = 10
        if (self._debug == None):
            self._debug = False
        if (self._observation_limit == None):
            self._observation_limit = 250
        if (uuid_list_string == None):
            self._uuids = []
        else:
            self._uuids = uuid_list_string.split(',')
            self._static = True
        if (self._adaptive_threads == None):
            self._adaptive_threads = True
        self._mp = None

        self._iteration_sleep = kwargs.get("iteration_sleep", 5)
        self._model_update_sleep = kwargs.get("model_update_sleep", 10)
        # log_manager.start_async()

    def cleanup(self):
        if not (self._mp == None):
            self._mp.finish_and_close()

    def processObservations(self, workers, uuid_list, mp):

        input_db = self._input_db
        output_db = self._output_db
        query = self._query
        input_tag_list = self._input_tag_list
        output_tag_list = self._output_tag_list
        observation_limit = self._observation_limit
        worker_threads = self._worker_threads

        # print "PROCESSING OBSERVATIONS"

        query_info = {}
        try:
            query_info["db_type"] = input_db.getType()
            query_info["db_host"] = input_db.getHost()
            query_info["db_name"] = input_db.getDBName()
            query_info["limit"] = observation_limit * worker_threads
        except Exception, e:
            print "Error %s, from object %s" % (str(e), input_db)
            logger.exception("Error %s, from object %s" % (str(e), input_db))
            sys.exit(0)

        if uuid_list:
            key_list = []
            sorted(uuid_list)
            query_info["query_name"] = ""
            query_info["query_find_list"] = []
            query_info["query_except_list"] = output_tag_list
            query_info["query_keys"] = uuid_list
        else:
            query_info["query_name"] = query
            query_info["query_find_list"] = input_tag_list
            query_info["query_except_list"] = output_tag_list
        #            document_view = hybrid.view.create_view(input_db, query, input_tag_list, output_tag_list,limit=observation_limit*worker_threads)
        #            document_view = input_db.loadView(query,limit=observation_limit*worker_threads)
        #            logger.info("Grabbing query",query

        maximum_retrievable_number_of_documents = observation_limit * worker_threads
        document_view = hybrid.view.create_view_from_query_info(query_info,
                                                                limit=maximum_retrievable_number_of_documents)

        rows = document_view.rows()

        # Increment the number of workers if we are maxed        
        #         if (self._adaptive_threads==True and not(self._debug==True)):
        #             if ((number_of_documents_retrieved == maximum_retrievable_number_of_documents) and (self._worker_threads < self._max_worker_threads)):
        #                 self._worker_threads = self._worker_threads + 1
        #                 logger.info("This executor is overwhelmed. Increasing the number of worker threads to",self._worker_threads
        #             elif (number_of_documents_retrieved < (maximum_retrievable_number_of_documents/2)):
        #                 if (self._worker_threads > 1):
        #                     self._worker_threads = self._worker_threads - 1
        #                     logger.info("This executor is underwhelmed. Decreasing the number of worker threads to",self._worker_threads



        for row in rows:
            if row == None:
                logger.error("Incomplete pull, source database must have been updating.")
                return True, False

                # Are there new documents to evaluate
            #        logger.info("document_view.num_rows()=",document_view.num_rows()
            #        logger.info("static "
            #        print self._static

        if (document_view.num_rows() == 0):
            if (self._static):
                return False, False  # Do not keep processing, and not waiting on data
            return True, True  # Do keep processing, and waiting on data

        tasks = utils.computeTaskProcessingRanges(rows, self._worker_threads)

        # Debugging mode
        if (mp == None):
            # Change to celery with 1 process or actually allow inline
            taskEvaluateDocuments(workers,
                                  tasks[0],
                                  input_db_type=input_db.getType(),
                                  input_db_host=input_db.getHost(),
                                  input_db_name=input_db.getDBName(),
                                  output_db_type=output_db.getType(),
                                  output_db_host=output_db.getHost(),
                                  output_db_name=output_db.getDBName(),
                                  output_tag_list=self._output_tag_list)

        # Normal mode
        else:
            for task in tasks:
                logger.info("Adding worker task")
                mp.add_task(taskEvaluateDocuments,
                            workers,
                            task,
                            input_db_type=input_db.getType(),
                            input_db_host=input_db.getHost(),
                            input_db_name=input_db.getDBName(),
                            output_db_type=output_db.getType(),
                            output_db_host=output_db.getHost(),
                            output_db_name=output_db.getDBName(),
                            func2=taskEvaluateDocuments,
                            output_tag_list=self._output_tag_list)

                logger.info("Waiting for tasks to complete")
                # Wait for the tasks to complete (but don't kill processes)
                mp.wait_completion()

        if (self._uuids):
            return False, False
        return True, False  # Do keep processing, and not waiting on data

    def run(self, **kwargs):
        workers = self._workers
        debug = self._debug
        uuids = self._uuids

        keep_processing = True

        worker_threads = self._worker_threads

        if (debug == True):
            worker_threads = 1
            self._mp = None
        else:

            if self._mp_type == "mp_pool":
                # Fire up the multiprocessing pool
                # mp_log = multiprocessing.log_to_stderr()
                # mp_log.setLevel(multiprocessing.SUBDEBUG)
                self._mp = hybrid.mp_pool.mp_pool(processes=worker_threads)
            elif self._mp_type == "mp_celery":
                self._mp = hybrid.mp_celery.mp_celery(processes=worker_threads)

        logger.info("Running manager")
        while (keep_processing == True):

            # self._mp = mp

            waiting_on_model_update = False
            models_updated_correctly = True

            # Check models
            for worker in workers:
                model_needs_updating = False
                model_loaded = False
                # print str(worker)[0:80]
                logger.info("worker=" + str(worker)[0:80])
                if worker.uses_model():
                    worker_model = worker.get_model()
                    # print "worker uses model"
                    logger.info("worker uses model")
                    loaded_model = worker.loadModel()

                    if not (loaded_model == None):
                        # Temporary
                        mom = loaded_model.getModelObservationsManager()
                        mom.loadFromDB()
                        mom.update()

                        logger.info("loaded model exists")
                        model_loaded = True
                        # print "loaded model != None"
                        if not (loaded_model.isUpdated()):
                            logger.info("model isn't up to date")
                            model_needs_updating = True
                        else:
                            logger.info("model is up to date nominally, but let's check hyperparameters")
                            # There is a model, and it is valid. Let's make sure it matches the current hyperparameters though..
                            if (worker.check_hyperparameters() == False):
                                logger.info("hyperparameters changed, model needs updating")
                                model_needs_updating = True
                            else:
                                logger.info("hyperparameters fine")
                    else:
                        print
                        print
                        print
                        print "loaded model doesn't exist"
                        logger.info("loaded model doesn't exist")
                        model_loaded = False
                        model_needs_updating = True

                    # print "model_loaded=%s" % (model_loaded,)
                    # print "model_needs_updating=%s" % (model_needs_updating,)
                    logger.info("model_loaded=%s" % (model_loaded,))
                    logger.info("model_needs_updating=%s" % (model_needs_updating,))

                    if (model_needs_updating or (model_loaded == False)):
                        print "model needs updating"
                        logger.info("model needs updating")
                        if (worker_model.isSelfUpdating()):
                            print "model is selfupdating. Updating through hybrid manager"
                            logger.info("model is selfupdating. Updating through hybrid manager")
                            correct_update = worker.update_model()
                            print "\t\tcorrect_update=%s" % (correct_update,)
                            logger.info("\t\tcorrect_update=%s" % (correct_update,))
                            models_updated_correctly = (correct_update and models_updated_correctly)

                            logger.info("Attempting to project observations through the model..")
                            # If there is a target database for the model observations manager of this worker, then
                            # project the model observations through their own model
                            model_observations_manager = worker_model.getModelObservationsManager()
                            model_observations_target_db = model_observations_manager.getTargetDB()
                            if not (model_observations_target_db == None):
                                print "projecting for worker model:", worker_model.getMetaData("_dataBlobID")
                                logger.info("Removing keys...%s" % (self._output_tag_list,))
                                utils.removeKeysFromManagerView(model_observations_manager, self._output_tag_list)
                                utils.removeWorkerMetaDataFromManagerView(model_observations_manager, worker)
                                mini_manager = hybrid.manager.manager(
                                    workers=[worker],
                                    query=self._query,  # Will be removed once we have tag to query methods
                                    input_tag_list=self._input_tag_list,
                                    output_tag_list=self._output_tag_list,
                                    static=True,
                                    worker_threads=self._worker_threads,
                                    input_db=model_observations_target_db,
                                    output_db=model_observations_target_db,
                                    observation_limit=self._observation_limit)
                                mini_manager.run()
                                mini_manager = None
                                print "finished projecting"
                                logger.info("Finished projecting!!!!")
                        else:
                            print "model is not selfupdating"
                            logger.info("model needs to do some things before it is updated")
                            waiting_on_model_update = True
                            models_updated_correctly = False
                            break

            print "waiting on model update", waiting_on_model_update
            print "models_updated_correctly", models_updated_correctly

            logger.info("waiting_on_model_update %s" % (waiting_on_model_update,))
            logger.info("models_updated_correctly %s" % (models_updated_correctly,))

            if (worker.uses_model() and (waiting_on_model_update or not (models_updated_correctly))):
                print "sleeping while we wait, models need updating"
                logger.info("sleeping while we wait, models need updating")
                time.sleep(self._model_update_sleep)
                continue

            keep_processing, waiting_on_data = self.processObservations(workers, uuids, self._mp)
            if waiting_on_data:
                print "Waiting on data..."
                logger.info("Waiting on data...")
                time.sleep(self._iteration_sleep)
        self._mp.finish_and_close()
        del self._mp
        gc.collect()

        return
