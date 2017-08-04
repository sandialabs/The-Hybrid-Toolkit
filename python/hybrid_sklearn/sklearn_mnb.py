'''
Created on May 2, 2013

@author: wldavis
'''
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import ConfigParser
import optparse
import socket
import time
import traceback

import hybrid.manager
from hybrid_sklearn import sklearn_utils,multinomialnb_worker

# Evaluation doc limit
__doc_limit = 50


if __name__ == "__main__":

    # Handle command-line arguments
    parser = optparse.OptionParser()
    parser.add_option("--cfg", default="", help="Name of the config file.  Default: %default")
    parser.add_option("--data-database-name", default=None, help="Name of input database.  Default: %default")
    parser.add_option("--data-database-host", default=None, help="Host of input database.  Default: %default")
    parser.add_option("--data-database-type", default=None, help="Type of input database.  Default: %default")
    parser.add_option("--model-database-name", default=None, help="Name of model database.  Default: %default")
    parser.add_option("--model-database-host", default=None, help="Host of input database.  Default: %default")
    parser.add_option("--model-database-type", default=None, help="Type of input database.  Default: %default")
    parser.add_option("--model-documents-database-name", default=None, help="Name of model database.  Default: %default")
    parser.add_option("--model-documents-database-host", default=None, help="Host of input database.  Default: %default")
    parser.add_option("--model-documents-database-type", default=None, help="Type of input database.  Default: %default")
    parser.add_option("--external-host", default="https://%h/couch", help="External name of database. %h expands to the name of localhost.  Default: %default")
    parser.add_option("--worker-threads",  type="int", default=4, help="Number of threads to use for processing data.  Default: %default")
    parser.add_option("--debug",  action="store_true", default=None, help="Run in single thread debug mode.  Default: %default")
    parser.add_option("--static",  action="store_true", default=None, help="Run the extraction loop just once (for static datasets).  Default: %default")
    parser.add_option("--reflexive",  action="store_true", default=False, help="Run the extraction loop just once (for static datasets).  Default: %default")
    parser.add_option("--uuids",  help="Specify a comma separated list of uuids to reprocess.")
    parser.add_option("--log-level", type="int", default=3, help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")

    parser.add_option("--query-name", default=None, help="Name of input query.  Default: %default")
    parser.add_option("--input-tags", default=None, help="Tags for input query.  Default: %default")
    parser.add_option("--output-tags", default=None, help="Output tags.  Default: %default")
    parser.add_option("--model-documents-query-name", default=None, help="Query for the model documents.  Default: same input query")
    parser.add_option("--model-documents-find-list", default=None, help="Input tags for model documents.  Default: same input tags")
    parser.add_option("--model-documents-except-list", default=None, help="Input tags for model documents.  Default: empty list")
    parser.add_option("--feature-vectors",  default=None, help="Paths to find features in the incoming data. Default: %default")
    parser.add_option("--truth-vectors",  default=None, help="Paths to find truth labels in the incoming data. Default: %default")
    parser.add_option("--alpha",  default=1, help="Additive (Laplace/Lidstone) smoothing parameter, Default: %default")
    parser.add_option("--fit-prior",  default=False, help="Whether to learn class prior probabilities or not. Default: %default")
    parser.add_option("--class-prior",  default=None, help="Prior probabilities of the classes. Default: %default")

    parser.add_option("--data-versions",  default="", help="Versions of workers on which the processors are dependent.  Default: %default")

    (options, arguments) = parser.parse_args()

    CONFIG_FILENAME = options.cfg
    if not(CONFIG_FILENAME==""):
        config = ConfigParser.ConfigParser()
        config.read(CONFIG_FILENAME)
        
        if (options.data_database_name==None):
            if config.has_option("sklearn_mnb:databases", "InputDBName"):
                options.data_database_name=config.get("sklearn_mnb:databases", "InputDBName")
        if (options.data_database_host==None):
            if config.has_option("sklearn_mnb:databases", "InputDBHost"):
                options.data_database_host=config.get("sklearn_mnb:databases", "InputDBHost")
        if (options.data_database_type==None):
            if config.has_option("sklearn_mnb:databases", "InputDBType"):
                options.data_database_type=config.get("sklearn_mnb:databases", "InputDBType")
        
        if (options.model_database_name==None):
            if config.has_option("sklearn_mnb:databases", "ModelDBName"):
                options.model_database_name=config.get("sklearn_mnb:databases", "ModelDBName")
        if (options.model_database_host==None):
            if config.has_option("sklearn_mnb:databases", "ModelDBHost"):
                options.model_database_host=config.get("sklearn_mnb:databases", "ModelDBHost")
        if (options.model_database_type==None):
            if config.has_option("sklearn_mnb:databases", "ModelDBType"):
                options.model_database_type=config.get("sklearn_mnb:databases", "ModelDBType")

        if (options.model_documents_database_name==None):
            if config.has_option("sklearn_mnb:databases", "ModelDBName"):
                options.model_documents_database_name=config.get("sklearn_mnb:databases", "ModelDocumentsDBName")
        if (options.model_documents_database_host==None):
            if config.has_option("sklearn_mnb:databases", "ModelDBHost"):
                options.model_documents_database_host=config.get("sklearn_mnb:databases", "ModelDocumentsDBHost")
        if (options.model_documents_database_type==None):
            if config.has_option("sklearn_mnb:databases", "ModelDBType"):
                options.model_documents_database_type=config.get("sklearn_mnb:databases", "ModelDocumentsDBType")

        if config.has_option("sklearn_mnb:general", "Debug"):
            options.debug=config.get("sklearn_mnb:general", "Debug")
        if config.has_option("sklearn_mnb:general", "LogLevel"):
            options.log_level=config.get("sklearn_mnb:general", "LogLevel")
        if config.has_option("sklearn_mnb:general", "WorkerThreads"):
            options.worker_threads=config.get("sklearn_mnb:general", "WorkerThreads")
        if config.has_option("sklearn_mnb:general", "UUIDs"):
            options.uuids=config.get("sklearn_mnb:general", "UUIDs")
        if config.has_option("sklearn_mnb:general", "QueryName"):
            options.query_name=config.get("sklearn_mnb:general", "QueryName")
        if config.has_option("sklearn_mnb:general", "InputTags"):
            options.input_tags=config.get("sklearn_mnb:general", "InputTags")
        if config.has_option("sklearn_mnb:general", "OutputTags"):
            options.output_tags=config.get("sklearn_mnb:general", "OutputTags")
        if (options.static==None):
            if config.has_option("sklearn_mnb:general", "Static"):
                options.static=config.get("sklearn_mnb:general", "Static")
        if (options.host==None):
            if config.has_option("sklearn_mnb:general", "Host"):
                options.host=config.get("sklearn_mnb:general", "Host")
            if (options.host==None):
                options.host="http://localhost:5984"

        if config.has_option("sklearn_mnb:parameters", "DataVersions"):
            options.data_versions=config.get("sklearn_mnb:features", "DataVersions")
        if config.has_option("sklearn_mnb:parameters", "FeatureVector"):
            options.feature_vectors=config.get("sklearn_mnb:features", "FeatureVector")
        if config.has_option("sklearn_mnb:parameters", "TruthVector"):
            options.truth_vectors=config.get("sklearn_mnb:features", "TruthVector")
        if config.has_option("sklearn_mnb:parameters", "Alpha"):
            options.alpha=config.get("sklearn_mnb:features", "Alpha")
        if config.has_option("sklearn_mnb:parameters", "FitPrior"):
            options.fit_prior=config.get("sklearn_mnb:features", "FitPrior")
        if config.has_option("sklearn_mnb:parameters", "ClassPrior"):
            options.class_prior=config.get("sklearn_mnb:features", "ClassPrior")
    
        if config.has_option("sklearn_mnb:general", "ModelDocumentsQueryName"):
            options.model_documents_query_name=config.get("sklearn_mnb:general", "ModelDocumentsQueryName")
        if config.has_option("sklearn_mnb:general", "ModelDocumentsFindList"):
            options.model_documents_find_list=config.get("sklearn_mnb:general", "ModelDocumentsFindList")
        if config.has_option("sklearn_mnb:general", "ModelDocumentsExceptList"):
            options.model_documents_except_list=config.get("sklearn_mnb:general", "ModelDocumentsExceptList")

    query_name = options.query_name
    
    input_db_host = options.data_database_host
    if (options.model_database_host==None):
      model_db_host = input_db_host
    else:
      model_db_host = options.model_database_host
    if (options.model_documents_database_host==None):
      model_documents_database_host = input_db_host
    else:
      model_documents_database_host = options.model_documents_database_host

    input_db_type = options.data_database_type
    if (options.model_database_type==None):
      model_db_type = input_db_type
    else:
      model_db_type = options.model_database_type
    if (options.model_documents_database_type==None):
      model_documents_database_type = input_db_type
    else:
      model_documents_database_type = options.model_documents_database_type

    input_db_name = options.data_database_name
    if (options.model_database_name==None):
      model_db_name = input_db_name
    else:
      model_db_name = options.model_database_name
    if (options.model_documents_database_name==None):
      model_documents_database_name = input_db_name
    else:
      model_documents_database_name = options.model_documents_database_name

    log_level = options.log_level
    external_host = options.external_host.replace("%h", socket.gethostname())
    uuid_list_string = options.uuids
    
    if ((options.static=="True") or (options.static==True)):
        options.static=True
    else:
        options.static=False
   
    # Get a logger handle (singleton)
    logger = hybrid.logger.logger()
    logger.setLogLevel(options.log_level)

    data_versions_list = hybrid.utils.clean_and_split(options.data_versions,";")

    if (options.input_tags == None):
      input_tags = ["tag_text_features_extracted"]
    else:
      input_tags = hybrid.utils.clean_and_split(options.input_tags,",")
    if (options.output_tags == None):
      output_tags = ["tag_multinomialnb"]
    else:
      output_tags = hybrid.utils.clean_and_split(options.output_tags,",")

    if (options.model_documents_query_name == None):
      model_documents_query_name = query_name
    else:
      model_documents_query_name = options.model_documents_query_name

    if (options.model_documents_find_list == None):
      model_documents_find_list = input_tags
    else:
      model_documents_find_list = input_tags

    if (options.model_documents_except_list == None):
      model_documents_except_list = []
    else:
      model_documents_except_list = options.model_documents_except_list 


    data_versions_dict={}
    for data_versions in data_versions_list:
        data_field,version_string = data_versions.split(",")
        version_list = version_string.split(":")
        data_versions_dict[data_field] = version_list
    
    feature_vectors = hybrid.utils.clean_and_split(options.feature_vectors,",")
    truth_vectors = hybrid.utils.clean_and_split(options.truth_vectors,",")
    alpha = options.alpha
    fit_prior = options.fit_prior
    class_prior = None
    if (options.class_prior != None):
      class_prior = hybrid.utils.clean_and_split(options.class_prior,",")

    while (True):
        try:
            mnb = sklearn_utils.multinomial_nb_model("multinomial_nb_model")
            mnb.setDBType(model_db_type)
            mnb.setDBHost(model_db_host)
            mnb.setDBName(model_db_name)

            # All string literal values should be user input instead
            model_observations_query_info={}
            model_observations_query_info["db_type"]=model_documents_database_type 
            model_observations_query_info["db_host"]=model_documents_database_host 
            model_observations_query_info["db_name"]=model_documents_database_name
            model_observations_query_info["query_name"]=model_documents_query_name
            model_observations_query_info["query_find_list"]=model_documents_find_list
            model_observations_query_info["query_except_list"]=model_documents_except_list

            my_mnb_worker = multinomialnb_worker.multinomialnb_worker(
                                        name="mnb",
                                        model_observations_query_info=model_observations_query_info,
                                        model_db_type=model_db_type,
                                        model_db_host=model_db_host,
                                        model_db_name=model_db_name,
                                        model=mnb,
                                        feature_vectors=feature_vectors,
                                        truth_vectors=truth_vectors,
                                        alpha=alpha,
                                        fit_prior=fit_prior,
                                        class_prior=class_prior,
                                        worker_data_dependencies=data_versions_dict
                                        )
        
     
        
            #example string
            #uuid_list_string = "key1,key2"
            
            # Create the module
            # Open connections to the databases
            data_db = hybrid.db.init(input_db_type, host=input_db_host, database=input_db_name)

            mnb_manager = hybrid.manager.manager(
                                        workers=[my_mnb_worker],
                                        query=query_name,
                                        input_tag_list=input_tags,
                                        output_tag_list=output_tags,
                                        debug=options.debug,
                                        uuids=uuid_list_string,
                                        static=options.static,
                                        worker_threads=int(options.worker_threads),
                                        logger=logger,
                                        input_db=data_db,
                                        output_db=data_db,
                                        observation_limit=__doc_limit)
        
            
            # Begin processing data
            mnb_manager.run()
            break
        #    atexit.register(mlp_module.cleanup())
        except Exception, e:
            print "Problem accessing the database..",e
            traceback.print_exc()
            time.sleep(30)
            
            



