'''
Created on May 2, 2013

@author: wldavis
'''
import ConfigParser
import optparse
import socket
import time
import traceback

import hybrid.manager
from hybrid_sklearn import sklearn_utils,decisiontreeregressor_worker
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
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
    parser.add_option("--criterion",  default='mse', help="str, The function to measure the quality of a split. The only supported criterion is 'mse' for the mean squared error, which is equal to variance reduction as feature selection criterion.")
    parser.add_option("--splitter",  default='best', help="str, The strategy used to choose the split at each node. Supported strategies are 'best' to choose the best split and 'random' to choose the best random split.")
    parser.add_option("--max-features",  default=None, help="""int, float, string or None. 
The number of features to consider when looking for the best split:
    If int, then consider max_features features at each split.
    If float, then max_features is a percentage and int(max_features * n_features) features are considered at each split.
    If 'auto', then max_features=n_features.
    If 'sqrt', then max_features=sqrt(n_features).
    If 'log2', then max_features=log2(n_features).
    If None, then max_features=n_features.
Note: the search for a split does not stop until at least one valid partition of the node samples is found, even if it requires to effectively inspect more than max_features features.
            """)
    parser.add_option("--max-depth",  default=None, help="int or None. The maximum depth of the tree. If None, then nodes are expanded until all leaves are pure or until all leaves contain less than min_samples_split samples. Ignored if max_leaf_nodes is not None.")
    parser.add_option("--min-samples-split",  default=2, help="The minimum number of samples required to split an internal node.  Default: %default%")
    parser.add_option("--min-samples-leaf",  default=1, help="The minimum number of samples required to be at a leaf node.  Default: %default%")
    parser.add_option("--min-weight-fraction-leaf",  default=0, help="The minimum weighted fraction of the input samples required to be at a leaf node.  Default: %default")
    parser.add_option("--max-leaf-nodes",  default=None, help="int or None. Grow a tree with max_leaf_nodes in best-first fashion. Best nodes are defined as relative reduction in impurity. If None then unlimited number of leaf nodes. If not None then max_depth will be ignored.")
    parser.add_option("--random-state",  default=None, help="int, RandomState instance, or None.  If int, random_state is the seed used by the random number generator; If RandomState instance, random_state is the random number generator; If None, the random number generator is the RandomState instance used by np.random.  Default: %default")
    parser.add_option("--presort",  default=False, help="Whether to presort the data to speed up the finding of best splits in fitting. For the default settings of a decision tree on large datasets, setting this to true may slow down the training process. When using either a smaller dataset or a restricted depth, this may speed up the training.  Default: %default")

    parser.add_option("--data-versions",  default="", help="Versions of workers on which the processors are dependent.  Default: %default")

    (options, arguments) = parser.parse_args()

    CONFIG_FILENAME = options.cfg
    if not(CONFIG_FILENAME==""):
        config = ConfigParser.ConfigParser()
        config.read(CONFIG_FILENAME)
        
        if (options.data_database_name==None):
            if config.has_option("sklearn_dtr:databases", "InputDBName"):
                options.data_database_name=config.get("sklearn_dtr:databases", "InputDBName")
        if (options.data_database_host==None):
            if config.has_option("sklearn_dtr:databases", "InputDBHost"):
                options.data_database_host=config.get("sklearn_dtr:databases", "InputDBHost")
        if (options.data_database_type==None):
            if config.has_option("sklearn_dtr:databases", "InputDBType"):
                options.data_database_type=config.get("sklearn_dtr:databases", "InputDBType")
        
        if (options.model_database_name==None):
            if config.has_option("sklearn_dtr:databases", "ModelDBName"):
                options.model_database_name=config.get("sklearn_dtr:databases", "ModelDBName")
        if (options.model_database_host==None):
            if config.has_option("sklearn_dtr:databases", "ModelDBHost"):
                options.model_database_host=config.get("sklearn_dtr:databases", "ModelDBHost")
        if (options.model_database_type==None):
            if config.has_option("sklearn_dtr:databases", "ModelDBType"):
                options.model_database_type=config.get("sklearn_dtr:databases", "ModelDBType")

        if (options.model_documents_database_name==None):
            if config.has_option("sklearn_dtr:databases", "ModelDBName"):
                options.model_documents_database_name=config.get("sklearn_dtr:databases", "ModelDocumentsDBName")
        if (options.model_documents_database_host==None):
            if config.has_option("sklearn_dtr:databases", "ModelDBHost"):
                options.model_documents_database_host=config.get("sklearn_dtr:databases", "ModelDocumentsDBHost")
        if (options.model_documents_database_type==None):
            if config.has_option("sklearn_dtr:databases", "ModelDBType"):
                options.model_documents_database_type=config.get("sklearn_dtr:databases", "ModelDocumentsDBType")

        if config.has_option("sklearn_dtr:general", "Debug"):
            options.debug=config.get("sklearn_dtr:general", "Debug")
        if config.has_option("sklearn_dtr:general", "LogLevel"):
            options.log_level=config.get("sklearn_dtr:general", "LogLevel")
        if config.has_option("sklearn_dtr:general", "WorkerThreads"):
            options.worker_threads=config.get("sklearn_dtr:general", "WorkerThreads")
        if config.has_option("sklearn_dtr:general", "UUIDs"):
            options.uuids=config.get("sklearn_dtr:general", "UUIDs")
        if config.has_option("sklearn_dtr:general", "QueryName"):
            options.query_name=config.get("sklearn_dtr:general", "QueryName")
        if config.has_option("sklearn_dtr:general", "InputTags"):
            options.input_tags=config.get("sklearn_dtr:general", "InputTags")
        if config.has_option("sklearn_dtr:general", "OutputTags"):
            options.output_tags=config.get("sklearn_dtr:general", "OutputTags")
        if (options.static==None):
            if config.has_option("sklearn_dtr:general", "Static"):
                options.static=config.get("sklearn_dtr:general", "Static")
        if (options.host==None):
            if config.has_option("sklearn_dtr:general", "Host"):
                options.host=config.get("sklearn_dtr:general", "Host")
            if (options.host==None):
                options.host="http://localhost:5984"

        if config.has_option("sklearn_dtr:parameters", "DataVersions"):
            options.data_versions=config.get("sklearn_dtr:features", "DataVersions")
        if config.has_option("sklearn_dtr:parameters", "FeatureVector"):
            options.feature_vectors=config.get("sklearn_dtr:features", "FeatureVector")
        if config.has_option("sklearn_dtr:parameters", "TruthVector"):
            options.truth_vectors=config.get("sklearn_dtr:features", "TruthVector")
        if config.has_option("sklearn_dtr:parameters", "Criterion"):
            options.criterion=config.get("sklearn_dtr:features", "Criterion")
        if config.has_option("sklearn_dtr:parameters", "MaxFeatures"):
            options.max_features=config.get("sklearn_dtr:features", "MaxFeatures")
        if config.has_option("sklearn_dtr:parameters", "MaxDepth"):
            options.max_depth=config.get("sklearn_dtr:features", "MaxDepth")
        if config.has_option("sklearn_dtr:parameters", "MinSamplesSplit"):
            options.min_samples_split=config.get("sklearn_dtr:features", "MinSamplesSplit")
        if config.has_option("sklearn_dtr:parameters", "MinSamplesLeaf"):
            options.min_samples_leaf=config.get("sklearn_dtr:features", "MinSamplesLeaf")
        if config.has_option("sklearn_dtr:parameters", "MinWeightFractionLeaf"):
            options.min_weight_fraction_leaf=config.get("sklearn_dtr:features", "MinWeightFractionLeaf")
        if config.has_option("sklearn_dtr:parameters", "max_leaf_nodes"):
            options.max_leaf_nodes=config.get("sklearn_dtr:features", "max_leaf_nodes")
        if config.has_option("sklearn_dtr:parameters", "RandomState"):
            options.random_state=config.get("sklearn_dtr:features", "RandomState")
        if config.has_option("sklearn_dtr:parameters", "Presort"):
            options.presort=config.get("sklearn_dtr:features", "Presort")
    
        if config.has_option("sklearn_dtr:general", "ModelDocumentsQueryName"):
            options.model_documents_query_name=config.get("sklearn_dtr:general", "ModelDocumentsQueryName")
        if config.has_option("sklearn_dtr:general", "ModelDocumentsFindList"):
            options.model_documents_find_list=config.get("sklearn_dtr:general", "ModelDocumentsFindList")
        if config.has_option("sklearn_dtr:general", "ModelDocumentsExceptList"):
            options.model_documents_except_list=config.get("sklearn_dtr:general", "ModelDocumentsExceptList")

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
      input_tags = []
    else:
      input_tags = hybrid.utils.clean_and_split(options.input_tags,",")
    if (options.output_tags == None):
      output_tags = ["tag_decisiontreeregressor"]
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

    while (True):
        try:
            dtr = sklearn_utils.decision_tree_regressor_model("decision_tree_regressor_model")
            dtr.setDBType(model_db_type)
            dtr.setDBHost(model_db_host)
            dtr.setDBName(model_db_name)

            # All string literal values should be user input instead
            model_observations_query_info={}
            model_observations_query_info["db_type"]=model_documents_database_type 
            model_observations_query_info["db_host"]=model_documents_database_host 
            model_observations_query_info["db_name"]=model_documents_database_name
            model_observations_query_info["query_name"]=model_documents_query_name
            model_observations_query_info["query_find_list"]=model_documents_find_list
            model_observations_query_info["query_except_list"]=model_documents_except_list

            my_dtr_worker = decisiontreeregressor_worker.decision_tree_regressor_worker(
                                        name="dtr",
                                        model_observations_query_info=model_observations_query_info,
                                        model_db_type=model_db_type,
                                        model_db_host=model_db_host,
                                        model_db_name=model_db_name,
                                        model=dtr,
                                        feature_vectors=feature_vectors,
                                        truth_vectors=truth_vectors,
                                        criterion=options.criterion,
                                        splitter=options.splitter,
                                        max_features=options.max_features,
                                        max_depth=options.max_depth,
                                        min_samples_split=options.min_samples_split,
                                        min_samples_leaf=options.min_samples_leaf,
                                        min_weight_fraction_leaf=options.min_weight_fraction_leaf,
                                        max_leaf_nodes=options.max_leaf_nodes,
                                        random_state=options.random_state,
                                        presort=options.presort,
                                        worker_data_dependencies=data_versions_dict
                                        )
        
     
        
            #example string
            #uuid_list_string = "key1,key2"
            
            # Create the module
            # Open connections to the databases
            data_db = hybrid.db.init(input_db_type, host=input_db_host, database=input_db_name)

            dtr_manager = hybrid.manager.manager(
                                        workers=[my_dtr_worker],
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
            dtr_manager.run()
            break
        #    atexit.register(mlp_module.cleanup())
        except Exception, e:
            print "Problem accessing the database..",e
            traceback.print_exc()
            time.sleep(30)
            
            



