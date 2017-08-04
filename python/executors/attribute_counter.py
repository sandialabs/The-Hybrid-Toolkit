"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import hybrid.manager
from hybrid.worker import attribute_indexer
from hybrid.worker import attribute_count_analyzer

import ConfigParser
import optparse
import socket
import os
import time

import traceback

# Evaluation doc limit
__doc_limit = 100

if __name__ == "__main__":

    # Handle command-line arguments
    parser = optparse.OptionParser()
    parser.add_option("--cfg", default="", help="Name of the config file.  Default: %default")
    parser.add_option("--alias", default="attribute_counter",
                      help="Alias for executor (primarily for config file).  Default: %default")
    parser.add_option("--data-database-name", default=None, help="Name of input database.  Default: %default")
    parser.add_option("--data-database-host", default=None, help="Host of input database.  Default: %default")
    parser.add_option("--data-database-type", default=None, help="Type of input database.  Default: %default")
    parser.add_option("--label-database-name", default=None, help="Name of label database.  Default: %default")
    parser.add_option("--label-database-host", default=None, help="Host of input database.  Default: %default")
    parser.add_option("--label-database-type", default=None, help="Type of input database.  Default: %default")
    parser.add_option("--model-database-name", default=None, help="Name of model database.  Default: %default")
    parser.add_option("--model-database-host", default=None, help="Host of input database.  Default: %default")
    parser.add_option("--model-database-type", default=None, help="Type of input database.  Default: %default")
    parser.add_option("--model-observations-manager-name", default=None,
                      help="Name of the model observations manager.  Default: %default")
    parser.add_option("--external-host", default="https://%h/couch",
                      help="External name of database. %h expands to the name of localhost.  Default: %default")
    parser.add_option("--worker-threads", type="int", default=1,
                      help="Number of threads to use for processing data.  Default: %default")
    parser.add_option("--debug", action="store_true", default=None,
                      help="Run in single thread debug mode.  Default: %default")
    parser.add_option("--static", action="store_true", default=None,
                      help="Run the extraction loop just once (for static datasets).  Default: %default")
    parser.add_option("--reflexive", action="store_true", default=False,
                      help="Run the executor on the label database.  Default: %default")
    parser.add_option("--uuids", help="Specify a comma separated list of uuids to reprocess.")
    parser.add_option("--log-level", type="int", default=3,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")

    parser.add_option("--query-name", default=None, help="Name of input query.  Default: %default")
    parser.add_option("--input-tags", default=None, help="Tags for input query.  Default: %default")
    parser.add_option("--output-tags", default=None, help="Output tags.  Default: %default")

    parser.add_option("--attributes-to-index", default="", help="Name of database.  Default: %default")
    parser.add_option("--antecedent-key-list", default="", help="Name of database.  Default: %default")
    parser.add_option("--antecedent-value-list", default="", help="Name of database.  Default: %default")
    parser.add_option("--consequent-key-list", default="", help="Name of database.  Default: %default")
    parser.add_option("--consequent-value-list", default="", help="Name of database.  Default: %default")
    parser.add_option("--count-threshold", default=100,
                      help="Logs attributes in the document if they are less than this number.  Default: %default")
    parser.add_option("-P", default=None, help="This option is for tracking the pid of the process  Default: %default")

    parser.add_option("--data-versions", default="",
                      help="Versions of workers on which the processors are dependent.  Default: %default")

    (options, arguments) = parser.parse_args()
    executor_alias = options.alias

    CONFIG_FILENAME = options.cfg
    if not (CONFIG_FILENAME == ""):
        config = ConfigParser.ConfigParser()
        config.read(CONFIG_FILENAME)

        if options.data_database_name is None:
            if config.has_option(executor_alias + ":databases", "InputDBName"):
                options.data_database_name = config.get(executor_alias + ":databases", "InputDBName")
        if options.data_database_host is None:
            if config.has_option(executor_alias + ":databases", "InputDBHost"):
                options.data_database_host = config.get(executor_alias + ":databases", "InputDBHost")
        if options.data_database_type is None:
            if config.has_option(executor_alias + ":databases", "InputDBType"):
                options.data_database_type = config.get(executor_alias + ":databases", "InputDBType")

        if options.label_database_name is None:
            if config.has_option(executor_alias + ":databases", "LabelDBName"):
                options.label_database_name = config.get(executor_alias + ":databases", "LabelDBName")
        if options.label_database_host is None:
            if config.has_option(executor_alias + ":databases", "LabelDBHost"):
                options.label_database_host = config.get(executor_alias + ":databases", "LabelDBHost")
        if options.label_database_type is None:
            if config.has_option(executor_alias + ":databases", "LabelDBType"):
                options.label_database_type = config.get(executor_alias + ":databases", "LabelDBType")

        if options.model_database_name is None:
            if config.has_option(executor_alias + ":databases", "ModelDBName"):
                options.model_database_name = config.get(executor_alias + ":databases", "ModelDBName")
        if options.model_database_host is None:
            if config.has_option(executor_alias + ":databases", "ModelDBHost"):
                options.model_database_host = config.get(executor_alias + ":databases", "ModelDBHost")
        if options.model_database_type is None:
            if config.has_option(executor_alias + ":databases", "ModelDBType"):
                options.model_database_type = config.get(executor_alias + ":databases", "ModelDBType")

        if options.model_observations_manager_name is None:
            if config.has_option("titan_supervised_machine_learning:databases", "ModelObservationsManagerName"):
                options.model_observations_manager_name = config.get("titan_supervised_machine_learning:databases",
                                                                     "ModelObservationsManagerName")

        if options.debug is None:
            if config.has_option(executor_alias + ":general", "Debug"):
                options.debug = config.get(executor_alias + ":general", "Debug")
        if config.has_option(executor_alias + ":general", "LogLevel"):
            options.log_level = config.get(executor_alias + ":general", "LogLevel")
        if config.has_option(executor_alias + ":general", "WorkerThreads"):
            options.worker_threads = config.get(executor_alias + ":general", "WorkerThreads")
        if config.has_option(executor_alias + ":general", "UUIDs"):
            options.uuids = config.get(executor_alias + ":general", "UUIDs")
        if options.static is None and config.has_option(executor_alias + ":general", "Static"):
            options.static = config.get(executor_alias + ":general", "Static")

        if config.has_option(executor_alias + ":general", "QueryName"):
            options.query_name = config.get(executor_alias + ":general", "QueryName")
        if config.has_option(executor_alias + ":general", "InputTags"):
            options.input_tags = config.get(executor_alias + ":general", "InputTags")
        if config.has_option(executor_alias + ":general", "OutputTags"):
            options.output_tags = config.get(executor_alias + ":general", "OutputTags")
        if config.has_option(executor_alias + ":general", "CountThreshold"):
            options.count_threshold = config.get(executor_alias + ":general", "CountThreshold")

        if config.has_option(executor_alias + ":features", "AttributesToIndex"):
            options.attributes_to_index = config.get(executor_alias + ":features", "AttributesToIndex")
        if config.has_option(executor_alias + ":features", "AntecedentKeys"):
            options.antecedent_key_list = config.get(executor_alias + ":features", "AntecedentKeys")
        if config.has_option(executor_alias + ":features", "AntecedentValues"):
            options.antecedent_value_list = config.get(executor_alias + ":features", "AntecedentValues")
        if config.has_option(executor_alias + ":features", "ConsequentKeys"):
            options.consequent_key_list = config.get(executor_alias + ":features", "ConsequentKeys")
        if config.has_option(executor_alias + ":features", "ConsequentValues"):
            options.consequent_value_list = config.get(executor_alias + ":features", "ConsequentValues")
        if config.has_option(executor_alias + ":features", "DataVersions"):
            options.data_versions = config.get(executor_alias + ":features", "DataVersions")

    query_name = "needs_features_transformed"
    query_name = options.query_name
    input_tags = hybrid.utils.clean_and_split(options.input_tags, ",")
    output_tags = hybrid.utils.clean_and_split(options.output_tags, ",")

    input_db_host = options.data_database_host
    label_db_host = options.label_database_host
    model_db_host = options.model_database_host

    input_db_type = options.data_database_type
    label_db_type = options.label_database_type
    model_db_type = options.model_database_type

    if options.reflexive:
        input_db_name = options.label_database_name
        input_db_host = options.label_database_host
        options.static = True
    else:
        input_db_name = options.data_database_name
        input_db_host = options.data_database_host

    label_db_name = options.label_database_name
    model_db_name = options.model_database_name

    model_observations_manager_name = options.model_observations_manager_name

    log_level = options.log_level
    external_host = options.external_host.replace("%h", socket.gethostname())
    uuid_list_string = options.uuids

    # Track the pid
    if options.P is not None:
        f = open(options.P, "w")
        f.write(str(os.getpid()))
        f.close()

    if options.static == "True" or options.static:
        options.static = True
    else:
        options.static = False

    # Get a logger handle (singleton)
    manager_logger = hybrid.logger.logger()
    manager_logger.setLogLevel(options.log_level)

    attributes_to_index_string = options.attributes_to_index
    antecedent_key_list_string = options.antecedent_key_list
    antecedent_value_list_string = options.antecedent_value_list
    consequent_key_list_string = options.consequent_key_list
    consequent_value_list_string = options.consequent_value_list
    data_versions_list_string = options.data_versions

    if attributes_to_index_string is None:
        attributes_to_index_string = ""
    if antecedent_key_list_string is None:
        antecedent_key_list_string = ""
    if antecedent_value_list_string is None:
        antecedent_value_list_string = ""
    if consequent_key_list_string is None:
        consequent_key_list_string = ""
    if consequent_value_list_string is None:
        consequent_value_list_string = ""
    if data_versions_list_string is None:
        data_versions_list_string = ""

    attributes_to_index = hybrid.utils.clean_and_split(attributes_to_index_string, ",")
    antecedent_key_list = hybrid.utils.clean_and_split(antecedent_key_list_string, ",")
    antecedent_value_list = hybrid.utils.clean_and_split(antecedent_value_list_string, ",")
    consequent_key_list = hybrid.utils.clean_and_split(consequent_key_list_string, ",")
    consequent_value_list = hybrid.utils.clean_and_split(consequent_value_list_string, ",")
    data_versions_list = hybrid.utils.clean_and_split(data_versions_list_string, ";")

    data_versions_dict = {}
    for data_versions in data_versions_list:
        data_field, version_string = data_versions.split(",")
        version_list = version_string.split(":")
        data_versions_dict[data_field] = version_list

    count_threshold = int(options.count_threshold)
    running = True
    while running:
        try:
            attribute_index_name = "attribute_count"

            attribute_index = attribute_indexer.attribute_index(
                attribute_index_name,
                attributes_to_index=attributes_to_index,
                logger=manager_logger,
                model_observations_manager_name=model_observations_manager_name,
                selfUpdate=False,
                count_threshold=count_threshold
            )
            attribute_index.setDBType(model_db_type)
            attribute_index.setDBHost(model_db_host)
            attribute_index.setDBName(model_db_name)

            # Get a logger handle (singleton)
            manager_logger = hybrid.logger.logger()
            manager_logger.setLogLevel(options.log_level)

            # All string literal values should be user input instead
            model_observations_query_info = {"db_type": label_db_type, "db_host": label_db_host,
                                             "db_name": label_db_name, "query_name": "labels",
                                             "query_find_list": ["label"], "query_except_list": []}

            my_attribute_counter = attribute_indexer.attribute_indexer(
                name="attribute_counter",
                update_index=True,
                index_uuid=False,
                model_observations_query_info=model_observations_query_info,
                model_db_type=model_db_type,
                model_db_host=model_db_host,
                model_db_name=model_db_name,
                model=attribute_index,
                worker_data_dependencies=data_versions_dict,
                logger=manager_logger
                # Send in with config later
            )

            # Open connections to the databases
            data_db = hybrid.db.init(input_db_type, host=input_db_host, database=input_db_name)

            attribute_counter_manager = hybrid.manager.manager(
                workers=[my_attribute_counter],
                query=query_name,
                input_tag_list=input_tags,
                output_tag_list=output_tags,
                debug=options.debug,
                uuids=uuid_list_string,
                static=options.static,
                worker_threads=int(options.worker_threads),
                logger=manager_logger,
                input_db=data_db,
                output_db=data_db,
                observation_limit=__doc_limit)

            # Begin processing data
            attribute_counter_manager.run()
            break
        # atexit.register(mlp_module.cleanup())
        except Exception, e:
            print "Attribute counter: Problem accessing the database..", e
            traceback.print_exc()
            time.sleep(30)
