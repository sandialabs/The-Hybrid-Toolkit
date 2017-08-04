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

import hybrid

CONFIG_FILENAME = 'mlp.cfg'

# Evaluation doc limit
__doc_limit = 50

if __name__ == "__main__":

    # Handle command-line arguments
    parser = optparse.OptionParser()
    parser.add_option("--cfg", default="", help="Name of the config file.  Default: %default")
    parser.add_option("--source-db-host", default=None, help="Name of database host.  Default: %default")
    parser.add_option("--source-db-name", default=None, help="Name of source database.  Default: %default")
    parser.add_option("--source-db-type", default=None, help="Type of source database.  Default: %default")
    parser.add_option("--target-db-name", default=None, help="Name of target database.  Default: %default")
    parser.add_option("--target-db-host", default=None, help="Host of target database.  Default: %default")
    parser.add_option("--target-db-type", default=None, help="Type of target database.  Default: %default")
    parser.add_option("--external-host", default="https://%h/couch",
                      help="External name of database. %h expands to the name of localhost.  Default: %default")
    parser.add_option("--static", action="store_true", default=False,
                      help="Run the extraction loop just once (for static datasets).  Default: %default")
    parser.add_option("--uuids", help="Specify a comma separated list of uuids to reprocess.")
    parser.add_option("--log-level", type="int", default=3,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    parser.add_option("--query-name", default="", help="Tags the queried documents should contain.  Default: %default")
    parser.add_option("--query-find-tags", default="",
                      help="Tags the queried documents should contain.  Default: %default")
    parser.add_option("--query-except-tags", default="",
                      help="Tags the queried documents should ~not~ contain.  Default: %default")

    (options, arguments) = parser.parse_args()

    CONFIG_FILENAME = options.cfg
    if not (CONFIG_FILENAME == ""):
        config = ConfigParser.ConfigParser()
        config.read(CONFIG_FILENAME)

        if config.has_option("Databases", "SourceDBHost"):
            options.source_db_host = config.get("Databases", "SourceDBHost")
        if config.has_option("Databases", "SourceDBName"):
            options.source_db_name = config.get("Databases", "SourceDBName")
        if config.has_option("Databases", "SourceDBType"):
            options.source_db_type = config.get("Databases", "SourceDBType")
        if config.has_option("Databases", "TargetDBName"):
            options.target_db_name = config.get("Databases", "TargetDBName")
        if config.has_option("Databases", "TargetDBHost"):
            options.target_db_host = config.get("Databases", "TargetDBHost")
        if config.has_option("Databases", "TargetDBType"):
            options.target_db_type = config.get("Databases", "TargetDBType")

        if config.has_option("General", "LogLevel"):
            options.log_level = config.get("General", "LogLevel")
        if config.has_option("General", "Static"):
            options.static = config.get("General", "Static")

        if config.has_option("Parameters", "UUIDs"):
            options.uuids = config.get("Parameters", "UUIDs")
        if config.has_option("Parameters", "QueryName"):
            options.query_name = config.get("Parameters", "QueryName")
        if config.has_option("Parameters", "QueryFindTags"):
            options.query_find_tags = config.get("Parameters", "QueryFindTags")
        if config.has_option("Parameters", "QueryExceptTags"):
            options.query_except_tags = config.get("Parameters", "QueryExceptTags")

    # Get a logger handle (singleton)
    logger = hybrid.logger.logger()
    logger.setLogLevel(options.log_level)

    source_db_host = options.source_db_host
    source_db_name = options.source_db_name
    source_db_type = options.source_db_type
    target_db_host = options.target_db_host
    target_db_name = options.target_db_name
    target_db_type = options.target_db_type

    log_level = options.log_level
    external_host = options.external_host.replace("%h", socket.gethostname())
    uuid_list_string = options.uuids

    data_db = hybrid.db.init(source_db_type, host=source_db_host, database=source_db_name)

    # Get old handle
    try:
        archive_db = hybrid.db.init(target_db_type, host=target_db_host, database=target_db_name,
                                    view_files=data_db.view_files,
                                    redirect_dirs=data_db.redirect_view_directories,
                                    push_views=True, create=False)
        # archive_db.delete(host=target_db_host, database=target_db_name)
    except:
        print "Creating new database:", target_db_name
        archive_db = hybrid.db.init(target_db_type, host=target_db_host, database=target_db_name,
                                    view_files=data_db.view_files,
                                    redirect_dirs=data_db.redirect_view_directories,
                                    push_views=True, create=True)

    # Set up the model_observations_db manager
    model_observations_manager = hybrid.model.db_management_model("extract_documents")
    model_observations_manager.setMetaData("delete_existing", False)
    model_observations_manager.setMetaData("target_db_type", target_db_type)
    model_observations_manager.setMetaData("target_db_host", target_db_host)
    model_observations_manager.setMetaData("target_db_name", target_db_name)
    model_observations_manager.setMetaData("target_db_view_files", data_db.view_files)
    model_observations_manager.setMetaData("target_db_redirect_dirs", data_db.redirect_view_directories)
    model_observations_manager.setDBType(None)

    query_name = options.query_name

    query_find_tag_string = options.query_find_tags
    if (query_find_tag_string == None):
        query_find_tag_string = ""

    query_find_tag_list = hybrid.utils.clean_and_split(query_find_tag_string, ",")

    query_except_tag_string = options.query_except_tags
    if (query_except_tag_string == None):
        query_except_tag_string = ""

    query_except_tag_list = hybrid.utils.clean_and_split(query_except_tag_string, ",")

    query_info = {}
    query_info["db_type"] = source_db_type
    query_info["db_host"] = source_db_host
    query_info["db_name"] = source_db_name
    query_info["query_name"] = query_name
    query_info["query_find_list"] = query_find_tag_list
    query_info["query_except_list"] = query_except_tag_list

    target_query_info = {}
    target_query_info["db_type"] = target_db_type
    target_query_info["db_host"] = target_db_host
    target_query_info["db_name"] = target_db_name
    target_query_info["query_name"] = ""
    target_query_info["query_find_list"] = []
    target_query_info["query_except_list"] = []

    # Add the archive db
    model_observations_manager.create_and_add_database(target_query_info)
    model_observations_manager.create_and_add_database(query_info)

    model_observations_manager.update()
