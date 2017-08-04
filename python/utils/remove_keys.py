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
import sys

import hybrid

CONFIG_FILENAME = 'mlp.cfg'

# Evaluation doc limit
__doc_limit = 50

if __name__ == "__main__":

    # Handle command-line arguments
    parser = optparse.OptionParser()
    parser.add_option("--cfg", default="", help="Name of the config file.  Default: %default")
    parser.add_option("--host", default="http://localhost:5984", help="Name of database host.  Default: %default")
    parser.add_option("--sourcedb", default="sourcedb", help="Name of database.  Default: %default")
    parser.add_option("--targetdb", default="targetdb", help="Name of database.  Default: %default")
    parser.add_option("--external-host", default="https://%h/couch",
                      help="External name of database. %h expands to the name of localhost.  Default: %default")
    parser.add_option("--static", action="store_true", default=False,
                      help="Run the extraction loop just once (for static datasets).  Default: %default")
    parser.add_option("--uuids", help="Specify a comma separated list of uuids to reprocess.")
    parser.add_option("--log-level", type="int", default=3,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    parser.add_option("--query-tags", default="", help="Tags the queried documents should contain.  Default: %default")
    parser.add_option("--except-tags", default="",
                      help="Tags the queried documents should ~not~ contain.  Default: %default")
    parser.add_option("--keep-keys", default="", help="Keys to keep.  Default: %default")
    parser.add_option("--force", action="store_true", default=False,
                      help="Force removal with empty keep-keys set.  Default: %default")
    parser.add_option("--test", action="store_true", default=False,
                      help="Test mode, no removals will be done.  Default: %default")

    (options, arguments) = parser.parse_args()

    CONFIG_FILENAME = options.cfg
    if not (CONFIG_FILENAME == ""):
        config = ConfigParser.ConfigParser()
        config.read(CONFIG_FILENAME)

        if config.has_option("Databases", "SourceDB"):
            options.sourcedb = config.get("Databases", "SourceDB")
        if config.has_option("Databases", "TargetDB"):
            options.targetdb = config.get("Databases", "TargetDB")

        if config.has_option("General", "LogLevel"):
            options.log_level = config.get("General", "LogLevel")
        if config.has_option("General", "UUIDs"):
            options.uuids = config.get("General", "UUIDs")
        if config.has_option("General", "Static"):
            options.static = config.get("General", "Static")
        if config.has_option("General", "Host"):
            options.host = config.get("General", "Host")
        if config.has_option("General", "Force"):
            options.force = config.get("General", "Force")
        if config.has_option("General", "Test"):
            options.test = config.get("General", "Test")

        if config.has_option("Parameters", "UUIDs"):
            options.uuids = config.get("Parameters", "UUIDs")
        if config.has_option("Parameters", "QueryTags"):
            options.query_tags = config.get("Parameters", "QueryTags")
        if config.has_option("Parameters", "ExceptTags"):
            options.except_tags = config.get("Parameters", "ExceptTags")
        if config.has_option("Parameters", "KeepKeys"):
            options.keep_keys = config.get("Parameters", "KeepKeys")

    if options.query_tags == "":
        query_name = ""
    else:
        query_name = "labels"

    # Get a logger handle (singleton)
    logger = hybrid.logger.logger()
    logger.setLogLevel(options.log_level)

    host = options.host
    source_db_name = options.sourcedb
    target_db_name = options.targetdb
    log_level = options.log_level
    external_host = options.external_host.replace("%h", socket.gethostname())
    uuid_list_string = options.uuids
    keep_keys = options.keep_keys
    force = options.force
    test = options.test
    query_tags = options.query_tags
    except_tags = options.except_tags

    query_tag_list = query_tags.split(",")
    except_tags_list = except_tags.split(",")

    if (keep_keys == ""):
        if not (force == True):
            print "Empty key set. Use the --force to continue, or add items to the list of keys to keep"
            sys.exit(0)

    keep_keys_list = keep_keys.split(",")

    sourcedb = hybrid.db.init("couchdb", host=host, database=source_db_name, push_views=False, create=False)
    targetdb = hybrid.db.init("couchdb", host=host, database=target_db_name, push_views=False, create=True)
    print sourcedb.getDBName()
    print targetdb.getDBName()

    view = hybrid.view.create_view(sourcedb, query_name, query_tag_list, except_tags_list)
    documents = view.rows()

    for doc in documents:
        # Grab the whole document
        key_delete_list = []
        # Skip any design documents
        if (doc.getMetaData("_dataBlobID").startswith(u"_design")): continue

        # Iterate through the document dictionary key list, finding the ones to delete and
        # adding them to the key_delete_list
        for k in doc.getMetaDataDict().iterkeys():
            if k.startswith(u"_"):
                continue
            if (k == "type"):
                continue
            if (k == "created"):
                continue
            if not (k in keep_keys_list):
                key_delete_list.append(k)

        # Iterate through the key_delete_list, deleting the keys
        if (test):
            print "Not really deleting"
        for key in key_delete_list:
            print "Deleting", key
            if not (test):
                doc.deleteMetaData(key)

        # Store the document back into the targetdb, which can be the same as the source
        if not (test):
            targetdb.storeDataBlob(doc)
