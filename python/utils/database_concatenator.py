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
__minimal = ["_dataBlobID", "_rev", "_attachments", "DATETIME", "import_hostname", "unique_id", "filename", "label"]


def reset_blob(blob):
    minimal_fields = __minimal
    for key in doc.getMetaDataDict().keys():
        if key not in minimal_fields:
            doc.deleteMetaData(key)


def delete_blob(uuid, rev):
    blob = hybrid.data_blob.create(uuid)
    blob.setDataBlobRevision(rev)
    # blob.setMetaData("_rev", rev)
    blob.setMetaData("_deleted", True)
    return blob


def operation_blob(blob, operation):
    action = 'update'
    uuid = blob.getDataBlobUUID()
    if operation == 'fix_labels':
        d = blob.getMetaDataDict()
        if "label" in d:
            action = 'update'
            v = d["label"]
            print "v=", v
            if isinstance(v, str) or isinstance(v, unicode):
                l = v.split(',')
                l2 = [i.strip() for i in l]
                l = list(set(l2))
            else:
                l = v

            if 'unknown' in l:
                action = "drop"

            # lstr = ','.join(l)
            d["label"] = l

            print "blob = " + uuid + " initial=" + str(v), " final=" + str(l) + " action=", action
    elif operation == 'drop':
        action = 'drop'

    return action


def rename_blob(blob, new_uuid):
    blob.setDataBlobUUID(new_uuid)
    blob.setMetaData("_dataBlobID", new_uuid)
    blob.setMetaData("_rev", 0)


def perform_action(doc_array, options, sourcedb, targetdb):
    action_docs = {}

    if options.reset:
        reset_blob(doc)

    for doc in doc_array:
        if options.operation:
            action = operation_blob(doc, options.operation)

            if action != "skip":
                if action == 'drop':
                    uuid = doc.getDataBlobUUID()
                    rev = doc.getDataBlobRevision()
                    doc = delete_blob(uuid, rev)

                if action not in action_docs:
                    action_docs[action] = [doc]
                else:
                    action_docs[action].append(doc)

    for action in action_docs.iterkeys():
        if options.inplace:
            sourcedb.storeDataBlobArray(action_docs[action])
        else:
            targetdb.storeDataBlobArray(action_docs[action])

        return


if __name__ == "__main__":

    # Handle command-line arguments
    parser = optparse.OptionParser()
    parser.add_option("--cfg", default="", help="Name of the config file.  Default: %default")
    parser.add_option("--source-db-host", default=None, help="Name of source database host.  Default: %default")
    parser.add_option("--source-db-name", default=None, help="Name of source database.  Default: %default")
    parser.add_option("--source-db-type", default=None, help="Name of source database.  Default: %default")
    parser.add_option("--target-db-host", default=None, help="Name of target database host.  Default: %default")
    parser.add_option("--target-db-name", default=None, help="Name of target database.  Default: %default")
    parser.add_option("--target-db-type", default=None, help="Name of target database host.  Default: %default")
    parser.add_option("--target-uuid", default=None, help="Name of target uuid.  Default: %default")
    #    parser.add_option("--host", default="http://localhost:5984", help="Name of database host.  Default: %default")
    #    parser.add_option("--sourcedb", default="", help="Name of database.  Default: %default")
    #    parser.add_option("--targetdb", default="", help="Name of database.  Default: %default")
    parser.add_option("--external-host", default="https://%h/couch",
                      help="External name of database. %h expands to the name of localhost.  Default: %default")
    parser.add_option("--start-key", default=None, help="Provide the start key for a view.  Default: %default")
    parser.add_option("--static", action="store_true", default=False,
                      help="Run the extraction loop just once (for static datasets).  Default: %default")
    parser.add_option("--uuids", help="Specify a comma separated list of uuids to reprocess.")
    parser.add_option("--log-level", type="int", default=3,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    parser.add_option("--query-name", default=None, help="Name of the input query.  Default: %default")
    parser.add_option("--query-tags", default="", help="Tags the queried documents should contain.  Default: %default")
    parser.add_option("--except-tags", default="",
                      help="Tags the queried documents should ~not~ contain.  Default: %default")
    parser.add_option("--reset", action="store_true", default=False, help="Reset doc to default fields: %default")
    parser.add_option("--operation", default=None, help="Run a custom operation on the input: %default")
    parser.add_option("--inplace", action="store_true", default=False,
                      help="modify document in place - don't require a destination: Default: %default")
    parser.add_option("--limit", default=None, help="maximum number of documents to act upon Default: %default")
    parser.add_option("--continuous", action="store_true",
                      help="run in an infinite loop - useful to run with limit Default: %default")
    parser.add_option("--view-only", default=None, action="store_true",
                      help="Don't bother to retrieve the document Default: %default")

    (options, arguments) = parser.parse_args()

    CONFIG_FILENAME = options.cfg
    if not (CONFIG_FILENAME == ""):
        config = ConfigParser.ConfigParser()
        config.read(CONFIG_FILENAME)

        if config.has_option("Databases", "SourceDBName"):
            options.source_db_name = config.get("Databases", "SourceDBName")
        if config.has_option("Databases", "SourceDBHost"):
            options.source_db_host = config.get("Databases", "SourceDBHost")
        if config.has_option("Databases", "SourceDBType"):
            options.source_db_type = config.get("Databases", "SourceDBType")

        if not options.inplace:
            if config.has_option("Databases", "TargetDBName"):
                options.target_db_name = config.get("Databases", "TargetDBName")
            if config.has_option("Databases", "TargetDBHost"):
                options.target_db_host = config.get("Databases", "TargetDBHost")
            if config.has_option("Databases", "TargetDBType"):
                options.target_db_type = config.get("Databases", "TargetDBType")

        if config.has_option("General", "LogLevel"):
            options.log_level = config.get("General", "LogLevel")
        if config.has_option("General", "UUIDs"):
            options.uuids = config.get("General", "UUIDs")
        if config.has_option("General", "Static"):
            options.static = config.get("General", "Static")

        if config.has_option("Parameters", "UUIDs"):
            options.uuids = config.get("Parameters", "UUIDs")
        if config.has_option("Parameters", "QueryName"):
            options.query_name = config.get("Parameters", "QueryName")
        if config.has_option("Parameters", "QueryTags"):
            options.query_tags = config.get("Parameters", "QueryTags")
        if config.has_option("Parameters", "ExceptTags"):
            options.except_tags = config.get("Parameters", "ExceptTags")
        if config.has_option("Parameters", "start-key"):
            options.start_key = config.get("Parameters", "start-key")

    if options.query_name == None:
        query_name = ""
    else:
        query_name = options.query_name

    if not options.source_db_type:
        options.source_db_type = "couchdb"

    if not options.target_db_type:
        options.target_db_type = "couchdb"

    # Get a logger handle (singleton)
    logger = hybrid.logger.logger()
    logger.setLogLevel(options.log_level)

    source_db_host = options.source_db_host
    source_db_name = options.source_db_name
    source_db_type = options.source_db_type
    target_db_host = options.target_db_host
    target_db_name = options.target_db_name
    target_db_type = options.target_db_type

    targetdb = None

    log_level = options.log_level
    external_host = options.external_host.replace("%h", socket.gethostname())
    if options.uuids:
        uuid_list_string = options.uuids.split(',')
    else:
        uuid_list_string = ''

    query_tags = options.query_tags
    except_tags = options.except_tags

    query_tag_list = query_tags.split(",")
    except_tags_list = except_tags.split(",")
    print source_db_type, source_db_host, source_db_name, query_name
    sourcedb = hybrid.db.init(source_db_type, host=source_db_host, database=source_db_name, push_views=False,
                              create=False)
    doc_array = []

    if not options.inplace:
        targetdb = hybrid.db.init(target_db_type, host=target_db_host, database=target_db_name, push_views=False,
                                  create=True)

    if uuid_list_string:
        for uuid in uuid_list_string:
            doc = sourcedb.loadDataBlob(uuid, include_binary=True)
            if not doc:
                logger.info("source doc " + uuid + " does not exist")
                continue

            if options.target_uuid != None:
                rename_blob(doc, options.target_uuid)

            doc_array.append(doc)

            if len(doc_array) > 250:
                print "peforming action %s itemcount: %d" % (options.operation, len(doc_array))
                perform_action(doc_array, options, sourcedb, targetdb)
                doc_array = []

        if doc_array:
            print "peforming action %s itemcount: %d" % (options.operation, len(doc_array))
            perform_action(doc_array, options, sourcedb, targetdb)
            doc_array = []
    else:
        doc_array = []
        while True:

            if options.start_key:
                view = hybrid.view.create_view(sourcedb, query_name, query_tag_list, except_tags_list,
                                               start_key=options.start_key)
            else:
                view = hybrid.view.create_view(sourcedb, query_name, query_tag_list, except_tags_list)

            if options.limit:
                view = hybrid.view.create_view(sourcedb, query_name, query_tag_list, except_tags_list, reduce=False,
                                               limit=options.limit, include_docs=not (options.view_only))

            rows = view.rows()

            for row in rows:
                if (row == None): continue
                # Grab the whole document
                row_id = row.getMetaData("_dataBlobID")
                # Skip any design documents
                if (row_id.startswith(u"_design")): continue

                if options.view_only:
                    doc = row
                else:
                    doc = sourcedb.loadDataBlob(row_id, include_binary=True)

                if doc:
                    doc_array.append(doc)

                if len(doc_array) > 250:
                    print "peforming action %s itemcount: %d" % (options.operation, len(doc_array))
                    perform_action(doc_array, options, sourcedb, targetdb)
                    doc_array = []

            if doc_array:
                print "peforming action %s itemcount: %d" % (options.operation, len(doc_array))
                perform_action(doc_array, options, sourcedb, targetdb)
                doc_array = []

            if not options.continuous:
                break
    # Status
    logger.info("Transfer complete!")
