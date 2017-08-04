#
# Remove items from database based on date
#
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import hybrid
import sys, datetime
import optparse
import traceback

minimal = ["_dataBlobID", "_rev", "_attachments", "DATETIME", "import_hostname", "unique_id", "filename", "label"]


def modDocumentFields(db, query_info, pull_window, options):
    max_docs = max(int(options.max_docs), 1)

    if options.uuid:
        documents = []
        for uuid in options.uuid:
            doc = db.loadDataBlob(uuid)
            if doc:
                documents.append(doc)
    else:
        view_uri = "_all_docs"
        logger.info("Pulling view", view_uri)
        view = hybrid.view.create_view_from_query_info(query_info)

        # Grab the rows in the view
        documents = view.rows()

    # Loop through rows removing the existing document tags
    for doc in documents:
        # Grab the document and delete the tag
        #        doc = db.loadDataBlob(row.id, include_binary=True)

        if ((not doc.hasMetaData(options.field)) and (not (options.action[:3] == "set"))):
            continue

        if (options.action == "res"):
            for key in doc.getMetaDataDict().keys():
                if key not in minimal:
                    doc.deleteMetaData(key)
        elif (options.action == "del"):
            doc.deleteMetaData(options.field)
        elif (options.action == "ren"):
            val = doc.getMetaData(options.field)
            doc.deleteMetaData(options.field)
            doc.setMetaData(options.field2, val)
        elif (options.action == "set-string"):
            doc.setMetaData(options.field, options.value)
        elif (options.action == "set-int"):
            doc.setMetaData(options.field, int(options.value))
        elif (options.action == "set-float"):
            doc.setMetaData(options.field, float(options.value))
        else:
            print "invalid command ", options.action
            exit(1)

        db.storeDataBlob(doc)

    logger.info("Document Field Modifier Complete!")


if __name__ == "__main__":

    # global server, database

    # Handle command-line arguments ...
    parser = optparse.OptionParser()
    parser.add_option("--db-host", default="http://localhost:5984", help="Name of database host.  Default: %default")
    parser.add_option("--db-name", default="", help="Name of database.  Default: %default")
    parser.add_option("--db-type", default="", help="Type of database.  Default: %default")
    parser.add_option("--action", default="del",
                      help="Action to be performed (del=delete, res=reset, ren=rename, set-string=assign a string, set-int=assign an int, set-float=assign a float).  Default: %default")
    parser.add_option("--field", default="tag_evaluated",
                      help="Name of the field on which the action will be performed.  Default: %default")
    parser.add_option("--field2", default="tag_evaluated",
                      help="Name of the second field on which the action will be performed, if renaming.  Default: %default")
    parser.add_option("--value", default="foo",
                      help="The value that will be assigned to the target field.  Default: %default")
    parser.add_option("--days", default=1000, type="int", help="Number of days to process.  Default: %default")
    parser.add_option("--max-docs", default=sys.maxint, type="int", help="Max number of documents.  Default: %default")
    parser.add_option("--log-level", type="int", default=3,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    #    parser.add_option("--keep-tags",  action="store_true", default=False, help="Keep old tags, to perserve feature coverage.  Default: %default")
    parser.add_option("--query-name", default="", help="Tags the queried documents should contain.  Default: %default")
    parser.add_option("--query-find-tags", default="",
                      help="Tags the queried documents should contain.  Default: %default")
    parser.add_option("--query-except-tags", default="",
                      help="Tags the queried documents should ~not~ contain.  Default: %default")
    parser.add_option("--uuid", default=None, help="One or more comma separated uuids to process Default: %default")
    (options, arguments) = parser.parse_args()

    # Get a logger
    logger = hybrid.logger.logger()
    logger.setLogLevel(options.log_level)

    # Create a default document pull window of now - DAYS
    now = datetime.datetime.utcnow()

    # Window of time for which to tag documents
    date_delta = now - datetime.timedelta(days=options.days)
    pull_window = date_delta.strftime("%Y-%m-%d %H:%MZ")

    query_name = options.query_name

    query_find_tag_string = options.query_find_tags
    if (query_find_tag_string == None):
        query_find_tag_string = ""

    query_find_tag_list = hybrid.utils.clean_and_split(query_find_tag_string, ",")

    query_except_tag_string = options.query_except_tags
    if (query_except_tag_string == None):
        query_except_tag_string = ""

    query_except_tag_list = hybrid.utils.clean_and_split(query_except_tag_string, ",")

    if not options.db_type:
        options.db_type = "couchdb"

    if options.uuid:
        options.uuid = options.uuid.split(',')

    query_info = {}
    query_info["db_type"] = options.db_type
    query_info["db_host"] = options.db_host
    query_info["db_name"] = options.db_name
    query_info["query_name"] = query_name
    query_info["query_find_list"] = query_find_tag_list
    query_info["query_except_list"] = query_except_tag_list

    # Open connection to the database
    try:
        db = hybrid.db.init(options.db_type, host=options.db_host, database=options.db_name)

        # Tag the documents within the pull window
        modDocumentFields(db, query_info, pull_window, options)

    except Exception, e:
        traceback.print_exc()
        logger.error("Exception while doing modify doc fields:%s" % (str(e)))
        sys.exit(1)

    sys.exit(0)
