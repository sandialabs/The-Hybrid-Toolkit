#
# Check every record in this database for unicode correctness
#
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import sys
import optparse
import hybrid

if __name__ == "__main__":
    # Handle command-line arguments
    parser = optparse.OptionParser()
    parser.add_option("--host", default="http://localhost:5984", help="Name of database host.  Default: %default")
    parser.add_option("--database", default="", help="Name of database.  Default: %default")
    parser.add_option("--convert", action="store_true", default=False,
                      help="Flag needed to convert string to unicode.  Default: %default")
    parser.add_option("--log-level", type="int", default=3,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    (options, arguments) = parser.parse_args()

    # Get a logger
    logger = hybrid.logger.logger()
    logger.setLogLevel(options.log_level)

    # Open the databases
    logger.info("Opening database: ", options.host + ":" + options.database)
    db = hybrid.db.init("couchdb", host=options.host, database=options.database)

    # Pull a view from this database with all the documents
    query_info = {"db_type": "couchdb", "db_host": options.host, "db_name": options.database, "query_name": ""}

    document_view = hybrid.view.create_view_from_query_info(query_info)

    documents = document_view.rows()

    if document_view is None:
        logger.error("Could not open view on:", query_info)
        sys.exit()

    # Simply loop over the documents in the database and run a validate on the docs
    # and push to the target database
    for document in documents:

        # Do they want to convert
        if (options.convert):
            document.convertStringsToUnicode()
            document.store()

    logger.info("Validation Complete!")
    sys.exit(0)
