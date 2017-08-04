#
# Remove items from database based on date
#
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import optparse
import sys

import hybrid

if __name__ == "__main__":
    #    global server, database

    # Handle command-line arguments ...
    parser = optparse.OptionParser()
    parser.add_option("--host", default="http://localhost:5984", help="Name of database host.  Default: %default")
    parser.add_option("--database", default="my_db", help="Name of database.  Default: %default")
    parser.add_option("--database-type", type="choice", choices=['couchdb'], default="couchdb",
                      help="Type of database.  Default: %default")
    parser.add_option("--days", default=1000, type="int", help="Number of days to process.  Default: %default")
    parser.add_option("--max-docs", default=1000, help="Max number of documents.  Default: %default")
    parser.add_option("--log-level", type="int", default=3,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    (options, arguments) = parser.parse_args()

    # Get a logger
    logger = hybrid.logger.logger()
    logger.setLogLevel(options.log_level)

    # Open connection to the database and remove lock
    db = hybrid.db.init(options.database_type, host=options.host, database=options.database)
    db.removeLock()

    sys.exit(0)
