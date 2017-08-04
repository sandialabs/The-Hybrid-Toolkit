#
# Copy one database to another (source-database -> target-database)
# Note: This script is primarily used when replication doesn't work
#       for whatever reason (often https dorks things up)
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
    parser.add_option("--source-db-host", default="http://localhost:5984",
                      help="Name of source database host.  Default: %default")
    parser.add_option("--source-db-name", default="", help="Name of source database.  Default: %default")
    parser.add_option("--source-db-type", default="couchdb", help="Name of source database.  Default: %default")
    parser.add_option("--target-db-host", default="http://localhost:5984",
                      help="Name of target database host.  Default: %default")
    parser.add_option("--target-db-name", default="", help="Name of target database.  Default: %default")
    parser.add_option("--target-db-type", default="mongodb", help="Name of target database host.  Default: %default")
    parser.add_option("--log-level", type="int", default=3,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    (options, arguments) = parser.parse_args()

    # Get a logger
    logger = hybrid.logger.logger()
    logger.setLogLevel(options.log_level)

    # Open the source and target databases
    logger.info("Opening source database: ", options.source_host + ":" + options.source_database)
    source_db = hybrid.db.init(options.source_db_type, host=options.source_db_host, database=options.source_db_name)

    logger.info("Opening target database: ", options.target_host + ":" + options.target_database)
    target_db = hybrid.db.init(options.target_db_type, host=options.target_db_host, database=options.target_db_name,
                               create=True, delete_existing=True)

    # Okay now copy the database
    source_db.copy_database(target_db)

    logger.info("Copying Complete!")
    sys.exit(0)
