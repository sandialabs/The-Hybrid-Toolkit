#
# Remove items from database based on date
#
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import sys, datetime
import optparse
from hybrid import logger, db


def count(db, view, startkey=None, endkey=None):
    # Pull a view from the database to see how many docs are in the filterd view
    if startkey == None and endkey == None:
        temp_view = db.loadView(view)
    else:
        temp_view = db.loadView(view, startkey=startkey, endkey=endkey)

    # Loop through rows counting the messages
    rows = temp_view.rows()
    row_count = len(rows)

    return row_count


if __name__ == "__main__":

    # Handle command-line arguments
    parser = optparse.OptionParser()
    parser.add_option("--view", default=None, help="View to perform action: Default: %default")
    parser.add_option("--host", default="http://localhost:5984", help="Name of DB host.  Default: %default")
    parser.add_option("--interval", default=0.0, type="float",
                      help="number of hours to count over, assumes keys are utc time values: Default: %default")
    parser.add_option("--database", default="my_db", help="Name of DB database.  Default: %default")
    parser.add_option("--log-level", type="int", default=3,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    (options, arguments) = parser.parse_args()

    # Get a logger
    logger = logger.logger()
    logger.setLogLevel(options.log_level)

    # Open connection to the database
    db = db.init("couchdb", host=options.host, database=options.database)

    if options.interval == 0:
        print count(db, options.view)
    else:
        # Create a default expiration time of now - KEEP_DAYS days
        now = datetime.datetime.utcnow()
        earilest = now - datetime.timedelta(hours=options.interval)
        endkey = now.strftime("%Y-%m-%d %H:%M")
        startkey = earilest.strftime("%Y-%m-%d %H:%M")

        print count(db, options.view, startkey=startkey, endkey=endkey)

    sys.exit(0)
