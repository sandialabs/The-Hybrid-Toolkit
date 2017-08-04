"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import optparse
import sys

import hybrid


def init_views(db, view_file, one_per_view, view_type, view_doc):
    # def push_view(self, view_file, del_existing, multiple=False, view_doc="views", lang="erlang"):
    db.push_view(view_file, True, multiple=one_per_view, lang=view_type, view_doc=view_doc)


if __name__ == "__main__":
    reader = None
    # Handle command-line arguments
    usage = "usage: %prog [options] -f view_file"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("--host", default='http://localhost:5984', help="Name of database host.  Default: %default")
    parser.add_option("--database", default='my_db', help="Name of database.  Default: %default")
    parser.add_option("--log-level", type="int", default=2,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    parser.add_option("--one-per-view", action="store_true", default=False,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    parser.add_option("--view-type", type="str", default='erlang',
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    parser.add_option("--view-doc", type="str", default='views', help="Name of the view document.   Default: %views")
    parser.add_option("-f", "--filename", type="str", default='',
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")

    (options, arguments) = parser.parse_args()

    if not options.filename:
        parser.print_help()
        sys.exit(1)

    # Get a logger
    logger = hybrid.logger.logger()
    logger.setLogLevel(options.log_level)
    content_labels = []

    # Call the main processing function
    db = hybrid.db.init("couchdb", host=options.host, database=options.database, create=True)

    # for some unknown reason the hybrid logger loglevel is static and overritten
    # with the default from hybrid db classes

    logger.setLogLevel(options.log_level)

    init_views(db, options.filename, options.one_per_view, options.view_type, options.view_doc)
