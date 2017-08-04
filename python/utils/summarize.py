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
import csv
import cStringIO
import codecs


# TODO add pagination so its obvious its doing something for large views

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


def summarize(db, output, view, fields, uuids, startkey=None, endkey=None):
    # Pull a view from the database to see how many docs are in the filterd view
    if startkey == None and endkey == None:
        temp_view = db.loadView(view, include_docs=True)
    else:
        temp_view = db.loadView(view, startkey=startkey, endkey=endkey, include_docs=True)

    # Loop through rows counting the messages
    rows = temp_view.rows()
    writer = UnicodeWriter(output)

    # write out the csv header
    writer.writerow(fields)

    for row in rows:
        meta = row["doc"]
        try:
            writer.writerow(get_fields(meta, fields))
        except Exception, e:
            print str(e)
            print get_fields(meta, fields)


def get_fields(d, fields):
    ans = []
    for field in fields:
        if field == 'uuid':
            field = "_dataBlobID"
        if field in d.keys():
            ans.append(unicode(d[field]))
        else:
            ans.append(unicode(''))
    return ans


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
    parser.add_option("--input", "-i", type="str", default=None,
                      help="Input file of newline separted uuids. Default: %default")
    parser.add_option("--output", "-o", type="str", default='stdout',
                      help="Output file for results or stdout for sys.stdout. Default: %default")
    parser.add_option("--fields", "-f", type="str", default="uuid",
                      help="comma separated list of fields to be pulled per document. Default: %default")

    (options, arguments) = parser.parse_args()

    # Get a logger
    logger = logger.logger()
    logger.setLogLevel(options.log_level)

    options.fields = [field.strip() for field in options.fields.split(',')]

    if len(arguments) > 0:
        options.uuids = arguments[0:]
    else:
        options.uuids = []

    if options.input:
        f = open(options.input, 'rb')
        options.uuids = [uuid.strip() for uuid in f.readlines()]

    if options.output:
        if options.output == "stdout":
            options.output = sys.stdout
        else:
            options.output = open(options.output, "wb")

    # Open connection to the database
    db = db.init("couchdb", host=options.host, database=options.database)

    if options.interval == 0 and options.view != None:
        summarize(db, options.output, options.view, options.fields, options.uuids)
    else:
        # Create a default expiration time of now - KEEP_DAYS days
        now = datetime.datetime.utcnow()
        earilest = now - datetime.timedelta(hours=options.interval)
        endkey = now.strftime("%Y-%m-%d %H:%M")
        startkey = earilest.strftime("%Y-%m-%d %H:%M")

        summarize(db, options.output, options.view, options.fields, options.uuids, startkey=startkey, endkey=endkey)

    sys.exit(0)
