"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import pymongo
import sys

# Use the commandline-specified hostname, if given.
host = "localhost"
if len(sys.argv) > 1:
    host = sys.argv[1]

# Try to make a connection to the DB.
try:
    c = pymongo.Connection(host)
except pymongo.errors.ConnectionFailure:
    print >>sys.stderr, "could not connect to host %s" % (host)

# Drop the collection used by the Hybrid examples.  If the collection doesn't
# exist, this function still silently succeeds.
c.local.test.drop()
