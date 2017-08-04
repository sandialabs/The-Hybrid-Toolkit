"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import datetime
import hybrid
import random
import string
import sys
import time
from hybrid import data_blob

if __name__ == "__main__":
    # Create a db view.
    db = hybrid.db.mongodb(database='model_words')
    # Get the system word list.
    dictfile = "/usr/share/dict/words"
    try:
        f = open(dictfile)
        words = [string.strip(x) for x in f.readlines()]
        f.close()
    except IOError:
        print >> sys.stderr, "could not open file '%s' for reading" % (dictfile)
        sys.exit(1)

    # At intervals, output a random word from the list.
    # start inserting data
    try:
        while (True):
            # Select the word and print it.
            word = words[random.randint(0, len(words) - 1)]
            print word

            # Store the word in the database.
            rec = {"timestamp": datetime.datetime.now(),
                   "word": word}

            blob = data_blob.dict2blob(rec)
            db.storeDataBlob(blob)

            # Pause for a random interval to slow the process down.
            time.sleep(random.random() * 1.0)
    except (KeyboardInterrupt, SystemExit):
        print "\n"
        print "closing db connection to \"words\""
        db.close()
        print "closed"
    except:
        raise
