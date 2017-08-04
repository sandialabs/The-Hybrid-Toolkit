.. _words-example:
====================================
 Building a Data Pipeline in Hybrid
====================================

Building a data processing pipeline with the Hybrid API requires the use of a
few Python classes, arranged in a particular way.  This document will go step by
step through the creation of a toy data processing pipeline example, showing how
a stream of English words can be operated on, the results being deposited in a
database.

Python Basics
=============

This tutorial makes heavy use of several Hybrid Python classes:
`abstract_worker` (found in the `hybrid.worker.worker` module), `manager` (in
the `hybrid.manager.manager` module), and `mongodb` (found in `hybrid.db`).
`abstract_worker` subclasses specify how to carry out some phase of analysis,
i.e., what kind of action to take on incoming data items, while `manager`
handles the retrieval and storage of input/output data, which workers to
dispatch, and specification of input and output dependencies.  Together, both
classes interact to create a dataflow pipeline or network.

A Tutorial Example: *Words*
===========================

The *words* example can be found in the Hybrid codebase, in the `python/words`
directory.  We will construct data analysis network that performs the following
tasks:

1. Generates a stream of English words.  This will serve as the **data source**.

2. Computes the **number of vowels** and **vowel proportion** of each word.

3. Computes the **Scrabble score** of each word.

4. Computes the **Wheel of Fortune vowel cost** of each word.

Notice that tasks 2 and 3 depend on task 1, while task 4 depends on task 2,
forming a directed acyclic graph, or network, of data dependencies.

Task 1: `words.py`
------------------

The following script reads in the system dictionary file and then emits random
words from it at random intervals, emulating a data stream of varying
"burstiness":

.. code-block:: python
    :linenos:

    import datetime
    import hybrid
    import pymongo
    import random
    import string
    import sys
    import time

    if __name__ == "__main__":
        # Create a db view.
        db = hybrid.db.mongodb()

        # Get the system word list.
        dictfile = "/usr/share/dict/words"
        try:
            f = open(dictfile)
            words = [string.strip(x) for x in f.readlines()]
            f.close()
        except IOError:
            print >>sys.stderr, "could not open file '%s' for reading" % (dictfile)
            sys.exit(1)

        # At intervals, output a random word from the list.
        while (True):
            # Select the word and print it.
            word = words[random.randint(0, len(words) - 1)]
            print word

            # Store the word in the database.
            rec = { "timestamp": datetime.datetime.now(),
                    "word": word }
            db.store(rec)

            # Pause for a random interval to slow the process down.
            time.sleep(random.random() * 1.0)

This script prints the emitted words to the screen and also deposits them in a
Mongo database.  Note that the Hybrid database API is being used (lines 11 and
32) to engage the Mongo database [#]_, but this is not strictly necessary.  In fact,
any process at all that winds up storing raw data in the database can be used as
the "data source" for a Hybrid data analysis network.

(*Note:* the `timestamp` field in the database record is placed there solely to
aid in the construction of the visualization application described below; it is
not necessary for just running the Hybrid application.)

Now that a data source has been created, we need to process the data to
accomplish the remainder of the tasks.

Task 2: `vowels.py`
-------------------

The vowel content of English words might be an interesting thing to know about.
Here is a script that will process the words flowing into our Mongo database to
compute some vocalic properties:

.. code-block:: python
    :linenos:

    import hybrid

    class VowelCounter(hybrid.worker.worker.abstract_worker):
        def __init__(self, **kwargs):
            hybrid.worker.worker.abstract_worker.__init__(self, **kwargs)
            self.set_uses_model(False)

            VowelCounter.vowels = "aeiouAEIOU"

        def process_observation_core(self, doc, **kwargs):
            word = doc.getMetaData("word")

            vowel_count = VowelCounter.count_vowels(word)
            vowel_fraction = float(vowel_count) / float(len(word))

            self.addMetaData(doc, "count", vowel_count)
            self.addMetaData(doc, "fraction", vowel_fraction)

            print "[vowels] processing '%s'" % (word)

            return doc

        @staticmethod
        def count_vowels(word):
            return len(filter(lambda x: x in VowelCounter.vowels, word))

    if __name__ == "__main__":
        db = hybrid.db.mongodb()

        worker = VowelCounter(name="vowels")
        manager = hybrid.manager.manager(workers=[worker],
                                         query="test",
                                         input_tag_list=[],
                                         output_tag_list=["tag_vowel_attributes"],
                                         input_db=db,
                                         output_db=db)

        manager.run()

The script centers on the class `VowelCounter` defined in line 3, which is
derived from the `hybrid.worker.worker.abstract_worker` class in the Hybrid API.
Each such derived class must define a `process_observation_core()` method, which
takes as input a single document from the database, and specifies what to
compute in order to transform or augment the database document with additional
information.

In this case, the method invokes `count_vowels()` to figure out how many vowels
are present in the word, and then divides this number by the total length of the
word to compute the vowel proportion.  The worker's `addMetaData()` method is
used to deposit these vowels into fields of a named sub-container of the document ("count"
and "fraction", respectively).  Line 30 instantiates the `VowelCounter` class
with the name "vowels"; this is the name by which the sub-container is keyed.
Here is (a subset of) an example Mongo document after `VowelCounter` has
processed it:

.. code-block:: javascript

    {
        "_id" : ObjectId("521b9fa8dd28a80d41f2abc8"),
        "word" : "Seton",
        "vowels" : {
            "count" : 2,
            "worker_data_version" : 0,
            "fraction" : 0.4
        },
        "tag_vowel_attributes" : "complete"
    }

The output of `vowels.py` is neatly packaged inside the "vowels" sub-document.

In order to drive the use of the `VowelCounter` class, there is a main script
starting on line 27.  A database handle to the default Mongo database and
collection is created, then the `VowelCounter` worker is instantiated, and
finally a `manager` class instance is created.  The `manager` handles workers,
input and output dependencies, and databases.  In this case, the `manager`
specifies that (1) our `VowelCounter` instance is the sole worker, (2) queries
should be made to the database collection named "test", (3) the manager does not
depend on anything in the database record [#]_, (4) it creates an output
dependency tag named "tag_vowel_attributes", (5) input records come from the
database handle created in line 28, and finally, (6) output records will go into
this same database.

Finally, the manager's `run()` method is called, which starts the manager
watching the database and working on database documents as they come in.

Task 3: `scrabble.py`
---------------------

The Scrabble module is very similar to `vowels.py` in that it also computes a
value (namely, the Scrabble value of the word) directly for the raw word itself;
it differs only in the nature of the `ScrabbleScore` class's particular
computations, the name given to the worker based on `ScrabbleScore`, and the
name of the output tag used by the manager:

.. code-block:: python
    :linenos:

    import hybrid
    import string

    def build_score_table():
        # These are scrabble tile scores for A through Z.
        score = [1, 3, 3, 2, 1, 4, 2, 4, 1, 8, 5, 1, 3, 1, 1, 4, 10, 1, 1, 1, 2, 1, 3, 10, 1, 1, 1, 1, 4, 4, 8, 4, 10]

        # Build a table of the scores, indexed by both lower and uppercase letters.
        table = {}
        for i in range(26):
            table[string.ascii_lowercase[i]] = score[i]
            table[string.ascii_uppercase[i]] = score[i]

        return table

    class ScrabbleScore(hybrid.worker.worker.abstract_worker):
        def __init__(self, scoretable, **kwargs):
            hybrid.worker.worker.abstract_worker.__init__(self, **kwargs)
            self.set_uses_model(False)

            self.scoretable = scoretable

        def process_observation_core(self, doc, **kwargs):
            word = doc.getMetaData("word")
            score = self.score(word)

            self.setMetaData(doc, "score", score)

            print "[scrabble] processing '%s' - score: %d" % (word, score)

            return doc

        def score(self, word):
            # For non-letter characters (e.g., apostrophes), just default to 0
            # score.
            return sum(self.scoretable.get(letter, 0) for letter in word)

    if __name__ == "__main__":
        # Get a DB handle.
        db = hybrid.db.mongodb()

        # Create a worker and a module to compute scrabble scores.
        worker = ScrabbleScore(build_score_table(), name="scrabble")
        manager = hybrid.manager.manager(workers=[worker],
                                         query="test",
                                         input_tag_list=[],
                                         output_tag_list=["tag_scrabble"],
                                         input_db=db,
                                         output_db=db)

        manager.run()

Task 4: `wheel.py`
------------------

The last task is to compute what it would cost to buy all the vowels in a word
on Wheel of Fortune:

.. code-block:: python
    :linenos:

    import hybrid

    class WheelOfFortune(hybrid.worker.worker.abstract_worker):
        def __init__(self, **kwargs):
            hybrid.worker.worker.abstract_worker.__init__(self, **kwargs)

        def process_observation_core(self, doc, **kwargs):
            word = doc.getMetaData("word")

            vowel_count = doc.getMetaData("vowels:count")
            vowel_cost = 250.0 * vowel_count
            self.addMetaData(doc, "vowel_cost", vowel_cost)

            print "[wheel of fortune] processing '%s' - vowel count: %d, vowel cost: $%d" % (word, vowel_count, vowel_cost)

            return doc

    if __name__ == "__main__":
        db = hybrid.db.mongodb()

        worker = WheelOfFortune(name="wheel of fortune")
        manager = hybrid.manager.manager(workers=[worker],
                                         query="test",
                                         input_tag_list=["tag_vowel_attributes"],
                                         output_tag_list=["tag_wheel_of_fortune"],
                                         input_db=db,
                                         output_db=db)

        manager.run()

This script is somewhat similar to the previous ones, with one major difference:
line 24 specifies an *input dependency tag*, meaning that this script will watch
for that dependency to complete before it runs on a particular document.  In
this case, the manager specifies that computing the vowel cost of a word depends
on first counting the vowels present in that word.

Putting It All Together
=======================

We now have four scripts: one to generate words, and three to process the words
in various ways.  To run the data analysis network and see everything working
together, simply open four terminals, and run each script in one window.
`words.py` will print words to the screen, and every five seconds, the other
three scripts will process whatever new word records are available, printing a
log of their activity on screen.

To see the effect of the data dependencies, you can kill `vowels.py`.  Once the
vowel processor stops running, the required dependency for `wheel.py` will be
interrupted.  You will notice that this causes `wheel.py` to stop doing its
work.  After several seconds, go ahead and restart `vowels.py`.  As expected,
`vowels.py` and `wheel.py` will take up the slack and process the glut of
untreated words, since the dependency chain is whole again.

Here is an example Mongo database record after the example analysis network has
run on it:

.. code-block:: javascript

    {
        "_id" : ObjectId("521b9fb2dd28a80d41f2abd8"),
        "tag_wheel_of_fortune" : "complete",
        "tag_scrabble" : "complete",
        "creation_datetime" : {
            "hour" : 18,
            "month" : 8,
            "second" : 27,
            "microsecond" : 27,
            "year" : 2013,
            "day" : 26,
            "minute" : 34
        },
        "created" : "2013-08-26 18:34",
        "timestamp" : ISODate("2013-08-26T14:34:26.121Z"),
        "_rev" : 3,
        "vowels" : {
            "count" : 5,
            "worker_data_version" : 0,
            "fraction" : 0.45454545454545453
        },
        "wheel of fortune" : {
            "worker_data_version" : 0,
            "vowel_cost" : 1250
        },
        "scrabble" : {
            "worker_data_version" : 0,
            "score" : 13
        },
        "word" : "talebearers",
        "type" : "unknown",
        "tag_vowel_attributes" : "complete"
    }

Note the three subdocuments containing all the computed attributes, as well as
the dependency tags all containing the value "complete", indicating that the
managers associated with each tag were able to run properly.

At this point, the database records can be used as input to visualization or
other applications.

A Note on Code Organization
---------------------------

For illustration purposes, the scripts in the preceding sections were written in
a peculiar way.  Each script has a class definition, specializing
`abstract_worker` to do a particular kind of work, and then a "main" section
that instantiates the class via a manager, then launches it.

Instead, it may make more sense to move all the "main" sections to a single
driver script.  The three illustrative scripts become three *modules* which are
imported in the driver script.  The driver then instantiates all of the managers
with instances of the worker classes, and runs all of them.  This allows the
"module" files to stand completely alone, decoupling their definition from their
use in various application.  It also allows the developer to specify the entire
dataflow dependency network in one place.

Footnotes
=========

.. [#] The `hybrid.db.mongodb()` function, by default, uses the "local"
    collection of the "test" database in the local instance of MongoDB.  The
    scripts in the words example all use these default settings, so the inputs
    and output can all be observed in that database collection.

.. [#] Strictly speaking of course, this manager *does* depend on something,
    namely the existence of a "word" field in the database record.  However, the
    manager does not depend on any other *managers* having already run, and the
    "word" field can be considered to be part of the primordial data soup that comes
    out of the database in the first place (i.e., placed there by some force
    external to the Hybrid API).
