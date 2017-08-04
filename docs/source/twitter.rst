====================================
 Exploring Twitter Data with Hybrid
====================================

After seeing how a data processing pipeline can be constructed in the
:ref:`words example <words-example>`, we now demonstrate a more real-world use
case.  Twitter exposes an `API <https://dev.twitter.com/docs/api>`_ for
exploring tweets in real time.  This example pulls tweets from the Twitter API,
stores them in a database, and sets a sentiment and anew analyzer to work examining the
text of each tweet.  At the of the pipeline, a Tangelo web application performs
some visualization of the results.

Getting Started with Twitter API
================================

To run this example, you will need Twitter app credentials.  If you already have
these, you may skip to :ref:`conf-json`.

Obtain Twitter App Credentials
------------------------------

Go to http://dev.twitter.com and create an app.  The consumer key and secret
will be generated for you.  After that, you will be redirected to your app's
page.  Create an access token under the "Your access token" section.  Once this
is completed, you should have four separate pieces of information: a `consumer
key`, a `consumer secret`, an `access token`, and an `access token secret`.
These four items constitute your Twitter app credentials.

.. _conf-json:

Record Credentials
------------------

Create a file in the `twitter` directory called `conf.json` that looks like this
(but fill in your keys within the quotes):

.. code-block:: javascript

    {
        "consumerKey": "",
        "consumerSecret": "",
        "accessToken": "",
        "accessTokenSecret": ""
    }

Now you are ready to pull data from Twitter for real-time analysis.

Running the Sentiment Analysis Network
======================================

This network has three components:

1. `tweets.py`, a **data source**: the Twitter API, engaged through the `tweepy` Python
   module, delivers a stream of tweets into a database.

2. `sentiment.py`, a **positive/negative sentiment analyzer**: NLTK, using movie
   reviews as training data, evaluating the sentiment in each
   tweet's text on a negative-to-positive spectrum.

3. `anew.py`, an **affective sentiment analyzer**: NLTK again, using the
   Affective Norms for English Words (ANEW) dataset, evaluates tweets for their
   *valence*, *arousal*, and *dominance* characteristics.

As with the :ref:`words example <words-example>`, each script can be run in its
own terminal window so that the logging output of each can be independently
observed.

`tweets.py`
-----------

This script is similar to `words.py` from the `words example <words-example>` in
that it engages an API external to Hybrid to produce a stream of raw data for
downstream modules to process.  In this case, it is the `Tweepy
<https://github.com/tweepy/tweepy>`_ Python module:

.. code-block:: python
    :linenos:

    import hybrid
    import json
    from tweepy.streaming import StreamListener
    from tweepy import OAuthHandler
    from tweepy import Stream

    class StdOutListener(StreamListener):
        """ A listener handles tweets are the received from the stream.
        This is a basic listener that just prints received tweets to stdout.

        """
        def __init__(self, db):
            self.db = db

        def on_data(self, data):
            tweet = json.loads(data)
            if "id" in tweet:
                tweet["_id"] = str(tweet["id"])
                self.db.store(tweet)
                print "Adding tweet: %s" % (tweet["text"])
            return True

        def on_error(self, status):
            print status

    if __name__ == "__main__":
        # EDIT: REMOVE THE FOLLOWING LINE
        db = hybrid.db.mongodb()
        # EDIT: UNCOMMENT THE FOLLOWING LINE
        #db = hybrid.db.mongodb(database="hybrid_tutorial", collection="twitter")
        l = StdOutListener(db)
        try:
            conf = json.load(open("conf.json"))
            auth = OAuthHandler(conf["consumerKey"], conf["consumerSecret"])
            auth.set_access_token(conf["accessToken"], conf["accessTokenSecret"])

            stream = Stream(auth, l)
            stream.sample()
        except IOError:
            print open("README").read()

The JSON data structure for each tweet is relatively extensive, but the text of
the tweet is stored in the top-level "text" property.

`sentiment.py`
--------------

This script performs sentiment analysis, using NLTK to train a classifier of
negative and positive sentiment based on movie review text (which is all
included in the NLTK data):

.. code-block:: python
    :linenos:

    import hybrid
    import nltk.classify.util
    from nltk.tokenize import wordpunct_tokenize
    from nltk.classify import NaiveBayesClassifier
    from nltk.corpus import movie_reviews

    def word_features(words):
        return {word: True for word in words}

    class Sentiment(hybrid.worker.worker.abstract_worker):
        def __init__(self, classifier, **kwargs):
            hybrid.worker.worker.abstract_worker.__init__(self, **kwargs)
            self.classifier = classifier

        def process_observation_core(self, tweet, **kwargs):
            text = tweet.getMetaData("text")
            prob = self.classifier.prob_classify(word_features(wordpunct_tokenize(text)))

            print "neg: %g, pos: %g" % (prob.prob("neg"), prob.prob("pos"))

            self.setMetaData(tweet, "neg", prob.prob("neg"))
            self.setMetaData(tweet, "pos", prob.prob("pos"))

            return tweet

    if __name__ == "__main__":
        db = hybrid.db.mongodb(database="hybrid_tutorial", collection="twitter")

        # Train a sentiment classifier based on movie reviews
        negids = movie_reviews.fileids('neg')
        posids = movie_reviews.fileids('pos')

        negfeats = [(word_features(movie_reviews.words(fileids=[f])), 'neg') for f in negids]
        posfeats = [(word_features(movie_reviews.words(fileids=[f])), 'pos') for f in posids]

        negcutoff = len(negfeats)*3/4
        poscutoff = len(posfeats)*3/4

        trainfeats = negfeats[:negcutoff] + posfeats[:poscutoff]
        testfeats = negfeats[negcutoff:] + posfeats[poscutoff:]

        print 'train on %d instances, test on %d instances' % (len(trainfeats), len(testfeats))
        classifier = NaiveBayesClassifier.train(trainfeats)

        worker = Sentiment(classifier, name="sentiment")
        manager = hybrid.manager.manager(workers=[worker],
                                         input_tag_list=[],
                                         output_tag_list=["tag_sentiment"],
                                         input_db=db,
                                         output_db=db)
        manager.run()

Note that this script depends on no previous phases having run (i.e., just the
"primordial data soup" of the Twitter JSON record), and produces an output
dependency of `tag_sentiment`.

`anew.py`
---------

Finally, this script analyzes the affective content of each tweet, along a few
dimensions.  NLTK is used again to perform the analysis, resulting in scores for
each tweet describing its "valence", "arousal", and "dominance".

.. code-block:: python
    :linenos:

    import hybrid
    import csv
    import nltk.classify.util
    from nltk.tokenize import wordpunct_tokenize
    from nltk.probability import FreqDist

    class Anew(hybrid.worker.worker.abstract_worker):
        def __init__(self, attributes, **kwargs):
            hybrid.worker.worker.abstract_worker.__init__(self, **kwargs)
            self.attributes = attributes

        def process_observation_core(self, tweet, **kwargs):
            text = tweet.getMetaData("text")
            fdist = FreqDist(wordpunct_tokenize(text))

            report = []
            for attr in self.attributes:
                val = reduce(lambda x, y: x + (self.attributes[attr][y] * fdist[y] if y in self.attributes[attr] else 0), fdist, 0)
                if val > 0:
                    val /= reduce(lambda x, y: x + (fdist[y] if y in self.attributes[attr] else 0), fdist, 0)

                self.setMetaData(tweet, attr, val)
                report.append("%s: %g" % (attr, val))

            print ",".join(report)
            return tweet

    if __name__ == "__main__":
        db = hybrid.db.mongodb(database="hybrid_tutorial", collection="twitter")

        attributes = {
            "valence": {r[0]: float(r[2]) for r in csv.reader(open("all.csv")) if r[0] != "Description"},
            "arousal": {r[0]: float(r[4]) for r in csv.reader(open("all.csv")) if r[0] != "Description"},
            "dominance": {r[0]: float(r[6]) for r in csv.reader(open("all.csv")) if r[0] != "Description"}
        }

        worker = Anew(attributes, name="anew")
        manager = hybrid.manager.manager(workers=[worker],
                                         input_tag_list=[],
                                         output_tag_list=["tag_anew"],
                                         input_db=db,
                                         output_db=db)
        manager.run()

As with `sentiment.py`, this script depends on no previous phases, and produces
a single output dependency, `tag_anew`.

Visualization with Tangelo and D3
=================================

The computed sentiment data can also be visualized to provide a new view on the
raw numbers.  The twitter example includes a `Tangelo
<http://tangelo.kitware.com>`_ web application that uses D3 to graph the
sentiment values as they flow into the database.

Installing Tangelo
------------------

Follow the instructions `here <https://github.com/Kitware/tangelo>`_ to
download, install, and run Tangelo.  You should now be able to visit the Tangelo
homepage at http://localhost:8080.

Setting Up the App
------------------

Create a `tangelo_html` directory in your home directory.  Then go into
`tangelo_html` and either create a symlink to the Hybrid twitter subdirectory,
or else move/copy the Hybrid directory into your `tangelo_html` directory.  On
linux, for a user named `kirk`, this might be accomplished as follows:

.. code-block:: sh

    cd ~
    mkdir tangelo_html
    cd tangelo_html
    ln -s ~/hybrid/python/twitter .

After starting the data analysis network as described above, you can visit
http://localhost:8080/~kirk/twitter to see the visualization in action.  As new
records are added to the database and processed, the visualization updates
itself, showing the status of the last 500 tweets analyzed:

.. image:: _static/twitter.png

The x-axis represents time, while the y-axis represents sentiment value, ranging
from -1 to 1.  The color of the dots redundantly encodes the sentiment, and
hovering the mouse over a dot will pop up information about the tweet
represented by it.
