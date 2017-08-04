===========================================
 Visualizing Hybrid Pipelines with Tangelo
===========================================

Installing Tangelo
==================

First, install `Vagrant <http://www.vagrantup.com/>`_ and `VirtualBox <https://www.virtualbox.org/>`_. Then you can install Tangelo into a virtual machine with:

.. code-block:: sh

    git clone https://github.com/Kitware/tangelo.git
    cd tangelo
    vagrant up

When this completes (will take a while),
visit `http://localhost:9000 <http://localhost:9000>`_ to see the running Tangelo instance.

Note that the Vagrant virtual machine also runs a CouchDB instance (available at `localhost:5984`)
and a MongoDB instance (available at `localhost:27017`). These may be used to run Hybrid pipelines
and will make the apps work out-of-the-box, but with minimal changes the apps can be
configured to use other database instances.

Deploying Hybrid Apps
=====================

Tangelo apps are bundled Python (server-side code), HTML, and Javascript files. To expose
an app to Tangelo, it must be present under the `build/deploy/web` directory inside
the Tangelo source. To expose all Hybrid Tangelo apps to your instance, execute:

.. code-block:: sh

    cp -rf /path/to/hybrid/tangelo_apps/* /path/to/tangelo/build/deploy/web

The following sections describe how to configure each of the apps.

Tracker App
===========

The tracker app is able to give a real-time picture of what is in the database(s)
for a Hybrid pipeline. The view consists of a set of bars, with each bar representing
one "watcher". Each watcher polls the database to count how many observations meet
specific criteria. To set up the watchers, edit the `conf.json` file in the `tracker`
directory to match your pipeline. An example JSON file is below:

.. code-block:: javascript

    [
        {
            "name": "Unprocessed Tweets",
            "type": "mongo",
            "host": "localhost:27017",
            "db": "local",
            "collection": "test",
            "requiredTags": ["text"],
            "missingTags": ["anew", "sentiment"]
        },
        {
            "name": "Sentiment",
            "type": "mongo",
            "host": "localhost:27017",
            "db": "local",
            "collection": "test",
            "requiredTags": ["text", "sentiment"],
            "missingTags": ["anew"]
        },
        {
            "name": "ANEW",
            "type": "mongo",
            "host": "localhost:27017",
            "db": "local",
            "collection": "test",
            "requiredTags": ["text", "anew"],
            "missingTags": ["sentiment"]
        },
        {
            "name": "Sentiment + ANEW",
            "type": "mongo",
            "host": "localhost:27017",
            "db": "local",
            "collection": "test",
            "requiredTags": ["text", "anew", "sentiment"],
            "missingTags": []
        },
        {
            "name": "Couch Example",
            "type": "couch",
            "host": "localhost:5984",
            "db": "local",
            "view": "_all_docs"
        }
    ]

The `name` specifies the name for the watcher that will appear in the interface, while
the `type` indicates the type of database (either `mongo` or `couch`). Both types
require a `host` and `db` field for the database, while the `mongo` type also requires
a `collection` field.

For a `mongo` database, include the tags that should and should not be in the observation
count with `requiredTags` and `missingTags`. These are used to define a MongoDB query
to count the number of observations in much the same way that the Hybrid `manager` object
queries observations.

For a `couch` database, specify the CouchDB view with the `view` tag. The watcher will
count the number of observations currently in the view.

Once it is configured, you can visit the Tracker app at
`http://localhost:9000/tracker/ <http://localhost:9000/tracker/>`_.
