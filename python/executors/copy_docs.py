"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import ConfigParser
import optparse
import time

import hybrid

try:
    import fcntl
except:
    pass

# Evaluation doc limit
__doc_limit = 100
__minimal = ["_dataBlobID", "_rev", "_attachments", "DATETIME", "import_hostname", "filename", "label"]


class copy_doc(hybrid.worker.worker.abstract_worker):
    def reset_blob(self, blob, fields):
        for key in doc.getMetaDataDict().keys():
            if key not in fields:
                doc.deleteMetaData(key)

    def batch_init(self, batch_context, **kwargs):

        targetdb = None

        if batch_context != None:

            target_db_type = self._target_db_type
            target_db_name = self._target_db_name
            target_db_host = self._target_db_host

            if batch_context.get("targetdb", None) == None:
                targetdb = hybrid.db.init(target_db_type, host=target_db_host, database=target_db_name,
                                          push_views=False, create=True)
                batch_context["targetdb"] = targetdb

        self._batch_context = batch_context

    def __init__(self, **kwargs):

        hybrid.worker.worker.abstract_worker.__init__(self, **kwargs)
        self._target_db_type = kwargs.get("target_db_type", None)
        self._target_db_name = kwargs.get("target_db_name", None)
        self._target_db_host = kwargs.get("target_db_host", None)
        self._fields = kwargs.get("fields", [])
        self._reset = kwargs.get("reset", False)

    def process_observations_core(self, observations, **kwargs):

        targetdb = None
        batch_context = self._batch_context
        reset = self._reset
        fields = self._fields

        if batch_context:
            targetdb = batch_context.get("targetdb", None)

        for observation in observations:

            # Grab the whole document
            doc_id = observation.getMetaData("_dataBlobID")

            # Skip any design documents
            if doc_id.startswith(u"_design"):
                continue

            if options.reset:
                observation2 = deepCopy(observation)
                reset_blob(observation2, fields)
            else:
                observation2 = observation

            targetdb.storeDataBlob(observation2)

        return observations


# Idea was from here http://blog.vmfarms.com/2011/03/cross-process-locking-and.html
if __name__ == "__main__":

    # Handle command-line arguments
    parser = optparse.OptionParser()
    parser.add_option("--cfg", default="", help="Name of the config file.  Default: %default")
    parser.add_option("--source-db-host", default=None, help="Name of source database host.  Default: %default")
    parser.add_option("--source-db-name", default=None, help="Name of source database.  Default: %default")
    parser.add_option("--source-db-type", default=None, help="Name of source database.  Default: %default")
    parser.add_option("--target-db-host", default=None, help="Name of target database host.  Default: %default")
    parser.add_option("--target-db-name", default=None, help="Name of target database.  Default: %default")
    parser.add_option("--target-db-type", default=None, help="Name of target database host.  Default: %default")
    parser.add_option("--target-uuid", default=None, help="Name of target uuid.  Default: %default")
    parser.add_option("--external-host", default="https://%h/couch",
                      help="External name of database. %h expands to the name of localhost.  Default: %default")
    parser.add_option("--worker-threads", type="int", default=5, help="Number of worker threads.  Default: %default")
    parser.add_option("--start-key", default=None, help="Provide the start key for a view.  Default: %default")
    parser.add_option("--log-level", type="int", default=3,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    parser.add_option("--query-name", default=None, help="Name of the input query.  Default: %default")
    parser.add_option("--reset", default=False, action="store_true", help="Reset doc to default fields: default")
    parser.add_option("--fields", default='', help="fields to keep")

    (options, arguments) = parser.parse_args()

    CONFIG_FILENAME = options.cfg
    if not (CONFIG_FILENAME == ""):
        config = ConfigParser.ConfigParser()
        config.read(CONFIG_FILENAME)

        if config.has_option("Databases", "SourceDBName"):
            options.source_db_name = config.get("Databases", "SourceDBName")
        if config.has_option("Databases", "SourceDBHost"):
            options.source_db_host = config.get("Databases", "SourceDBHost")
        if config.has_option("Databases", "SourceDBType"):
            options.source_db_type = config.get("Databases", "SourceDBType")
        if config.has_option("Databases", "TargetDBName"):
            options.target_db_name = config.get("Databases", "TargetDBName")
        if config.has_option("Databases", "TargetDBHost"):
            options.target_db_host = config.get("Databases", "TargetDBHost")
        if config.has_option("Databases", "TargetDBType"):
            options.target_db_type = config.get("Databases", "TargetDBType")

        if config.has_option("General", "LogLevel"):
            options.log_level = config.get("General", "LogLevel")
        if config.has_option("General", "UUIDs"):
            options.uuids = config.get("General", "UUIDs")
        if config.has_option("General", "Static"):
            options.static = config.get("General", "Static")

        if config.has_option("Parameters", "UUIDs"):
            options.uuids = config.get("Parameters", "UUIDs")
        if config.has_option("Parameters", "QueryName"):
            options.query_name = config.get("Parameters", "QueryName")
        if config.has_option("Parameters", "StartKey"):
            options.start_key = config.get("Parameters", "StartKey")

    if options.query_name is None:
        query_name = ""
    else:
        query_name = options.query_name

    options.fields = options.fields.split(',') + __minimal

    if not options.source_db_type:
        options.source_db_type = "couchdb"

    if not options.target_db_type:
        options.target_db_type = "couchdb"

    # Get a logger handle (singleton)
    logger = hybrid.logger.logger()
    logger.setLogLevel(options.log_level)

    start_key = options.start_key

    while True:
        data_db = None
        try:
            print "Beginning copy_doc setup"
            worker = copy_doc(name="copy_doc", target_db_type=options.target_db_type,
                              target_db_host=options.target_db_host, target_db_name=options.target_db_name,
                              fields=options.fields, reset=options.reset)

            print "Worker established"
            data_db = hybrid.db.init(options.source_db_type, host=options.source_db_host,
                                     database=options.source_db_name)
            print "datadb established"

            # Create the module
            # Open connections to the databases
            print "Beginning manager"
            if start_key:
                manager = hybrid.manager.manager(workers=[worker],
                                                 query=query_name,
                                                 input_tag_list=["tag_copy1"],
                                                 output_tag_list=["tag_copy1"],
                                                 input_db=data_db,
                                                 output_db=data_db,
                                                 worker_threads=int(options.worker_threads),
                                                 observation_limit=__doc_limit,
                                                 start_key=start_key)
            else:
                manager = hybrid.manager.manager(workers=[worker],
                                                 query=query_name,
                                                 input_tag_list=["tag_copy1"],
                                                 output_tag_list=["tag_copy1"],
                                                 input_db=data_db,
                                                 output_db=data_db,
                                                 worker_threads=int(options.worker_threads),
                                                 observation_limit=__doc_limit)

            # Begin processing data
            manager.run()
            break
        # atexit.register(mlp_module.cleanup())
        except Exception, e:
            print "Exception e..", str(e)
            print "Problem accessing the database..", str(data_db)
            time.sleep(30)

    manager.run()
