"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import hybrid.manager
from hybrid.worker.text_extraction import plaintextProcessor

import optparse
import socket

# Evaluation doc limit
__doc_limit = 250

if __name__ == "__main__":
    import os

    # Handle command-line arguments
    parser = optparse.OptionParser()
    parser.add_option("--host", default="http://localhost:5984", help="Name of database host.  Default: %default")
    parser.add_option("--database", default="my_db", help="Name of database.  Default: %default")
    parser.add_option("--external-host", default="https://%h/couch",
                      help="External name of database. %h expands to the name of localhost.  Default: %default")
    parser.add_option("--ignore-comments", default=False, help="Ignore comments in html code")

    parser.add_option("--word-file", default="../../data/word_lists/english_words.txt",
                      help="Path to english_words.txt data file.  Default: %default")
    parser.add_option("--stopword-file", default="../../data/word_lists/stopwords.txt",
                      help="Path to stopwords.txt data file.  Default: %default")
    parser.add_option("--worker-threads", type="int", default=4,
                      help="Number of threads to use for processing data.  Default: %default")
    parser.add_option("--liberal-features", action="store_true", default=False,
                      help="Generate a liberal set of features.  Default: %default")
    parser.add_option("--debug", action="store_true", default=False,
                      help="Run in single thread debug mode.  Default: %default")
    parser.add_option("--static", action="store_true", default=False,
                      help="Run the extraction loop just once (for static datasets).  Default: %default")
    parser.add_option("--read-only", action="store_true", default=False,
                      help="Don't update the database Default: %default")
    parser.add_option("--web", action="store_true", default=False,
                      help="Assumes that the documents are web pages.: %default")
    parser.add_option("--text-only", action="store_true", default=False,
                      help="Exclude any meta data features.  Default: %default")
    parser.add_option("--uuid", help="Specify a uuid to reprocess.")
    parser.add_option("--log-level", type="int", default=3,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    parser.add_option("-P", default="pid_file",
                      help="This option is for tracking the pid of the process  Default: %default")
    (options, arguments) = parser.parse_args()

    # Track the pid
    f = open(options.P, "w")
    f.write(str(os.getpid()))
    f.close()

    document_dict_uri = "_design/views/_view/needs_text_feature_extraction"

    # Track the pid
    f = open(options.P, "w")
    f.write(str(os.getpid()))
    f.close()

    # Get a logger handle (singleton)
    logger = hybrid.logger.logger()
    logger.setLogLevel(options.log_level)

    # Determine external host name
    options.external_host = options.external_host.replace("%h", socket.gethostname())

    # Open connections to the databases
    data_db = hybrid.db.init("couchdb", host=options.host, database=options.database)

    #    data_db.removeLock()

    # Data for plain text extraction
    english_words = set(line.strip() for line in open(options.word_file))
    stop_words = set(line.strip() for line in open(options.stopword_file))

    plaintext_worker = plaintextProcessor(english_words=english_words,
                                          stop_words=stop_words,
                                          db=data_db,
                                          database=options.database,
                                          text_only=options.text_only,
                                          db_host=options.host,
                                          read_only=options.read_only,
                                          binary_text_fields=["contents"],
                                          name="plaintext_worker")

    key_list = ['key1', 'key2']
    key_list = None

    # Create the module
    foo_module = hybrid.manager.manager(workers=[plaintext_worker],
                                        query=document_dict_uri,  # Will be removed once we have tag to query methods
                                        output_tag_list=["tag_text_features_extracted"],
                                        debug=options.debug,
                                        keys=key_list,
                                        static=options.static,
                                        worker_threads=options.worker_threads,
                                        logger=logger,
                                        input_db=data_db,
                                        output_db=data_db,
                                        observation_limit=__doc_limit)

    # Begin processing data
    foo_module.run()
