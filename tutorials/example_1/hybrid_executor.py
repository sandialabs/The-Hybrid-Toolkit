'''
Created on May 2, 2013

@author: wldavis
'''
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import logging, logging.handlers
import logging.config
import optparse
from hybrid import utils
import time

from hybrid import model
import traceback

logger = None


def setup_logging():
    global logger
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)
    socketHandler = logging.handlers.SocketHandler('localhost',
                                                   logging.handlers.DEFAULT_TCP_LOGGING_PORT)

    # don't bother with a formatter, since a socket handler sends the event as
    # an unformatted pickle
    logger.addHandler(socketHandler)


if __name__ == "__main__":

    # Handle command-line arguments
    parser = optparse.OptionParser()
    parser.add_option("--cfg", default="", help="Name of the config file.  Default: %default")
    parser.add_option("--load-only", action="store_true", default=False, help="Don't run, just load objects.: %default")
    parser.add_option("--manager", default=None, help="Name of a specific manager to run.  Default: %default")
    parser.add_option("--verbose", action="store_true", default=False, help="Print out the loaded classes.: %default")

    (options, arguments) = parser.parse_args()

    setup_logging()

    config_filename = options.cfg
    manager_name = options.manager
    json_definitions, classes, aliases = utils.loadJSON(config_filename)

    for temp_class in classes:
        if isinstance(temp_class, model.model):
            print(temp_class)
            print "STORING     ", temp_class
            temp_class.storeToDB()
        else:
            print "NOT STORING ", temp_class

    print "Done loading JSON"

    managers = []
    for i in range(0, len(json_definitions)):
        json_defintion = json_definitions[i]
        print "json_defintion[_jsontype]", json_defintion["_jsontype"]
        if json_defintion["_jsontype"] == "hybrid.manager.manager":
            managers.append(classes[i])

    if options.verbose:
        for class_instance in classes:
            logger.debug('class_instance=' + str(type(class_instance)))

    if options.verbose:
        for json_def in json_definitions:
            logger.debug('json_def=' + str(json_def))

    if options.load_only:
        exit(1)

    while (True):
        try:
            logger.debug('Beginning managers-- run threads in the future')
            if not (manager_name == None):
                my_manager = aliases[manager_name]
                my_manager.run()
            else:
                managers[0].run()
            break
        except Exception, e:
            print "Problem accessing the database..", e
            logger.exception("Problem accessing the database..")
            traceback.print_exc()
            time.sleep(30)
