#!/usr/bin/python

"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
__author__ = 'mletter'

# Written with pymongo
# Documentation: http://api.mongodb.org/python/
# A python script connecting to a MongoDB given a MongoDB Connection URI. This script will
# generate data database objects to example_db until cancelled by keyboard interrupt
import copy
import csv
import random
import time
import uuid
from argparse import ArgumentParser

import hybrid
from hybrid import data_blob

parser = ArgumentParser(description='Process iris data and write to database for hybrod consumption')
parser.add_argument("--time-between-guesses", default=5, help="time between data generation.  Default: %(default)s")
parser.add_argument("--add-training_data", default=True, help="add training data.  Default: %(default)s")
parser.add_argument("--mongodb-uri", default="example_db",
                    help="uri of mongo instance.  Default: %(default)s")
parser.add_argument("--mongodb-training-uri", default="example_training_db",
                    help="uri of mongo traing set instance.  Default: %(default)s")
parser.add_argument("--training-set-size", default=8, help="mongo traing set size  Default: %(default)s")
arguments = parser.parse_args()
print arguments.time_between_guesses
print arguments.mongodb_uri
print arguments.mongodb_training_uri

dbtype = "mongodb"
TIME_BETWEEN_GUESSES = arguments.time_between_guesses
# Standard URI format: mongodb://[dbuser:dbpassword@]host:port/dbname
MONGODB_URI = arguments.mongodb_uri
MONGODB_TRAINING_URI = arguments.mongodb_training_uri
TRAINING_SIZE = arguments.training_set_size
DATA_SKELETON = {
    "data_vector": [
    ],
    "class": "",
    "submitted": True
}


def simple_generator_function(data):
    """
  generate data for the example_db
  :param data: a 2d array with data [[1,2,3,4 ....],[1,2,3,4 ....]]
  :return: a row from 2d array with on the first 4 columns
  """
    if data:
        random.shuffle(data)

    for data_row in data:
        print "\ngenerating data:"
        guess_data = [data_row[0], data_row[1], data_row[2], data_row[3]]
        iris = copy.deepcopy(DATA_SKELETON)
        iris['data_vector'] = guess_data
        iris['class'] = data_row[4]
        iris['_id'] = str(uuid.uuid4())
        # iris[0]['created'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        yield iris


def generate_db_data(data):
    """
    add data to example_db and runs until keyboard interrupt
    :param data: 2d array with data [[1,2,3,4 ....],[1,2,3,4 ....]]
    :return: not used
    """
    # pipeline database default is mongodb://localhost:27017/example_db
    db = hybrid.db.init(dbtype, database=MONGODB_URI, view_files=[], push_views=False, create=True,
                        delete_existing=True)

    # training database for classifier default is mongodb://localhost:27017/example_training_db
    train_db = hybrid.db.init(dbtype, database=MONGODB_TRAINING_URI, view_files=[], push_views=False, create=True,
                              delete_existing=True)

    # start inserting data
    try:
        if arguments.add_training_data:
            train = simple_generator_function(data)
            for _ in range(TRAINING_SIZE):
                train_data = train.next()
                print "%s\n inserting training data into \t::::: example_training_db :::::" % train_data
                db.storeDataBlobArray([data_blob.dict2blob(train_data)])

        for iris in simple_generator_function(data):
            print "%s\n inserting data into \t::::: example_db :::::" % iris
            train_db.storeDataBlobArray([data_blob.dict2blob(iris)])
            time.sleep(TIME_BETWEEN_GUESSES)

    except (KeyboardInterrupt, SystemExit):
        print "\n"
        print "closing db connection to example_db and example_training_db"
        db.close()
        train_db.close()
        print "closed"
    except:
        raise


# open iris data and populate it to a 2d array
data = list()
with open('iris.csv', 'rb') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        data_row = [row['Sepal Length'], row['Sepal Width'], row['Petal Length'], row['Petal Width'], row['Species']]
        data.append(data_row)
generate_db_data(data)
