"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import unittest

import data_blob
import db


class BlobTests(unittest.TestCase):
    def __init__(self, testname, **kwargs):
        super(BlobTests, self).__init__(methodName=testname)
        self._kwargs = kwargs
        self._db = None

    def setUp(self):
        if self._kwargs.get("subclass") is None:
            self._kwargs["subclass"] = "mongodb"
        try:
            self._db = db.init(**self._kwargs)
            self._db._logger.setLogLevel(0)
        except:
            self.fail("Could not connect to database.")

    def tearDown(self):
        self._db.delete()
        self._db.close()
    
    def test_blob(self):
        new_blob = {'test_data': 'some_data',
                    'binary_data': bytes('\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00')}
        self._db.store(new_blob)
        blob = data_blob.dict2blob(new_blob)
        self._db.store(data_blob.blob2dict(blob))