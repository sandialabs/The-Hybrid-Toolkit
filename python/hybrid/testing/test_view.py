"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import unittest

import hybrid.data_blob as data_blob
import hybrid.db as db
import hybrid.view as view


class ViewTests(unittest.TestCase):
    def __init__(self, testname, **kwargs):
        super(ViewTests, self).__init__(methodName=testname)
        self._kwargs = kwargs
        self._db = None

    def setUp(self):
        if self._kwargs.get("subclass", None) is None:
            self._kwargs["subclass"] = "mongodb"
        try:
            self._db = db.init(**self._kwargs)
            self._db._logger.setLogLevel(0)
        except:
            self.fail("Could not connect to database.")
        self.cleanDatabase()

    def tearDown(self):
        self.cleanDatabase()
        self._db.close()

    def getDataBlobs(self):
        blob_list = [self.getDataBlob()]

        blob = data_blob.create(uuid=2)
        blob.setMetaData("name", "Jane")
        blob.setMetaData("gender", "female")
        blob.setMetaData("age", 20)
        blob.setMetaData("occupation", "nurse")
        blob.setMetaData("field1", "value1")
        blob.setMetaData("field2", "value4")
        blob.setMetaData("field3", "value5")
        blob_list.append(blob)

        blob = data_blob.create(uuid=3)
        blob.setMetaData("name", "Jack")
        blob.setMetaData("gender", "male")
        blob.setMetaData("age", 53)
        blob.setMetaData("field1", "value1")
        blob.setMetaData("field2", "value2")
        blob.setMetaData("field3", "value5")
        blob_list.append(blob)

        blob = data_blob.create(uuid=4)
        blob.setMetaData("name", "Wendy")
        blob.setMetaData("gender", "female")
        blob.setMetaData("field1", "value1")
        blob.setMetaData("field2", "value2")
        blob.setMetaData("field3", "value3")
        blob_list.append(blob)
        return blob_list

    def getDataBlob(self):
        blob = data_blob.create(uuid=1)
        blob.setMetaData("name", "Jim")
        blob.setMetaData("gender", "male")
        blob.setMetaData("age", 10)
        blob.setMetaData("field1", "value1")
        blob.setMetaData("field2", "value2")
        blob.setMetaData("field3", "value3")
        return blob

    def getBinaryDataBlob(self):
        blob = data_blob.create(uuid=1234)
        blob.setMetaData("field1", "value1")
        blob.setMetaData("field2", "value2")
        blob.setMetaData("field3", "value3")
        blob.setBinaryData(field="some_binary_data", mime_type="bytes",
                           data=bytes('This will represent some binary data.'))
        return blob

    def cleanDatabase(self):
        self._db.delete()

    def test_create_view(self):
        self._db.storeDataBlobArray(self.getDataBlobs())
        new_view = view.create_view(database=self._db,
                                    query_name="test",
                                    input_tags=["age"],
                                    output_tags=["some_tag"])
        documents = []
        for doc in self._db._raw_db.test.find({"age": {"$exists": True}}):
            documents.append(doc)
        self.assertEqual(documents, [data_blob.blob2dict(new_doc) for new_doc in
                                     new_view.rows()])

    def test_iterator(self):
        self._db.storeDataBlobArray(self.getDataBlobs())
        new_view = view.create_view(database=self._db,
                                    query_name="test",
                                    input_tags=["field1"],
                                    output_tags=["some_tag"])
        documents = []
        for doc in new_view.iterdocs():
            documents.append(doc)
        self.assertEqual([data_blob.blob2dict(new_doc) for new_doc in
                          new_view.rows()], documents)


if __name__ == '__main__':
    unittest.main()
