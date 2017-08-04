"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import unittest

import hybrid.data_blob as data_blob
import hybrid.db as db


class SimpleDBTests(unittest.TestCase):
    def __init__(self, testname, **kwargs):
        print "kwargs", kwargs

        kwargs = {"subclass": "couchdb", "host": "http://localhost:5984/", "database": "automated_tests",
                  "create": True, "delete_existing": True}

        super(SimpleDBTests, self).__init__(methodName=testname)
        self._kwargs = kwargs
        self._db = None

    def setUp(self):
        if self._kwargs.get("subclass", None) is None:
            self._kwargs["subclass"] = "couchdb"
        try:
            self._db = db.init(**self._kwargs)
            self._db._logger.setLogLevel(0)
        except:
            self.fail("Could not connect to database.")
        self.cleanDatabase()

    def tearDown(self):
        self.cleanDatabase()
        # self._db.close()

    def getDataBlobs(self):
        blob_list = [self.getDataBlob()]

        blob = data_blob.create(uuid=2)
        blob.setMetaData("name", "Jane")
        blob.setMetaData("gender", "female")
        blob.setMetaData("age", 20)
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

    '''
    def test_interface(self):
        for method,value in inspect.getmembers(db.abstract_db(), predicate=inspect.ismethod):
            self.assertTrue(method in dir(self._db))

    def test_open_connection(self):
        self.assertTrue(self._db.open())
        self._db.close()

    def test_delete_database(self):
        try:
            self._db.delete()
        except:
            self.fail("Could not delete database.")

    def test_copy_database(self):
        self.test_store_array_blob()
        try:
            db2 = db.init(subclass='mongodb', database='local2', collection="test_copy_database", delete_existing=True)
        except:
            self.fail("Could not create second database.")
        self._db.storeDataBlobArray(data_blob_array=self.getDataBlobs())
        self._db.copy_database(db2)
        db1list = [item.get("_dataBlobID", random.randint(1,100)) for item in self._db.loadView(name="test").rows()]
        db2list = [item.get("_dataBlobID", random.randint(1,100)) for item in db2.loadView(name="test_copy_database").rows()]
        self.assertEqual(db1list, db2list)
        db2.delete()

    def test_locking(self):
        import pdb; pdb.set_trace()
        db2 = db.init(**self._kwargs)
        lockInfo = db2.getLockInfo("my_random_lock_attrib")
        self.assertFalse(self._db.isLocked())
        self._db.setLock(lockInfo, "my_random_lock_attrib")
        self.assertTrue(db2.isLocked())
        self._db.removeLock(lockInfo, "my_random_lock_attrib")
        self.assertFalse(db2.isLocked(""))
        #db2.close()

    def test_locking2(self):
        import pdb; pdb.set_trace()
        db2 = db.init(**self._kwargs)
        lockInfo = db2.getLockInfo("my_random_lock_attrib")
        self.assertFalse(self._db.isLocked())
        self._db.setLock(lockInfo, "my_random_lock_attrib")
        self.assertTrue(db2.isLocked())
        self._db.removeLock(lockInfo, "my_random_lock_attrib")
        self.assertFalse(db2.isLocked(""))
        #db2.close()


    def test_store_array_blob(self):
        self.cleanDatabase()
        blob_list = self.getDataBlobs()
        self._db.storeDataBlobArray(data_blob_array=blob_list)
        for blob in blob_list:
            try:
                stored_blob = self._db.loadDataBlob(uuid=blob.getDataBlobUUID())
            except:
                self.fail("Could not retrieve data blob from database.")
            self.assertDictContainsSubset(data_blob.blob2dict(blob),
                            data_blob.blob2dict(stored_blob))

    def test_create_read_blob(self):
        self.cleanDatabase()
        blob = self.getDataBlob()
        self._db.storeDataBlob(blob)
        try:
            stored_blob = self._db.loadDataBlob(uuid=blob.getDataBlobUUID())
        except:
            self.fail("Could not retrieve data blob from database.")
        self.assertDictContainsSubset(blob.getMetaDataDict(), stored_blob.getMetaDataDict())

    def test_delete_blob(self):
        self.cleanDatabase()
        blob = self.getDataBlob()
        self._db.storeDataBlob(blob)
        try:
            stored_blob = self._db.loadDataBlob(uuid=blob.getDataBlobUUID())
        except:
            self.fail("Could not retrieve data blob from database.")
        self.assertDictContainsSubset(blob.getMetaDataDict(), stored_blob.getMetaDataDict())
        self._db.deleteDataBlob(uuid=blob.getDataBlobUUID())
        self.assertRaises(exceptions.Exception, self._db.loadDataBlob, blob.getDataBlobUUID())

    def test_update_blob(self):
        self.cleanDatabase()
        blob = self.getDataBlob()
        self._db.storeDataBlob(blob)
        try:
            stored_blob = self._db.loadDataBlob(uuid=blob.getDataBlobUUID())
        except:
            self.fail("Could not retrieve data blob from database.")
        self.assertDictContainsSubset(blob.getMetaDataDict(),
                                      stored_blob.getMetaDataDict())
        rev = stored_blob.getDataBlobRevision()
        blob.setMetaData("new_field","new_value")
        self._db.storeDataBlob(blob)
        try:
            stored_blob = self._db.loadDataBlob(uuid=blob.getDataBlobUUID())
        except:
            self.fail("Could not retrieve data blob from database.")
        self.assertDictContainsSubset(self.getDataBlob().getMetaDataDict(),
                                      stored_blob.getMetaDataDict())
        #self.assertTrue(int(rev)+1==int(stored_blob.getDataBlobRevision()))

    def test_binary_blob(self):
        self.cleanDatabase()
        blob = self.getBinaryDataBlob()
        self._db.storeDataBlob(blob)
        try:
            stored_blob = self._db.loadDataBlob(uuid=blob.getDataBlobUUID(), include_binary=True)
        except:
            self.fail("Could not retrieve data blob from database.")
        self.assertDictContainsSubset(blob.getMetaDataDict(), stored_blob.getMetaDataDict())
        for k in blob.getBinaryDataDict().iterkeys():
            self.assertEqual(blob.getBinaryData(k),stored_blob.getBinaryData(k))

    def test_view(self):
        self.cleanDatabase()
        blob_list = self.getDataBlobs()
        try:
            db2 = db.init(subclass='mongodb', database='local', collection="test_view", delete_existing=True)
        except:
            self.fail("Could not create second database.")
        db2.storeDataBlobArray(data_blob_array=blob_list)
        self._db.storeDataBlobArray(data_blob_array=blob_list)
        self.assertEqual(self._db.loadView(name="test").rows(), db2.loadView(name="test_view").rows())
        db2.delete()

    def test_query(self):
        self.cleanDatabase()
        blob_list = self.getDataBlobs()
        try:
            db2 = db.init(subclass='mongodb', database='local',
                          collection="test_query", delete_existing=True)
        except:
            self.fail("Could not create second database.")
        db2.storeDataBlobArray(data_blob_array=blob_list)
        self._db.storeDataBlobArray(data_blob_array=blob_list)
        self.assertEqual(self._db.query().rows(),
                         db2.query(statement={"_dataBlobID" : {"$exists": True}}).rows())
        db2.delete()

    def test_gridfs(self):
        self.cleanDatabase()
        try:
            db2 = db.init(subclass='mongodb', database='local',
                          collection="test_gridfs", use_gridfs=True,
                          delete_existing=True)
        except Exception:
            self.fail("Could not create second database.")
        N = 1048576
        large_doc = {}#{"_dataBlobID" : "507f191e810c19729de860ea"}
        for i in xrange(3):
            bits = bytes("\x00"+os.urandom(8)*N+"\x00")
            large_doc[str(i)] = base64.b64encode(bits)
        regular_mongo_failure = False
        # TODO: figure out how to catch exceptions thrown by db
        try:
            self._db.store(large_doc)
        except Exception:
            regular_mongo_failure = True
        self.assertEqual(regular_mongo_failure, True)
        uuid = db2.storeDataBlob(data_blob.dict2blob(large_doc))
        db2.storeDataBlob(self.getDataBlob())
        db2.storeDataBlob(self.getDataBlob())
        uuid = db2.store(large_doc)
        self.assertDictContainsSubset(large_doc,
                          data_blob.blob2dict(db2.loadDataBlob(uuid)))
        '''


if __name__ == '__main__':
    unittest.main()
