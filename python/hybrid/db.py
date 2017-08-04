"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
"""
Database interaction methods.
"""
import base64
import copy

try:
    import couchdb as couchdb_interface
except:
    pass
import data_blob
import json
import mimetypes
import os
import sys
import time
import utils
import marshal
import random
from hybrid import encoding
import logging

try:
    import bson
    import gridfs
    import pymongo
except:
    pass

logger = logging.getLogger(__name__)


def init(subclass, **kwargs):
    """
    Create a db object of a particular subclass.

    :param subclass: db subclass type (string)
        eg: 'mongodb','couchdb', None
    :param kwargs: dict parameters to pass to __init__ method (key=value args)
    :return: database obj
    """

    if subclass == 'couchdb':
        return couchdb(**kwargs)
    elif subclass == 'mongodb':
        return mongodb(**kwargs)
    elif subclass == None:
        return None
    else:
        raise TypeError("Cannot create db subclass: %s" % (subclass))


# function that will hash multiple data types (e.g., list, dictionary)
def make_hash(o):
    if isinstance(o, set) or isinstance(o, tuple) or isinstance(o, list):
        return tuple([make_hash(e) for e in o])
    elif not isinstance(o, dict):
        return hash(o)

    new_o = copy.deepcopy(o)
    for k, v in new_o.items():
        new_o[k] = make_hash(v)
    return hash(tuple(frozenset(new_o.items())))


# function to hash the values in a dictionary
def make_hash_dict(o):
    return hash(tuple([v for k, v in o.iteritems()]))


class abstract_db():
    """
    Abstract interface for a database object.
    """

    def storeDataBlob(self, data_blob, **kwargs):
        """
        Saves a data blob to the database. Returns the ID of the object in the database if saved; otherwise None.
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def storeDataBlobArray(self, data_blob_array, **kwargs):
        """
        Saves an array of data blobs to the database.
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def loadDataBlob(self, name, **kwargs):
        """Gets a data blob from the database. Returns a reference to the data blob if loaded; otherwise None.
         Input:
            name:   unique name for the data blob (string)
            kwargs: optional parameters like subclass, (key=value args)
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def deleteDataBlob(self, uuid):
        """Delete the data blob from the database.
            Input:
               uuid:   unique id (unique name) for the data blob (string)
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def loadView(self, name, **kwargs):
        """Loads a view from the database. Returns a reference to the view object if loaded; otherwise None.
         Input:
            name:   unique name for the view (string)
            kwargs: parameters to pass to view's __init__ method (key=value args)
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def delete(self, **kwargs):
        """Delete the database.
         Input:
            kwargs: parameters to pass to the database delete function (key=value args)
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def getInfo(self):
        """
        Get a string of information the database. Specific DBs fill in this method with whatever.
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def getNumDocuments(self):
        """
        Find out how many documents are in database, efficiently
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def setLock(self):
        """
        Lock the database
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def isLocked(self):
        """
        Return True/False based on database locked state
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def removeLock(self):
        """
        Remove the lock on the database
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def waitOnLock(self):
        """
        Waits on a locked database
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def compact(self):
        """
        Runs compaction on the database
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def query(self):
        """
        Ad-hoc query for the database
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def update_doc(self):
        """
        Ad-hoc update handlers database
        """
        raise NotImplementedError("This method is part of a pure virtual class.")

    def createQueryInfo(self):
        query_info = {"db_type": self._type, "db_host": self._host, "db_name": self._database, "query_name": "_all",
                      "query_find_list": [], "query_except_list": []}

        return query_info

    class view():
        """
        Abstract interface for a database view object.
        """

        def rows(self):
            """
            Get the results of the view as a python list
            """
            raise NotImplementedError("This method is part of a pure virtual class.")

        def getInfo(self):
            """
            Get the results of the view as a python list
            """
            raise NotImplementedError("This method is part of a pure virtual class.")

        def num_rows(self):
            """
            Get the number of rows contained in the results of the view
            """
            raise NotImplementedError("This method is part of a pure virtual class.")

    class view_pager():
        """
        Abstract interface for a database view paging object.
        """

        def rows(self):
            """
            Get the results of the view as a ^generated^ python list (using yield)
            """
            raise NotImplementedError("This method is part of a pure virtual class.")


# class dblock():
#     DEFAULT_HOST = 'http://localhost:5984'
#     DEFAULT_DATABASE = 'my_couchdb'
#     DEFAULT_USERNAME = 'default_username'
#     DEFAULT_PASSWORD = 'default_wrong_password'
# 
#     def __init__(self, **kwargs):
#         ''' Create instance of database lock
#             Input:
#                 kwargs:   parameters for opening a database connection (key=value args)
#                     host:     URL for CouchDB host
#                     database: name of database
#         '''
#         self._type = 'couchdb'
#         self._server = None
#         self._raw_db = None
#         self._kwargs = kwargs

class couchdb(abstract_db):
    """
    CouchDB database
    """

    # Default host and database names
    DEFAULT_HOST = 'http://localhost:5984'
    DEFAULT_DATABASE = 'my_couchdb'
    DEFAULT_USERNAME = 'default_username'
    DEFAULT_PASSWORD = 'default_wrong_password'

    def __init__(self, **kwargs):
        """
        Create instance of couchDB database
            Input:
                kwargs:   parameters for opening a database connection (key=value args)
                    host:     URL for CouchDB host
                    database: name of database
        """
        self._type = 'couchdb'
        self._server = None
        self._raw_db = None
        self._kwargs = kwargs
        self._host = kwargs.get('host', self.DEFAULT_HOST)
        self._database = kwargs.get('database', self.DEFAULT_DATABASE)
        self._username = kwargs.get('username', self.DEFAULT_USERNAME)
        self._password = kwargs.get('password', self.DEFAULT_PASSWORD)
        self._push_views = kwargs.get('push_views', False)
        self._multiple_view_docs = kwargs.get('multiple_view_docs', False)
        self._view_files = kwargs.get('view_files', [])
        self._redirect_view_dirs = kwargs.get('redirect_view_dirs', [])
        self._staging_suffix = kwargs.get('staging_suffix', '')

        print "Done setting first params"
        log_level = kwargs.get('log_level', None)
        if log_level is None:
            log_level = 2

        if (self._view_files == None):
            self._view_files = []
        if (self._redirect_view_dirs == None):
            self._redirect_view_dirs = []

        self.open()
        print "Done opening"

    def __str__(self):
        return self.getInfo()

    @staticmethod
    def loadFromJSON(json_data):
        jsontype = json_data["_jsontype"]
        if not (jsontype == "hybrid.db.couchdb"):
            return None

        return couchdb(**json_data)

    def addViewFile(self, view_file):
        self._view_files.append(view_file)

    @property
    def view_files(self):
        return self._view_files

    @view_files.setter
    def view_files(self, view_files):
        if view_files is None:
            self._view_files = []
        else:
            self._view_files = view_files

    @view_files.deleter
    def view_files(self):
        self._view_files = []

    ################ \/ Deprecated as of 3/26/2016 \/ ################
    def setViewFiles(self, view_files):
        logger.warning("This accessor is deprecated please remove it from your code use; .view_files = x")
        if (view_files == None):
            self._view_files = []
        else:
            self._view_files = view_files

    def getViewFiles(self):
        logger.warning("This accessor is deprecated please remove it from your code use; .view_files")
        return self._view_files

    def clearViewFiles(self):
        logger.warning("This accessor is deprecated please remove it from your code use; del .view_files")
        self._view_files = []

    ################ ^Deprecated as of 3/26/2016^ ################

    def addRedirectViewDirectory(self, redirect_view_directory):
        self._redirect_view_dirs.append(redirect_view_directory)

    @property
    def redirect_view_directories(self):
        return self._redirect_view_dirs

    @redirect_view_directories.setter
    def redirect_view_directories(self, redirect_view_directories):
        if (redirect_view_directories == None):
            self._redirect_view_dirs = []
        else:
            self._redirect_view_dirs = redirect_view_directories

    @redirect_view_directories.deleter
    def redirect_view_directories(self):
        self._redirect_view_dirs = []

    ################ \/ Deprecated as of 3/26/2016 \/ ################
    def setRedirectViewDirectories(self, redirect_view_directories):
        logger.warning(
            "This accessor is deprecated please remove it from your code use; .redirect_view_directories = x")
        if (redirect_view_directories == None):
            self._redirect_view_dirs = []
        else:
            self._redirect_view_dirs = redirect_view_directories

    def getRedirectViewDirectories(self):
        logger.warning("This accessor is deprecated please remove it from your code use; .redirect_view_directories")
        return self._redirect_view_dirs

    def clearRedirectViewDirectories(self):
        logger.warning(
            "This accessor is deprecated please remove it from your code use; del .redirect_view_directories")
        self._redirect_view_dirs = []

    ################ ^Deprecated as of 3/26/2016^ ################

    @property
    def info(self):
        """Get a string of information the database. """
        return self._type + ": " + str(self._kwargs)

    @property
    def type(self):
        """Get the type of the database. """
        return self._type

    @property
    def host(self):
        """Get the host of the database. """
        return self._host

    @property
    def db_name(self):
        """Get the database name of the database. """
        return self._database

    @property
    def raw_server_handle(self):
        """Get a handle to the raw couchdb server. """
        # TODO: determine if we want to add this property based on known deprecation
        logger.warning("This accessor is deprecated please remove it from your code")
        return self._server

    @property
    def raw_db_handle(self):
        # TODO: determine if we want to add this property based on known deprecation
        """Get a handle to the raw couchdb database. """
        logger.warning("This accessor is deprecated please remove it from your code")
        return self._raw_db

    ################ \/ Deprecated as of 3/26/2016 \/ ################

    def getInfo(self):
        """Get a string of information the database. """
        logger.warning("This accessor is deprecated please remove it from your code use .info")
        return self._type + ": " + str(self._kwargs)

    def getType(self):
        """Get the type of the database. """
        logger.warning("This accessor is deprecated please remove it from your code use .type")
        return self._type

    def getHost(self):
        """Get the host of the database. """
        logger.warning("This accessor is deprecated please remove it from your code use .host")
        return self._host

    def getDBName(self):
        """Get the database name of the database. """
        logger.warning("This accessor is deprecated please remove it from your code use .bd_name")
        return self._database

    def getRawServerHandle(self):
        """Get a handle to the raw couchdb server. """
        logger.warning("This accessor is deprecated please remove it from your code")
        return self._server

    def getRawDBHandle(self):
        """Get a handle to the raw couchdb database. """
        logger.warning("This accessor is deprecated please remove it from your code")
        return self._raw_db

    ################ ^Deprecated as of 3/26/2016^ ################


    def copy_database(self, target_db):
        """
        Simply copy all the documents from this database to the target.
         Input:
            kwargs: parameters to pass to the replication (key=value args)
        """
        # Pull a view from this database with all the documents
        logger.info("Copying _all_docs from:", self._host + ":", self._database, "to",
                    target_db.getHost() + ":" + target_db.getDBName())
        view_uri = "_all_docs"
        view = self.loadView(view_uri, paging=True)
        if view is None:
            logger.error("Could not open view on:" + view_uri)
            sys.exit()

        # Simply loop over the documents in the source database
        # and push to the target database
        for row in view.rows():
            # Grab the whole document
            doc = self.loadDataBlob(row.id, include_binary=True)

            # Set target database
            doc.setDB(target_db)

            # Store to target
            doc.store(delete_existing=True)

    def delete(self, **kwargs):
        """
        Delete the database.
         Input:
            kwargs: parameters to pass to the database delete function (key=value args)
        """
        # Delete the couch database
        if (self._database in self._server):
            self._server.delete(self._database)

    def open(self):
        logger.debug("Opening database for task on process", os.getpid())
        '''Opens connection to self._server and self._database.'''
        # CouchDB handles  (for remote use couchdb.Server('http://foo.bar:5984/'))
        self._server = couchdb_interface.Server(self._host)

        if self._username == 'default_username':
            if not self._server:
                logger.error("Could not connect to couch server: %s" % (self._host))
                raise RuntimeError("Could not connect to couch server: %s" % (self._host))
        else:
            self._server.resource.credentials = (self._username, self._password)
            if self._database not in self._server:
                logger.error("Could not connect to couch database:" + str(self._database))
                raise RuntimeError("Could not connect to couch database: %s" % self._database)

        # Do they want to delete the existing one before creating a new one
        if self._kwargs.get('delete_existing', False):
            self.delete()
            self._server.create(self._database)  # Create it
        # Do they want to create the database if it doesn't currently exist
        elif self._kwargs.get('create', False):
            if self._database not in self._server:
                self._server.create(self._database)  # Create it

        # Does the database exist
        if self._database not in self._server:
            logger.error(
                "Could not connect to couch database:" + str(self._database) + " with args -- " + str(self._kwargs))
            raise RuntimeError("Could not connect to couch database: %s" % (self._database))

        # Open the database
        self._raw_db = self._server[self._database]

        if self._push_views:
            self.push_all_views(multiple=self._multiple_view_docs)

        return True

    def update_doc(self, name, docid=None, **kwargs):
        val = self._raw_db.update_doc(name, docid=docid)
        return val

    def create_native_doc(self, data_blob, **kwargs):

        # Validate the data blob
        data_blob.validate()

        # Every data blob must have certain fields enforced by the data blob super class
        uuid = data_blob.getDataBlobUUID()
        rev = data_blob.getDataBlobRevision()
        data_blob_type = data_blob.getDataBlobType()
        date_string = data_blob.getCreationDateString()

        # Create the couch doc and add a few special fields
        couch_doc = {"type": data_blob_type}

        creation_datetime = data_blob.getCreationDate()

        datetime_dict = utils.getDateTimeDict(creation_datetime)

        couch_doc["creation_datetime"] = datetime_dict

        couch_doc["created"] = date_string

        # Check for uuid not set (could happen when the object is first created)
        if uuid is not None:
            couch_doc["_dataBlobID"] = uuid
            if rev != "0":
                couch_doc["_rev"] = rev

        # Now put all the meta data into couch
        for k, v in data_blob.getMetaDataDict().iteritems():
            #             couch_doc[k] = json.loads(v)
            if type(v) == unicode:
                couch_doc[k] = encoding.convertToAscii(v)
            else:
                couch_doc[k] = v
                #             if type(v)==type({}):
                #                 print "CONVERTING DICT"
                #                 print v
                #                 couch_doc[k] = json.load(v)
                #             else:
                #                 print "Not converting -- type =",(type(v))
                #                 couch_doc[k] = v

        ignore_conflict = kwargs.get('ignore_conflict', False)

        # Any binary fields will be added as inline attachments
        attachment_dict = {}

        for k, v in data_blob.getBinaryDataDict().iteritems():
            attachment_dict[k] = {"content_type": v["mime_type"], "data": base64.b64encode(v["data"])}

        if attachment_dict != {}:
            couch_doc["_attachments"] = attachment_dict

        return couch_doc

    def storeDataBlob(self, data_blob, lockInfo=None, **kwargs):
        uuid = data_blob.getDataBlobUUID()
        if lockInfo is None:
            lockInfo = self.getLockInfo(uuid)

        if data_blob is None:
            return False
        '''Store the data blob to the database using any optional parameters'''

        # All DB operations check for locks

        self.waitOnLock(lockInfo, uuid)

        couch_doc = self.create_native_doc(data_blob, **kwargs)

        ignore_conflict = kwargs.get('ignore_conflict', False)
        try:
            # Should we delete any existing data blob
            if uuid != None and kwargs.get('delete_existing', True):
                self.deleteDataBlob(uuid, lockInfo=lockInfo)
                if "_rev" in couch_doc:
                    del couch_doc["_rev"]

            # Okay now save the whole thing
            if couch_doc.has_key("target_db_host"):
                items = couch_doc.iteritems()

            try:
                doc_info = self._raw_db.save(couch_doc, **kwargs)

                # Storing the id and rev back to the data blob
                data_blob.setDataBlobUUID(doc_info[0])
                data_blob.setDataBlobRevision(doc_info[1])
            # print "Data blob stored!"
            except Exception, e:
                logger.exception("ERROR SAVING DOCUMENT")

        except couchdb_interface.ResourceConflict:

            if ignore_conflict:
                return

            logger.error("Conflict for doc " + uuid + ' ' + str(couch_doc))
            logger.error("Pulling out big hammer and trying again")
            new_revision = self._raw_db[uuid]["_rev"]
            couch_doc["_rev"] = new_revision
            try:
                doc_info = self._raw_db.save(couch_doc, **kwargs)
                data_blob.setDataBlobRevision(doc_info[1])
            except couchdb_interface.ResourceConflict:
                logger.error("Hammer failed")
                raise
            logger.error("Hammer worked")

    def storeDataBlobArray(self, data_blob_array, lockInfo=None, **kwargs):
        """Stores an array of data blobs to the database."""
        if lockInfo is None:
            lockInfo = self.getLockInfo(None)

        couch_array = []
        for blob in data_blob_array:
            couch_array.append(self.create_native_doc(blob), **kwargs)

        # All DB operations check for locks
        self.waitOnLock(lockInfo, None)

        logger.info("Storing " + str(len(couch_array)) + " docs to couch:" + str(kwargs))
        self._raw_db.update(couch_array, **kwargs)  # Fixme: this is really a big bypass of the data blob class

        ''' If things get squirrely you can check all the responses
        response_list = self._raw_db.update(data_blob_array, **kwargs)   # Fixme: this is really a big bypass of the data blob class
        for response in response_list:
            success = response[0]
            doc_id = response[1]
            rev_error = response[2]
            if (success == False):
                logger.error( "Failed to store document", doc_id, "error code:", rev_error)
                sys.exit(1)
         '''

    def storeObservationArray(self, observations, lockInfo=None, **kwargs):
        """Stores an array of data blobs to the database."""
        if lockInfo is None:
            lockInfo = self.getLockInfo(None)

        # All DB operations check for locks
        self.waitOnLock(lockInfo, None)
        logger.info("Storing " + str(len(observations)) + " docs to couch:" + str(kwargs))
        #        data_blob_array = []
        for observation in observations:
            observation.store()
        # data_blob_array.append(observation.getMetaDataDict())

        #        self._raw_db.update(data_blob_array, **kwargs)   # Fixme: this is really a big bypass of the data blob class

        ''' If things get squirrely you can check all the responses
        response_list = self._raw_db.update(data_blob_array, **kwargs)   # Fixme: this is really a big bypass of the data blob class
        for response in response_list:
            success = response[0]
            doc_id = response[1]
            rev_error = response[2]
            if (success == False):
                logger.error( "Failed to store document", doc_id, "error code:", rev_error)
                sys.exit(1)
         '''

    def loadDataBlob(self, uuid, lockInfo=None, **kwargs):
        """
        Loads a data blob from the database. Returns a reference to the object if loaded; otherwise None.
            Input:
               uuid:   unique id (unique name) for the data blob (string)
               kwargs: parameters to pass to data blob's __init__ method (key=value args)
                   include_binary: flag for pulling down all binary fields (defaults to False)
                   subclass: data blob subclass type (string)
        """
        if lockInfo is None:
            lockInfo = self.getLockInfo(uuid)

        # All DB operations check for locks
        self.waitOnLock(lockInfo, uuid)
        # Grab the attachment info
        attachments_inline = kwargs.get('include_binary', False)

        # Error if the blob doesn't exist?
        must_exist = kwargs.get('must_exist', False)

        # See if the document is in couch
        if uuid not in self._raw_db:
            if must_exist:
                logger.error(
                    "Could not find data blob " + uuid + " in couch database " + self._host + ' ' + self._database)
                raise RuntimeError("Data Retrieval Error")
            else:
                logger.info(
                    "Could not find data blob " + uuid + " in couch database " + self._host + ' ' + self._database)
            return None

        print "grabbing doc from couch", self._host, uuid
        print "attachments_inline", attachments_inline
        # Grab the document from couch
        try:
            print "attempting couchdb get"
            couchdoc = self._raw_db.get(uuid, attachments=attachments_inline)

        except couchdb.http.ServerError, err:
            logger.exception("Loading data blob " + uuid + " from database " + self._host + ' ' + str(self._database))
            raise

        print "if couchdoc==none"
        if couchdoc is None:
            logger.info("Loading data blob " + uuid + " from database: document==None " + self._host + ' ' + str(
                self._database))
            return None

        print "creating blob"
        # Create the data blob
        _data_blob = data_blob.create(uuid, **kwargs)

        # Set the database
        _data_blob.setDB(self)

        # Set the id and revision
        _data_blob.setDataBlobUUID(couchdoc["_dataBlobID"])
        _data_blob.setDataBlobRevision(couchdoc["_rev"])

        print "looping thru metadata"
        # Loop through the meta data
        for k, v in couchdoc.iteritems():
            if (attachments_inline and (
                        k == "_attachments")):  # Don't include attachments if they are being pulled inline
                continue
            _data_blob.setMetaData(k, v)

        print "grabbing attachments"
        # Grab the attachments
        if attachments_inline:
            if "_attachments" in couchdoc:
                for k, v in couchdoc["_attachments"].iteritems():
                    _data_blob.setBinaryData(k, v["content_type"], base64.b64decode(v["data"]))

        print "validating blob"
        # Now validate the data blob
        _data_blob.validate(**kwargs)

        print "Returning"
        # Return the data blob
        return _data_blob

    def deleteDataBlob(self, uuid, no_checks=False, lockInfo=None):
        """
        Delete the data blob from the database.
            Input:
               uuid:   unique id (unique name) for the data blob (string)
        """
        # Fast path
        if lockInfo is None:
            lockInfo = self.getLockInfo(uuid)

        if no_checks:
            self._raw_db.delete(self._raw_db[uuid])
            return

        # All DB operations check for locks
        self.waitOnLock(lockInfo, uuid)

        if self._raw_db.get(uuid) is None:
            logger.debug(
                "data blob " + uuid + " not found in the database so skipping delete " + self._host + ' ' + str(
                    self._database))
            return
        logger.debug("Deleting data blob " + uuid + " from database " + self._host + ' ' + str(self._database))
        try:
            self._raw_db.delete(self._raw_db[uuid])
        except Exception, e:
            logger.exception(
                "Couch threw exception when trying to delete " + uuid + " from couch database " + self._host + ' ' + str(
                    self._database))
            # traceback.print_exc()

    def getBlobRevision(self, uuid):

        # See if the document is in couch
        couchdoc = None
        try:
            couchdoc = self._raw_db.get(uuid)
        except:
            logger.warning("Couch threw exception when trying to get %s in couch database %s %s" % (
                uuid, self._host, self._database))
        if couchdoc is None:
            logger.error("Could not find data blob %s in couch database %s %s" % (uuid, self._host, self._database))
            raise RuntimeError(
                "Could not find data blob %s in couch database %s %s" % (uuid, self._host, self._database))

        return couchdoc["_rev"]

    def loadView(self, name, lockInfo=None, **kwargs):
        """
        Loads a view from the database. Returns a reference to the view object if loaded; otherwise None.
         Input:
            name:   unique name for the view (string)
            kwargs: parameters to pass to view's __init__ method (key=value args)
        """
        if lockInfo is None:
            lockInfo = self.getLockInfo(name)

        # All DB operations check for locks
        self.waitOnLock(lockInfo, name)

        # Do they want a paging view
        if (kwargs.get("paging", False)):
            return self.paging_view(self, name, logger, **kwargs)
        else:
            return self.view(self, name, logger, **kwargs)

    def getLockInfo(self, doc_id):
        lockInfo = {"name": "database", "process_id": id(self), "db": self._database}
        if doc_id is None:
            lockInfo["doc_id"] = "_all"
        else:
            lockInfo["doc_id"] = doc_id

        return lockInfo

    def setLock(self, lockInfo=None, doc_id=None):

        logger.info("<<<Attempting to lock the database>>>")

        if lockInfo is None:
            lockInfo = self.getLockInfo(doc_id)

        first = True
        locked = True
        lock = {"_dataBlobID": "database_lock", "lock_instance": lockInfo}

        while locked:

            if locked and not first:
                '''Wait on a locked database'''
                logger.info("<<<Waiting on Locked Database>>>")
                time.sleep(random.randint(2, 6))

            first = False
            locked = self.isLocked(lockInfo, doc_id)
            if not locked:
                try:
                    self._raw_db.save(lock)
                    locked = False
                    logger.info("lock set to " + lock["lock_instance"])
                except couchdb_interface.ResourceConflict, e:
                    locked = True

    def isLocked(self, lockInfo, doc_id):
        """Return True/False based on database locked state"""
        if "database_lock" in self._raw_db:

            # If the lock is someone elses then it's locked
            lock_doc = self._raw_db["database_lock"]
            if doc_id is None:
                doc_id = "_all"

            if (lock_doc["lock_instance"] != lockInfo):
                return True

        return False

    def removeLock(self, lockInfo=None, doc_id=None):
        """Remove the lock on the database"""
        if self._raw_db.get("database_lock") is not None:
            lock_doc = self._raw_db["database_lock"]
            lock_instance = lock_doc.get("lock_instance")

            if lock_instance == lockInfo:
                logger.info("<<<Unlocking Database>>>")
                self._raw_db.delete(lock_doc)
            else:
                logger.error("Attempting to remove lock that doesn't belong to me!!" + str(lockInfo))
                #                self._raw_db.delete(self._raw_db["database_lock"])
                #            self._raw_db.delete(self._raw_db["database_lock"])
                #

    def waitOnLock(self, lockInfo, doc_id):
        """Wait on a locked database"""
        try:
            while self.isLocked(lockInfo, doc_id):
                time.sleep(5)
                logger.info("<<<Waiting on Locked Database>>>")
        except couchdb_interface.ResourceNotFound:
            # If the lock isn't found that is fine (it's no longer locked :)
            return

    def compact(self, ddoc=None):
        """Send the compact command to the raw database"""
        if (not self._raw_db.compact(ddoc)):
            logger.error("Compaction of database(" + str(ddoc) + ") failed")

    def viewExists(self, view_name):
        """ Okay this is really silly, but I don't know of another way"""
        try:
            len(self._raw_db.view(view_name, limit=0).rows)
        except Exception, e:
            return False
        return True

    # View functionality
    class view():
        """ Interface for a database view object."""

        def __init__(self, db, name, logger, **kwargs):
            """ Create instance of couchDB view
                Input:
                    name:   unique name for the view (string)
                    kwargs:   parameters for opening the database view (key=value args)
            """
            # See if the view exists
            if not db.viewExists(name):
                logger.error("Could not open view " + name + " in couch database " + str(db.getInfo()))
                raise RuntimeError("Could not open view " + name + " in couch database " + str(db.getInfo()))
            self._view = db._raw_db.view(name, **kwargs)
            self._name = name
            self._dbinfo = db.getInfo()
            self._kwargs = kwargs
            self._rows = self._view.rows
            self._num_rows = len(self._rows)

        def getInfo(self):
            return "db:" + self._dbinfo + " view:" + self._name + ": " + str(self._kwargs)

        def rows(self):
            """ Get the results of the view as a python list """
            return self._rows

        def num_rows(self):
            """ Get the number of rows contained in the results of the view """
            return self._num_rows

        def get_hash(self):
            return make_hash(self._rows)

        def compare(self, other_view):
            if (other_view.get_hash() == self.get_hash()):
                return True
            return False

    # This class pages in views for larger databases
    class paging_view():
        """ View paging interface for a database view object."""

        def __init__(self, db, name, logger, **kwargs):
            """ Create instance of couchDB view
                Input:
                    name:   unique name for the view (string)
                    kwargs:   parameters for opening the database view (key=value args)
            """
            # See if the view exists
            if (not db.viewExists(name)):
                logger.error("Could not open view " + name + " in couch database " + str(db.getInfo()))
                raise RuntimeError("Could not open view " + name + " in couch database " + str(db.getInfo()))

            # Set the view pager
            self._row_pager = self.row_pager(db._raw_db, name, **kwargs)
            self._name = name
            self._kwargs = kwargs
            self._startkey = None
            self._startkey_docid = None
            self._dbinfo = db.getInfo()
            self._endkey = None
            self._endkey_docid = None
            self._pagesize = kwargs.get('page_size', 5000)

        def getInfo(self):
            return "db:" + self._dbinfo + " view:" + self._name + ": " + str(self._kwargs)

        def rows(self):
            """ Return the row pager generator """
            return self._row_pager

        def num_rows(self):
            """ Get the number of rows contained in the results of the view """
            raise NotImplementedError("This method isn't meaningful in a paging view")

        def row_pager(self, db, view_name, **kwargs):

            # Request one extra row to resume the listing there later.
            kwargs['limit'] = self._pagesize + 1
            if self._startkey:
                kwargs['startkey'] = self._startkey
                if self._startkey_docid:
                    kwargs['startkey_docid'] = self._startkey_docid
            if self._endkey:
                kwargs['endkey'] = self._endkey
                if self._endkey_docid:
                    kwargs['endkey_docid'] = self._endkey_docid
            done = False
            while not done:
                logger.info("paging view (page_size:%d)..." % (self._pagesize))
                view = db.view(view_name, **kwargs)
                rows = []
                # If we got a short result (< limit + 1), we know we are done.
                if len(view) <= self._pagesize:
                    done = True
                    rows = view.rows
                else:
                    # Otherwise, continue at the new start position.
                    rows = view.rows[:-1]
                    last = view.rows[-1]
                    kwargs['startkey'] = last.key
                    kwargs['startkey_docid'] = last.id

                for row in rows:
                    yield row

    def push_all_views(self, multiple=False):
        print "pushing all views"
        for view_file_name in self._view_files:

            if self._view_files.index(view_file_name) == 0:
                del_existing = True
            else:
                del_existing = False
            print "pushing first views"
            self.push_view(view_file_name, del_existing, multiple, desc=view_file_name)

        print "pushing redirs"
        for redirect_dir in self._redirect_view_dirs:
            self.push_redirect_view(redirect_dir)

    # multiple means put each view into its own doc
    def push_view(self, view_file, del_existing, multiple=False, view_doc=None, lang="erlang", desc="undefined"):

        suffix = self._staging_suffix
        # support view_file_path#views_document_name
        if not view_doc and "#" in view_file:
            l = view_file.find('#')
            view_doc = view_file[l + 1:]
            view_file = view_file[0:l]

        if not view_doc:
            view_doc = 'views'

        view_file = os.path.abspath(view_file)
        view_content = open(view_file, "r").read()
        view_json = json.loads(view_content, strict=False)

        if multiple:
            objs = view_json.items()
            prime = ''
            for obj in objs:
                # Special naming for design docs
                design_doc_name = "_design/" + view_doc + suffix + "_" + str(obj[0])

                # convert obj from a tuple back into a dictionary and push it back in
                self.push_view_str(view_file, {obj[0]: obj[1]}, design_doc_name, del_existing, lang, desc)
                prime += "curl {0}/{1}/{2}/_view/{3}?limit=0\n".format(self._host, self._database,
                                                                       design_doc_name, str(obj[0]))
            logger.info("run these commands to prime the views (cntrl c to break if they take too long) %s\n" % (prime))
        else:
            design_doc_name = "_design/" + view_doc + suffix
            print "design_doc_name", design_doc_name
            self.push_view_str(view_file, view_json, design_doc_name, del_existing, lang=lang, desc=desc)
            print "done.."

    def push_view_str(self, view_file, view_json, design_doc_name, del_existing, lang="erlang", desc="undefined"):

        # view_json = json.loads(obj, strict=False)

        # We simply open up a view content file per design doc, the view content file
        # may have one of more views defined in it
        view = self.loadDataBlob(design_doc_name)
        if not (view is None):
            print "notviewnone"
            views_dict = view.getMetaData("views")
            new_views_dict = dict(views_dict.items() + view_json.items())
            view.setMetaData("views", new_views_dict)
        else:
            print "viewnone"
            view = data_blob.create(design_doc_name, subclass="view")
            view.setMetaData("views", view_json)
        # print "PUSHING VIEWS"
        #        print view_json
        print "about to setmetadata on view"
        view.setMetaData("language", lang)
        view.setMetaData("description", desc)

        print "view about to setdb"
        # Now save the view to the database
        view.setDB(self)
        print "view about to store"
        view.store(delete_existing=True)

        # Status
        print "view logging"
        logger.info("Design document created:", view_file, "for database", self.getInfo(), " at " + design_doc_name)

    def push_redirect_view(self, view_dir):

        suffix = self._staging_suffix

        # Special naming for design docs
        design_doc_name = "_design/redirect" + suffix

        # Redirect lookup functionality
        design = data_blob.create(design_doc_name)
        for f in os.listdir(view_dir):
            content = open(view_dir + "/" + f, "r").read()
            mytype = mimetypes.guess_type(f)
            design.setBinaryData(f, mytype[0], content)

        # Now save the view to the database
        design.setDB(self)
        design.store(delete_existing=True)

        # Status
        logger.info("Redirect design document created at " + design_doc_name)


class mongodb(abstract_db):
    """MongoDB database"""

    # Default host and database names
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 27017
    DEFAULT_DATABASE = 'local'
    DEFAULT_COLLECTION = 'test'

    def __init__(self, **kwargs):
        """ Create instance of MongoDB database
            Input:
                kwargs:   parameters for opening a database connection (key=value args)
                    host:     URL for MongoDB host
                    database: name of database
        """
        self._type = 'mongodb'
        self._host = kwargs.get('host', self.DEFAULT_HOST)
        self._database = kwargs.get('database', self.DEFAULT_DATABASE)
        self._port = kwargs.get('port', self.DEFAULT_PORT)

        self._slave_okay = kwargs.get('slave_okay', False)
        self._collection = kwargs.get('collection', self.DEFAULT_COLLECTION)
        self._use_gridfs = kwargs.get('use_gridfs', False)
        self._raw_gridfs = None
        self._raw_db = None
        self._raw_collection = None
        self._kwargs = kwargs

        # Get a logger handle (singleton)
        if not (self._host == self.DEFAULT_HOST):
            host = self._host
            host_list = host.split("/")
            index = len(host_list) - 1
            host_and_port = host_list[index]
            host_and_port_list = host_and_port.split(":")
            self._host = host_and_port_list[0]
            self._port = int(host_and_port_list[1])

        # Open the database
        self.open()

    @staticmethod
    def loadFromJSON(json_data):
        jsontype = json_data["_jsontype"]
        if not (jsontype == "hybrid.db.mongodb"):
            return None

        return mongodb(**json_data)

    @property
    def info(self):
        """Get a string of information the database. """
        return self._type + ": " + str(self._kwargs) + "; "

    @property
    def type(self):
        """Get the type of the database. """
        return self._type

    @property
    def host(self):
        """Get the host of the database. """
        return self._host

    @property
    def db_name(self):
        """Get the database name of the database. """
        return self._database

    @property
    def collection_name(self):
        """Get collection name."""
        return self._collection

    @property
    def num_documents(self):
        return self._raw_db.get_collection(self._collection).find({}).count()

    def __str__(self):
        return self.getInfo()

    ################ \/ Deprecated as of 3/26/2016 \/ ################
    def getInfo(self):
        logger.warning("This accessor is deprecated please remove it from your code use .info")
        '''Get a string of information the database. '''
        return self._type + ": " + str(self._kwargs) + "; "

    def getType(self):
        logger.warning("This accessor is deprecated please remove it from your code use .type")
        '''Get the type of the database. '''
        return self._type

    def getHost(self):
        logger.warning("This accessor is deprecated please remove it from your code use .host")
        '''Get the host of the database. '''
        return self._host

    def getDBName(self):
        logger.warning("This accessor is deprecated please remove it from your code use .db_name")
        '''Get the database name of the database. '''
        return self._database

    def getCollectionName(self):
        logger.warning("This accessor is deprecated please remove it from your code use .collection_name")
        '''Get collection name.'''
        return self._collection

    def getNumDocuments(self):
        logger.warning("This accessor is deprecated please remove it from your code use .num_documents")
        return self._raw_db.get_collection(self._collection).find({}).count()

    ################ ^ Deprecated as of 3/26/2016 ^ ################


    def copy_database(self, target_db):
        """Simply copy all the documents from this database to the target.
         Input:
            kwargs: parameters to pass to the replication (key=value args)
        """
        # Pull a view from this database with all the documents
        logger.info("Copying _all_docs from:" + self._host + ":" + str(self._database) + " to " +
                    target_db.getHost() + ":" + target_db.getDBName())
        view = self.loadView(self._collection, paging=False)
        if (view == None):
            logger.error("Could not open view on:" % (str(self._collection)))
            raise RuntimeError("Could not open view.")

        # Simply loop over the documents in the source database
        # and push to the target database
        for row in view.rows():
            # Grab the whole document
            doc = self.loadDataBlob(row["_dataBlobID"], include_binary=True)
            # Set target database
            doc.setDB(target_db)
            # Store to target
            doc.store(delete_existing=True)

    def delete(self, **kwargs):
        """Delete the database.
         Input:
            kwargs: parameters to pass to the database delete function (key=value args)
        """
        # Delete the Mongo database
        if (self._database in self._server.database_names()):
            self._server.drop_database(self._database, **kwargs)

    def open(self):
        """Opens connection to self._server and self._database."""
        self._server = pymongo.mongo_client.MongoClient(host=self._host, port=self._port, connectTimeoutMS=10000)

        if not self._server:
            logger.error("Could not connect to Mongo server: %s" % (self._host))
            raise RuntimeError("Could not connect to Mongo server: %s" % (self._host))

        # Do they want to delete the existing one before creating a new one
        if (self._kwargs.get('delete_existing', False)):
            self.delete()

        # Open the database
        self._raw_db = self._server[self._database]
        self._raw_collection = self._raw_db[self._collection]

        if self._use_gridfs:
            self._raw_gridfs = gridfs.GridFS(self._raw_db,
                                             collection=self._collection + "_gfs")

        return True

    def close(self):
        """Close connection to MongoDB."""
        self._server.close()

    def store(self, document, **kwargs):
        if self._raw_gridfs is not None:
            doc_info = document.get("_dataBlobID")
            if isinstance(doc_info, bson.objectid.ObjectId):
                document["_dataBlobID"] = str(doc_info)
            if doc_info is not None:
                if self._raw_gridfs.exists({"_dataBlobID": doc_info}):
                    self._raw_gridfs.delete(doc_info)
                gridfs_file = self._raw_gridfs.new_file(**{"_dataBlobID": doc_info})
            else:
                doc_info = make_hash(mongo_doc)
                gridfs_file = self._raw_gridfs.new_file(**{"_dataBlobID": doc_info})
            gridfs_file.write(marshal.dumps(document))
            gridfs_file.close()
            return doc_info

        saved = self._raw_collection.save(document, manipulate=True)

        return saved

    def storeDataBlob(self, blob, update_rev=False, **kwargs):
        """Store the data blob to the database using any optional parameters"""
        if blob is None:
            return False

        # All DB operations check for locks
        self.waitOnLock()

        mongo_doc = data_blob.blob2dict(blob)

        # TODO Temporary fix
        self.deleteDataBlob(blob.getMetaData("_dataBlobID"))

        doc_info = self.store(mongo_doc, **kwargs)

        return doc_info

    def storeDataBlobArray(self, data_blob_array, **kwargs):
        """Stores an array of data blobs to the database."""
        # All DB operations check for locks
        self.waitOnLock()
        for blob in data_blob_array:
            self.storeDataBlob(blob)

    def loadDataBlob(self, uuid, **kwargs):
        """Loads a data blob from the database. Returns a reference to the
           object if loaded; otherwise None.

            Input:
               uuid:   unique id (unique name) for the data blob (string)
               kwargs: parameters to pass to data blob's __init__ method
                            (key=value args)
                   include_binary: flag for pulling down all binary fields
                            (defaults to False)
                   subclass: data blob subclass type (string)
        """

        # All DB operations check for locks
        self.waitOnLock()
        # Grab the attachment info
        attachments_inline = kwargs.get('include_binary', False)

        # Error if the blob doesn't exist?
        must_exist = kwargs.get('must_exist', True)

        if self._raw_gridfs is not None:
            try:
                mongo_doc = marshal.loads(self._raw_gridfs.get(uuid, **kwargs).read())
            except:
                if (must_exist):
                    raise RuntimeError("Data Retrieval Error")
                return None
        else:
            cursor = self._raw_collection.find({"_dataBlobID": uuid})
            # See if the document is in db
            if cursor.count() == 0:
                if must_exist:
                    raise RuntimeError("Data Retrieval Error")
                return None

            mongo_doc = cursor[0]

        # Create the data blob
        blob = data_blob.dict2blob(mongo_doc)

        # Return the data blob
        return blob

    def deleteDataBlob(self, uuid, no_checks=False):
        """Delete the data blob from the database.
            Input:
               uuid:   unique id (unique name) for the data blob (string)
        """
        # Fast path
        if (not no_checks):
            self.waitOnLock()

        if self._raw_gridfs is not None:
            self._raw_gridfs.delete(uuid)
        else:
            self._raw_collection.remove({"_dataBlobID": uuid})

    def getBlobRevision(self, uuid):
        doc = self.loadDataBlob(uuid)
        rev = doc.getDataBlobRevision()
        return rev

    def loadView(self, name, **kwargs):
        """Loads a view from the database. Returns a reference to the view
           object if loaded; otherwise None.

         Input:
            name:   unique name for the view (string)
            kwargs: parameters to pass to view's __init__ method (key=value args)
        """
        # All DB operations check for locks
        self.waitOnLock()

        # Do they want a paging view?
        if kwargs.get("paging", False):
            raise NotImplementedError("Paging view not implemented for Mongo.")
        else:
            return self.view(self, name, logger, **kwargs)

    def setLock(self):
        """Lock the database"""
        self.waitOnLock()

        lock = {"_dataBlobID": "database_lock", "lock_instance": id(self)}
        self._raw_collection.insert(lock)
        logger.info("<<<Locking Database>>>")

    def isLocked(self):
        """Return True/False based on database locked state"""
        cursor = self._raw_collection.find({"_dataBlobID": "database_lock"})
        if cursor.count() == 1:
            # If the lock is someone elses then it's locked
            lock = cursor[0]
            if (lock["lock_instance"] != id(self)):
                return True
        return False

    def removeLock(self):
        """Remove the lock on the database"""
        self._raw_collection.remove({"_dataBlobID": "database_lock",
                                     "lock_instance": id(self)})
        logger.info("<<<Unlocking Database>>>")

    def waitOnLock(self):
        """Wait on a locked database"""
        while self.isLocked():
            time.sleep(5)
            logger.info("<<<Waiting on Locked Database>>>")

    def compact(self):
        """Send the compact command to the raw database"""
        self._raw_db.command("compact", self._collection)

    def query(self, name=None, statement=None, limit=0, **kwargs):
        """Ad-hoc query for the Mongo DB.  Note: a limit of 0 is equal to no limit"""
        if name == None:
            name = self._collection
        return self.view(self, name, **kwargs)

    def update_doc(self):
        """Ad-hoc update handlers database"""
        raise NotImplementedError("This method is part of a pure virtual class.")

    # View functionality
    class view():
        """ Interface for a database view object."""

        def __init__(self, db, name, **kwargs):
            """ Create instance of MongoDB view
                Input:
                    name:   unique name for the view (string)
                    kwargs:   parameters for opening the database view (key=value args)
            """
            self._view = db._raw_db[name]
            self._name = name
            self._dbinfo = db.getInfo()
            self._kwargs = kwargs
            self._query = kwargs.get("query", None)
            try:
                del kwargs["query"]
            except:
                pass
            self._rows = [record for record in self._view.find(self._query, **kwargs)]
            self._num_rows = len(self._rows)

        def getInfo(self):
            return "db:" + self._dbinfo + " view:" + self._name + ": " + str(self._kwargs)

        def rows(self):
            """ Get the results of the view as a python list """
            return self._rows

        def num_rows(self):
            """ Get the number of rows contained in the results of the view """
            return self._num_rows

        def get_hash(self):
            make_hash(self._rows)

        def compare(self, other_view):
            if other_view.get_hash() == self.get_hash():
                return True
            return False
