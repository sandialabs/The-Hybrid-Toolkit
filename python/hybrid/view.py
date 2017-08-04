"""

TODO:

    - View currently returns Python dictionaries.  Need to return blobs.
    - Need to add an alias variable (i.e., a unique name for the view)?
        Currently, the name variable is for the MongoDB collection to pull from.


"""
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import collections
import copy

import db
import hybrid


def create_view(database, query_name, input_tags, output_tags, **kwargs):
    query_info = {}
    query_info["db_type"] = database.getType()
    query_info["db_name"] = database.getDBName()
    query_info["db_host"] = database.getHost()
    query_info["query_name"] = query_name
    query_info["query_find_list"] = input_tags
    query_info["query_except_list"] = output_tags
    if isinstance(database, db.mongodb):
        if not isinstance(input_tags, collections.Iterable):
            input_tags = [input_tags]
        if not isinstance(output_tags, collections.Iterable):
            output_tags = [output_tags]
        query = {}
        for tag in input_tags:
            query = dict({tag: {"$exists": True}}, **query)
        for tag in output_tags:
            query = dict({tag: {"$exists": False}}, **query)
        mview = mongo_view(query_info, db=database, query_name=query_name, query=query)
        return mview
    elif isinstance(database, db.couchdb):
        return couch_view(query_info, **kwargs)


def create_view_from_query_info(query_info, **kwargs):
    db_type = query_info.get("db_type")
    db_host = query_info.get("db_host")
    db_name = query_info.get("db_name")
    query_name = query_info.get("query_name")
    query_find_list = query_info.get("query_find_list", [])
    query_except_list = query_info.get("query_except_list", [])
    push_views = query_info.get("push_views")

    if (db_type == "couchdb"):
        return couch_view(query_info, **kwargs)
    else:
        database = db.init(db_type, host=db_host, database=db_name, push_views=False, create=False)
        view = create_view(database, query_name, query_find_list, query_except_list, **kwargs)

        return view


def make_hash(o):
    if isinstance(o, set) or isinstance(o, tuple) or isinstance(o, list):
        return tuple([make_hash(e) for e in o])
    elif isinstance(o, hybrid.data_blob.data_blob):
        return o.get_hash()
    elif not isinstance(o, dict):
        return hash(o)

    new_o = copy.deepcopy(o)
    for k, v in new_o.items():
        new_o[k] = make_hash(v)
    return hash(tuple(frozenset(new_o.items())))


class couch_view():
    ''' Interface for a MongoDB view object.'''

    def __init__(self, query_info, **kwargs):
        ''' Create instance of MongoDB view
            Input:
                name:   collection name from which the view is created
                kwargs:   parameters for opening the database view
        '''

        if (query_info.has_key("query_name")):
            query_name = query_info.get("query_name")
        else:
            query_name = ""

        if (query_name == ""):
            view_uri = "_all_docs"
        elif '/' in query_name:
            view_uri = "_design/" + query_name
        else:
            view_uri = "_design/views/_view/" + query_name

        self._view_uri = view_uri

        self._db_type = query_info.get("db_type")
        self._db_host = query_info.get("db_host")
        self._db_name = query_info.get("db_name")
        self._db_keys = query_info.get("query_keys", None)

        # TODO handle other keyword args like doclimits

        database = db.init(self._db_type, host=self._db_host, database=self._db_name, push_views=False, create=False)

        if not database.viewExists(self._view_uri):
            # logger.error( "Could not open view", name, "in couch database", db.getInfo())
            raise RuntimeError("Could not open view", self._view_uri, "in couch database", database.getInfo())

        self._view = database._raw_db.view(self._view_uri, **kwargs)
        self._name = query_name
        self._dbinfo = database.getInfo()
        self._kwargs = kwargs
        #        self._rows = []
        self._row_dict = {}
        self._num_rows = 0

    def __str__(self):
        output_string = "db: " + self._dbinfo + " view: "
        output_string += self._name + ": " + str(self._kwargs)
        return output_string

    def iterdocs(self):
        """ Iterate over the documents in the database collection. """
        cursor = self._view.find(self._query, **self._kwargs)
        for document in cursor:
            yield document

    def rows(self):
        """ Get the results of the view as a python list. """
        db_name = self._db_name
        db_host = self._db_host
        db_type = self._db_type
        db_keys = self._db_keys

        database = db.init(db_type, host=db_host, database=db_name, push_views=False, create=False)

        if db_keys:
            couch_view = database.loadView(self._view_uri, keys=db_keys, **self._kwargs)  # kwargs
        else:
            couch_view = database.loadView(self._view_uri, **self._kwargs)  # kwargs

        rows = couch_view.rows()
        observation_dict = {}
        observations = []
        for row in rows:
            if row.id.startswith("_"):
                continue
            observation = database.loadDataBlob(row.id, include_binary=True)
            if observation is None: continue
            observations.append(observation)
            observation_dict[row.id] = observation

        #        self._rows = observations
        self._row_dict = observation_dict
        self._num_rows = len(observation_dict)
        #         self._rows = [record for record in
        #                       self._view.find(self._query, **self._kwargs)]
        #         self._num_rows = len(self._rows)
        return observations

    def num_rows(self):
        """ Get the number of rows contained in the results of the view. """

        if self._row_dict is None:
            return 0

        return self._num_rows

    def num_rows_skip_load(self):
        """ Get the number of rows contained in the results of the view. """

        db_name = self._db_name
        db_host = self._db_host
        db_type = self._db_type
        db_keys = self._db_keys

        database = db.init(db_type, host=db_host, database=db_name, push_views=False, create=False)

        if db_keys:
            couch_view = database.loadView(self._view_uri, keys=db_keys, **self._kwargs)  # kwargs
        else:
            couch_view = database.loadView(self._view_uri, **self._kwargs)  # kwargs

        rows = couch_view.rows()

        return len(rows)

    def get_hash(self):
        hash_list = []
        for k, v in self._row_dict.iteritems():
            temp_hash_list = []
            metaData = v.getMetaDataDict()
            binaryData = v.getBinaryDataDict()

            for data_k, data_v in metaData.iteritems():
                temp_hash_list.append(hybrid.utils.make_hash((data_k, data_v)))

            for data_k, data_v in binaryData.iteritems():
                temp_hash_list.append(hybrid.utils.make_hash((data_k, data_v)))

            temp_hash_list.sort()
            hash_list.append(hybrid.utils.make_hash(temp_hash_list))
        hash_list.sort()

        return make_hash(hash_list)

    #         print "Making hash from ",(self._rows)
    #         new_hash = make_hash(self._rows)
    #         print "Hash=",new_hash
    #         return new_hash

    def compare(self, view2):
        return hybrid.utils.compareDataBlobDicts(self._row_dict, view2._row_dict)


# return self.get_hash() == view2.get_hash()

class mongo_view():
    """ Interface for a MongoDB view object."""

    def __init__(self, query_info, **kwargs):
        """ Create instance of MongoDB view
            Input:
                name:   collection name from which the view is created
                kwargs:   parameters for opening the database view
        """
        query_name = query_info.get("query_name")
        self._db_type = query_info.get("db_type")
        self._db_host = query_info.get("db_host")
        self._db_name = query_info.get("db_name")
        self._db_collection_name = query_info.get("collection_name", "test")

        database = db.init(self._db_type, host=self._db_host, database=self._db_name, push_views=False, create=False)

        self._view = database._raw_db[self._db_collection_name]
        self._name = query_name
        self._dbinfo = database.getInfo()
        self._kwargs = kwargs
        self._query = kwargs.get("query")
        #        self._rows = []
        self._row_dict = {}
        self._num_rows = 0

    def __str__(self):
        output_string = "db: " + self._dbinfo + " view: "
        output_string += self._name + ": " + str(self._kwargs)
        return output_string

    def iterdocs(self):
        """ Iterate over the documents in the database collection. """
        cursor = self._view.find(self._query, **self._kwargs)
        for document in cursor:
            yield document

    def rows(self):
        """ Get the results of the view as a python list. """
        mongo_rows = [record for record in
                      self._view.find(self._query)]

        db_type = self._db_type
        db_host = self._db_host
        db_name = self._db_name
        database = db.init(db_type, host=db_host, database=db_name, push_views=False, create=False)

        observation_dict = {}
        observations = []
        for row in mongo_rows:
            uuid = str(row.get("_dataBlobID"))
            if uuid.startswith("_"): continue
            observation = database.loadDataBlob(row.get("_dataBlobID"), include_binary=True)
            observations.append(observation)
            observation_dict[row.get("_dataBlobID")] = observation

        #        self._rows = observations
        self._row_dict = observation_dict
        self._num_rows = len(observation_dict)

        return observations

    def num_rows(self):
        ''' Get the number of rows contained in the results of the view. '''
        if self._row_dict is None:
            return 0
        return self._num_rows

    #     def get_hash(self):
    #         return make_hash(self._rows)
    def get_hash(self):
        hash_list = []
        for k, v in self._row_dict.iteritems():
            temp_hash_list = []
            metaData = v.getMetaDataDict()
            binaryData = v.getBinaryDataDict()

            for data_k, data_v in metaData.iteritems():
                if (data_k.startswith("_")): continue
                if (data_k == "creation_datetime"): continue
                if (data_k == "created"): continue

                temp_hash_list.append(hybrid.utils.make_hash((data_k, data_v)))

            for data_k, data_v in binaryData.iteritems():
                temp_hash_list.append(hybrid.utils.make_hash((data_k, data_v)))

            temp_hash_list.sort()
            hash_list.append(hybrid.utils.make_hash(temp_hash_list))
        hash_list.sort()
        return make_hash(hash_list)

    def compare(self, view2):
        return hybrid.utils.compareDataBlobDicts(self._row_dict, view2._row_dict)

# return self.get_hash() == view2.get_hash()
