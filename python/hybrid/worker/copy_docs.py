"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""

import hybrid

import worker


class copy_docs(worker.abstract_worker):

    def __init__(self, **kwargs):
        # Call super class init copy_docs
        worker.abstract_worker.__init__(self, **kwargs)
        self._version = "19.07.2003"
        self._data_version = "19.07.2003"

        self._db_host = kwargs.get("src_db", None)
        self._database = None
        if "database" in kwargs:
            self._database = kwargs.get("database") + "_cache"

        self.param_destdb = kwargs.get("dest_db", None)
        self.param_desthost = kwargs.get("dest_host", None)
        self.param_startkey = kwargs.get("startkey", None)
        self.param_startkey = kwargs.get("reset", False)

        #self.param_endkey = kwargs.get("endkey", None)
        #self.param_streaming = kwargs.get("streaming", None)
        #self.param_count = kwargs.get("count", None)
        #self.param_fields = kwargs.get("fields", None)

    def copy_doc_meta(self, meta_src, meta_dst, fields = None):
        for field in fields:
            meta_dst[field] = meta_src[field]
        else:
            meta_dst = meta_src.copy()

            if "_dataBlobID" in doc_dst:
                del meta_dst["_dataBlobID"]

            if "_rev" in doc_dst:
                del meta_dst["_rev"]

        return

    def process_observation_core(self,doc,**kwargs):
    
        db_host = self.param_desthost 
        database = self.param_destdb

        if copy_docs_dict:
            doc_meta = doc.getMetaDataDict()

        db = hybrid.db.init("couchdb", host=db_host, database=database, create=True)

        new_doc = hybrid.data_blob.create()
        new_doc.setDB(db)
        new_doc_meta = new_doc.getMetaDataDict()
        new_doc_meta = self.copy_doc_meta(doc_meta, new_doc_meta, self._param_fields)
        new_doc.store(ignore_conflict=False)

        return doc
