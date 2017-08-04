"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import datetime
import hashlib
import json
import logging
import shlex

from hybrid import utils,view,encoding

import data_blob
import db

# Multimap stuff
from collections import defaultdict


logger = logging.getLogger(__name__)

def loadFromCache(subclass, uuid, **kwargs):

    """ Load a model of a particular subclass from cache, if not in cache
        it will return a newly created model
        Input:
            subclass:   model subclass type (string)
            uuid:   unique id (unique name) for the model (string)
            kwargs: parameters to pass to __init__ method (keyword args)
    """
    global model_cache
    if 'model_cache' not in globals():
        model_cache = {}

    # If the model is in the cache return it, else create it and return
    if (uuid in model_cache.keys()):
        return model_cache[uuid]
    else:
        model = subclass(uuid, **kwargs)
        model_cache[uuid] = model
        return model


class model():
    '''Superclass for a model. A model interacts with data_blobs(in memory storage),
       algorithms(vtkLatentDirichletAllocation) and the database (backing store like couchdb)
       to compute some set of artifacts. Those artifacts are then made accessible to the user
       or put into the backing store or in most cases both.
       '''
    
    

    
    def __init__(self, uuid, **kwargs):
        ''' Create instance of model
            Input:
                kwargs: various model parameters for the model
        '''

        if (uuid == None):
            uuid = kwargs.get('uuid')

        # Besides in memory handles (like logger, db, input data) models should store ^all^ data
        # in their parameters storage object. Doing this allows proper serialization and deserialization,
        # it allows means that validation of the model works correctly.
        self._parameters = data_blob.data_blob(uuid)
        self._hyperparameter_list = []
        
        self._parameters_uuid = uuid

        # The model will also have input data which will be set by the setModelData method
        self._model_data = None

        self._db_type = kwargs.get('db_type',None)        
        self._db_host = kwargs.get('host', None)
        self._db_name = kwargs.get('database', None)

        model_observations_manager_name = kwargs.get("model_observations_manager_name")
        
        selfUpdate=kwargs.get("selfUpdate",True)
        selfValidate=kwargs.get("selfValidate",True)

        # Handle to storage for model parameters for model parameters
        params = self._parameters

        # Validation fields
        params.addRequiredMetaFields(["model_type","model_desc"])
        params.addRequiredBinaryFields([])

        # Various bits of meta data
        params.setMetaData("model_type",u"unknown")
        params.setMetaData("model_desc",u"unknown")
        #params.setMetaData("kwargs", kwargs)
        #params.setMetaData("db_views", [])
        params.setMetaData("valid",False)
        params.setMetaData("readyForRecomputation",True)
        params.setMetaData("type","model")
        params.setMetaData("selfUpdate",selfUpdate)
        params.setMetaData("selfValidate",selfValidate)
        params.setMetaData("model_observations_manager_name",model_observations_manager_name)
        params.setMetaData("model_observations_manager_last_updated_datetime_dict", {})
        
        self._hyperparameter_list.append("model_type")


    def __str__(self):
        '''Prints out the parameters and the model_data'''
        print_str = "<<<Parameters>>>\n" + self.getParameters().__str__()
        print_str = print_str + "\n<<<Model Data>>>\n" + self.getModelData().__str__()
        return print_str

    def getRevision(self):
        ''' Get the revision of the model '''
        return self._parameters.getDataBlobRevision()

    def getRevisionFromDB(self):
        ''' Get the revision of the model stored in the DB '''

        # Grab my uuid and db from the current storage object
        uuid = self.getParameters().getDataBlobUUID()
        db = self.getDB()
        if (db is None):
            return None
        #db = self.getParameters().getDB()
        rev = db.getBlobRevision(uuid)
        return rev


    def getParameters(self):
        ''' Get the parameters from the model '''
        return self._parameters

    def isValid(self):
        if self.getParameters().hasMetaData("valid"):
            return self.getParameters().getMetaData("valid")
        
        self.getParameters().setMetaData("valid",False)
        return False

    def setValid(self):
        now = datetime.datetime.utcnow()
        datetime_dict = utils.getDateTimeDict(now)
        self.getParameters().setMetaData("valid_datetime_dict", datetime_dict)
        self.getParameters().setMetaData("valid",True)

    def getSelfValidate(self):
        return self.getMetaData("selfValidate")
    
    def getUpdateDatetimeDict(self):
        if (self.getParameters().hasMetaData("updated_datetime_dict")):
            return self.getParameters().getMetaData("updated_datetime_dict")
        
        return {}

    def getValidDatetimeDict(self):
        if (self.getParameters().hasMetaData("valid_datetime_dict")):
            return self.getParameters().getMetaData("valid_datetime_dict")
        
        return {}
    
    def finalize(self):
        if self.getParameters().hasMetaData("model_observation_manager_name"):
            model_observations_manager = self.getModelObservationsManager()
            model.setMetaData("model_observations_manager_last_updated_datetime_dict",model_observations_manager.getUpdateDatetimeDict())

        self.setUpdated(True)
        
        if self.getSelfValidate()==True:
            self.setValid()
    
        if (self.getDB()==None):
            return True
        return self.storeToDB()

    def isUpdated(self):
        if self.getParameters().hasMetaData("updated"):
            return self.getParameters().getMetaData("updated")
        
        self.getParameters().setMetaData("updated",False)
        return False
    
    def setUpdated(self,updated_status):
        now = datetime.datetime.utcnow()
        datetime_dict = utils.getDateTimeDict(now)
        self.getParameters().setMetaData("updated", updated_status)
        if (updated_status):
            self.getParameters().setMetaData("updated_datetime_dict", datetime_dict)
                
    def setInvalid(self):
        self.getParameters().setMetaData("valid",False)

    def isReadyForRecomputation(self):
        if self.getParameters().hasMetaData("readyForRecomputation"):
            return self.getParameters().hasMetaData("readyForRecomputation")
        
        self.getParameters().setMetaData("readyForRecomputation",False)
        return False

    def setReadyForRecomputation(self):
        self.getParameters().setMetaData("readyForRecomputation",True)

    def setNotReadyForRecomputation(self):
        self.getParameters().setMetaData("readyForRecomputation",False)

    def isSelfUpdating(self):
        if self.getParameters().hasMetaData("selfUpdate"):
            return self.getParameters().hasMetaData("selfUpdate")
        
        self.getParameters().setMetaData("selfUpdate",False)
        return False

    def setSelfUpdating(self):
        self.getParameters().setMetaData("selfUpdate",True)

    def setNotSelfUpdating(self):
        self.getParameters().setMetaData("selfUpdate",False)

    def getModelObservationsManager(self):
        if (self.getParameters().hasMetaData("model_observations_manager_name")):
            model_observations_manager_name = self.getParameters().getMetaData("model_observations_manager_name")
#            print "model,model_observations_manager_name",self.getParameters().getDataBlobUUID(),model_observations_manager_name
        else:
            print "model_observations_manager_name not in parameters"
            return None
        
        #self._db_host
        model_observations_manager = db_management_model(model_observations_manager_name)
        model_observations_manager.setDBHost(self._db_host)
        model_observations_manager.setDBType(self._db_type)
        model_observations_manager.setDBName(self._db_name)
        
#        print "Loading",model_observations_manager_name,"From the database"
        loadFromDBStatus=model_observations_manager.loadFromDB()
#        print "loadFromDBStatus",loadFromDBStatus
        
        return model_observations_manager

    
    def checkModelObservationsCurrent(self):
        model_observations_manager = self.getModelObservationsManager()
        
        if (self.getParameters().hasMetaData("model_observations_manager_last_updated_datetime_dict")):
            last_updated_datetime_dict = self.getParameters().getMetaData("model_observations_manager_last_updated_datetime_dict")
        
            current_updated_datetime_dict = model_observations_manager.getParameters().getMetaData("updated_datetime_dict")
            
            print "last",last_updated_datetime_dict
            print "current",current_updated_datetime_dict
            print "compare=",utils.compareDateTimeDict(current_updated_datetime_dict,last_updated_datetime_dict)

            if (utils.compareDateTimeDict(current_updated_datetime_dict,last_updated_datetime_dict)==0):
                return True
        
        return False
    
    def compareHyperParameters(self,other_model):
        print "Checking hyperparameters"
        for hyperparameter_name in self._hyperparameter_list:
            if not(hyperparameter_name in self.getParameters().getMetaDataDict()):
                print "error", "Hyperparameters for this model are not correct. "+hyperparameter_name+", for example..."
                logger.error( "Hyperparameters for this model are not correct. "+hyperparameter_name+", for example...")
                return False

            val = self.getMetaData(hyperparameter_name)
            
            if not(hyperparameter_name in other_model.getParameters().getMetaDataDict()):
                print "Missing hyperparameter_name",hyperparameter_name,"in other model"
                return False

            other_val = other_model.getMetaData(hyperparameter_name)
            if not(val==other_val):
                print "Value", val," not the same as ",other_val
                return False
        print "Hyperparams ok here..."
        return True
        
    def compareModelParameters(self,other_model):
        
        if not(self.compareModelMetaData(other_model)):
            return False
        if not(self.compareModelBinaryData(other_model)):
            return False
        return True
    
    def compareModelMetaData(self,other_model):
        param_meta_data = self.getParameters()._meta_data
        other_param_meta_data = other_model.getParameters()._meta_data
        
        for k,v in param_meta_data.iteritems():
            if (k.startswith('_')):
                continue
            if (k=="valid"):
                continue
            if (k=="created"):
                continue
            if (k=="creation_datetime"):
                continue
            if not(other_param_meta_data.has_key(k)):
                print "No",k,"while comparing model meta data for",self.getMetaData("uuid")
                return False
            if not(other_param_meta_data[k]==v):
                print "Different",k,"while comparing model meta data for",self.getMetaData("uuid"),"::",v,other_param_meta_data[k]
                return False
        
        return True

    def compareModelBinaryData(self,other_model):
        param_binary_data = self.getParameters()._binary_data
        other_param_binary_data = other_model.getParameters()._binary_data
        
        if not(len(param_binary_data)==len(other_param_binary_data)):
            return False
        
        for k,v in param_binary_data.iteritems():
            if not(other_param_binary_data.has_key(k)):
                return False
            if not(other_param_binary_data[k]==v):
                return False
        
        return True
    
    def setModelData(self, model_data):
        ''' Set the model data for the model '''
        self._model_data = model_data
    def getModelData(self):
        ''' Get the model data from the model '''
        return self._model_data

    def setMetaData(self, field, value):
        ''' Syntactic sugar method: Set meta data for the parameters of this model '''
        self.getParameters().setMetaData(field,value)
    def getMetaData(self, field):
        ''' Syntactic sugar method: Get meta data from the parameters of this model '''
        return self.getParameters().getMetaData(field)

    def setBinaryData(self, field, mime_type, params):
        ''' Syntactic sugar method: Set binary data for the parameters of this model '''
        self.getParameters().setBinaryData(field, mime_type, params)
    def getBinaryData(self, field, mime_type=None):
        ''' Syntactic sugar method: Get binary data from the parameters of the model '''
        return self.getParameters().getBinaryData(field, mime_type)

    def setDBType(self,db_type):
        self._db_type = db_type

    def setDBHost(self,db_host):
        self._db_host = db_host

    def setDBName(self,db_name):
        self._db_name = db_name

#    def setDB(self, db):
#        ''' Syntactic sugar method: Set database for the parameters '''
#        self.getParameters().setDB(db)
    def getDB(self):
        ''' Syntactic sugar method: Set database for the parameters '''
        if (self._db_type==None):
            return None
        
        db_handle = db.init(self._db_type,host=self._db_host,database=self._db_name)
        
        
        return db_handle

    def loadFromDB(self, **kwargs):
        ''' This method replaces the internal parameters storage object with the one from the database '''

        # Grab my uuid and db from the current storage object
        print "model.loadFromDB"
#        print "self.getParameters()",self.getParameters()
        uuid = self.getParameters().getDataBlobUUID()
        print "model uuid",uuid
        db = self.getDB()
        if (db==None):
            return False
#        db = self.getParameters().getDB()
        rev = self.getRevision()

        try: 
            db_rev = self.getRevisionFromDB()
        except RuntimeError, e:
            if rev != None and rev != 0:
                print "Can't find model in database - hopefully its just re-loading - keep using the current version for the time being %s %s",rev, str(e)
                logger.warning( "Can't find model in database - hopefully its just re-loading - keep using the current version for the time being %s %s",
                    rev, str(e))
            return False

        # TODO Check revision
#         if (rev == db_rev):
#             print "Model revision the same: skipping load..."
#             logger.info( "Model revision the same: skipping load...")
#             return True
#         else:
#             print "revisions are different"
#             logger.info( "Revisions are different (" + 
#                                     self.getRevision() + ' ' +  self.getRevisionFromDB() + ") loading...")

        # Delete current storage
        logger.info( "Deleting model parameter storage")
        del self._parameters

        logger.info( "Loading model parameters from database")
        self._parameters = db.loadDataBlob(uuid, include_binary=True, **kwargs)
        if (self._parameters==None):
            print "model has no parameters!!!"
            logger.warning( "Model has no parameters!!",uuid,kwargs)
        else:
            # Make sure my parameters are properly loaded
            print "validating"
            self._parameters.validate()

        return True

    def storeToDB(self,lock_id=None):
        ''' This method saves the internal parameters storage object to the database '''
        print 
        print
        print "Storing blob"
        print
        if (lock_id==None):
            lock_id=id(self)
        # Store my parameters to the database
#        db = self.getParameters().getDB()
        db = self.getDB()
        if (db==None):
            return False
        db.storeDataBlob(self.getParameters(), lock_id, delete_existing=True) # Review is this isn't here get version error

    def update(self):
        '''update is the workhorse method of the model. The model data will be accessed/pulled,
           given to the algorithms, artifacts computed and stored in the model parameters object.'''
        raise NotImplementedError("This method should be overloaded by the subclass.")



class feature_comparison_model(model):
    '''Feature Comparison Model'''

    def __init__(self, uuid, **kwargs):
        ''' Create instance of the feature comparison model
            Input:
                kwargs: various parameters for the feature comparison model
        '''
        # Call super class init first
        model.__init__(self, uuid, **kwargs)

        # Handle to storage for model parameters
        params = self._parameters

        # Set various bits of meta data, these are defaults and can be changed later
        params.setMetaData("model_type", u"feature_comparison_model")
        params.setMetaData("model_desc", u"The most awesome feature comparison model ever")
        params.setMetaData("proximity", u"binary_overlap")
        params.setMetaData("feature_extraction_version", u"2012-05-03:v1") # Review: Fix this

        # Validation fields
        params.addRequiredMetaFields(["proximity","feature_extraction_version"])
        params.addRequiredBinaryFields(["comparison_features.json"])

        # Internal temporary results dictionary (not saved to db)
        __comparison_results = {}


    def prepareComparisonSets(self, feature_list, feature_name):

        # Convert the features into a set per document (dictionary on uuid of sets)
        comparison_sets = defaultdict(set)
        for item in feature_list:
            comparison_sets[item["uuid"]].add(item[feature_name])

        return comparison_sets

    def setComparisonFeatures(self, comparison_features):
        self.setBinaryData("comparison_features.json","application/json",json.dumps(comparison_features))

    def getComparisonResults(self):
        return self.__comparison_results

    def evaluate(self):

        # Handle to storage for model parameters
        params = self._parameters

        # Make sure all my meta data is ready to go
        params.validateMeta()

        # Make sure my model data is ready to go
        self._model_data.validate()

        # Grab the extracted feature version
        feature_extraction_version = params.getMetaData("feature_extraction_version")
        if (not feature_extraction_version):
            logger.error( "Running feature comparison without a known features extraction version!")
            raise RuntimeError("Feature comparison model freak out")

        # Grab the comparison features
        comparison_feature_list = json.loads(params.getBinaryData("comparison_features.json"))
#        if (not comparison_feature_list):
#            logger.error( "Running feature comparison without features to compare against!")
#            raise RuntimeError("Feature comparison model freak out")

        # Quick label lookup
        label_lookup = {}
        for item in comparison_feature_list:
            label_lookup[item["uuid"]] = item["label"]

        # Make a dictionary of sets (key = uuid)
        comparison_sets = self.prepareComparisonSets(comparison_feature_list,"feature")

        # Convert the model data feature table to json and then to dict of sets (will make comparison easier)
        feature_table = self._model_data.getBinaryData("feature_table")
        features_json = utils.convertData(feature_table, "application/x-vtk-table", "application/json")
        feature_list = json.loads(features_json)

        # Make a dictionary of sets (key = uuid)
        feature_sets = self.prepareComparisonSets(feature_list,"text")


        # Okay actually compare features now
        self.__comparison_results = {}

        # Okay now run through all the incoming features sets and compare
        # them to the stored features sets (from labeled data typically)
        for k,v in feature_sets.iteritems():
            my_uuid = k
            my_feature_set = v
            highest_score_overlap = {}
            high_overlap_score = -1.0
            for k2,v2 in comparison_sets.iteritems():
                comp_uuid = k2
                comp_label = label_lookup[k2]
                comp_feature_set = v2
                norm = float(len(comp_feature_set))  # Review: what's the correct normalization
                common = my_feature_set.intersection(comp_feature_set)
                overlap = round(len(common)/norm,2) # Round to the nearest .01
                if (overlap > high_overlap_score):
                    high_overlap_score = overlap
                    highest_score_overlap = {"comp_uuid":comp_uuid, "comp_label":comp_label, \
                                             "overlap":overlap, "features":list(common)}

            # Now capture the highest scoring overlap
            self.__comparison_results[my_uuid] =  highest_score_overlap




    
class db_management_model(model):
# DB Manager The DB Manager provides a mechanism to give a single view to
# potentially multiple databases. Often times, a worker will need to access
# records from multiple databases. This set of databases may be shared across
# multiple workers.
# 
# The first advantage is that multiple databases can be treated as one view.
# This way, code does not have to exist in every single worker to account for
# iterating through various database and view lists. In addition, the code
# supports databases and views on different hosts, making the document retrieval
# process seamless.
# 
# Furthermore, a joint database view simplifies data access across workers. For
# instance, an attribute indexer worker may index all labeled documents from a
# set of databases. In addition, various supervised learning models may use the
# same documents in those databases to form their predictive models. This set of
# databases may change from time to time, and coordinating this between all the
# individual workers can be a tedious task.
# 
# The DB Manager allows a single reference to be passed to all of the worker
# instances. This reference can be changed as needed without modifying all
# dependent workers.
    
    def __init__(self, uuid, **kwargs):
        ''' Create instance of the db management model
            Input:
                kwargs: various parameters
        '''
        # Call super class init first
        model.__init__(self, uuid, **kwargs)

        self._target_db = None
        # Handle to storage for model parameters
        params = self._parameters

        if (kwargs.has_key("target_db_type")):
            target_db_type = kwargs["target_db_type"]
        else:
            target_db_type = None

        if (kwargs.has_key("target_db_host")):
            target_db_host = kwargs["target_db_host"]
        else:
            target_db_host = None
        
        if (kwargs.has_key("target_db_name")):
            target_db_name = kwargs["target_db_name"]
        else:
            target_db_name = None
        
        if (kwargs.has_key("target_db_view_files")):
            target_db_view_files = kwargs["target_db_view_files"]
        else:
            target_db_view_files = []
        
        if (kwargs.has_key("target_db_redirect_dirs")):
            target_db_redirect_dirs = kwargs["target_db_redirect_dirs"]
        else:
            target_db_redirect_dirs = []
        
        if (kwargs.has_key("delete_existing")):
            target_db_delete_on_update = kwargs["delete_on_update"]
        else:
            target_db_delete_on_update = False

        # Set various bits of meta data, these are defaults and can be changed later
        params.setMetaData("model_type", u"db_management_model")
        params.setMetaData("model_desc", u"The most awesome db management model ever")
        params.setMetaData("target_db_type",target_db_type)
        params.setMetaData("target_db_host",target_db_host)
        params.setMetaData("target_db_name",target_db_name)
        params.setMetaData("target_db_view_files",target_db_view_files)
        params.setMetaData("target_db_redirect_dirs",target_db_redirect_dirs)
        params.setMetaData("target_db_delete_on_update",target_db_delete_on_update)

        # Validation fields
        params.addRequiredMetaFields(["target_db_type","target_db_host","target_db_name"])
#        params.addRequiredBinaryFields(["label_table.json"])

        # Validator information
#        params.setMetaData("db_views", ["label_view"])

        # Create the target DB if there is one specified
        self.getTargetDB()
        
        params.setMetaData("datasource_hashes",{})

        if (kwargs.has_key("query_info_list")):
            query_info_list = kwargs["query_info_list"]
        else:
            query_info_list = []

        for query_info in query_info_list:
            self.create_and_add_database(query_info)

        self.setInvalid()
        self.setUpdated(False)

    @staticmethod
    def loadFromJSON(json_data):
        jsontype = json_data["_jsontype"]
        if not (jsontype == "hybrid.model.db_management_model"):
            return None
        name = json_data["name"]
        print json_data
        new_db_management_model = db_management_model(name,**json_data)
        print "Loading db_management_model from DB"
        new_db_management_model.loadFromDB()
        new_db_management_model.setValid()
        
        new_db_management_model.storeToDB()
        
        return new_db_management_model
        
    def get_hash_index(self,db_type,db_host,database_name_to_import,view_uri):
        key = db_type+";"+db_host+";"+database_name_to_import+";"+view_uri
#        key = (db_type,db_host,database_name_to_import,view_uri)
        if (self._datasource_hash_indices.has_key(key)):
            return self._datasource_hash_indices[key]
        else:
            return -1
        

    def create_and_add_database(self,query_info):
        #db_type,db_host,database_name_to_import,query_name):

##        view_hash = [db_type,db_host,database_name_to_import,view_uri,""]
        self.setUpdated(False)

        key = self.getHashKeyFromQueryInfo(query_info)

        datasource_hashes = self.getMetaData("datasource_hashes")
         
        datasource_hashes[key]=()

#         
#         try :
#             logger.info( "Opening database: " , db_host + " " + db_name)
#             new_db = db.init(db_type, host=db_host, database=db_name)
#         #	    self.add_database(new_db,view_uri)
#             return new_db
#         except Exception, e:
#             logger.info(db_name + " not available (continuing, ignore errors): ",e)

    def save_docs(self):
        target_db = self.getTargetDB()
        if (target_db==None):
            return
        print "saving docs",len(self._documents)
        for document in self._documents:
            # Set label database
            document.setDB(target_db)
            
            # Store to label
            try:
                target_db.storeDataBlob(document)
#                document.store(delete_existing=True)
            except Exception,e:   
                print "store data blob wrong",Exception,e
                logger.exception("Error storing document with id " + document.getMetaData("_dataBlobID"))
    
    def getHashKeyFromQueryInfo(self,query_info):
        db_type = encoding.convertToUnicode(query_info.get("db_type"))
        db_host = encoding.convertToUnicode(query_info.get("db_host"))
        db_name = encoding.convertToUnicode(query_info.get("db_name"))
        query_name = encoding.convertToUnicode(query_info.get("query_name",""))
        query_find_list = encoding.convertToUnicode(query_info.get("query_find_list",[]))
        query_except_list = encoding.convertToUnicode(query_info.get("query_except_list",[]))
        
        key = db_type+" "+db_host+" "+db_name+" "+"\"" + query_name+"\" "
        
        key += "\""
        count = 0
        for query_find_item in query_find_list:
            count += 1
            key += query_find_item
            if not(count == len(query_find_list)):
                key += ";" 
        key += "\""

        key += " "

        key += "\""
        count = 0
        for query_except_item in query_except_list:
            count += 1
            key += query_except_item
            if not(count == len(query_except_list)):
                key += ";" 
        key += "\""
        
        return key
                
    def getQueryInfoFromHashKey(self,key):
            key = key.encode('utf-8')

            tokens = shlex.split(key)
            
            query_info = {}
            
            query_info["db_type"] = tokens[0].encode('utf8')
            query_info["db_host"] = tokens[1].encode('utf8')
            query_info["db_name"] = tokens[2].encode('utf8')
            query_info["query_name"] = tokens[3].encode('utf8')

            query_find_list_string = tokens[4].encode('utf8')
            query_except_list_string = tokens[5].encode('utf8')

            if (query_find_list_string==""):
                query_find_list=[]
            else:
                query_find_list = query_find_list_string.split(";")

            if (query_except_list_string==""):
                query_except_list=[]
            else:
                query_except_list = query_except_list_string.split(";")

            query_info["query_find_list"] = query_find_list
            query_info["query_except_list"] = query_except_list
            
            return query_info
        
    def import_dbs(self):
        self.setUpdated(False)
        self.storeToDB()
        
        datasource_hashes = self.getMetaData("datasource_hashes")
        
        self._documents = []
        
        datasource_hash_pairs = datasource_hashes.iterkeys()
        for datasource_key in datasource_hash_pairs:
            query_info = self.getQueryInfoFromHashKey(datasource_key)
            db_type = query_info.get(u"db_type")
            db_host = query_info.get(u"db_host")
            db_name = query_info.get(u"db_name")
            query_name = query_info.get(u"query_name")
            query_find_list = query_info.get(u"query_find_list")
            query_except_list = query_info.get(u"query_except_list")

            try :
                logger.info( "Opening database: " + db_host + " " + db_name)
                new_db = db.init(db_type, host=db_host, database=db_name)

            except Exception, e:
                logger.exception(db_name + " not available (continuing, ignore errors): ")
        
            view_to_add = view.create_view(new_db, query_name, query_find_list, query_except_list)
            view_to_add.rows()
            view_hash = view_to_add.get_hash()
            datasource_hashes[datasource_key] = view_hash
            
            rows = view_to_add.rows()
            for row in rows:
                # Grab the whole document
                row_id = row.getMetaData("_dataBlobID") 
#                doc = new_db.loadDataBlob(row.get("_dataBlobID"), include_binary=True)
            
                # Skip any design documents
                if row_id.startswith(u"_design"): continue
            
                self._documents.append(row)
        
        self.setMetaData("datasource_hashes", datasource_hashes)
        target_db = self.getTargetDB()
        if (target_db==None):
            self.setValid()
            return
        
        self.save_docs() 
        
        # Status
        logger.info( "Transfer complete!")

    


    def getTargetDB(self):
        if self._target_db == None:
            if (self._db_type == None):
                return None
            self.initTargetDB(delete_existing=False)
            
        return self._target_db
    
    def setTargetDB(self,target_db):
        self._target_db = target_db   


    def check_views_unchanged(self):
        datasource_hashes = self.getMetaData("datasource_hashes")
        
        datasource_hash_pairs = datasource_hashes.iteritems()
        for datasource_key,stored_hash in datasource_hash_pairs:
            query_info = self.getQueryInfoFromHashKey(datasource_key)

            db_type = query_info.get(u"db_type")
            db_host = query_info.get(u"db_host")
            db_name = query_info.get(u"db_name")
            query_name = query_info.get(u"query_name")
            query_find_list = query_info.get(u"query_find_list")
            query_except_list = query_info.get(u"query_except_list")

            try :
                print "Opening database: " + db_host + " " + db_name
                logger.debug( "Opening database: " + db_host + " " + db_name)
                new_db = db.init(db_type, host=db_host, database=db_name)
            except Exception, e:
                print db_name + " not available (continuing, ignore errors)",e
                logger.exception(db_name + " not available (continuing, ignore errors)")
                return False
        
            db_view = view.create_view_from_query_info(query_info)
            
#            db_view = view.create_view(new_db, query_name, query_find_list, query_except_list)
            rows=db_view.rows()
            
            view_hash = db_view.get_hash()
#            print "view_hash",view_hash
            
            
#            print "utils.tuples_to_lists(stored_hash)",utils.tuples_to_lists(stored_hash)
#            print "utils.tuples_to_lists(view_hash)",utils.tuples_to_lists(view_hash)
            # Had weird problem here where one hash was a tuple and one was a list. FIXME
            if not(utils.tuples_to_lists(stored_hash)==utils.tuples_to_lists(view_hash)):
#                 print "stored_hash"
#                 print utils.tuples_to_lists(stored_hash)
#                 print
#                 print
#                 print
#                 print "view_hash"
#                 print utils.tuples_to_lists(view_hash)
#                 
                self.getParameters().setMetaData("updated",False)
                return False

        return True

    def initTargetDB(self,**kwargs):
        params = self._parameters
        
        target_db_type = params.getMetaData("target_db_type")
        target_db_host = params.getMetaData("target_db_host")
        target_db_name = params.getMetaData("target_db_name")
        target_db_view_files = params.getMetaData("target_db_view_files")
        target_db_redirect_dirs = params.getMetaData("target_db_redirect_dirs")

        delete_existing = kwargs.get("delete_existing")

            
        
        if (delete_existing == True):
            try:
                self._target_db = db.init(target_db_type, host=target_db_host, database=target_db_name,
                                         create=False,push_views=False)
                self._target_db.delete(host=target_db_host, database=target_db_name)
                logger.info( "Creating database: " + target_db_name)
            except:
                self._target_db =None

        
        self._target_db = db.init(target_db_type, host=target_db_host, database=target_db_name, 
                                     view_files=target_db_view_files,
                                     redirect_view_dirs=target_db_redirect_dirs,
                                     create=True,push_views=True)
                
    def update(self):
        params = self._parameters
        if (self.check_views_unchanged()==True and self.isUpdated()):
            print "===Asked to update, but not gonna do it. because views are unchanged and the model is updated"
            return

        if (params.hasMetaData("target_db_delete_on_update")):
            target_db_delete_on_update = params.getMetaData("target_db_delete_on_update")
        else:
            target_db_delete_on_update=False
        
        if not(params.getMetaData("target_db_type") == None):
            self.initTargetDB(delete_existing=target_db_delete_on_update)
            
        self.import_dbs()

        self.finalize()
                
    def getRows(self):
        targetDB = self.getTargetDB()
        if targetDB is None:
            model_observations=[]
            datasource_hashes = self.getMetaData("datasource_hashes")
            
            datasource_hash_pairs = datasource_hashes.iteritems()
            for datasource_key,stored_hash in datasource_hash_pairs:
                query_info = self.getQueryInfoFromHashKey(datasource_key)
    
                db_type = query_info.get(u"db_type")
                db_host = query_info.get(u"db_host")
                db_name = query_info.get(u"db_name")
                query_name = query_info.get(u"query_name")
                query_find_list = query_info.get(u"query_find_list")
                query_except_list = query_info.get(u"query_except_list")
    
                try :
                    print "Opening database: " + db_host + " " + db_name
                    logger.debug( "Opening database: " + db_host + " " + db_name)
                    new_db = db.init(db_type, host=db_host, database=db_name)
                except Exception, e:
                    print db_name + " not available (continuing, ignore errors)",e
                    logger.exception(db_name + " not available (continuing, ignore errors)")
                    return False
            
                db_view = view.create_view_from_query_info(query_info)
                
    #            db_view = view.create_view(new_db, query_name, query_find_list, query_except_list)
                rows=db_view.rows()
                
                model_observations=model_observations+rows
        else:
            model_observations_view = view.create_view(targetDB, "", [], [])
            model_observations = model_observations_view.rows()
             
        return model_observations
                 
    #         targetDB = self.getTargetDB()
#         model_observations_view = view.create_view(targetDB, "", [], [])
#         model_observations = model_observations_view.rows()
#         return model_observations
        
    
class label_management_model(model):
    '''In general the management of labels has been complex and
       error prone, thus the label management model. '''

    def __init__(self, uuid, **kwargs):
        ''' Create instance of the label model
            Input:
                kwargs: various parameters
        '''
        # Call super class init first
        model.__init__(self, uuid, **kwargs)

        # Handle to storage for model parameters
        params = self._parameters

        # Set various bits of meta data, these are defaults and can be changed later
        params.setMetaData("model_type", u"label_management_model")
        params.setMetaData("model_desc", u"The most awesome label management model ever")
        params.setMetaData("label_table_list", [])
        params.setMetaData("stored_hash", u"0")
        params.setMetaData("new_labels", True)

        # Validation fields
        params.addRequiredMetaFields(["label_table_list","stored_hash","new_labels"])
        params.addRequiredBinaryFields(["label_table.json"])

        # Validator information
        params.setMetaData("db_views", ["label_view"])

    def checkForNewLabels(self):
        return self.getMetaData("new_labels")

    def update(self):

        # Handle to storage for model parameters
        params = self._parameters

        # Make sure all my meta data is ready to go
        params.validateMeta()

        # Make sure my model data is ready to go
        self._model_data.validate()
        self._model_data.validateViews(self.getMetaData("db_views"))

        # Grab label table
        label_table = self._model_data.getBinaryData("label_view")

        # Convert the label table to json and then to a python list of dicts (will make comparison easier)
        label_table_json = utils.convertData(label_table, "application/x-vtk-table", "application/json")
        label_table_list = json.loads(label_table_json)
        current_hash = unicode(hashlib.sha1(label_table_json).hexdigest(),"utf-8")
        stored_hash = params.getMetaData("stored_hash")
        if (current_hash != stored_hash):
            params.setMetaData("stored_hash", current_hash)
            params.setMetaData("new_labels", True)
        else:
            params.setMetaData("new_labels", False)


        # In memory storage of label information
        params.setMetaData("label_table_list", label_table_list)
        params.setBinaryData("label_table.json","application/json", label_table_json)


    def injectUnknownExemplarLabels(self, data_db, centroid_uuids):

        # First remove any existing unknown labels
        self.removeAllUnknownLabels()

        # Review 2: Not handling cluster overlap yet

        # Simply loop over the documents and push into the label database
        #model_observations_db = self.getParameters().getDB()
        model_observations_db = self.getDB()
        if (db==None):
            return False
        for uuid in centroid_uuids:

            # Grab the whole document
            doc = data_db.loadDataBlob(uuid, include_binary=True, must_exist=False)

            # The document might not exist if it was pulled from label database
            if (not doc):
                continue

            # Add the unknown label
            doc.setMetaData("label", u"unknown")
            doc.setMetaData("description", u"auto-generated centroid label")

            # Set label database
            doc.setDB(model_observations_db)

            # Store to label database
            doc.store(delete_existing=True)

        # Re-pull label view and update yourself so you are totally synced with the changes just made
        self.getModelData().update()
        self.update()

        return True


    def removeAllUnknownLabels(self):

        # Remove everything that is labeled as "unknown"
        label_table_list = self.getParameters().getMetaData("label_table_list")

        # Loop through rows removing unknown labels
        # db = self.getParameters().getDB()
        db = self.getDB()
        if (db==None):
            return False
        for row in label_table_list:

            # Grab the document and look at the label
            doc = db.loadDataBlob(row["uuid"])
            doc_meta = doc.getMetaDataDict()

            # Wack it
            if (doc_meta["label"] == "unknown"):
                db.deleteDataBlob(row["uuid"])

        # Re-pull label view and update yourself so you are totally synced with the changes just made
        self.getModelData().update()
        self.update()
        
        return True

    def printLabels(self):

        # Lets look at the label list and see if we have collisions
        # for unknown labels with known labels for the same cluster id
        label_table_list = self.getMetaData("label_table_list")

        for label in label_table_list:
            logger.info( label["label"] + ": cluster(" + label["cluster"] + ")")

    def _remove_unknowns(self, label_list):

        # If you only have one item on the list; just return
        if (len(label_list) == 1):
            return label_list

        # Grab the database handle
        db = self.getDB()
        if (db==None):
            return None

        # Build a keep_list, note: always keep at least one (even if unknown)
        keep_list = []
        for label_info in label_list:
            if (label_info["label"] != "unknown"):
                keep_list.append(label_info)
        if (len(keep_list) == 0):
            keep_list.append(label_list[0])

        # Remove unknowns
        for label_info in label_list:
            if (label_info not in keep_list):
                db.deleteDataBlob(label_info["uuid"])
                ''' Review: This should be the right thing to do
                doc = db.loadDataBlob(label_info["uuid"])
                doc_meta = doc.getMetaDataDict()
                del doc_meta["label"]
                doc.store() # Fixme: not using batch="ok")
                '''
                logger.info( "Removing collision unknown label for:" + label_info["uuid"])

        # Okay done
        return keep_list


    def removeOverlappingUnknowns(self):

        # Lets look at the label list and see if we have collisions
        # for unknown labels with known labels for the same cluster id
        label_table_list = self.getMetaData("label_table_list")

        # MultiMap for label collision detection
        multi_map = defaultdict(list)

        # Loop through rows seeing if we have any label 'collisions'
        for label in label_table_list:
            multi_map[label["cluster"]].append({"label":label["label"],"uuid":label["uuid"]})

        # Now list out cluster/label map
        logger.info( "<<<Before Removal>>>")
        for cluster,label_list_dict in multi_map.iteritems():
            label_list = []
            for label_info in label_list_dict:
                label_list.append(label_info["label"])
            labels = ", ".join(label_list)
            logger.info( "Labels Cluster(",cluster,"):",labels)

        # Now remove any unknown label collisions
        for cluster,label_list in multi_map.iteritems():
            multi_map[cluster] = self._remove_unknowns(label_list)


        # Now list out cluster/label map
        logger.info( "<<<After Removal>>>")
        for cluster,label_list_dict in multi_map.iteritems():
            label_list = []
            for label_info in label_list_dict:
                label_list.append(label_info["label"])
            labels = ", ".join(label_list)
            logger.info( "Labels Cluster(" + str(cluster) + "):" + labels)

        # Re-pull label view and update yourself so you are totally synced with the changes just made
        self.getModelData().update()
        self.update()




''' Storage Code '''
'''
def markKnownLabelsForOffline(self):

    # Handle to storage for model parameters
    params = self._parameters

    # Make sure that all labels are marked for offline processing
    label_table_list = params.getMetaData("label_table_list")

    # Loop through all labels marking for offline
    db = self.getParameters().getDB()
    for row in label_table_list:

        # Grab the document and delete the label
        doc = db.loadDataBlob(row["uuid"])
        doc_meta = doc.getMetaDataDict()
        if (doc_meta["label"] != "unknown"):
            doc_meta["view_offline_process"] = True
            doc.store() # Fixme: not using batch="ok")
            logger.debug( "Marked", row["uuid"],"for offline processing")

    # Re-pull label view and update yourself so you are totally synced with the changes just made
    self.getModelData().update()
    self.update()
'''

'''

'''

'''
# Use the clustering centriods to create exemplar labels ("unknown" labels)
def createExemplarLabels(self, clustering, label_list_info):


    # Okay all the shiz is so we can track which clusters already have labels
    # Create a set of uuids
    label_set = set()
    for item in label_list_info:
        label_set.add(item["uuid"])
    cluster_results_table = clustering.getBinaryData("cluster_results.vtk")
    top_cluster_array = cluster_results_table.GetRowData().GetArray("cluster_0")
    cluster_uuid_array = cluster_results_table.GetRowData().GetAbstractArray("uuid")
    cluster_map = {}
    for i in range(cluster_uuid_array.GetNumberOfTuples()):
        if (cluster_uuid_array.GetValue(i) in label_set):
            cluster_map[top_cluster_array.GetValue(i)] = i

    # Centroid info
    centroid_info_table = clustering.getBinaryData("clustering_centroids.vtk")
    centroid_uuid_array = centroid_info_table.GetRowData().GetAbstractArray("uuid")

    # Add the unknown labels for the docs in the db
    db = self.getDB()
    for i in range(centroid_uuid_array.GetNumberOfTuples()):

        # First check to see if I have a collision with an existing "known" label
        if (i in cluster_map):
            continue

        # Set the document label to "unknown"
        doc = db.loadDataBlob(centroid_uuid_array.GetValue(i))
        if ('label' not in doc.getMetaDataDict()):
            doc.setMetaData("label",u"unknown")
            doc.store() # Fixme: not using batch="ok")
            logger.debug( centroid_uuid_array.GetValue(i), "set to unknown")

    # Re-pull label view and update yourself so you are totally synced with the changes just made
    self.getModelData().update()
    self.update()
'''
