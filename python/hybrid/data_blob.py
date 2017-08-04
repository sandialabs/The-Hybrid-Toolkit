"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import base64
import datetime
import logging
import sys
from uuid import uuid4

import encoding
import utils

try:
    from vtk import *
except ImportError:
    pass

try:
    from titan.DataAnalysis import *
    from titan.TextAnalysis import *
    from titan.Common import *
    from titan.ClusteringFilters import *
except ImportError:
    pass

logger = logging.getLogger(__name__)


def create(uuid=None, **kwargs):
    """ Create a data blob of a particular type.
        Input:
            uuid:   unique id (unique name) for the data blob (string)
            kwargs: optional object creation parameters
                 subclass: data blob subclass type (string)
    """
    subclass = kwargs.get('subclass', None)
    if subclass is None:
        return data_blob(uuid, **kwargs)
    elif subclass == 'view':
        return view(uuid, **kwargs)
    else:
        raise TypeError("Cannot create data blob of subclass: %s" % (subclass))


def dict2blob(in_dict, **kwargs):
    if "_dataBlobID" in in_dict:
        blob_id = in_dict["_dataBlobID"]
    else:
        blob_id = uuid4().hex
    # blob = data_blob(in_dict["_dataBlobID"], **kwargs)
    blob = data_blob(blob_id, **kwargs)
    blob_id = blob.getDataBlobUUID()
    # Set the id and revision
    # blob.setDataBlobUUID(in_dict["_dataBlobID"])
    rev = None
    if "_rev" in in_dict:
        rev = in_dict.get("_rev")
    if rev is not None:
        blob.setDataBlobRevision(rev)

    # Loop through the meta data
    for k, v in in_dict.iteritems():
        # Don't include attachments if they are being pulled inline
        if k == "_attachments":
            continue
        if k == "_dataBlobID":
            continue
        blob.setMetaData(k, v)

    # Grab the attachments
    if "_attachments" in in_dict:
        for k, v in in_dict["_attachments"].iteritems():
            blob.setBinaryData(k, v["content_type"],
                               base64.b64decode(v["data"]))

    # Now validate the data blob
    blob.validate(**kwargs)

    # Return the data blob
    return blob


def blob2dict(blob):
    # Validate the data blob
    blob.validate()

    # Every data blob must have certain fields enforced by the
    # data blob super class
    uuid = blob.getDataBlobUUID()
    rev = blob.getDataBlobRevision()
    data_blob_type = blob.getDataBlobType()
    date_string = blob.getCreationDateString()

    # Create the document and add a few special fields
    out_dict = {"type": data_blob_type, "created": date_string}

    creation_datetime = blob.getCreationDate()
    datetime_dict = {"year": creation_datetime.year, "month": creation_datetime.month, "day": creation_datetime.day,
                     "hour": creation_datetime.hour, "minute": creation_datetime.minute,
                     "second": creation_datetime.second, "microsecond": creation_datetime.second}
    out_dict["creation_datetime"] = datetime_dict

    # Check for uuid not set (could happen when the object is first created)
    if uuid is not None:
        out_dict["_dataBlobID"] = uuid

    # Now put all the meta data into the document
    for k, v in blob.getMetaDataDict().iteritems():
        out_dict[k] = v

    attachment_dict = {}
    for k, v in blob.getBinaryDataDict().iteritems():
        attachment_dict[k] = {"content_type": v["mime_type"],
                              "data": base64.b64encode(v["data"])}

    if attachment_dict != {}:
        out_dict["_attachments"] = attachment_dict

    return out_dict


class data_blob():
    """Superclass for a data_blob. A data_blob is basically two python dictionaries
       (one for meta, one for binary data) with some API and bookkeeping.
       A blob interacts with a database (views and as a backing store), a blob
       also has validating logic, serialization and deserialization. """

    def __init__(self, uuid, **kwargs):
        """ Create instance of data blob
            Input:
                kwargs: various data blob parameters for the data blob
        """

        # Various bits of meta data
        self._type = 'unknown'
        self._uuid = uuid
        self._rev = 0
        self._kwargs = kwargs
        self._creation_date = datetime.datetime.utcnow()

        # Initialize the database handle
        self._db = None

        # Internal database view information
        self._db_view_info = {}

        # Internal artifacts dictionaries
        self._meta_data = {"_dataBlobID": uuid}
        self._binary_data = {}

        # Validator information
        self._required_meta_fields = []
        self._required_binary_fields = []

    def str_conversion(self, value):
        return_string = ""
        #        print "====in string conversion"
        if type(value) is list:
            #            print "====list"
            return_string += "<<<List>>>\n"
            for item in value:
                return_string += "\t\t[" + encoding.convertToUnicode(self.str_conversion(item)) + "]\n"
        elif type(value) is dict:
            #            print "====dict"
            return_string += "<<<Dict>>>\n"
            for key, value in value.iteritems():
                #                print "\tTrying to convert:",key,value
                return_string += "\t\t[" + encoding.convertToUnicode(key) + "] = " + encoding.convertToUnicode(
                    self.str_conversion(value)) + "\n"
        else:
            #            print "====else",value
            return_string += encoding.convertToUnicode(value) + "\n"
        # print "Finished converting",value
        return return_string

    def __str__(self):
        '''Prints out the various fields in the data blob'''
        return_string = ""
        return_string += "DataBlob UUID: %s\n" % (self._uuid)
        return_string += "DataBlob Revision: %s\n" % (self._rev)
        return_string += "DataBlob Type: %s\n" % (self._type)
        return_string += "Creation Time: %s\n" % (self.getCreationDateString())
        return_string += "Meta data\n"
        for k, v in self._meta_data.iteritems():
            print "     :::Converting:", k, v
            return_string += "\t\t[" + encoding.convertToUnicode(k) + "] = " + self.str_conversion(v)
            print "     :::Finished conversion", k, v

        return_string += "Binary data\n"
        for k, v in self._binary_data.iteritems():
            # Sanity check
            print "     :::binary iteritems"
            print k, v
            if v["data"] is None:
                return_string += "\t[ " + encoding.convertToUnicode(k) + " ] ( Empty!!! )"
            else:
                return_string += "\t[ " + encoding.convertToUnicode(k) + " ] ( " + encoding.convertToUnicode(
                    v["mime_type"]) + " ) = binary object of length: " + encoding.convertToUnicode(
                    len(v["data"])) + "\n"
        print
        print
        print
        print return_string
        return return_string

    def setDataBlobUUID(self, uuid):
        """ Set the data blob uuid """
        self._uuid = uuid
        self._meta_data["_dataBlobID"] = uuid

    def getDataBlobUUID(self):
        """ Get the data blob uuid """
        return self._uuid

    def setDataBlobRevision(self, rev):
        """ Set the data blob revision """
        self._rev = rev

    def getDataBlobRevision(self):
        """ Get the data blob revision """
        return encoding.convertToUnicode(self._rev)

    def setDataBlobType(self, data_type):
        """ Set the data blob type """
        self._type = data_type

    def getDataBlobType(self):
        """ Get the data blob type """
        return self._type

    def hasMetaDataValue(self, complete_value_string, **kwargs):
        field_array = complete_value_string.rsplit(':', 1)

        # TODO modify to accept base level dicts and lists
        if len(field_array) < 2:
            logger.warning("hasMetaDataValue() requires a complete, colon delimited, key value pairing.")
            return False

        value = utils.get_field_value(self._meta_data, field_array[0], {})
        if value is None:
            return False
        else:
            if isinstance(value, dict):
                for value_iterkey in value.iterkeys():
                    if field_array[1] == value_iterkey:
                        return True
            if isinstance(value, list):
                for item in value:
                    if field_array[1] == encoding.convertToUnicode(item):
                        return True
            if field_array[1] == encoding.convertToUnicode(value):
                return True

        return False

    def hasMetaData(self, field, **kwargs):
        field_array = field.rsplit(':', 1)
        if len(field_array) > 1:
            f = utils.get_field_value(self._meta_data, field_array[0], {})
            if f is None:
                return False
            else:
                return field_array[1] in f
        else:
            if field in self._meta_data:
                return True
            return False

    def __getitem__(self, field, mime_type=None):
        if self.hasMetaData(field):
            return self.getMetaData(field)
        elif self.hasBinaryData(field):
            return self.getBinaryData(field, mime_type)
        else:
            raise KeyError("No meta or binary data field", field)

    def __setitem__(self, field, value, mime_type=None):
        if self.hasMetaData(field):
            self.setMetaData(field, value)
        elif self.hasBinaryData(field):
            self.setBinaryData(field, mime_type, value)
        else:
            raise KeyError("No meta or binary data field", field)

    def addMetaData(self, field, value, **kwargs):
        ''' Add the meta data to the data blob if it isn't there. If they meta data key
        already exists, however, then this means there is a data field collision, and raise an exception '''
        if self.hasMetaData(field):
            return False
        self.setMetaData(field,
                         value)  # the key wasn't in there already, so just add it to the observation and set the value
        return True

    ''' Add meta data to the data blob, regardless of whether it has the field already '''

    def setMetaData(self, field, value, **kwargs):

        if field == "_dataBlobID":
            self.setDataBlobUUID(value)
            return

        field_array = field.rsplit(':', 1)
        if len(field_array) > 1:
            # For nested fields, add an empty dictionary for the higher level nested containers.
            # If they already exist, adding them will do nothing and return false
            self.addMetaData(field_array[0], {})

            # At this point, we are sure that the previous containers exist
            meta_data_dict = utils.get_field_value(self._meta_data, field_array[0], {})
            final_field = field_array[1]
        else:
            meta_data_dict = self._meta_data
            final_field = field

        # meta_data_dict[final_field] = encoding.convertToUnicode(value)
        meta_data_dict[final_field] = value

    #
    #     ''' Add meta data to the data blob, regardless of whether it has the field already '''
    #     def setMetaData(self, field, value,**kwargs):
    #         field_array = field.rsplit(':',1)
    #         if (len(field_array)>1):
    #             suffix_array = field_array[0].rsplit(':',1)
    #             if (len(suffix_array)>1):
    #                 prev_dict = utils.get_field_value(self._meta_data, suffix_array[0], {})
    #                 nested_dict_name = suffix_array[1]
    #             else:
    #                 prev_dict = self._meta_data
    #                 nested_dict_name = suffix_array[0]
    #             if (prev_dict==None):
    #                 print "Field does not exist:",suffix_array[0]
    #
    #
    #             # At this point, the previous dictionary exists. If the final dictionary doesn't exist, create it
    #             if not(nested_dict_name in prev_dict):
    #                 prev_dict[nested_dict_name]={}
    #
    #             prev_dict[nested_dict_name][field_array[1]]=value
    #         else:
    #             self._meta_data[field] = value
    #

    def getMetaDataSafe(self, field, **kwargs):
        if self.hasMetaData(field):
            return self.getMetaData(field)
        else:
            return None

    def getMetaData(self, field, **kwargs):
        field_array = field.rsplit(':', 1)
        if len(field_array) > 1:
            f = utils.get_field_value(self._meta_data, field_array[0], {})
            if f is None:
                raise KeyError("No meta data field", field)
            if isinstance(f, dict):
                return f[field_array[1]]
            if isinstance(f, list):
                return f[int(field_array[1])]
            else:
                raise KeyError("Weird data field", field)
        else:
            return self._meta_data[field]

        ''' Get meta data from the data blob '''
        if field in self._meta_data:
            return self._meta_data[field]
        else:
            raise KeyError("No meta data field", field)

    def deleteMetaData(self, field, **kwargs):

        if field == "_dataBlobID":
            return False

        field_array = field.rsplit(':', 1)
        if len(field_array) > 1:
            f = utils.get_field_value(self._meta_data, field_array[0], {})
            if f is None:
                return False
            else:
                del f[field_array[1]]
                return True
        else:
            if field in self._meta_data:
                del self._meta_data[field]
                return True
            return False

    def setBinaryData(self, field, mime_type, data):
        """ Add binary data to the data blob, if the data is a vtk object then
            it will be serialized to the specified mime_type and then stored """

        # Is the object serializable
        if utils.canSerialize(data):
            serialize_data = utils.serializeData(data, mime_type)

        # Or if I can cast to str than just store it
        elif type(data) == str:
            serialize_data = data

        # Using utf-8 for the serialization of unicode
        elif type(data) == unicode:
            #            serialize_data = encoding.convertToUTF8(data)
            serialize_data = data

        else:
            logger.error("I don't know how to serialize this object " + field + ' ' + mime_type + ' ' +
                         "\nI received data of type:" + type(data) + " perhaps convert to string first?")
            raise RuntimeError("Serialization Error")

        # Store the serialized data
        data = {"mime_type": mime_type, "data": serialize_data}
        self._binary_data[field] = data

    def getBinaryDataMimeType(self, field):
        """
        Get binary mime type from the data blob
        """
        if field in self._binary_data:
            return self._binary_data[field]["mime_type"]
        else:
            raise KeyError("No binary data field", field)

    def hasBinaryData(self, field):
        """
        Just a check on a binary field from the data blob
        """
        # Do we have the binary field
        return (field in self._binary_data)

    def getBinaryData(self, field, mime_type=None):
        """
        Get a binary field from the data blob
        Input:
                mime_type: specifies that the object should be serialized in that mime_type,
                           leave unspecified for a de-serialized object
        """

        # logger.debug("Looking for binary field %s with value %s" % (field, self._binary_data))

        # Do we have the binary field
        if field in self._binary_data:
            # If they haven't specified a mime type then de-serialize the object and return
            if mime_type is None:
                if self._binary_data[field]["mime_type"] in ["application/x-vtk-table", "application/x-vtk-graph",
                                                             "application/x-vtk-array-data",
                                                             "application/x-vtk-data-object"]:

                    return utils.deserializeData(self._binary_data[field]["data"],
                                                 self._binary_data[field]["mime_type"])
                else:
                    logger.debug("returning data")

                    return self._binary_data[field]["data"]
            # If they have specified the mime type and its the same as the storage just return
            elif mime_type == self._binary_data[field]["mime_type"]:
                return self._binary_data[field]["data"]

            # They want a different mime type so convert
            else:
                source_data = self._binary_data[field]["data"]
                source_mime_type = self._binary_data[field]["mime_type"]
                target_mime_type = mime_type
                return utils.convertData(source_data, source_mime_type, target_mime_type)

        else:
            raise KeyError("No binary data field", field)

    def getMetaDataDict(self):
        """
        Returns the meta data dictionary
        """
        return self._meta_data

    def setMetaDataDict(self, meta_data):
        """
        set the meta data dictionary
        """
        self._meta_data = meta_data.copy()
        return

    def getBinaryDataDict(self):
        """
        Returns the binary data dictionary
        """
        return self._binary_data

    def getCreationDate(self):
        return self._creation_date

    def getCreationDateString(self):
        return self._creation_date.strftime("%Y-%m-%d %H:%M")

    def resetRequiredBinaryFields(self):
        self._required_binary_fields = []

    def addRequiredBinaryFields(self, field_array):
        self._required_binary_fields.extend(field_array)

    def getRequiredBinaryFields(self):
        return self._required_binary_fields

    def resetRequiredMetaFields(self):
        self._required_meta_fields = []

    def addRequiredMetaFields(self, field_array):
        self._required_meta_fields.extend(field_array)

    def getRequiredMetaFields(self):
        return self._required_meta_fields

    def compareData(self, other_blob):
        if not self.compareMetaData(other_blob):
            return False

        if not self.compareBinaryData(other_blob):
            return False

        return True

    def compareMetaData(self, other_blob):
        meta_data_dict = self.getMetaDataDict()
        other_meta_data = other_blob.getMetaDataDict()

        if not (len(meta_data_dict) == len(other_meta_data)):
            return False

        for k, v in meta_data_dict.iteritems():
            if k.startswith('_'):
                continue
            if k == "created":
                continue
            if k == "creation_datetime":
                continue
            if not (other_meta_data.has_key(k)):
                return False
            if not (other_meta_data[k] == v):
                print other_meta_data[k], "not", v
                return False

        return True

    def compareBinaryData(self, other_blob):
        binary_data = self.getBinaryData()
        other_binary_data = other_blob.getBinaryData()

        if not (len(binary_data) == len(other_binary_data)):
            return False

        for k, v in binary_data.iteritems():
            if not (other_binary_data.has_key(k)):
                return False
            if not (other_binary_data[k] == v):
                return False

        return True

    def validateMeta(self):
        """
        Validate the meta data fields in the data blob, and make sure they have sane values.
           For more advanced validation a subclass can overwrite this method
        """
        self.getDataBlobUUID()
        self.getDataBlobType()
        self.getCreationDate()
        for field in self.getRequiredMetaFields():
            self.getMetaData(field)

        # Do not allow non unicode strings
        meta = self.getMetaDataDict()
        for k, v in meta.iteritems():
            if isinstance(v, str):
                self.convertStringsToUnicode(False)
                break
                # logger.error("DataBlob contains non-unicode value at key:",k,"value:",v)
                # raise TypeError("Detected non-Unicode string.. not happy!...", k,":",v)

        # Couch freaks out on field names that start with an underscore
        meta = self.getMetaDataDict()
        names_to_fix = []
        for k, v in meta.iteritems():
            if ((k[0] == "_") and (k != "_id") and (k != "_dataBlobID") and (k != "_rev") and (
                        k != "_attachments") and (k != "_deleted")):
                names_to_fix.append(k)

        for name in names_to_fix:
            logger.warning(
                self.getDataBlobUUID() + "(" + self.getDataBlobType() + ") Validation error: Field name that starts with an underscore " + name + " changing to err " + name)
            data = meta[name]
            del meta[name]
            meta["err" + name] = data

    def validateBinary(self):
        """
        Validate the binary fields in the data blob, and make sure they have sane values.
           For more advanced validation a subclass can overwrite this method
        """
        for field in self.getRequiredBinaryFields():
            self.getBinaryDataMimeType(field)
        for field in self.getRequiredBinaryFields():
            blob = self.getBinaryData(field)
            if blob is None:
                raise RuntimeError("Empty binary field:" + field)

        # Couch freaks out on field names that start with an underscore
        binary = self.getBinaryDataDict()
        names_to_fix = []
        for k, v in binary.iteritems():
            if k[0] == "_":
                names_to_fix.append(k)

        for name in names_to_fix:
            logger.warning(
                self.getDataBlobUUID() + "(" + self.getDataBlobType() + ") Validation error:" + "Field name that starts with an underscore" + name + "changing to err" + name)
            data = binary[name]
            del binary[name]
            binary["err" + name] = data

    def validateDB(self):
        """
        Validate the database for the data blob. Not having one is okay, but if a load
           or store is invoked a RuntimeError will be thrown
        """
        # Validate my db existence
        if self._db is None:
            return  # A data_blob may not have a db, that is fine
        self._db.getInfo()

    def validateViews(self, view_array):
        """
        Validate the views for the data blob. This method is called by code
           using the blob and needing a set of required views to be provided
        """
        # Make sure the views are there
        for view in view_array:
            if view not in self._db_view_info:
                raise RuntimeError("Required view not present", view)
            view_url = self._db_view_info[view]["url"]
            self._db.loadView(view_url, limit=0)  # Don't need to actually get the data

    def validate(self, **kwargs):
        """
        Validate all of the fields in the data blob, and make sure they have sane values.
           For more advanced validation a subclass can overwrite this method
        """
        try:
            self.validateMeta()
            self.validateBinary()
            self.validateDB()
        except:
            e = sys.exc_info()[1]

            validate_exceptions_okay = kwargs.get('validate_exceptions_okay', None)
            if validate_exceptions_okay:
                logger.warning(self.getDataBlobUUID() + "(" + self.getDataBlobType() + ") Validation error:" + str(e))
            else:
                logger.exception(self.getDataBlobUUID() + "(" + self.getDataBlobType() + ") Validation error:")
                raise

    def convertStringsToUnicode(self, verbose=True):
        """
        Validate the meta data fields in the data blob, and make sure they have sane values.
           For more advanced validation a subclass can overwrite this method
        """
        self.getDataBlobUUID()
        self.getDataBlobType()
        self.getCreationDate()
        for field in self.getRequiredMetaFields():
            self.getMetaData(field)

        # Convert all non unicode strings
        meta = self.getMetaDataDict()
        for k, v in meta.iteritems():
            if isinstance(v, str):
                if verbose:
                    logger.info("DataBlob contains non-unicode value at key:" + k + " value: " + v + "converting...")
                v = encoding.convertToUnicode(v)

    def setDB(self, db):
        """
        Set the DB for the data blob
        """
        self._db = db

    def getDB(self):
        """
        Get the DB for the data blob
        """
        return self._db

    def addView(self, view_alias, view_url, view_column_info, **view_kwargs):
        """
        Add a view alias dictionary entry (view_alias,blah blah
        """
        self._db_view_info[view_alias] = {"url": view_url, "column_info": view_column_info, "kwargs": view_kwargs}

    def getViewURL(self, view_alias):
        return self._db_view_info[view_alias]["url"]

    def getViewColumnInfo(self, view_alias):
        return self._db_view_info[view_alias]["column_info"]

    def getViewArguments(self, view_alias):
        return self._db_view_info[view_alias]["kwargs"]

    def update(self, **kwargs):
        """
        Prepare the data blob for use. This method will pull any of the specified views
            and store a 'snap shot' of those views as internal data. This ensures consistant
            data as the data blob is used by various consumers
        """
        for view_alias in self._db_view_info:
            table = self._loadViewAsTable(view_alias)
            self.setBinaryData(view_alias, "application/x-vtk-table", table)

    def store(self, **kwargs):
        """
        Store the data blob into the database
        """

        rev = self.getDataBlobRevision()

        self.setDataBlobRevision(rev + 1)
        # Make sure I have a db handle
        if self._db is None:
            raise RuntimeError("No db handle to store data_blob!")

        self._db.storeDataBlob(self, **kwargs)

    def _loadViewAsTable(self, view_alias):
        """
        This internal method should not be called by an outside user
            The method loads a view from a database in the form of a table
            Input:
               view_name: name of the database view (string)
               name_and_type_dict: dictionary of column names and types that are expected to be in the resulting table
                  Example: {"uuid":vtkStringArray(),"text":vtkUnicodeStringArray(),"label":vtkStringArray()}
        """
        if view_alias not in self._db_view_info:
            raise RuntimeError("No view", view_alias, "in data_blob!")

        view_url = self._db_view_info[view_alias]["url"]
        column_info = self._db_view_info[view_alias]["column_info"]
        view_kwargs = self._db_view_info[view_alias]["kwargs"]
        view = self._db.loadView(view_url, **view_kwargs)
        return utils.viewToVTKTable(view, column_info, autofill=True)  # Fixme: is autofill always the right thing?


class view(data_blob):
    """
    View data blob
    """

    def __init__(self, uuid, **kwargs):
        """
        Create instance of database view object
            Input:
                kwargs: various database view parameters
        """
        # Call super class init first
        data_blob.__init__(self, uuid, **kwargs)

        # Various bits of meta data
        self.setDataBlobType("view")

        # Validator fields
        self.addRequiredMetaFields(["language", "views"])
        self.addRequiredBinaryFields([])
