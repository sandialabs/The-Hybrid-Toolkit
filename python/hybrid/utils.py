'''Utility methods.'''
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import copy
import datetime
import importlib
import json
import math
import os
import sys

from pkg_resources import resource_filename

import encoding

try:
    from vtk import *
except ImportError:
    pass

try:
    from titan.Web import *
    from titan.DataAnalysis import *
    from titan.TextAnalysis import *
    from titan.Common import *
    from titan.ClusteringFilters import *
except ImportError:
    pass

# Splunk stuff
import socket, struct

try:
    import syslog
except ImportError:
    pass

# HTML stuff
from HTMLParser import HTMLParser

# Zip/compress stuff
import string
import bz2

# Hash stuff
import hashlib

import hybrid.db
import data_blob
import logging

import subprocess
import re

logger = logging.getLogger(__name__)

_special_chars = re.compile('[\\r\\n\\t]+')


def cleanup(process_list):
    for p in process_list:  # list of your processes
        p.kill()  # supported from python 2.6
    logger.debug("Processes cleaned up")
    logger.info("Testing Script Complete!")


def launchProcess(args, wait=True):
    #    logger.info( "Launching Process: ", args[1:])

    p = None
    try:
        if wait:
            subprocess.check_call(args)
        else:
            p = subprocess.Popen(args)
    except:  # Catch all exceptions
        e = sys.exc_info()[1]
        #        logger.error( "Process:", args[1:])
        #        logger.error( "Exception:", e)
        sys.exit()
    return p


def str_to_class(class_prefix, class_string):
    print "class_prefix", class_prefix
    print "class_string", class_string
    return reduce(getattr, class_string.split("."), class_prefix)


def replace_pkg_strs(item, pkg_name=None):
    if isinstance(item, basestring):
        m = re.search('\$PKG{([^)]*)}', item)
        if m:
            specified_pkg = m.group(1)
            specified_pkg = specified_pkg.strip()

            if specified_pkg:
                pkg_name = specified_pkg

            if pkg_name == None:
                pkg_name = ''

            pkg_dir = resource_filename(pkg_name, '')
            item = re.sub('\$PKG{\s*%s\s*}' % specified_pkg, pkg_dir, item)
    elif isinstance(item, list):
        output = []
        for v in item:
            output.append(replace_pkg_strs(v, pkg_name))
        return output
    elif isinstance(item, dict):
        for k, v in item.iteritems():
            item[k] = replace_pkg_strs(v, pkg_name)
    elif isinstance(item, tuple):
        for i in xrange(len(item)):
            item[i] = replace_pkg_strs(item[i], pkg_name)

    return item


def loadSet(filepath):
    return set(line.strip() for line in open(filepath))


def instantiateClassFromJSON(json_data):
    is_function = False
    function_args = None
    classname = json_data["_jsontype"]
    if classname == "_json":
        obj = json_data
    else:
        if classname == "_function":
            is_function = True
            function_args = json_data["function_args"]
            classname = json_data["function_call"]
        importlist = classname.rsplit(".", 1)
        print "importlist", importlist
        if len(importlist) == 1:
            print "Improper class specification. Returning None." + classname + ' ' + importlist + ' ' + str[json_data][
                                                                                                         :70]
            logger.error(
                "Improper class specification. Returning None." + classname + ' ' + importlist + ' ' + str[json_data][
                                                                                                       :70])
            return None

        logger.debug("****importlist=" + str(importlist))
        mod = importlib.import_module(importlist[0])

        try:
            classInstantiation = str_to_class(mod, importlist[1])
            print "classInstantiation", classInstantiation
            logger.debug(str(classInstantiation)[:50])
        except Exception, e:
            print "Error loading class module:%s, class:%s" % (str(mod), str(importlist[1])), e
            logger.exception("Error loading class module:%s, class:%s" % (str(mod), str(importlist[1])))
            # sys.exit(0)

        if is_function:
            function_args = replace_pkg_strs(function_args, importlist[0])
            obj = classInstantiation(function_args)
        else:
            try:
                # do a global search and replace for PKG strings
                json_data = replace_pkg_strs(json_data)

                # See if there is an explicit json loading method defined
                print "trying to load from json explicitly", classInstantiation
                obj = classInstantiation.loadFromJSON(json_data)
                print "JSON Loaded Explicitly:" + str(classInstantiation)[:50]
                logger.debug("JSON Loaded Explicitly:" + str(classInstantiation)[:50])

            except Exception, e:
                try:
                    # If there is no explicit loadFromJSON method, try just sending the data in as kwargs
                    print "no explicit json loader defined. loading through kwargs", e
                    obj = classInstantiation(**json_data)
                    print "JSON Loaded through standard kwargs:" + str(classInstantiation)[:50]
                    logger.info("JSON Loaded through standard kwargs:" + str(classInstantiation)[:50])
                except Exception, e2:
                    try:
                        obj = classInstantiation(json_data["name"], **json_data)
                    except Exception, e3:
                        print "JSON not loaded through standard kwargs:" + str(json_data)[:500]
                        print e3
                        logger.exception("JSON not loaded through standard kwargs:" + str(json_data)[:500])
                        # None of the above was successful, so just give up and return the json_data. Most likely this is just pure json, no other instantiation required
                        obj = json_data

    if not (obj == json_data):
        print json_data["_alias"] + " instantiated successfully."
        logger.info(json_data["_alias"] + " instantiated successfully.")
    else:
        print json_data["_alias"] + " remaining as json definition."
        logger.warning(json_data["_alias"] + " remaining as json definition.")
    return obj


def instantiateJSONAliases(json_dict, alias_dict):
    for k in json_dict.iterkeys():
        v = json_dict[k]
        json_dict[k] = instantiateAlias(v, alias_dict)
        if isinstance(v, basestring):
            if v.startswith("_alias_"):
                alias = v[7:]
                json_data_instance = alias_dict[alias]
                instantiateJSONAliases(json_data_instance, alias_dict)
                instantiation = instantiateClassFromJSON(json_data_instance)
                json_dict[k] = instantiation
        elif isinstance(v, dict):
            instantiateJSONAliases(v, alias_dict)
        elif isinstance(v, list):
            for list_element in v:
                if isinstance(list_element, dict):
                    instantiateJSONAliases(list_element, alias_dict)


def instantiateAlias(value, alias_dict):
    if isinstance(value, basestring):
        if value.startswith("_alias_"):
            alias = value[7:]
            json_data_instance = alias_dict[alias]
            instantiateJSONAliases(json_data_instance, alias_dict)
            instantiation = instantiateClassFromJSON(json_data_instance)
            return instantiation
        return value
    elif isinstance(value, dict):
        for k in value.iterkeys():
            v = value[k]
            value[k] = instantiateAlias(v, alias_dict)
        return value
    elif isinstance(value, list):
        for list_index in range(0, len(value)):
            list_element = value[list_index]
            value[list_index] = instantiateAlias(list_element, alias_dict)
        #             if isinstance(list_element,dict):
        #                 return instantiateJSONAliases(list_element,alias_dict)

    # This isn't an alias. Just return the original value.
    return value


def loadJSON(filename):
    json_data_array = []
    json_data_alias_dict = {}
    with open(filename) as f:
        jsonfile_array = json.load(f)

    for json_data in jsonfile_array:
        json_data_array.append(json_data)
        if json_data.has_key("_alias"):
            alias = json_data["_alias"]
            if json_data_alias_dict.has_key(alias):
                # Duplicate alias, so returning None
                return None, None
            json_data_alias_dict[alias] = json_data

    for json_data in json_data_array:
        instantiateAlias(json_data, json_data_alias_dict)

    classes = []

    alias_class_dict = {}

    for json_data in json_data_array:
        print
        print
        print "instantiating", json_data
        instantiation = instantiateClassFromJSON(json_data)
        classes.append(instantiation)
        alias_class_dict[
            json_data["_alias"]] = instantiation  # duplicate processing for now until we find out what we want

    print "returning from loadJSON"
    return json_data_array, classes, alias_class_dict


def getObservationListFromQueryInfo(query_info):
    #    db_type = query_info.get("db_type")
    #    db_host = query_info.get("db_host")
    #    db_name = query_info.get("db_name")

    #    observations_db = hybrid.db.init(db_type,host=db_host,database=db_name)
    observations_db = getDbFromQueryInfo(query_info)
    observations_view = hybrid.view.create_view_from_query_info(query_info)
    try:
        observations = hybrid.utils.getDocsFromView(observations_db, observations_view)
    except:
        return None
    return observations


def getDbFromQueryInfo(query_info):
    db_type = query_info.get("db_type")
    db_host = query_info.get("db_host")
    db_name = query_info.get("db_name")

    db = hybrid.db.init(db_type, host=db_host, database=db_name)
    return db


def getDocsFromView(db, view):
    docs = []
    logger.info("Paging in view...")
    rows = view.rows()

    for row in rows:
        db_doc = db.loadDataBlob(row.getMetaData("_dataBlobID"), include_binary=True)
        if (db_doc == None):
            logger.error(row.getMetaData("_dataBlobID") + " missing from database.")
            continue

        docs.append(db_doc)
    return docs


def clean_and_split(list_string, delimiter):
    new_list = list_string.split(delimiter)
    new_list = filter(None, new_list)
    new_list = strip_list_elements(new_list)
    return new_list


def valid_key_or_index(container, field_name_or_number):
    if type(container) is type({}):
        if container.has_key(field_name_or_number):
            return True
        return False
    elif type(container) is type([]):
        if len(container) > int(field_name_or_number):
            return True
        return False


def get_field_value(data_dict, field_name, field_defaults):
    field_array = field_name.split(':', 1)
    first_field_name = field_array[0]
    if len(field_array) > 1:
        second_field_name = field_array[1]
        if type(data_dict) is type([]):
            first_field_name = int(first_field_name)

        valid = valid_key_or_index(data_dict, first_field_name)
        if valid:
            new_dict = data_dict[first_field_name]

            if not (isinstance(new_dict, dict) or isinstance(new_dict, list)):
                # We have a value here. Let's check if the value equals the required value
                if encoding.convertToUnicode(new_dict) == second_field_name:
                    return 1
                else:
                    return 0
        else:

            if field_defaults.has_key(first_field_name):
                val = field_defaults[first_field_name]
            else:
                val = None
            return val

        return get_field_value(new_dict, second_field_name, field_defaults)
    else:
        if isinstance(data_dict, list):
            try:
                field_name = int(field_name)
            except:
                return None
        valid = valid_key_or_index(data_dict, field_name)
        if valid:
            return data_dict[field_name]
        else:
            if field_defaults and field_defaults.has_key(field_name):
                val = field_defaults[field_name]
            else:
                val = None

            return val

    return val


def is_number(s):
    try:
        x = float(s)
        return (x == x) and (x - 1 != x)
    except ValueError:
        return False


def strip_list_elements(list_to_strip):
    new_list = []
    for list_element in list_to_strip:
        new_list.append(list_element.strip())
    return new_list


# def create_observation_data(input_rows,field_names,field_defaults={}):
#     observations = {}
#
#     for row in input_rows:
#         observation = {}
#         obs_meta = row.getMetaDataDict()   # The database document representing a row
#         obs_uuid = obs_meta["_dataBlobID"] #
#
#         for field_name in field_names:
#             val = get_field_value(obs_meta,field_name,field_defaults)
#             observation[field_name] = val
#
#         observations[obs_uuid] = observation
#     return observations


def create_observation_data(input_rows, field_names, field_defaults={}):
    observations = []

    for row in input_rows:
        observation = {}
        obs_meta = row.getMetaDataDict()  # The database document representing a row
        if obs_meta.has_key("_dataBlobID"):
            obs_uuid = obs_meta.get("_dataBlobID")  #
            observation["uuid"] = obs_uuid

        for field_name in field_names:
            val = get_field_value(obs_meta, field_name, field_defaults)
            observation[field_name] = val

        observations.append(observation)
    return observations


def create_observation_datum(row, field_names, field_defaults={}):
    observation = {}
    obs_meta = row.getMetaDataDict()  # The database document representing a row
    if obs_meta.has_key("_dataBlobID"):
        obs_uuid = obs_meta.get("_dataBlobID")  #
        observation["uuid"] = obs_uuid

    for field_name in field_names:
        val = get_field_value(obs_meta, field_name, field_defaults)
        observation[field_name] = val

    return observation


def tuples_to_lists(x):
    if x is None:
        return []
    if not (isinstance(x, tuple)):
        return x
    temp_list = []
    for elem in x:
        temp_list.append(tuples_to_lists(elem))

    return temp_list


def removeKeysFromManagerView(db_manager, key_list):
    target_db = db_manager.getTargetDB()
    documents = db_manager.getRows()
    for doc in documents:
        for key in key_list:
            doc.deleteMetaData(key)
            target_db.storeDataBlob(doc)


def removeWorkerMetaDataFromManagerView(db_manager, worker):
    target_db = db_manager.getTargetDB()
    documents = db_manager.getRows()
    for doc in documents:
        worker.removeMetaDataDict(doc)
        target_db.storeDataBlob(doc)


def calculateEntropy(data):
    byteCounts = [0] * 256
    size = len(data)
    for d in data:
        byteCounts[ord(d)] += 1

    entropy = 0.0
    for count in byteCounts:
        if count == 0:
            continue

        p = 1.0 * count / size
        entropy -= p * math.log(p, 2)

    return entropy


def anyDocumentLabelChanges(data_db, model_observations_db, labels):
    # Build a map where keys are doc ids and values have the form
    # [data_db_label, label_db_label].
    label_view_uri = "_design/views/_view/labels"
    data_view = data_db.loadView(label_view_uri)
    data_view_rows = data_view.rows()

    label_view = model_observations_db.loadView(label_view_uri)
    label_view_rows = label_view.rows()
    label_map = {}

    for row in data_view_rows:
        label_map[row.key] = [row.value, None]

    for row in label_view_rows:
        if row.key in label_map:
            label_map[row.key][1] = row.value
        else:
            label_map[row.key] = [None, row.value]

    # Copy any documents with new or changed labels from data_db to model_observations_db
    for (id, label_values) in label_map.iteritems():
        if label_values[0] is None:

            # Note: Label in label db but not in data db
            pass

        elif label_values[0] != label_values[1]:

            # Copy to label db
            logger.info("Copying changed doc to label db")
            doc = data_db.loadDataBlob(id, include_binary=True)

            # Set target database
            doc.setDB(model_observations_db)

            # Store to target
            doc.store(delete_existing=True)

    # Re-Pull the documents that have a user label attached
    labels.getModelData().update()
    labels.update()

    # If we have new labels, run a label table fix and return True
    if labels.checkForNewLabels():
        logger.info("*******************************")
        logger.info("* New Labels! I'm so excited! *")
        logger.info("*******************************")
        return True
    else:
        return False


def loadOccuranceSet(db, blob_name, view_uri):
    # Check if the blob exists
    logger.info("Loading blob:", blob_name)
    blob = db.loadDataBlob(blob_name, include_binary=True, must_exist=False)
    if blob:

        # Decompress it and return it as a python set
        compressed_data = blob.getBinaryData("compressed_data")
        json_data = bz2.decompress(compressed_data)
        python_set = set(json.loads(json_data))  # Comes back as a list, so convert to set
        return python_set

    # Hmph, couldn't find the blob so error out and exit
    else:
        logger.warning("Could not find blob:", blob_name, "returning empty set...")
        return set([])


def dataBlobIsModified(db, blob_name):
    # Store blob content hashes
    global _blob_content_hash_table
    if '_blob_content_hash_table' not in globals():
        _blob_content_hash_table = {}
    if blob_name not in _blob_content_hash_table:
        _blob_content_hash_table[blob_name] = 0

    # Check if the blob exists
    blob = db.loadDataBlob(blob_name, include_binary=False, must_exist=False)
    if blob:

        old_content_hash = _blob_content_hash_table[blob_name]
        new_content_hash = blob.getMetaData("content_hash")

        # Is the content hash different
        if new_content_hash != old_content_hash:
            _blob_content_hash_table[blob_name] = new_content_hash
            return True
        else:
            return False

    # Hmph, couldn't find the blob
    else:
        logger.info("Could not find blob:", blob_name)
        return True  # Not totally well defined


def convertViewToPython(db, view_uri, **kwargs):
    output_list_of_dicts = []

    # Pull a view from the database and build up a python list of dicts
    logger.info("Pulling", view_uri, "...")
    view = db.loadView(view_uri, **kwargs)
    if view is None:
        logger.error("Could not open view on:", view_uri)
        sys.exit()

    # Loop through rows adding each one to the list
    logger.info("Paging in view...")
    rows = view.rows()
    for row in rows:

        row_dict = {}

        # Views might have compound tuple (dict) for key or just be flat
        if type(row.key) == dict:
            row_dict = row.key
        else:
            row_dict["key"] = row.key

        # Same thing for value field
        if type(row.value) == dict:
            row_dict.update(row.value)
        else:
            row_dict["value"] = row.value

        # Also add the uuid of the row if it's there
        if row.id:
            row_dict["uuid"] = row.id

        # Now place on the list
        output_list_of_dicts.append(row_dict)

    return output_list_of_dicts


def convertViewKeysToJSONSet(db, view_uri, blob_name, **kwargs):
    item_set = set()

    # Pull a view from the database and build up a list
    logger.info("Pulling", view_uri, "...")
    view = db.loadView(view_uri, group=True, paging=True)
    if view is None:
        logger.warning("Could not open view on:", view_uri)
        return

    # Do they want to combine this with the existing set
    combine = kwargs.get('combine', False)
    if combine:
        blob = db.loadDataBlob(blob_name, include_binary=True, must_exist=False)
        if blob:
            compressed_data = blob.getBinaryData("compressed_data")
            json_data = bz2.decompress(compressed_data)
            item_set = set(json.loads(json_data))

    # Loop through rows adding each one to the list
    logger.info("Paging in view...")
    rows = view.rows()
    for row in rows:
        item_set.add(row.key)

    # Convert the set to a list and then to json
    item_list = list(item_set)
    json_data = json.dumps(item_list)
    compressed_data = bz2.compress(json_data)
    content_hash = hashlib.sha1(compressed_data).hexdigest()

    # Make a data blob and store it to the database
    blob = data_blob.data_blob(blob_name)
    blob.setDB(db)

    # Validation fields
    blob.addRequiredMetaFields(["name", "rows", "compressed", "size", "content_hash"])
    blob.addRequiredBinaryFields(["compressed_data"])

    # Various bits of meta data
    blob.setMetaData("name", blob_name)
    blob.setMetaData("rows", len(item_list))
    blob.setMetaData("compressed", True)
    blob.setMetaData("size", len(compressed_data))
    blob.setMetaData("content_hash", unicode(content_hash, "utf-8"))
    blob.setBinaryData("compressed_data", "application/octet", compressed_data)
    blob.store(delete_existing=True)

    logger.info("Blob storage complete...")


def markModelsInvalid(model_list):
    for model in model_list:
        model.setInvalid()
        model.storeToDB()


def markModelsNotReadyForRecomputation(model_list):
    for model in model_list:
        model.setNotReadyForRecomputation()
        model.storeToDB()


def markModelsSick(model_list):
    for model in model_list:
        model.setNotReadyForRecomputation()
        model.setInvalid()
        model.storeToDB()


def markModelsReadyForRecomputation(model_list):
    for model in model_list:
        model.setReadyForRecomputation()
        model.storeToDB()


def compareDataBlobDicts(a, b):
    if not (len(a) == len(b)):
        return False

    for a_id, a_blob in a.iteritems():
        if not (b.has_key(a_id)):
            return False

        b_blob = b.get(a_id)

        if not (a_blob.compareData(b_blob)):
            return False

    return True


def make_hash(o):
    if isinstance(o, set) or isinstance(o, tuple) or isinstance(o, list):
        return tuple([make_hash(e) for e in o])
    elif not isinstance(o, dict):
        return hash(o)

    new_o = copy.deepcopy(o)
    for k, v in new_o.items():
        new_o[k] = make_hash(v)
    return hash(tuple(frozenset(new_o.items())))


def isNew(value, occurance_set, docid="None", set_name=None):
    if occurance_set and value not in occurance_set:
        logger.info("New value being added to set name:" + set_name + " of len(" + str(
            len(occurance_set)) + "):" + encoding.convertToAscii(value) + " from docid = " + str(docid))
        occurance_set.add(value)
        return True
    else:
        return False


def getDateTimeFromDict(dtd):
    converted_datetime = datetime.datetime(dtd["year"], dtd["month"], dtd["day"], dtd["hour"], dtd["minute"],
                                           dtd["second"], dtd["microsecond"])
    time_str = converted_datetime.strftime("%Y-%m-%d %H:%M:%SZ")
    return time_str


def getDateTimeDict(iDateTime):
    datetime_dict = {"year": iDateTime.year, "month": iDateTime.month, "day": iDateTime.day, "hour": iDateTime.hour,
                     "minute": iDateTime.minute, "second": iDateTime.second, "microsecond": iDateTime.microsecond}

    return datetime_dict


def getCurrentDateTimeDict():
    current_datetime = datetime.datetime.utcnow()
    return getDateTimeDict(current_datetime)


# returns -1 if a<b,1 if a>b, and 0 if they are equal. Returns -2 if their structure is not compatible
def compareDateTimeDict(datetime_dict1, datetime_dict2):
    if not (len(datetime_dict1) == len(datetime_dict2)):
        return -2

    datetime_order_list = ["year", "month", "day", "hour", "minute", "second", "microsecond"]

    for k in datetime_order_list:
        if not (datetime_dict2.has_key(k)):
            return -2

        if datetime_dict1[k] == datetime_dict2[k]:
            continue
        if datetime_dict1[k] < datetime_dict2[k]:
            return -1
        return 1

    return 0


def viewToVTKTable(view, arrays, **kwargs):
    """ Create a vtk table object from a database view object
        Input:
            view:  database view (object)
            arrays: Names and Types of the arrays to be built in the vtkTable (dict: of form {"uuid":vtkStringArray(),"text":vtkUnicodeStringArray()})
            kwargs: parameters for the table construction (keyword args)
    """

    # Pull known parameters
    autofill = kwargs.get('autofill', False)

    # Go through the rows in the view and mash into a table
    rows = view.rows()
    num_rows = len(rows)

    # Check for empty view
    if num_rows == 0:
        # logger.warning( "Empty View (no data to convert to table) for view = ",view.getInfo(), view,arrays, kwargs)
        return vtkTable()  # Returning an empty table

    # Now check the first row of the view
    # We typically have two kinds of views:
    #   1) key, value pairs (plain)
    #   2) Dictionaries for both the key and the value (compound)
    view_type = "plain"
    first_row = rows[0]
    if type(first_row.key) is dict:
        if type(first_row.value) is dict:
            view_type = "compound"
        else:  # I don't know what I have
            logger.error("viewToVTKTable freaked out on view:", view, arrays, kwargs)
            raise RuntimeError("viewToVTKTable freaked out on view:", view, arrays, kwargs)

    # Plain views types are straight forward just key and value
    if view_type == "plain":

        # Sanity check that the application has passed in the correct array structure when getting a plain view
        if type(arrays) != list or len(arrays) != 2:
            logger.error("invalid array data for plain key/value view, should be a list of tuples\
                                        [(\"foo\"),vtkIntArray()),(\"bar\",vtkStringArray())]")
            raise RuntimeError("viewToVTKTable invalid array data for plain key/value view")

        # Okay now setup the arrays
        for array in arrays:
            array[1].SetName(array[0])
            array[1].SetNumberOfTuples(num_rows)

        # Loop through the rows setting the values on the arrays
        i = 0
        for row in rows:
            arrays[0][1].SetValue(i, fix_types(row.key))
            arrays[1][1].SetValue(i, fix_types(row.value))
            i += 1

        # Add the arrays to the table and return
        table = vtkTable()
        for array in arrays:
            table.GetRowData().AddArray(array[1])

        return table



    else:  # View type == compound

        # Create arrays for columns that aren't specified (autofill makes vtkDoubleArrays)
        try:
            if autofill:
                if first_row.value:
                    first_row.key.update(first_row.value)  # this combines the python dictionaries
                for field in first_row.key.keys():
                    if field not in arrays:
                        arrays[field] = vtkDoubleArray()
        except:
            logger.error("viewToVTKTable freaked out on view:", view, arrays, kwargs)
            raise

        # Sanity check that the application isn't expected a row that isn't there
        for field in arrays:
            if field not in first_row.key.keys():
                logger.error("invalid field given to viewToVTKTable:", field, "is not in view:", view._name, kwargs)
                raise RuntimeError("viewToVTKTable invalid field given")

        # Okay now setup the arrays (either given or autofilled)
        for k, v in arrays.items():
            v.SetName(k)
            v.SetNumberOfTuples(num_rows)

        # Loop through the rows setting the values on the arrays
        i = 0
        for row in rows:
            if row.value:
                row.key.update(row.value)  # this combines the python dictionaries
            for field in row.key.keys():
                if field in arrays:
                    arrays[field].SetValue(i, fix_types(row.key[field]))
            i += 1

        # Add the arrays to the table and return
        table = vtkTable()
        for k, v in arrays.items():
            table.GetRowData().AddArray(v)

        return table


def convertData(source_data, source_mime_type, target_mime_type, **kwargs):
    """ Convert data from one mime type to another
        Input:
            source_data: source data in the form of mime_type_source
            source_mime_type: mime type of the source data
            target_mime_type: desired mime type for the target data
            kwargs: parameters for the data conversion (keyword args)
    """

    # JSON source
    if source_mime_type == "application/json":

        # Target type
        if target_mime_type == "application/x-vtk-table":
            json_table = vtkJSONTableReader()
            json_table.SetJSONString(source_data)
            json_table.ReadFromInputStringOn()
            json_table.SetFormat(vtkJSONTableReader.ROW)
            json_table.Update()
            return json_table.GetOutput()
        elif target_mime_type == "application/x-vtk-array-data":
            json_array = vtkJSONArrayDataReader()
            json_array.SetJSONString(source_data)
            json_array.ReadFromInputStringOn()
            json_array.Update()
            return json_array.GetOutput()
        elif target_mime_type == "application/x-vtk-data-object":
            json_data_object = vtkJSONDataObjectReader()
            json_data_object.SetJSONString(source_data)
            json_data_object.ReadFromInputStringOn()
            json_data_object.Update()
            return json_data_object.GetOutput()
        else:
            logger.error("Convert: unsupported mime type", target_mime_type)
            sys.exit(1)

    # VTK Table source
    elif source_mime_type == "application/x-vtk-table":

        # Target type
        if target_mime_type == "application/json":
            json_writer = vtkJSONTableWriter()
            json_writer.SetInputData(source_data)
            json_writer.SetWriteToOutputString(True)
            json_writer.Write()
            return json_writer.GetOutputString()
        else:
            logger.error("Convert: unsupported mime type", target_mime_type)
            sys.exit(1)

    # Array Data source
    elif source_mime_type == "application/x-vtk-array-data":

        # Target type
        if target_mime_type == "application/x-vtk-table":
            to_table_filter = vtkArrayToTable()
            to_table_filter.SetInputData(source_data)  # Fixme: this will only create a table out of the first array
            to_table_filter.Update()
            return to_table_filter.GetOutput()
        else:
            logger.error("Convert: unsupported mime type", target_mime_type)
            sys.exit(1)

    # Only supporting some conversions right now
    else:
        logger.error("Convert: unsupported mime type", source_mime_type)
        sys.exit(1)


def listOfDictsToVTKTable(json_list_of_dicts, vtk_arrays):
    num_rows = len(json_list_of_dicts)

    # Okay now setup the arrays
    for k, v in vtk_arrays.items():
        v.SetName(k)
        v.SetNumberOfTuples(num_rows)

    # Loop through the the json list setting the values on the arrays
    i = 0
    for row in json_list_of_dicts:
        for k, v in row.iteritems():
            if k in vtk_arrays:
                vtk_arrays[k].SetValue(i, fix_types(v))
        i += 1

    # Add the arrays to the table and return
    table = vtkTable()
    for k, v in vtk_arrays.items():
        table.GetRowData().AddArray(v)

    return table


def canSerialize(unknown_object):
    """ Only certain objects can be currently serialized, this function
        returns True or False based on the object type
        Input:
            unknown_object: the unknown object in question
    """
    # Figure out whether it's one of the supported data types
    try:
        if sys.modules.has_key('vtk'):
            table_object = vtk.vtkTable.SafeDownCast(unknown_object)
        else:
            return False
    except TypeError:
        return False
    array_object = vtk.vtkArrayData.SafeDownCast(unknown_object)
    data_object = vtk.vtkDataObject.SafeDownCast(unknown_object)
    if table_object or array_object or data_object:
        return True
    else:
        return False


def serializeData(unknown_object, mime_type, **kwargs):
    ''' serialize a vtk data object
        Input:
            unknown_object: data in the form of vtk data object
            mime_type: mime type of the serialized output data (application/json or application/x-vtk-table, etc)
            kwargs: parameters for the data serialization (keyword args)
    '''
    # Sanity Checks
    if not unknown_object or not canSerialize(unknown_object):
        logger.error("Unsupported serialization of data object", unknown_object)
        sys.exit(1)

    # Figure out the vtk data object type
    object_class = unknown_object.GetClassName()
    table_object = vtk.vtkTable.SafeDownCast(unknown_object)
    graph_object = vtk.vtkGraph.SafeDownCast(unknown_object)
    array_object = vtk.vtkArrayData.SafeDownCast(unknown_object)
    data_object = vtk.vtkDataObject.SafeDownCast(unknown_object)

    # If it's a table
    if object_class == "vtkTable":
        # Switch on mime type
        if mime_type == "application/x-vtk-table":
            vtk_writer = vtkTableWriter()
            vtk_writer.SetInputData(table_object)
            vtk_writer.SetFileTypeToBinary()
            vtk_writer.WriteToOutputStringOn()
            vtk_writer.Write()
            return vtk_writer.GetOutputStdString()
        elif mime_type == "application/json":
            json_writer = vtkJSONTableWriter()
            json_writer.SetInputData(table_object)
            json_writer.SetWriteToOutputString(True)
            json_writer.Write()
            return json_writer.GetOutputString()
        else:
            logger.error("Serialize: Unsupported mime type", mime_type)
            sys.exit(1)
    elif object_class == "vtkArrayData":
        if mime_type == "application/x-vtk-array-data":
            vtk_writer = vtkArrayDataWriter()
            vtk_writer.SetInputData(array_object)
            vtk_writer.BinaryOn()
            vtk_writer.WriteToOutputStringOn()
            vtk_writer.Write()
            return vtk_writer.GetOutputString()
        elif mime_type == "application/json":
            json_writer = vtkJSONArrayDataWriter()
            json_writer.SetInputData(array_object)
            json_writer.SetWriteToOutputString(True)
            json_writer.Write()
            return json_writer.GetOutputString()
        else:
            logger.error("Serialize: Unsupported mime type", mime_type)
            sys.exit(1)
    elif object_class == "vtkUndirectedGraph" or object_class == "vtkDirectedGraph":
        if mime_type == "application/x-vtk-graph":
            vtk_writer = vtkGraphWriter()
            vtk_writer.SetInputData(graph_object)
            vtk_writer.SetFileTypeToBinary()
            vtk_writer.WriteToOutputStringOn()
            vtk_writer.Write()
            return vtk_writer.GetOutputStdString()
        elif mime_type == "application/json":
            json_writer = vtkJSONGraphWriter()
            json_writer.SetInputData(graph_object)
            json_writer.SetWriteToOutputString(True)
            json_writer.SetWriteCoordinates(True)
            json_writer.Write()
            return json_writer.GetJSONString()
        else:
            logger.error("Serialize: Unsupported mime type", mime_type)
            sys.exit(1)
    elif object_class == "vtkDataObject":
        if mime_type == "application/x-vtk-data-object":
            vtk_writer = vtkDataObjectWriter()
            vtk_writer.SetInputData(data_object)
            vtk_writer.SetFileTypeToBinary()
            vtk_writer.WriteToOutputStringOn()
            vtk_writer.Write()
            return vtk_writer.GetOutputStdString()
        elif mime_type == "application/json":
            json_writer = vtkJSONDataObjectWriter()
            json_writer.SetInputData(data_object)
            json_writer.SetWriteToOutputString(True)
            json_writer.Write()
            return json_writer.GetOutputString()
        else:
            logger.error("Serialize: Unsupported mime type", mime_type)
            sys.exit(1)
    else:
        logger.error("Serialize: Unsupported vtk object", data_object)
        return None


def deserializeData(serialized_object, mime_type, **kwargs):
    """ deserialize a vtk data object
        Input:
            serialized_object: serialized data
            mime_type: mime type of the serialized data (application/x-vtk-table, application/x-vtk-data-object etc)
            kwargs: parameters for the data deserialization (keyword args)
    """
    # Sanity check
    if serialized_object == None:
        return None

    # Switch on mime type
    if mime_type == "application/x-vtk-table":
        vtk_reader = vtkTableReader()
        vtk_reader.SetBinaryInputString(serialized_object, len(serialized_object))
        vtk_reader.ReadFromInputStringOn()
        vtk_reader.Update()
        return vtk_reader.GetOutput()
    elif mime_type == "application/x-vtk-graph":
        vtk_reader = vtkGraphReader()
        vtk_reader.SetBinaryInputString(serialized_object, len(serialized_object))
        vtk_reader.ReadFromInputStringOn()
        vtk_reader.Update()
        return vtk_reader.GetOutput()
    elif mime_type == "application/x-vtk-array-data":
        vtk_reader = vtkArrayDataReader()
        vtk_reader.SetInputString(serialized_object)
        vtk_reader.ReadFromInputStringOn()
        vtk_reader.Update()
        return vtk_reader.GetOutput()
    elif mime_type == "application/x-vtk-data-object":
        vtk_reader = vtkDataObjectReader()
        vtk_reader.SetBinaryInputString(serialized_object, len(serialized_object))
        vtk_reader.ReadFromInputStringOn()
        vtk_reader.Update()
        return vtk_reader.GetOutput()

    # If the mime type is already a serialized type then just return it
    elif (mime_type == "application/json" or mime_type == "text/plain" or mime_type == "text/html" or
                  mime_type == "application/octet" or mime_type == "application/octet-stream" or
                  mime_type == "application/zip"):
        return serialized_object

    else:
        logger.error("Deserialize: Unsupported mime type", mime_type)
        sys.exit(1)


def import_files_from_path(db, path):
    if os.path.isdir(path):
        import_directory(db, path)
    else:
        import_file(db, path)


def import_directory(db, path):
    children = os.listdir(path)
    for child in children:
        child_path = os.path.join(path, child)
        if os.path.isdir(child_path) and options.recursive:
            import_directory(db, child_path)
        elif os.path.isdir(child_path):
            continue
        else:
            import_file(db, child_path)


def import_file(db, path):
    # Slap on the content as a contents attachment
    file_name = os.path.basename(path)
    file_content = open(path, "r").read()

    file_blob = data_blob.create(file_name)
    file_blob.setMetaData("path", path)
    file_blob.setBinaryData("contents", "text/plain", file_content)

    # Now save the view to the database
    file_blob.setDB(db)
    file_blob.store()

    # Status
    logger.info("Read in file:", path, "stored as", file_name, "in database")


def import_string(db, s, **kwargs):
    if 'uuid' in kwargs:
        uuid = kwargs['uuid']
    else:
        uuid = hashlib.sha1((s + encoding.convertToUnicode(datetime.datetime.utcnow()))).hexdigest()

    # Slap on the content as a contents attachment
    file_blob = data_blob.create(uuid)
    file_blob.setBinaryData("contents", "text/plain", s)

    # Add metadata
    for k, v in kwargs.items():
        if not (k == 'uuid'):
            file_blob.setMetaData(k, v)

    # Now save the view to the database
    file_blob.setDB(db)
    file_blob.store()

    # Status
    logger.info("String stored as", uuid, "in database")


def stop_words_from_file(path):
    file_name = os.path.basename(path)
    file_content = open(path, "r").read()
    stop_word_list = file_content.split("\n")

    # Status
    logger.info("Read in stop list file:", path)

    return set(stop_word_list)


class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def dns_lookup_reverse(ip):
    """ Returns a hostname for this ip """
    try:
        return socket.gethostbyaddr(ip)
    except:
        logger.warning("Reverse DNS lookup failed for %s" % ip)
        return ["unknown"]


def dns_lookup(hostname):
    """ Returns a list of ips associated with this hostname """

    try:
        host_ip_info_list = socket.getaddrinfo(hostname, 80)
    except:
        return ["0.0.0.0"]
    host_addr_set = set()
    for item in host_ip_info_list:
        host_addr_set.add(item[4][0])
    return list(host_addr_set)


def dottedQuadToNum(ip):
    """convert decimal dotted quad string to long integer"""
    return struct.unpack('!L', socket.inet_aton(ip))[0]


def numToDottedQuad(n):
    """convert long int to dotted quad string"""
    return socket.inet_ntoa(struct.pack('!L', n))


def makeMask(n):
    """return a mask of n bits as a long integer"""
    return (1L << n) - 1


def ipToSubNet(ip, maskbits):
    """returns the network subnet dotted-quad addresses given IP and mask size"""
    # (by Greg Jorgensen)

    n = dottedQuadToNum(ip)
    m = makeMask(maskbits)

    host = n & m
    net = n - host

    return numToDottedQuad(net)


def is_valid_ipv4_address(address):
    try:
        addr = socket.inet_pton(socket.AF_INET, address)
    except AttributeError:  # no inet_pton here, sorry
        try:
            addr = socket.inet_aton(address)
        except socket.error:
            return False
        return address.count('.') == 3
    except socket.error:  # not a valid address
        return False

    return True


def is_valid_ipv6_address(address):
    try:
        addr = socket.inet_pton(socket.AF_INET6, address)
    except socket.error:  # not a valid address
        return False
    return True


def computeTaskProcessingRanges(rows, threads):
    tasks = []

    num_documents = len(rows)

    # Small numbers of documents can result in key 'collisions'
    # so just have one process do them all
    if num_documents < threads:
        start_key = rows[0].getMetaData("_dataBlobID")
        end_key = rows[num_documents - 1].getMetaData("_dataBlobID")
        num_docs = num_documents
        uuids = []
        for i in range(0, num_documents):
            uuids.append(rows[i].getMetaData("_dataBlobID"))
        tasks.append({"start_key": start_key, "end_key": end_key, "num_docs": num_docs, "uuids": uuids})
        return tasks

    rows_per_worker = num_documents / threads
    for i in range(threads - 1):
        start_key = rows[rows_per_worker * i].getMetaData("_dataBlobID")
        end_key = rows[rows_per_worker * (i + 1) - 1].getMetaData("_dataBlobID")
        uuids = []
        for i in range(rows_per_worker * i, rows_per_worker * (i + 1)):
            uuids.append(rows[i].getMetaData("_dataBlobID"))

        # create new task
        tasks.append({"start_key": start_key, "end_key": end_key, "num_docs": rows_per_worker, "uuids": uuids})

    # Last segment has whatever remains
    start_key = rows[rows_per_worker * (threads - 1)].getMetaData("_dataBlobID")
    end_key = rows[num_documents - 1].getMetaData("_dataBlobID")
    num_docs = num_documents - rows_per_worker * (threads - 1)
    uuids = []
    for i in range(rows_per_worker * (threads - 1), num_documents):
        uuids.append(rows[i].getMetaData("_dataBlobID"))

    tasks.append({"start_key": start_key, "end_key": end_key, "num_docs": num_docs, "uuids": uuids})

    return tasks


# convert full hostname to truncated domain name
def host2domain(host):
    domain = host

    # strip trailing port number = all-digit field after ":"

    index = string.rfind(domain, ":")
    if index >= 0:
        trailing = domain[index + 1:]
        flag = 1
        for char in trailing:
            if char not in string.digits: flag = 0
        if flag: domain = domain[:index]

    # if length < 6, return it
    # if last char = digit, assume IP address, return it

    if len(domain) < 6: return domain
    if domain[-1] in string.digits: return domain

    # field delimiter = "."
    # domain = last 2 fields unless last field < 3 chars, then last 3 fields

    fields = domain.split(".")
    if len(fields) < 3: return domain

    if len(fields[-1]) < 3:
        domain = ".".join(fields[-3:])
    else:
        domain = ".".join(fields[-2:])
    return domain


# takes a dictionary, a key, and a new item
# if the item is None do Nothing
# if the key exists in
# the dictionary it creates a list for the results if needed
# and merges the item into that list, otherwise it just adds it
# as the value - before returning the resulsts are dedupped
def dict_add_or_append(d, k, i):
    if i is None:
        return

    if k in d:
        v = d[k]
        v2 = list_append(v, i)
        d[k] = v2
    else:
        d[k] = i


# updates or creates a list of items
# dedups the results

def list_append(i1, i2):
    res = None

    list_found = False

    if type(i1) == list or type(i2) == list:
        list_found = True

    if i1 is None:
        res = i2
    elif i2 is None:
        res = i1
    elif type(i1) == list and type(i2) != list:
        res = i1[:]
        res.append(i2)
    elif type(i2) == list and type(i1) != list:
        res = [i1] + i2
    elif type(i1) == list and type(i2) == list:
        res = i1 + i2
    elif type(i1) != list and type(i2) != list:
        res = [i1, i2]

    # dedup the results
    if type(res) == list:
        res = list(set(res))

        # don't make it a list if the inputs weren't lists
        if not list_found and len(res) == 1:
            res = res[0]

    return res


# merges d1 with d2
def dict_merge(d1, d2):
    if d1 is None:
        d1 = {}

    if d2 is None:
        return d1

    for key in d2.keys():
        val2 = d2[key]
        val1 = None
        if key in d1.keys():
            val1 = d1[key]

        d1[key] = list_append(val1, val2)

    return d1


def dict_intersection(dicts):
    return dict(set.intersection(*(set(d.iteritems()) for d in dicts)))


def dict_difference(dicts):
    if len(dicts) == 0:
        return {}
    if len(dicts) == 1:
        return {}

    new_dict = copy.deepcopy(dicts[0])
    for d in dicts[1:]:
        for k, v in d.iteritems():
            if new_dict.get(k) == v:
                new_dict.pop(k, None)

    return new_dict


def dict_union(dicts):
    new_dict = {}
    for d in dicts:
        new_dict = dict(d, **new_dict)

    return new_dict


# take a nested dictionary as input and an id_list or identifier
# in the format a.b.c and it will create a flat dictionary
# of lists with the key of identifier.

def dict_query(obj, identifier, id_list=None):
    if id_list is None:
        id_list = identifier.split('.')

    result = {}

    # search for the final result
    if id_list == []:
        if type(obj) == list:
            for part in obj:
                rval = None
                if identifier in result.keys():
                    rval = result[identifier]

                result[identifier] = list_append(rval, part)
        else:
            # just keep the rest
            rval = None
            if identifier in result.keys():
                rval = result[identifier]

            result[identifier] = list_append(rval, obj)

        return result

    key = id_list[0]
    if type(obj) == dict and key in obj.keys():
        val = obj[key]
        if not type(val) == list:
            val = [val]

        # find all the matching sub structures
        for item in val:
            res = dict_query(item, identifier, id_list[1:])
            if res != {}:
                rval = None
                if identifier in result.keys():
                    rval = result[identifier]

                result[identifier] = list_append(res[identifier], rval)

    return result


# General recursive walk of a directory
# Usage: directory_walk(path, file_function)
#        Where the file_fuction takes a filename arg plus kwargs
def directory_walk(path, file_function, **kwargs):
    children = os.listdir(path)
    for child in children:
        child_path = os.path.join(path, child)
        if os.path.isdir(child_path):
            directory_walk(child_path)
        elif os.path.isdir(child_path):
            continue
        else:
            file_function(child_path, **kwargs)


'''
def import_path(path, file_action_function):
    if os.path.isdir(path):
        import_directory(path)
    else:
        file_action_function(path)
'''


# Helper methods
def is_number(s):
    try:
        float(s)
        return True
    except:
        return False


def fix_types(val):
    if isinstance(val, str):
        val = unicode(val)
    return val


def unravelToString(container, container_name):
    new_string = ""
    if type(container) is dict:
        for key, value in container.iteritems():
            new_string += "\t" + container_name + ":" + key + unravelToString(value, key)
    elif type(container) is list:
        for i in range(0, len(container)):
            new_string += "\t" + container_name + unravelToString(container[i], str(i))
    else:
        if hybrid.utils.is_number(container):

            # this prevents scientific notation
            if isinstance(container, float):
                container = '{0:.10f}'.format(container)
            return "=" + str(container)
        else:
            return "=" + hybrid.encoding.convertToUTF8(container)

    return new_string


def LogObservationToStr(doc, field_white_list, datetime_field, max_value_len=150):
    ''' Send field in a couch document to a log file
        Input:
            doc_meta:         python dictionary of document meta data
            field_white_list: the fields that you want to include in the log ['ALL'] for all fields
            datetime_field:   this field is handled separately
           '''

    log_string = unicode("", 'utf-8', errors='ignore')

    if datetime_field:
        # Validate document
        if not (doc.hasMetaData(datetime_field)):
            errmsg = "Field:" + datetime_field + " doesn't exist in document"
            return None, errmsg

        # Make sure datetime is the first key/value
        log_string += doc.getMetaData(datetime_field)
    else:
        log_string += datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")

    # Okay big loop on all the fields, checking for types and doing the correct thing (hopefully)
    for item in field_white_list:

        maxlength = None
        label = None

        # extract key
        val = item.split(';')
        k = val[0]

        if len(val) > 1 and val[1] != '':
            label = val[1]

        if len(val) > 2 and val[2] != '':
            maxlength = int(val[2])

        if maxlength is None:
            maxlength = max_value_len

        if not (doc.hasMetaData(k)):
            #                print "Field:", k, "doesn't exist in document"
            #                logger.debug( "Field:", k, "doesn't exist in document")
            continue

        # Check datatype
        v = doc.getMetaData(k)
        if type(v) is list:
            # dedup the list
            t = unravelToString(v, k)
            log_string += '\t' + hybrid.encoding.convertToUnicode(t)
        elif type(v) is dict:
            t = unravelToString(v, k)
            log_string += '\t' + unicode(t)[:maxlength]
        else:  # Primitive type
            log_string += '\t' + label + "=" + unicode(v)[:maxlength]

    # Encode the string as utf-8
    encoded_string = log_string.encode('utf-8')

    return encoded_string, None


class single_event():
    def __init__(self, event_id):

        self.event_id = event_id
        self.start_time = 0
        self.finish_time = 0
        self.elapsed_time = 0
        self.count = 1
        self.complete = False

    def start(self, now=0, count=1):

        if not now:
            now = time.time()

        self.start_time = now
        self.complete = False
        self.count = count
        self.elapsed_time = 0

    def finish(self, now=0):

        if not now:
            now = time.time()
            self.finish_time = now
            self.complete = True
            self.elapsed_time = self.finish_time - self.start_time


class event_stats():
    def __init__(self, event_type):

        self.elapsed_time = 0
        self.stats = {}
        self.recent = []
        self.count = 0
        self.event_type = event_type

    def create_stats(self):

        self.rate = 0

        if self.count > 0:
            self.rate = self.count / self.elapsed_time

        two_min_rate = 0
        two_min_count = 0
        two_min_elapsed = 0

        now = time.time()

        two_min_ago = now - (60 * 2)

        for event in self.recent:
            if event.start_time >= two_min_ago and event.complete:
                two_min_count = two_min_count + event.count
                two_min_elapsed = two_min_elapsed + event.elapsed_time
            else:
                break;

        if two_min_count:
            two_min_rate = two_min_count / two_min_elapsed

        logger.warning(
            "name=%s, avg rate=%f items/second, avg rate=%f seconds/item count=%f, elapsed=%f, two_min_rate=%f items/second, two_min_count=%f, two_min_elapsed=%f" % (
            self.event_type, self.rate, 1 / self.rate, self.count, self.elapsed_time, two_min_rate, two_min_count,
            two_min_elapsed))
        return

    def start(self, event_id, count=1):

        event = single_event(event_id)
        event.start(count=count)
        self.recent.insert(0, event)
        self.stats[event_id] = event
        self.count += event.count

    def finish(self, event_id):

        if event_id in self.stats:
            event = self.stats[event_id]
            event.finish()
            self.elapsed_time = self.elapsed_time + event.elapsed_time


def debuglog(filename, text):
    f = open(filename, 'a')
    f.write(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ") + " " + repr(text) + '\n')
    f.close()
