#
# Map Reduce utilities for MongoDB
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import datetime
import logging
import time
import sys
import bson

logger = logging.getLogger(__name__)


class mr_manager():
    """Map Reduce Manager for Mongo database"""

    def __init__(self, db):
        """ Create instance of Map Reduce Manager for Mongo database
        """

        # Get a logger handle (singleton)
        self._logger = logger.logger()

        # Set the database
        self._db = db

        # Pull the MapReduce manager collection
        self._storage = db["mr_manager"]

        '''
        # Pull the MapReduce manager
        self._storage = db["mr_manager"].find_one({"_dataBlobID":"mr_manager"})
        if (not self._storage):
            self._logger.warning("Didn't find the MapReduce manager: creating it...")
            db["mr_manager"].save({"_dataBlobID":"mr_manager", 'desc':"MapReduce Manager",'mr_job_array':[]})
            self._storage = db["mr_manager"].find_one({"_dataBlobID":"mr_manager"})


        # Make sure we have the time zone info all set
        mr_job_array = self._storage['mr_job_array']
        for mr_job in mr_job_array:
            mr_job['start'] = pytz.UTC.localize(mr_job['start'])
            mr_job['end'] = pytz.UTC.localize(mr_job['end'])
        '''

    # def save(self):
    #    ''' Save the MapReduce manager to the database '''
    #    self._db["mr_manager"].save(self._storage)


    def getInfo(self):
        """ List the current jobs in the MapReduce manager. """
        mr_job_cursor = self._storage.find()
        self._logger.info("Current jobs registered in MapReduce manager:")
        for mr_job in mr_job_cursor:
            self._logger.info("\t%s: Processed from %s to (%s --> %s)" %
                              (mr_job["_dataBlobID"], mr_job['initial'], mr_job['start'], mr_job['end']))

    def getTimeWindow(self, mr_start):
        """ Define the time window associated with incremental map reduce jobs. """
        _time_delta = 60  # seconds

        mr_end = mr_start + datetime.timedelta(seconds=_time_delta)  # This will process a 1 min window
        max_datetime = datetime.datetime.utcnow() - datetime.timedelta(
            seconds=5)  # This gives the feature scripts time to finish
        if mr_end > max_datetime:
            mr_end = max_datetime
        return mr_end

    def getMRJobTimeRange(self, mr_job_desc):
        #  Pull the index array, find the job and return the time window
        mr_job_cursor = self._storage.find()
        mr_start = None
        mr_end = None
        for mr_job in mr_job_cursor:
            if mr_job["_dataBlobID"] == mr_job_desc:
                mr_start = mr_job['start']
                mr_end = mr_job['end']

        return mr_start, mr_end

    def updateMRJobTimeRange(self, mr_job_desc):
        #  Pull the job info and update the time window
        mr_job_cursor = self._storage.find()
        for mr_job in mr_job_cursor:
            if mr_job["_dataBlobID"] == mr_job_desc:
                mr_job['start'] = mr_job['end']
                mr_job['end'] = self.getTimeWindow(mr_job['start'])
                self._storage.update({"_dataBlobID": mr_job_desc}, mr_job)
                return

    def addMRJob(self, mr_job_desc, mr_start):
        # Add the MR Job
        self._logger.info("Adding MapReduce job: %s starting at %s" % (mr_job_desc, mr_start.isoformat()))
        self._storage.insert(
            {"_dataBlobID": mr_job_desc, 'initial': mr_start, 'start': mr_start, 'end': self.getTimeWindow(mr_start)})

    def incrementalMapReduce(self,
                             map_f,
                             reduce_f,
                             db,
                             source_table_name,
                             target_table_name,
                             mr_job_desc=None,
                             query_qualifier=None):

        """ This method performs an incremental map-reduce on any new data in 'source_table_name'
        into target_table_name.  Each execution will process only the new, unprocessed records.
        Note: Right now each execution will only process a minute of data, so call this method often.
        Note2: The mr_job_desc must be set and ^must^ be unique as it's used for bookkeeping
              of which records have and have not be run through that particular map reduce job.
              String descriptions are good "mr_domain_rareness" is an example.
        """

        # Grab the time ranges associated with this MR job
        mr_start, mr_end = self.getMRJobTimeRange(mr_job_desc)

        # Make sure I'm registered; if not compute my initial start time and register myself
        if not mr_start:

            # Assuming they want to start at the very beginning of the source collection
            cursor = db[source_table_name].find().sort("_dataBlobID").limit(1)
            for doc in cursor:
                id = doc["_dataBlobID"]
            mr_start = id.generation_time.replace(tzinfo=None)
            self.addMRJob(mr_job_desc, mr_start)
            mr_start, mr_end = self.getMRJobTimeRange(mr_job_desc)

        # Create fake ObjectIds for the window of time
        start_datetime_objid = bson.ObjectId.from_datetime(mr_start)
        end_datetime_objid = bson.ObjectId.from_datetime(mr_end)
        mr_query = {"_dataBlobID": {'$gte': start_datetime_objid, '$lt': end_datetime_objid}}

        # Adding any query qualifiers as specified by the caller
        if query_qualifier:
            mr_query.update(query_qualifier)

        # Perform the incremental map_reduce on any new records.
        ret = db[source_table_name].map_reduce(
            map_f,
            reduce_f,
            out={'reduce': target_table_name},
            query=mr_query,
            full_response=True
        )

        # Inform the MR manager this time window is done
        self.updateMRJobTimeRange(mr_job_desc)
        self._logger.info("Processed records from %s through %s.\nmap_reduce details: %s" % (mr_start, mr_end, ret))

        # Throttle
        if ret["counts"]["input"] == 0:
            time.sleep(1)
        return ret

    def derivedMapReduce(self,
                         map_f,
                         reduce_f,
                         db,
                         source_table_name,
                         target_table_name,
                         mr_job_desc=None,
                         query_qualifier=None):

        """ This method performs an derived map-reduce on a MapReduce collection in 'source_table_name'
        into target_table_name.  Each execution will process the whole source table!
        """
        # Check that job description is properly set
        if not mr_job_desc:
            logger.error("mr_job_desc must be set by the caller! Exiting...")
            sys.exit(1)

        # No MapReduce query as we simply process the whole source table
        mr_query = {}

        # Adding any query qualifiers as specified by the caller
        if query_qualifier:
            mr_query.update(query_qualifier)

        # Perform the map_reduce on any records that match for this MR job
        ret = db[source_table_name].map_reduce(
            map_f,
            reduce_f,
            out={'replace': target_table_name},
            query=mr_query,
            full_response=True
        )

        num_records_processed = ret["counts"]["input"]
        self._logger.info("Processed all records for MapReduce job: %s" % (mr_job_desc))
