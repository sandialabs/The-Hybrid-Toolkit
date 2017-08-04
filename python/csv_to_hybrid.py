"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import hybrid
from hybrid import data_blob
import os
import optparse
import pandas
import DataFrame as df
import numpy as np

def load_from_csv(filepath):
    """
    takes a csv file and loads it into a list of dictionaries

    :param filepath: [string] path to
    :return:
        [{}] list of rows as dictionary, column names are linked to
        there respective row index

        eg:
        [{"column0":row1[0],"column2":row1[1]},{"column0":row2[0],"column2":row2[1]}]
    """
    return pandas.read_csv(filepath).to_dict('records')




def import_from_csv(filepath,dbname,dbtype,shuffle):
    """
    takes a csv and converts it to a hybrid data blob
    storing it in the given database

    :param filepath: [String] os path to the file
    :param dbname: [String] name of the data base to use or create
    :param dbtype: [String]type of db eg "mongodb"
    :return: The database obj where the data blob was stored
    """
    if dbname is None:
        filename, file_extension = os.path.splitext(filepath)
        dbname=os.path.basename(filename)
    
    # Load the data into a list of dicts
    data=load_from_csv(filepath)
    
    if shuffle:
        data=df.reindex(np.random.permutation(df.index)).reset_index(drop=True)

    # Create the database
    data_db = hybrid.db.init(dbtype, database=dbname, view_files=[], push_views=False, create=True, delete_existing=True)
    
    # Iterate through the list of dicts, turning each of the latter into DataBlobs and storing them in an array
    # Writing the data blob array
    return data_db.storeDataBlobArray([data_blob.dict2blob(data_row) for data_row in data])
    
    
if __name__ == "__main__":

    # Handle command-line arguments
    parser = optparse.OptionParser()
    
    parser.add_option("--csv", default=None, help="Name of input .csv file.  Default: %default")
    parser.add_option("--dbtype", default="mongodb", help="Type of database [mongodb, couchdb].  Default: %default")
    parser.add_option("--dbname", default=None, help="Name of the target database. Defaults to csv basename")
    parser.add_option("--shuffle",  action="store_true", default=False, help="Shuffle database entries.  Default: %default")

    (options, arguments) = parser.parse_args()

    dbtype=options.dbtype
    filepath=options.csv
    dbname=options.dbname
    shuffle=options.shuffle
    
    import_from_csv(filepath, dbname, dbtype,shuffle)
