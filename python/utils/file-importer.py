"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import optparse
import os
import sys
import time
import traceback

import hybrid
import magic


# Call the specified function for all the files in given path
# Usage: for_files_in_directory(path, file_function)
#        Where the file_function takes a filename arg plus kwargs
def for_files_in_directory(path, file_function, **kwargs):
    children = sorted(os.listdir(path))
    recurse = kwargs.get("recurse")
    for child in children:
        child_path = os.path.join(path, child)
        if os.path.isdir(child_path):  # Skip directories
            if (recurse):
                for_files_in_directory(child_path, file_function, **kwargs)
            else:
                continue
        else:
            file_function(child_path, **kwargs)


def duplicate_file_check(identifier):
    view_name = "_design/views/_view/filename"
    view_name = "_all_docs"

    hash_view = db.loadView(view_name, include_docs=False, key=identifier)
    view_rows = hash_view.rows()

    for item in view_rows:
        return True

    return False


def import_file(path, **kwargs):
    file_basename = os.path.basename(path)
    absolute_path = os.path.abspath(path)
    file_dirname = os.path.dirname(absolute_path)
    load_hidden = kwargs.get("hidden")

    if (file_basename.startswith(".") and not load_hidden):
        return

    if (kwargs.get("directory_constraint") != None):
        if (file_dirname.split(os.sep)[-1] != kwargs.get("directory_constraint")):
            return

    if (kwargs.get("extension_constraint") != None):
        fileName, fileExtension = os.path.splitext('/path/to/somefile.ext')
        if (fileExtension != "." + kwargs.get("extension_constraint")):
            return

    try:
        # Grab the logger
        rm = kwargs.get('remove', None)
        logger = kwargs.get('logger', None)
        if (not logger):
            raise RuntimeError(
                "This function needs a logger in kwargs; please add a 'logger=my_logger' arg to the calling routine")

        logger.logMessage('info', "input path=" + str(path))

        # Grab the database
        db = kwargs.get('database', None)
        '''
        if (not db):
            logger.error("No database set in kwargs; please add a 'database=my_db' arg to the calling routine")
            raise RuntimeError()
        '''

        doc = hybrid.data_blob.create()
        doc.setDB(db)

        if os.sep in path:
            filename = path[path.rfind('/') + 1:]
        else:
            filename = path

        if duplicate_file_check(filename):
            logger.warning("filename " + filename + " is already in database skipping...")
            # remove the input file
            if rm:
                os.remove(path)
            return

        stats = os.stat(path)

        doc.setMetaData("ts", stats.st_mtime)

        tm = time.gmtime(float(stats.st_mtime))

        doc.setMetaData("DATETIME", time.strftime("%Y-%m-%d %H:%M:%SZ", tm))
        doc.setMetaData("file_id", filename)

        new_filename = absolute_path
        if (db.getType() == "mongodb"):
            new_filename = new_filename.replace(".", "_")

        try:
            f = open(path, 'rb')
            raw_content = f.read()

        except Exception, e:
            logger.logMessage('error', "Error opening file " + path + " err:" + str(e))
            traceback.print_exc()

        doc.setMetaData("import_path", path)
        mime = magic.from_file(path, mime=True)
        doc.setMetaData("file_names", [filename])

        if "text" in mime:
            # unicode_content=encoding.convertToUnicode(raw_content)
            doc.setMetaData("content", raw_content)

        doc.setBinaryData(new_filename, mime, raw_content)

        doc.setMetaData("filepath", absolute_path)
        doc.setMetaData("_dataBlobID", absolute_path)
        doc.setMetaData("file_basename", file_basename)
        doc.setMetaData("source", 'directory scan')
        doc.setMetaData("db", db.getDBName())
        doc.setMetaData("source_directory", file_dirname)
        doc.setMetaData("filename", filename)

        print "Importing", absolute_path
        db.storeDataBlob(doc)
        #        doc.store(ignore_conflict=True)

        # remove the input file
        if rm:
            os.remove(path)
    except Exception, e:
        logger.error("Something happened: " + str(e))

    return


def monitor_content_dir(db, logger, path, remove, daemon, hidden, recurse, extension_constraint, directory_constraint):
    # Import static files from this directory
    if os.path.isdir(path):
        for_files_in_directory(path, import_file, database=db, logger=logger, remove=remove, hidden=hidden,
                               recurse=recurse, extension_constraint=extension_constraint,
                               directory_constraint=directory_constraint)
        while daemon:
            for_files_in_directory(path, import_file, database=db, logger=logger, remove=remove, hidden=hidden,
                                   recurse=recurse, extension_constraint=extension_constraint,
                                   directory_constraint=directory_constraint)
    else:
        import_file(path, database=db, logger=logger, remove=remove, hidden=hidden, recurse=recurse,
                    extension_constraint=extension_constraint, directory_constraint=directory_constraint)


if __name__ == "__main__":
    # Handle command-line arguments
    parser = optparse.OptionParser()
    parser.add_option("--host", default='http://localhost:5984', help="Name of database host.  Default: %default")
    parser.add_option("--database", default='my_database', help="Name of database.  Default: %default")
    parser.add_option("--dbtype", default='couchdb', help="Name of database.  Default: %default")
    parser.add_option("--log-level", type="int", default=3,
                      help="Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: %default")
    parser.add_option("-d", "--daemon", action="store_true", default=False,
                      help="Run as a daemon monitoring the log file for updates:  This enables state tracking Default: %default")
    parser.add_option("-f", "--filename", type='str', default='',
                      help="The directory or file to import: Default: %default")
    parser.add_option("-r", "--remove", action="store_true", default=False,
                      help="Delete the input file after importing it Default: %default")
    parser.add_option("-a", "--hidden", action="store_true", default=False, help="Include hidden files (.*): %default")
    parser.add_option("-R", "--recurse", action="store_true", default=False, help="Recurse subdirectories: %default")
    parser.add_option("--extension-constraint", default=None,
                      help="Specify file extension to import.  Default: %default")
    parser.add_option("--directory-constraint", default=None,
                      help="Specify common directory name while recursing.  Default: %default")

    (options, arguments) = parser.parse_args()

    if options.daemon:
        options.state = True

    # Get a logger
    logger = hybrid.logger.logger()
    logger.setLogLevel(options.log_level)

    dbtype = options.dbtype

    # Call the main processing function
    db = hybrid.db.init(dbtype, host=options.host, database=options.database, create=True)

    monitor_content_dir(db, logger, options.filename, options.remove, options.daemon, options.hidden, options.recurse,
                        options.extension_constraint, options.directory_constraint)

    sys.exit(0)
