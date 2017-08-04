#
# Logging class
#
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import sys
import inspect
import os.path
import encoding
import datetime
import pytz

_max_log_item=50000

class logger(object):
    ''' Logger class; logs a message with a category of verbose, info, warning, or error '''

    _errors_found = False

    # Singleton pattern
    _instance = None
    def __new__(self, *args, **kwargs):
        if not self._instance:
            self._instance = super(logger, self).__new__(self, *args, **kwargs)
            self._instance.__one_time_init__()
        return self._instance

    def __one_time_init__(self):

        # Various bits of state
        self._verbose = False
        self._info = True
        self._warning = True
        self._silent = False
        self._auto_flush = True
        self._log_level = 3
        self._hostname = None

    def getLogLevel(self):
        return self._log_level

    def setLogLevel(self, level):
        """Sets the log level: Log level (0=silent,1=Errors,2=Warnings,3=Info,4=Verbose  Default: 3"""
        self._log_level = level
        self._verbose = (self._log_level >= 4)
        self._info = (self._log_level >= 3)
        self._warning = (self._log_level >= 2)
        self._silent = (self._log_level == 0)

    def setVerbose(self, state):
        """Set whether to log verbose messages or not (defaults to False)"""
        self._verbose = state

    def getVerbose(self):
        """Get whether to log verbose messages or not (defaults to False)"""
        return self._verbose

    def setInfo(self, state):
        """Set whether to log info messages or not (defaults to True)"""
        self._info = state

    def setWarning(self, state):
        """Set whether to log warning messages or not (defaults to True)"""
        self._warning = state

    def setHostname (self, state):
        """Stash this for logging in couch or the filesystem"""
        self._hostname = state

    def getHostname (self):
        return self._hostname

    def setSilent(self, state):
        """Set whether to be totally silent or not (defaults to False)"""
        self._silent = state

    def setAutoFlush(self, state):
        """Set whether to be flush output all the time or not (defaults to True)"""
        self._auto_flush = state

    def getErrorsFound(self):
        """Return whether logMessage was called for an error."""
        return self._errors_found

    def logMessage(self, category, *args):

        calling_function = "N/A"
        if inspect.stack:
            calling_function = os.path.basename(inspect.stack()[1][1]).split('.')[0]

        # If silent do nothing
        if self._silent:
            return

        # Conditionals on verbose, info and warning
        if category == "verbose":
            if self._verbose:
                print curtime() + "\tVerbose:",
                print "(%s)" % (calling_function),
                for a in args:
                    print encoding.convertToUnicode(repr(a)).encode("utf-8")[:_max_log_item],
                print
        elif category == "info":
            if self._info:
                print curtime() + "\tInfo:",
                print "(%s)" % (calling_function),
                for a in args:
                    print encoding.convertToUnicode(repr(a)).encode("utf-8")[:_max_log_item],
                print
        elif category == "warning":
            if self._warning:
                print curtime() + "\tWarning:",
                print "(%s)" % calling_function,
                for a in args:
                    print encoding.convertToUnicode(repr(a)).encode("utf-8")[:_max_log_item],
                print
        elif category == "error":
            self._errors_found = True
            print curtime() + "\tError:",
            for a in args:
                print encoding.convertToUnicode(repr(a)).encode("utf-8")[:_max_log_item],
            print
            # Print compressed stack trace
            for s in inspect.stack()[1:]:
                print '\t',
                print '%s:%d:%s ' % (os.path.basename(s[1]),s[2],s[3]),
                print s[4]
        else:
            raise TypeError("Unknown logger category:",category)

        # Flush output
        if self._auto_flush:
            sys.stdout.flush()

def curtime():
    now = datetime.datetime.utcnow()
    now=now.replace(tzinfo=pytz.utc)
    return now.strftime("%Y-%m-%d %H:%M:%S %Z")