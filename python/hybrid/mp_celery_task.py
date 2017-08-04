#!/usr/bin/env python
"""
mp_celery is a clone of the mp_pool interface implemented in celery rather than the python multi_processing pool
"""
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
from celery import task
from celery.registry import tasks

# Module info
__author__  = "Christopher Nebergall"
__email__   = "cneberg@sandia.gov"
__version__ = "0.1"
__status__  = "Development"

class mp_celery_task(task.Task):
    """
    wrapper for Celery
    """

    # this is run from the celery server
    def run(self, *args, **kwargs):

        #func = kwargs.get('func', None)
        print "args=", args
        print "kwargs=", kwargs

        func = args[0]

        # look up the module and function specified in kwargs 
        #module_name = kwargs.get('module_name',None)
        #function_name = kwargs.get('function_name',None)
        func = kwargs.get('func2',None)

        # we should probably cache the result in a dictionary.
        #mod = __import__('%s' % (module_name))
        #func = getattr(mod, function_name)

        r = func(*args, **kwargs)

tasks.register(mp_celery_task)
