#!/usr/bin/env python
"""
mp_celery is a clone of the mp_pool interface implemented in celery rather than the python multi_processing pool
"""

import Queue

import mp_celery_task

"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
# Module info
__author__  = "Christopher Nebergall"
__email__   = "cneberg@sandia.gov"
__version__ = "0.1"
__status__  = "Development"

class mp_celery(object):
    """
    wrapper for Celery
    """

    def __init__(self, *args, **kwargs):

        #TODO use this for configuring celery process count somehow
        self._processes = kwargs.get('processes', 0)
        self._tasks = Queue.Queue()
        return

    def __del__(self):
        """
        Destructor
        """
        self.finish_and_close()
        del self._tasks

    # this is run from the celery client
    def add_task(self, func, *args, **kwargs):

        task = mp_celery_task.mp_celery_task()
        self._tasks.put(task.delay(*args, **kwargs))

        return True

    def wait_completion(self, timeout=None):
        """
        Blocks until all the tasks currently in the queue have completed.
        """
        rvals = []
        while not self._tasks.empty():
            r = self._tasks.get()
            r.get(timeout=timeout)
            rvals.append(r.successful())
        return rvals

    def finish_and_close(self):
        """
        Since there is nothing to close this is identical to wait_completion
        but I want to keep the interface symantecs consistent with mp_pool
        so I just call the other block function and return
        """
        self.wait_completion()
        return True

    def __str__(self):
        s  = 80*"="+"\n"
        s += "mp_celery instance\n"
        s += 80*"="+"\n"
        return s
