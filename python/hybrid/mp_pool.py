#!/usr/bin/env python
"""
mp_pool provides some wrapper/helper to make it a little easier to use the
multiprocessing.Pool module from Python.

author: William McLendon
email : wcmclen@sandia.gov
"""
"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""
import Queue
import multiprocessing
import time

# Module info
__author__ = "William McLendon"
__email__ = "wcmclen@sandia.gov"
__version__ = "0.1"
__status__ = "Development"


class mp_pool(object):
    """
    wrapper for multiprocessing.Pool
    """

    def __init__(self, *args, **kargs):
        """
        mp_pool constructor.  This method passes the arguments on to
        multiprocessing.Pool()

        Parameters:

        mp_pool([processes[, initializer[, initargs[, maxtasksperchild]]]])
        """
        self._tasks = Queue.Queue()
        self._pool = multiprocessing.Pool(*args, **kargs)
        return

    def __del__(self):
        """
        Destructor
        """
        self.finish_and_close()
        del self._pool
        del self._tasks

    def add_task(self, func, *args, **kargs):
        """
        Add a task to the work pool using multiprocessing.Pool.apply_async

        func(*args, **kargs) is placed on the task queue for the process pool.
        """
        r = self._pool.apply_async(func, args, kargs)
        self._tasks.put(r)
        return True

    def wait_completion(self, timeout=None):
        """
        Blocks until all the tasks currently in the queue have completed.
        """
        rvals = []
        while not self._tasks.empty():
            r = self._tasks.get()
            r.wait(timeout=timeout)
            rvals.append(r.successful())
        return rvals

    def finish_and_close(self):
        """
        Blocks until all the current tasks have finished and then
        kills off the workers.
        """
        self._pool.close()
        self._pool.join()
        self._pool.terminate()
        return True

    @property
    def Pool(self):
        return self._pool

    def __str__(self):
        s = 80 * "=" + "\n"
        s += "mp_pool\n"
        s += "\tprocesses: %s\n" % (self._pool._processes)
        s += "\tmax tasks per child: "
        if self._pool._maxtasksperchild is None:
            s += "inf\n"
        else:
            s += "%d\n" % (self._pool._maxtasksperchild)

        for p in self._pool._pool:
            s += "\t\t%s\tpid=%s\tdaemon=%s\talive=%s\n" % \
                 (p.name, p.pid, int(p.daemon), int(p.is_alive()))
        s += 80 * "=" + "\n"
        return s


class process_manager(object):
    def __init__(self, **kwargs):
        self._auto_restart = kwargs.get("auto_restart", True)
        self._done = False
        self._watcher = {}
        return

    @staticmethod
    def loadFromJSON(json_data):
        jsontype = json_data["_jsontype"]
        # if not (jsontype == "process.lda_worker.lda_worker"):
        #    return None
        return process_manager(**json_data)

    # make sure you set func=real function in the kwargs
    def exec2(self, *args, **kwargs):
        self._watcher = {"handle": None, "args": args, "kwargs": kwargs}
        handle = multiprocessing.Process(target=process_manager.watch, args=(self._watcher, self._auto_restart))
        handle.start()
        self._watcher["handle"] = handle
        time.sleep(2)
        return

    @staticmethod
    def watch(watcher, auto_restart):

        handle = None
        args = watcher["args"]
        tmp = watcher["kwargs"]
        func = tmp["func"]

        # we don't want to pass the function as an argument into itself
        del tmp["func"]
        kwargs = tmp

        while True:

            if not handle or (auto_restart and not handle.is_alive()):
                handle = multiprocessing.Process(target=func, args=args, kwargs=kwargs)
                handle.start()

            if not handle:
                return

            time.sleep(1)
