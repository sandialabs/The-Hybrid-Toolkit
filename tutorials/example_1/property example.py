"""
Copyright 2017 Sandia Corporation.
Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
the U.S. Government retains certain rights in this software.
"""


class C(object):
    def __init__(self):
        self._type = "db_type"
        self._kwargs = {"my": "args"}

    @property
    def info(self):
        '''Get a string of information the database. '''
        return self._type + ": " + str(self._kwargs) + "; "


if __name__ == "__main__":
    my_class = C()
    print my_class.info
