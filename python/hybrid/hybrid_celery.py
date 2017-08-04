#!/usr/bin/env python

from __future__ import absolute_import
from celery import Celery

# instantiate Celery object
celery = Celery(include=['hybrid.mp_celery_task'])

# import celery config file
celery.config_from_object('hybrid.celeryconfig')

if __name__ == '__main__':
        celery.start()
