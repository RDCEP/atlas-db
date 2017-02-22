#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
from datetime import datetime
from pymongo.errors import PyMongoError
from config import DEBUG


def mongo_ingestion(name):

    def decorator(f):

        def wrapper(*args, **kwargs):

            start_time = datetime.now()

            if DEBUG:
                print('*** Start {} ***\n{}\n\n'.format(name, start_time))

            try:
                f(*args, **kwargs)

            except PyMongoError:
                print('Error while committing on MongoDB')
                raise
            except:
                print('Unexpected error:', sys.exc_info()[0])
                raise

            if DEBUG:
                end_time = datetime.now()
                print('\n*** End {} ***\n{}\n'.format(name, end_time))
                elapsed_time = end_time - start_time
                print('\n*** Elapsed ***\n{}\n'.format(elapsed_time))

        return wrapper

    return decorator
