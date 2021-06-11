# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 00:05:59 2021

@author: brend
"""

import pymongo


# Send multiple MongoDb operations and execute in bulk, skip errors
def bulk_write(mycol, data):
    if len(data) == 0:
        return {}
    bulk = pymongo.bulk.BulkOperationBuilder(mycol, ordered = False)
    for item in data.copy():
        bulk.insert(item)
    try:
        response = bulk.execute()
    except pymongo.errors.BulkWriteError:
        response = {}
    return response