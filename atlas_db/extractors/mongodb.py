#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
from atlas_db.constants import MONGO
from atlas_db.extractors import AtlasExtractor
from atlas_db.ingestors.mongodb import AtlasMongoDocument


class AtlasMongoExtractor(AtlasExtractor):
    def __init__(self, *args, **kwargs):
        super(AtlasMongoExtractor, self).__init__(*args, **kwargs)
        uri = "mongodb://{}:{}@{}/{}?authMechanism=SCRAM-SHA-1".format(
            MONGO['user'], MONGO['password'], MONGO['domain'],
            MONGO['database']
        )
        client = MongoClient(uri) if not MONGO['local'] \
            else MongoClient('localhost', MONGO['port'])
        self.db = client[MONGO['database']]
        self.schema = AtlasMongoDocument
        self.meta_db = self.db['grid_meta']
        self.grid_db = None

    def quadrilateral(self, a_x, a_y, b_x, b_y, c_x, c_y, d_x, d_y, ):
        """Returns the GeoJSON documents within a quadrilateral

        :return: List of GeoJSON files
        :rtype: list
        """
        cursor = self.grid_db.find(
            {'geometry': {'$geoIntersects': {
                '$geometry': {'type': 'Polygon', 'coordinates': [
                    [[a_x, a_y], [b_x, b_y],
                     [c_x, c_y], [d_x, d_y],
                     [a_x, a_y]]]}}}},
            projection={'_id': False, 'type': True,
                        'properties.centroid': True,
                        'properties.value': True, })

        return list(cursor)
