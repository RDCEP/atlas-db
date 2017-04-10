#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
from pymongo.cursor import Cursor
from itertools import product
from atlas_db.constants import MONGO, URI
from atlas_db.extractors import AtlasExtractor
from atlas_db.ingestors.mongodb import AtlasMongoDocument


class AtlasMongoExtractor(AtlasExtractor):
    def __init__(self, *args, **kwargs):
        super(AtlasMongoExtractor, self).__init__(*args, **kwargs)
        client = MongoClient(URI) if not MONGO['local'] \
            else MongoClient('localhost', MONGO['port'])
        self.db = client[MONGO['database']]
        self.schema = AtlasMongoDocument
        self.meta_db = self.db['grid_meta']
        self.grid_db = None

    @staticmethod
    def bbox(bbox):
        """Returns the GeoJSON documents within a quadrilateral bounding box

        :param bbox: Top-left and bottom-right coordinates of the bounding box
        :type bbox: list
        :return: Dictionary suitable for `.find()` or `.aggregation(pipeline)`
        :rtype: dict
        """
        coords = [[
            bbox[0], [bbox[0][0], bbox[1][1]],
            bbox[1], [bbox[1][0], bbox[0][1]],
            bbox[0],
        ]]
        return {
            'loc': {'$geoIntersects': {
                '$geometry': {'type': 'Polygon', 'coordinates': coords}
            }}
        }

    @staticmethod
    def tmp_spatial_mean(variable, idx_key='i', dist_key='d', weight_key='w'):
        # Calculate mean of values
        return [
            {'$group': {
                '_id': '${}'.format(idx_key),
                'sum_{}'.format(weight_key):
                    {'$sum': {'$exp': {
                        '$multiply': [-.002, '${}'.format(dist_key)]}}},
                'val': {'$push': {
                    '$multiply': [
                        '$val.{}'.format(variable),
                        {'$exp': {
                            '$multiply': [-.002, '${}'.format(dist_key)]}}]
                }}
            }},
            {'$project': {
                '_id': 1,
                'mean': {'$divide': [ {'$sum': '$val'},
                                      '$sum_{}'.format(weight_key)] }
            }},
        ]

    def quadrilateral(self, bbox, variable):
        cursor = self.grid_db.find(
            self.bbox(bbox),
            projection={'_id': False, 'loc': True,
                        'val.{}'.format(variable): True, })
        return list(cursor)

    def resample(self, bbox, lons, lats, variable):
        points = []

        for lonlat in product(lons, lats):
            pipeline = [
                # Get 10 nearest points within 10km
                {'$geoNear': {
                    'near': {
                        'type': 'Point',
                        'coordinates': lonlat,
                    },
                    'distanceField': 'd',
                    'maxDistance': 1000,
                    'num': 10,
                    'spherical': True,
                }},
                # Remove extra variables
                {'$project': {
                    '_id': False,
                    'val.{}'.format(variable): True,
                    'd': True,
                }},
                # Unwind along relevant dimension of variable
                {'$unwind': {
                    'path': '$val.{}'.format(variable),
                    'includeArrayIndex': 'i',
                    'preserveNullAndEmptyArrays': False,
                }},
            ]
            pipeline += self.tmp_spatial_mean(variable)
            points.append(self.grid_db.aggregate(pipeline))
        return points

    def resample2(self, bbox, lons, lats, variable):
        points = list()
        for lonlat in product(lons, lats):
            points.append(self.grid_db.find({
                'loc': {'$near': {
                    '$geometry': {'type': 'Point', 'coordinates': lonlat},
                    'maxDistance': 1000,
                }}
            }).limit(1))
        return points


if __name__ == '__main__':
    from datetime import datetime
    import numpy as np
    from itertools import product
    extractor = AtlasMongoExtractor()
    metadata = list(extractor.meta_db.find({'name': 'gsde'}))[0]
    extractor.grid_db = extractor.db['gsde']

    lons = np.arange(100) / 10. + 80.
    lats = np.arange(100) / 10. + 20.

    print(lons)
    print(lats)
    t0 = datetime.now()
    pts = extractor.resample([[80., 30.], [90., 20.]], lons, lats, 'caco3')
    t1 = datetime.now()
    print(t1 - t0)

    t0 = datetime.now()
    pts = extractor.resample2([[80., 30.], [90., 20.]], lons, lats, 'caco3')
    t1 = datetime.now()
    print(t1-t0)

    print(len(pts))
