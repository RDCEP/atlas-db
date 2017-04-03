#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import multiprocessing as mp
import numpy as np
from six import iteritems
from pymongo import MongoClient, GEOSPHERE
from atlas_db.constants import MONGO, URI
from atlas_db.ingestors import AtlasIngestor, AtlasSchema
from atlas_db.ingestors.decorators import mongo_ingestion


class AtlasMongoIngestor(AtlasIngestor):
    def __init__(self, *args, **kwargs):
        super(AtlasMongoIngestor, self).__init__(*args, **kwargs)
        client = MongoClient(URI) if not MONGO['local'] \
            else MongoClient('localhost', MONGO['port'])
        db = client[MONGO['database']]
        self.meta_db = db['grid_meta']
        self.schema = AtlasMongoDocument
        self.values = None
        self.lons_lats = None
        self.meta_name = None

    def ingest(self, values, lons_lats, metadata, no_index=False):
        """Callable ingestion method.

        :param values: n-d array of values.
        :type values: np.array
        :param lons_lats: iterator over enumerated latitude and longitude
        :type lons_lats: iter
        :param metadata: `name` attribute from metadata
        :type metadata: str
        :param no_index: Quash indexing for tiled datasets
        :type no_index: bool
        :return:
        :rtype:
        """
        self.values = values
        self.lons_lats = lons_lats
        self.meta_name = metadata
        self._parallel_ingest(no_index)

    def _parallel_ingest(self, no_index=False):
        """Crudely parallelized ingestion for Mongo. `values` should be
        at least 2 dimensions, with the first dimension corresponding to
        latitude and the second to longitude.

        :param no_index: Quash indexing for tiled datasets
        :type no_index: bool
        :return: Ingestion success
        :rtype: bool
        """
        try:
            jobs = []
            n = mp.cpu_count()

            all_lons_lats = np.array([x for x in self.lons_lats])

            for i in range(n):
                lons_lats = np.array_split(all_lons_lats, n)[i]
                p = mp.Process(target=self._ingest_data,
                               args=(lons_lats, ))
                jobs.append(p)
                p.start()
            for j in jobs:
                j.join()

            if not no_index:
                self.index_grid(self.meta_name)

            return True

        except:

            return False

    def _serial_ingest(self, no_index=False):
        """Non-parallelized ingestion for Mongo. `values` should be
        at least 2 dimensions, with the first dimension corresponding to
        latitude and the second to longitude.

        :param no_index: Quash indexing for tiled datasets
        :type no_index: bool
        :return: Ingestion success
        :rtype: bool
        """
        try:

            all_lons_lats = np.array([x for x in self.lons_lats])

            self._ingest_data(all_lons_lats, no_index)

            if not no_index:
                self.index_grid(self.meta_name)

            return True

        except:

            return False

    @mongo_ingestion('Raster')
    def _ingest_data(self, lons_lats):
        """Ingest one 'slice' of data per CPU core.

        :param lons_lats: array of enumerated latitudes and longitudes
        :type lons_lats: np.array
        :return:
        :rtype:
        """

        grid_db = self.get_grid_db(self.meta_name)
        docs = list()
        n = 0

        for (lat_idx, lat), (lon_idx, lon) in lons_lats:

            try:
                vals = dict()
                for k, v in iteritems(self.values):
                    pixel_values = self.num_or_null(
                        v[int(lat_idx), int(lon_idx)])
                    if pixel_values is not None:
                        vals[k] = pixel_values

                docs.append(AtlasMongoDocument(
                    lon, lat, vals, self.scaling
                ).as_dict)
                n += 1

                if n % 800 == 0 or n == len(lons_lats):
                    result = grid_db.insert_many(docs)
                    docs = list()

            except:
                print('Unexpected error:', sys.exc_info()[0])
                raise

        return True

    def get_grid_db(self, meta_name):
        client = MongoClient(URI) if not MONGO['local'] \
            else MongoClient('localhost', MONGO['port'])
        db = client[MONGO['database']]
        return db['{}'.format(meta_name)]

    def drop_metadata(self, meta_name):
        self.meta_db.delete_one({'name': meta_name['name']})

    @mongo_ingestion('Metadata')
    def ingest_metadata(self, meta_name):
        self.drop_metadata(meta_name)
        self.meta_db.insert_one(meta_name)

    @mongo_ingestion('Index')
    def index_grid(self, meta_name):
        self.get_grid_db(meta_name)\
            .create_index([('loc', GEOSPHERE)])


class AtlasMongoDocument(AtlasSchema):
    def __init__(self, *args, **kwargs):
        """Schema for storing ATLAS data in Mongo.
        """
        super(AtlasMongoDocument, self).__init__(*args, **kwargs)
        # self.value = [None if np.isnan(x) else int(x * 10 ** scaling)
        #               for x in value]

    @property
    def __geo_interface__(self):
        """Define centroid (x, y) as a GeoJSON point. n-d array of values
         in the `properties` attribute.

        :return: GeoJSON object representing data point
        :rtype: dict
        """

        document = {
            # 'type': 'Feature',
            'loc': [self.x, self.y],
            'val': {str(k): [None if np.isnan(x) else int(x * 10**self.scaling)
                    for x in v] for k, v in iteritems(self.value)}
            }

        return document
