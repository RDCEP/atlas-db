#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import multiprocessing as mp
import numpy as np
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

    def parallel_ingest(self, values, all_lons_lats, metadata, variable,
                        no_index=False):
        """Crudely parallelized ingestion for Mongo. `values` should be
        at least 2 dimensions, with the first dimension corresponding to
        latitude and the second to longitude.

        :param values: n-d array of values.
        :type values: np.array
        :param all_lons_lats: iterator over enumerated latitude and longitude
        :type all_lons_lats: iter
        :param metadata: `name` attribute from metadata
        :type metadata: str
        :param variable: Variable name
        :type variable: str
        :param no_index: Quash indexing for tiled datasets
        :type no_index: bool
        :return: Ingestion success
        :rtype: bool
        """
        try:
            jobs = []
            n = mp.cpu_count()

            all_lons_lats = np.array([x for x in all_lons_lats])

            for i in range(n):
                lons_lats = np.array_split(all_lons_lats, n)[i]
                p = mp.Process(target=self.ingest_variable,
                               args=(values, lons_lats, metadata, variable,
                                     no_index))
                jobs.append(p)
                p.start()
            for j in jobs:
                j.join()

            if not no_index:
                self.index_grid(metadata, variable)

            return True

        except:

            return False

    def ingest(self, values, all_lons_lats, metadata, variable,
               no_index=False):
        """Non-parallelized ingestion for Mongo. `values` should be
        at least 2 dimensions, with the first dimension corresponding to
        latitude and the second to longitude.

        :param values: n-d array of values.
        :type values: np.array
        :param all_lons_lats: iterator over enumerated latitude and longitude
        :type all_lons_lats: iter
        :param metadata: `name` attribute from metadata
        :type metadata: str
        :param variable: Variable name
        :type variable: str
        :param no_index: Quash indexing for tiled datasets
        :type no_index: bool
        :return: Ingestion success
        :rtype: bool
        """
        try:

            all_lons_lats = np.array([x for x in all_lons_lats])

            self.ingest_variable(values, all_lons_lats, metadata, variable,
                                 no_index)

            if not no_index:
                self.index_grid(metadata, variable)

            return True

        except:

            return False

    @mongo_ingestion('Raster')
    def ingest_variable(self, values, lons_lats, metadata, variable):
        """Ingest one 'slice' of data per CPU core.

        :param values: n-d array of values
        :type values: np.array
        :param lons_lats: array of enumerated latitudes and longitudes
        :type lons_lats: np.array
        :param metadata: name of metadata
        :type metadata: str
        :param variable: name of variable
        :type variable: str
        :return:
        :rtype:
        """

        grid_db = self.get_grid_db(metadata, variable)
        docs = list()
        n = 0

        for (lat_idx, lat), (lon_idx, lon) in lons_lats:

            try:
                vals = dict()
                for k, v in values.iteritems():
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

    def get_grid_db(self, metadata, variable):
        client = MongoClient(URI) if not MONGO['local'] \
            else MongoClient('localhost', MONGO['port'])
        db = client[MONGO['database']]
        # return db['{}_{}'.format(metadata, variable)]
        return db['{}'.format(metadata)]

    def drop_metadata(self, metadata):
        self.meta_db.delete_one({'name': metadata['name']})

    @mongo_ingestion('Metadata')
    def ingest_metadata(self, metadata):
        self.drop_metadata(metadata)
        self.meta_db.insert_one(metadata)

    @mongo_ingestion('Index')
    def index_grid(self, metadata, variable):
        self.get_grid_db(metadata, variable)\
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
                    for x in v] for k, v in self.value.iteritems()}
            }

        return document
