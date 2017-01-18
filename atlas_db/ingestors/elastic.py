#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import multiprocessing as mp
import numpy as np
from elasticsearch import Elasticsearch
from atlas_db.ingestors import AtlasIngestor, AtlasSchema
from atlas_db.ingestors.decorators import mongo_ingestion


class AtlasElasticIngestor(AtlasIngestor):
    def __init__(self, *args, **kwargs):
        super(AtlasElasticIngestor, self).__init__(*args, **kwargs)
        # TODO: More robust ES connector
        self.db = Elasticsearch()
        self.schema = AtlasMongoDocument
        self.meta_db = self.db['grid_meta']
        self.grid_db = None

    def ingest(self, values, all_lons_lats, metadata, variable):
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
        :return: Ingestion success
        :rtype: bool
        """
        try:
            jobs = []
            n = mp.cpu_count()

            all_lons_lats = np.array([x for x in all_lons_lats])

            self.grid_db = self.db['{}_{}'.format(metadata, variable)]

            for i in range(n):
                lons_lats = np.array_split(all_lons_lats, n)[i]
                p = mp.Process(target=self.ingest_variable,
                               args=(values, lons_lats))
                jobs.append(p)
                p.start()
            for j in jobs:
                j.join()

            self.index_grid()
            return True

        except:

            return False

    @mongo_ingestion('Raster')
    def ingest_variable(self, values, lons_lats):
        """Ingest one 'slice' of data per CPU core.

        :param values: n-d array of values
        :type values: np.array
        :param lons_lats: array of enumerated latitudes and longitudes
        :type lons_lats: np.array
        :return:
        :rtype:
        """
        for (lat_idx, lat), (lon_idx, lon) in lons_lats:

            try:
                pixel_values = self.num_or_null(
                    values[int(lat_idx), int(lon_idx)])
                if pixel_values is None:
                    continue

                tile = AtlasMongoDocument(
                    lon, lat, pixel_values,
                ).as_dict
                result = self.grid_db.insert_one(tile)

                return True

            except:
                print('Unexpected error:', sys.exc_info()[0])
                raise

    @mongo_ingestion('Metadata')
    def ingest_metadata(self, metadata):
        self.meta_db.insert_one(metadata)

    @mongo_ingestion('Index')
    def index_grid(self):
        self.grid_db.create_index([('geometry', GEOSPHERE)])


class AtlasMongoDocument(AtlasSchema):
    def __init__(self, *args, **kwargs):
        """Schema for storing ATLAS data in Mongo.
        """
        super(AtlasMongoDocument, self).__init__(*args, **kwargs)

    @property
    def __geo_interface__(self):
        """Define centroid (x, y) as a GeoJSON point. n-d array of values
         in the `properties` attribute.

        :return: GeoJSON object representing data point
        :rtype: dict
        """

        document = {
            'location': [self.x, self.y],
            'properties': {
                'values': self.value,
            }}

        return document
