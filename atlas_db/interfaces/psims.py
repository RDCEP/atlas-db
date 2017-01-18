#!/usr/bin/env python
# -*- coding: utf-8 -*-
import itertools
import numpy as np
from atlas_db.inputs.nc4 import AtlasNc4Input
from atlas_db.ingestors.mongodb import AtlasMongoIngestor


class AtlasPsims(AtlasNc4Input):
    """Ingestion object for PSIMS.

    """
    def __init__(self, backend, *args, **kwargs):
        super(AtlasPsims, self).__init__(*args, **kwargs)
        self.name = ''.join(self.nc_file.split('/')[-1].split('.')[:-1])
        params = ['agricultural_model', 'climate_model', None, 'harms',
                  'irrigation', 'variable', 'crop']
        self.parameters = {
            params[i]: v for i, v in enumerate(self.name.split('_'))
            if i in [0, 1, 2, 3, 4, 5, 6]}
        # self.name = '_'.join(self.name.split('_')[3:5])
        self.human_name = 'pSIMS: {0} {1} {2}'.format(
            self.parameters['agricultural_model'],
            self.parameters['climate_model'],
            self.parameters['irrigation'])
        self.backend = backend

    def ingest(self):
        self.backend.ingest_metadata(self.metadata)
        for variable in self.variables:
            self.ingest_variable(variable)

    def ingest_variable(self, variable):

        print(variable)

        values = self.nc_dataset[variable][:]

        lons_lats = itertools.product(
            enumerate(self.lats), enumerate(self.lons))

        values = np.swapaxes(
            values, self.nc_dataset.variables[variable].dimensions.index(
                self.lat_var), 0)

        values = np.swapaxes(
            values, self.nc_dataset.variables[variable].dimensions.index(
                self.lon_var), 1)

        self.backend.ingest(values, lons_lats, self.metadata['name'], variable)


if __name__ == '__main__':
    import os
    from atlas_db.constants import BASE_DIR
    _filename = '{}_{}_{}_{}_{}_{}_{}_{}_{}.nc4'.format(
        'papsim', 'wfdei.cru', 'hist', 'default', 'firr', 'whe',
        'annual', 1979, 2012)
    NC_FILE = os.path.join(BASE_DIR, 'data', 'netcdf', 'psims', _filename)
    p = AtlasPsims(AtlasMongoIngestor(), NC_FILE)
    p.ingest()
