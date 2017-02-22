#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import itertools
import numpy as np
import requests
from lxml import html
from atlas_db.inputs.nc4 import AtlasNc4Input
from atlas_db.ingestors.mongodb import AtlasMongoIngestor
from atlas_db.constants import BASE_DIR


class AtlasGsdeTile(AtlasNc4Input):
    def __init__(self, backend, *args, **kwargs):
        super(AtlasGsdeTile, self).__init__(*args, **kwargs)
        self.name = 'gsde'
        self.human_name = 'Global Soil Dataset for Earth System Modeling'
        self.backend = backend
        self.excluded_vars = ['cropland', 'fieldsize', 'elev', 'sldr', 'salb',
                              'slu1', 'slro']

    def ingest(self):
        self.backend.ingest_metadata(self.metadata)
        for variable in self.variables:
            self.ingest_variable(variable)

    def ingest_variable(self, variable):

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


class AtlasGsde(object):
    """Ingestion object for PSIMS.

    """
    def __init__(self, *args, **kwargs):
        self.name = 'gsde'
        self.url = 'http://users.rcc.uchicago.edu' \
                   '/~davidkelly999/gsde.2deg.tile/'
        self.bounds = dict(lonmin=0, lonmax=180, latmin=0, latmax=90)

    def get_all_tile_dirs(self):

        response = requests.get(self.url)
        parsed = html.fromstring(response.text)
        links = parsed.xpath('//tr//td//a/@href')
        for link in links:
            if link[0] == '/':
                continue
            if float(link[:4]) < self.bounds['latmin']:
                continue
            if float(link[:4]) > self.bounds['latmax']:
                continue
            print(link)
            self.get_nc4s_from_tile(self.url+link)

    def get_nc4s_from_tile(self, url):
        response = requests.get(url)
        parsed = html.fromstring(response.text)
        links = parsed.xpath('//tr//td//a/@href')
        for link in links:
            if link[0] == '/':
                continue
            if float(link.split('.')[0].split('_')[-1]) < self.bounds['lonmin']:
                continue
            if float(link.split('.')[0].split('_')[-1]) > self.bounds['lonmax']:
                continue
            print(link)
            nc_file = self.download_nc4(url+link)
            tile = AtlasGsdeTile(AtlasMongoIngestor(), nc_file)
            tile.ingest()
            os.remove(nc_file)

    @staticmethod
    def download_nc4(url):
        local_file = os.path.join(BASE_DIR, 'data', 'netcdf', 'gsde',
                                  url.split('/')[-1])
        r = requests.get(url, stream=True)
        with open(local_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        return local_file


if __name__ == '__main__':

    def lat2g(v):
        return (((v - -90) * (90 - 0)) / (90 - -90)) + 0

    def lon2g(v):
        return (((v - -180) * (180 - 0)) / (180 - -180)) + 0

    # India
    p = AtlasGsde()
    p.bounds = dict(lonmin=lon2g(60), lonmax=lon2g(98),
                    latmin=lat2g(6), latmax=lat2g(36))
    p.get_all_tile_dirs()

    # United States
    p = AtlasGsde()
    p.bounds = dict(lonmin=lon2g(-124), lonmax=lon2g(-67),
                    latmin=lat2g(24), latmax=lat2g(50))
    p.get_all_tile_dirs()
