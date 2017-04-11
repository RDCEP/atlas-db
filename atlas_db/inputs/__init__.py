#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from glob import glob
import itertools
import numpy as np
import requests
from lxml import html
from atlas_db.constants import BASE_DIR, SCALE
from atlas_db.ingestors.mongodb import AtlasMongoIngestor
from atlas_db.interfaces.nc4 import AtlasNc4Interface


class Atlas2DegTile(AtlasNc4Interface):
    def __init__(self, backend, *args, **kwargs):
        super(Atlas2DegTile, self).__init__(*args, **kwargs)
        self.name = None
        self.human_name = None
        self.backend = backend
        self.excluded_vars = list()
        self.no_index = True

    def ingest(self):
        if not self.no_index:
            self.backend.ingest_metadata(self.metadata)
        self.ingest_variable()

    def ingest_variable(self):

        lons_lats = itertools.product(
            enumerate(self.lats), enumerate(self.lons))

        variables = dict()

        for v in self.variables:
            values = self.nc_dataset[v][:]

            values = np.swapaxes(
                values, self.nc_dataset.variables[v].dimensions.index(
                    self.lat_var), 0)

            values = np.swapaxes(
                values, self.nc_dataset.variables[v].dimensions.index(
                    self.lon_var), 1)

            variables[v] = values

        # TODO: Add lons and lats to meta.
        # db.grid_meta.update( { "name" : "gsde" }, { $set: { "lats" : db.gsde.distinct( "loc.1" ) } } )
        # db.grid_meta.update( { "name" : "gsde" }, { $set: { "lons" : db.gsde.distinct( "loc.0" ) } } )

        self.backend.ingest(variables, lons_lats, self.metadata['name'],
                            no_index=self.no_index)


class Atlas2DegInput(object):
    def __init__(self):
        self.url = 'http://users.rcc.uchicago.edu' \
                   '/~davidkelly999/gsde.2deg.tile/'
        self.name = ''
        self.local_dir = ''
        self.lons = set()
        self.lats = set()
        self.bounds = dict(lonmin=0, lonmax=180, latmin=0, latmax=90)
        self.tile_obj = Atlas2DegTile
        self.ingestor = AtlasMongoIngestor

    def ingest_remote_tiles(self):
        lon_lat_links = self.get_all_tile_dirs()
        for i, link in enumerate(lon_lat_links):

            nc_file = self.download_nc4(link)
            tile = self.tile_obj(self.ingestor(SCALE), nc_file, SCALE)

            self.lons = self.lons.union(set([x for x in tile.lons]))
            self.lats = self.lats.union(set([x for x in tile.lats]))

            tile.no_index = i + 1 < len(lon_lat_links)
            if not tile.no_index:
                tile.metadata['lons'] = [float(x) for x in self.lons]
                tile.metadata['lats'] = [float(x) for x in self.lats]
            tile.ingest()
            os.remove(nc_file)

    def ingest_local_tiles(self):
        tiles = glob.glob(os.path.join(BASE_DIR, 'data', 'netcdf',
                          self.name, '*'))

        for i, f in enumerate(tiles):
            tile = self.tile_obj(self.ingestor(SCALE), f, SCALE)

            self.lons = self.lons.union(set([x for x in tile.lons]))
            self.lats = self.lats.union(set([x for x in tile.lats]))

            tile.no_index = i + 1 < len(tiles)
            if not tile.no_index:
                tile.metadata['lons'] = [float(x) for x in self.lons]
                tile.metadata['lats'] = [float(x) for x in self.lats]
            tile.ingest()

    def get_all_tile_dirs(self):
        response = requests.get(self.url)
        parsed = html.fromstring(response.text)
        links = parsed.xpath('//tr//td//a/@href')
        lat_links = [
            link for link in links
            if link[0] != '/' and (
                self.bounds['latmax'] <= int(float(link[:4])) <=
                self.bounds['latmin'])
        ]
        lon_lat_links = list()
        for i, lat_link in enumerate(lat_links):
            response = requests.get(self.url + lat_link)
            parsed = html.fromstring(response.text)
            links = parsed.xpath('//tr//td//a/@href')
            lon_links = [
                link for link in links
                if link[0] != '/' and (
                    self.bounds['lonmax'] >= int(float(
                        link.split('.')[0].split('_')[-1])) >=
                    self.bounds['lonmin'])
            ]
            lon_lat_links += [self.url + lat_link + lon_link
                              for lon_link in lon_links]
        return lon_lat_links

    def download_nc4(self, url):
        local_file = os.path.join(BASE_DIR, 'data', 'netcdf', self.name,
                                  url.split('/')[-1])
        r = requests.get(url, stream=True)
        with open(local_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        return local_file
