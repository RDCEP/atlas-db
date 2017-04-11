#!/usr/bin/env python
# -*- coding: utf-8 -*-
from atlas_db.inputs import Atlas2DegTile, Atlas2DegInput


class AtlasGsdeTile(Atlas2DegTile):
    def __init__(self, backend, *args, **kwargs):
        super(AtlasGsdeTile, self).__init__(*args, **kwargs)
        self.name = 'gsde'
        self.human_name = 'Global Soil Dataset for Earth System Modeling'
        self.backend = backend
        self.excluded_vars = ['cropland', 'fieldsize', 'elev', 'sldr', 'salb',
                              'slu1', 'slro']
        self.no_index = True


class AtlasGsde(Atlas2DegInput):
    """Ingestion object for PSIMS.

    """
    def __init__(self, *args, **kwargs):
        super(AtlasGsde, self).__init__(*args, **kwargs)
        self.name = 'gsde'
        self.human_name = 'Global Soil Dataset for Earth System Modeling'
        self.input = None
        self.url = 'http://users.rcc.uchicago.edu' \
                   '/~davidkelly999/gsde.2deg.tile/'
        self.lons = set()
        self.lats = set()
        self.bounds = dict(lonmin=0, lonmax=180, latmin=0, latmax=90)
        self.tile_obj = AtlasGsdeTile


if __name__ == '__main__':

    def lat2g(v):
        return (((v - -90) * (0 - 90)) / (90 - -90)) + 90

    def lon2g(v):
        return (((v - -180) * (180 - 0)) / (180 - -180)) + 0

    # India
    p = AtlasGsde()
    p.bounds = dict(lonmin=lon2g(60), lonmax=lon2g(98),
                    latmin=lat2g(6), latmax=lat2g(36))
    p.get_all_tile_dirs()

