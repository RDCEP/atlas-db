#!/usr/bin/env python
# -*- coding: utf-8 -*-
from netCDF4 import Dataset
from atlas_db.inputs import AtlasInput


class AtlasNc4Input(AtlasInput):
    def __init__(self, nc_file, *args, **kwargs):
        """Object for parsing netCDF files as data inputs to the ATLAS.

        :param nc_file: Path to netCDF input file
        :type nc_file: str
        :return: None
        :rtype: None
        """
        super(AtlasNc4Input, self).__init__(*args, **kwargs)
        self.nc_file = nc_file
        self.nc_dataset = Dataset(self.nc_file, 'r')
        self.name = None
        self.human_name = None
        self._lon_var = None
        self._lat_var = None
        self._variables = None
        self._dimensions = None
        self._parameters = dict()
        self.excluded_vars = []

        try:
            self._lats = self.nc_dataset.variables[self.lat_var][:]
        except KeyError:
            raise Exception('Dataset must have a latitude dimension.')
        try:
            self._lons = self.nc_dataset.variables[self.lon_var][:]
        except KeyError:
            raise Exception('Dataset must have a longitude dimension.')

    @property
    def dimensions(self):
        """List of dimensions other than longitude and latitude.

        :return: List of dimensions in NetCDF file (excluding lonlat)
        :rtype: list
        """
        if self._dimensions is None:
            self._dimensions = [d for d in self.nc_dataset.dimensions.keys()
                                if d not in [self.lon_var, self.lat_var]]
        return self._dimensions

    @property
    def variables(self):
        """List of variables in NetCDF, other than dimensions in NetCDF.

        :return: List of variables in NetCDF file (excluding dimensions)
        :rtype: list
        """
        if self._variables is None:
            self._variables = [v for v in self.nc_dataset.variables.keys()
                               if v not in self.nc_dataset.dimensions.keys()
                               and v not in self.excluded_vars]
        return self._variables

    @property
    def lat_var(self):
        """String used for latitude variable in the netCDF file.

        :return: Name of latitude variable.
        :rtype: str
        """
        if self._lat_var is None:
            self._lat_var = 'lat'
        return self._lat_var

    @property
    def lon_var(self):
        """String used for longitude variable in the netCDF file.

        :return: Name of longitude variable.
        :rtype: str
        """
        if self._lon_var is None:
            self._lon_var = 'lon'
        return self._lon_var
