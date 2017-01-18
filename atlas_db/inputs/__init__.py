#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime
import numpy as np


class AtlasInput(object):
    def __init__(self, *args, **kwargs):
        self.nc_file = None
        self.nc_dataset = None
        self.name = None
        self.human_name = None
        self._lon_var = None
        self._lat_var = None
        self._lats = None
        self._lons = None
        self._variables = None
        self._dimensions = None
        self._parameters = None

    @property
    def lats(self):
        """List of all latitude values in dataset.

        :return: List of all latitude values in dataset.
        :rtype: list
        """
        return self._lats

    @property
    def lons(self):
        """List of all longitude values in dataset.

        :return: List of all longitude values in dataset.
        :rtype: list
        """
        return self._lons

    @property
    def dimensions(self):
        """List of dimensions other than longitude and latitude.

        :return: List of dimensions in NetCDF file (excluding lonlat)
        :rtype: list
        """
        return self._parameters

    @property
    def variables(self):
        """List of variables in NetCDF, other than dimensions in NetCDF.

        :return: List of variables in NetCDF file (excluding dimensions)
        :rtype: list
        """
        return self._variables

    @property
    def parameters(self):
        """Dictionary of parameter names and values for the dataset.

        :return: Dataset parameters
        :rtype: dict
        """
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        self._parameters = value

    @property
    def metadata(self):
        """Dictionary of all metadata for the current dataset.

        :return: Metadata for the current dataset.
        :rtype: dict
        """
        return {
            'name': self.name,
            'human_name': self.human_name,
            'date_created': datetime.now(),
            'date_inserted': datetime.now(),
            'dimensions': [
                {'name': self.nc_dataset.variables[d].name,
                 'human_name': self.nc_dataset.variables[d].long_name,
                 'min': float(np.min(self.nc_dataset.variables[d][:])),
                 'max': float(np.max(self.nc_dataset.variables[d][:])),
                 'size': int(self.nc_dataset.variables[d].size),
                 'unit': self.nc_dataset.variables[d].units,
                 } for d in self.dimensions],
            'variables': [
                {'name': self.nc_dataset.variables[v].name,
                 'human_name': self.nc_dataset.variables[v].long_name,
                 'min': float(np.min(self.nc_dataset.variables[v][:])),
                 'max': float(np.max(self.nc_dataset.variables[v][:])),
                 'unit': self.nc_dataset.variables[v].units,
                 'dimension_idxs': [i for i, d in enumerate(self.dimensions)
                                    if d in self.nc_dataset.variables[
                                        v].dimensions],
                 'dimensions': [d for i, d in enumerate(self.dimensions)
                                if
                                d in self.nc_dataset.variables[v].dimensions],
                 } for v in self.variables],
            'parameters': [
                {'name': k,
                 'value': v,
                 } for k, v in self.parameters.iteritems()]
        }
