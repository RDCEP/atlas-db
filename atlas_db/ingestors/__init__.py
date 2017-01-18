#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np


class AtlasIngestor(object):
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def num_or_null(arr):
        """Represent null values from netCDF as '--' and numeric values
        as floats.
        """
        try:
            if np.ma.getmask(arr).any():
                if arr.count() == 0:
                    return None
                return np.ma.filled(arr, np.nan)
            try:
                return arr.tolist()
            except SyntaxError:
                print(arr)

        except ValueError:
            print(
                '\n*** Encountered uncoercible non-numeric ***\n{}\n\n'.format(
                    arr
                ))
            pass


class AtlasSchema(object):
    def __init__(self, x, y, value):
        self.x = x
        self.y = y
        self.value = [None if np.isnan(x) else float(x) for x in value]

    @property
    def __geo_interface__(self):
        return dict()

    @property
    def as_dict(self):
        return self.__geo_interface__
