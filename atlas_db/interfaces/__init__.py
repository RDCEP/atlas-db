#!/usr/bin/env python
# -*- coding: utf-8 -*-
from atlas_db.ingestors import AtlasIngestor


class AtlasInterface(object):
    def __init__(self, *args, **kwargs):
        """Generic object describing a model interface to a backend.
         Interface objects need to specify the following attributes:

         * name (str): slugified, computer-readable name of dataset
         * human_name (str): human-readable name of dataset
         * parameters (dict): names and vales of dataset parameters
         * backend (object): an instance of an object inheriting from
           AtlasIngestor

         Interface objects need to declare the following methods:

         * ingest()
        """
        self.name = str()
        self.human_name = str()
        self.parameters = dict()
        self.backend = AtlasIngestor()

    def ingest(self):
        pass
