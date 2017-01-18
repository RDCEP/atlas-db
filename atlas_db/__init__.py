#!/usr/bin/env python
# -*- coding: utf-8 -*-


class AtlasDB(object):
    def __init__(self, backend):
        self._backend = backend

    def get_backend(self):
        if self._backend in ['mongo', ]:
            pass
