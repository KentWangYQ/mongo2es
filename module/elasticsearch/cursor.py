# -*- coding: utf-8 -*-


class Cursor(object):
    def __init__(self, total=0, limit=100, skip=0, count=0, filter=None, projection=None, pop_fields=None,
                 pipeline=None):
        self.total = total
        self.limit = limit
        self.skip = skip
        self.count = count
        self.filter = filter
        self.projection = projection
        self.pop_fields = pop_fields
        self.pipeline = pipeline
