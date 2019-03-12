# -*- coding: utf-8 -*-


class ErrorStatus(object):
    __open = 'open'
    __resolved = 'resolved'
    __closed = 'closed'

    @property
    def open(self):
        return self.__open

    @property
    def resolved(self):
        return self.__resolved

    @property
    def closed(self):
        return self.__closed
