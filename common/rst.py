# -*- coding: utf-8 -*-

from flask import jsonify


class Rst(object):
    @property
    def data(self): return self.__data

    @property
    def error(self): return self.__error

    @property
    def message(self): return self.__message

    @property
    def status_code(self): return self.__status_code

    @property
    def extra_data(self): return self.__extra_data

    def __init__(self, data=None, message=None, status_code=None, extra_data=None, error=False):
        self.__status_code = 200

        self.__data = data
        self.__error = error
        self.__message = message
        if status_code is not None:
            self.__status_code = status_code
        self.__extra_data = extra_data

    def to_dict(self):
        return {
            'data': self.data,
            'error': self.error,
            'message': self.message,
            'statusCode': self.status_code,
            'extraData': self.extra_data
        }

    def to_json(self):
        return jsonify(self.to_dict())

    def set_message(self, message):
        self.__message = message
