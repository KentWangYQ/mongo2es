# -*- coding: utf-8 -*-


from flask import jsonify


class BusinessException(Exception):
    @property
    def error(self): return self.__error

    @property
    def message(self): return self.__message

    @property
    def code(self): return self.__code

    @property
    def status_code(self): return self.__status_code

    @property
    def extra_data(self): return self.__extra_data

    def __init__(self, message, code, status_code=None, extra_data=None):
        self.__status_code = 500

        self.__error = True
        self.__message = message
        self.__code = code
        if status_code is not None:
            self.__status_code = status_code
        self.__extra_data = extra_data

    def to_dict(self):
        return {
            'error': self.error,
            'message': self.message,
            'statusCode': self.status_code,
            'extraData': self.extra_data
        }

    def to_json(self):
        return jsonify(self.to_dict())

    def set_message(self, message):
        self.__message = message
