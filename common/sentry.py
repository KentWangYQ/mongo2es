# -*- coding: utf-8 -*-

from raven.contrib import flask
from raven import Client

from config import settings


class SentryClient(object):
    client = None

    def __init__(self, ignore_exceptions=None):
        self.client = Client(dsn=settings.SENTRY_DSN, ignore_exceptions=ignore_exceptions)

    def error(self, message):
        return self.client.captureMessage(message, level='error')

    def warning(self, message):
        return self.client.captureMessage(message, level='warning')

    def info(self, message):
        return self.client.captureMessage(message, level='info')

    def debug(self, message):
        return self.client.captureMessage(message, level='debug')

    def fatal(self, message):
        return self.client.captureMessage(message, level='fatal')

    def capture_exception(self):
        try:
            self.client.captureException()
        except:
            pass


class FlaskSentry(SentryClient):
    def __init__(self, app=None):
        super(FlaskSentry, self).__init__()
        self.__sentry = flask.Sentry(app, client=super(FlaskSentry, self).client)

    def handle_exception(self):
        self.__sentry.handle_exception()
