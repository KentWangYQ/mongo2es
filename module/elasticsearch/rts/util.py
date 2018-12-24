# -*- coding: utf-8 -*-

import traceback

from common.sentry import SentryClient
from module.elasticsearch.sync import es_errors


def elasticsearch_error(error):
    try:
        es_errors.exception.index_one(error, traceback.format_exc())
    except Exception:
        sentry_client = SentryClient(ignore_exceptions=['KeyboardInterrupt'])
        sentry_client.capture_exception()
