# -*- coding: utf-8 -*-

import bson
import re


def generate_query(**options):
    q = {}

    ts = options.get('ts')

    ns = options.get('ns')
    include_ns = options.get('include_ns')
    exclude_ns = options.get('exclude_ns')

    op = options.get('op')
    include_ops = options.get('include_ops')
    exclude_ops = options.get('exclude_ops')

    if isinstance(ts, bson.Timestamp):
        q['ts'] = {'$gt': ts}

    # ns
    q_ns = []
    if isinstance(ns, str):
        if ns.endswith('.*'):
            q_ns.append({'ns': {'$regex': '^{0}\\.'.format(ns.replace('.*', ''))}})
    if isinstance(include_ns, list):
        q_ns.append({'ns': {'$in': include_ns}})
    if isinstance(exclude_ns, list):
        q_ns.append({'ns': {'$nin': exclude_ns}})
    if len(q_ns) > 0:
        q['$and'] = q_ns

    # op
    if isinstance(op, str):
        q['op'] = op
    elif isinstance(include_ops, list):
        q['op'] = {'$in': include_ops}
    elif isinstance(exclude_ops, list):
        q['op'] = {'$nin': exclude_ops}

    return q
