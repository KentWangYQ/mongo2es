# -*- coding: utf-8 -*-


class ReadOnlyProperty(object):
    # TODO 自动通过私有属性生成只读属性,防止信息修改
    pass


class ErrorStatus(ReadOnlyProperty):
    open = 'open'
    resolved = 'resolved'
    closed = 'closed'
