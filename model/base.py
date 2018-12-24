# -*- coding: utf-8 -*-

import pydash as _
from pymongo import collection

from model import models


class BaseModel(collection.Collection):
    """
    扩展方法
    """

    def populates(self, filter=None, projection=None, sort=None, skip=0, limit=0, pop_fields=None,
                  field_value_filter=lambda v: v):
        """
        :param filter:
        :param projection:
        :param sort:
        :param skip:
        :param limit:
        :param field_value_filter:lambda v:str(v)
        :param pop_fields: {
                                'user': {
                                    'from':'users',
                                    'local_field':' support',
                                    'foreign_field':'_id',
                                    'as':'support',
                                    'projection': {'_id':1, 'name':1, 'phone':1},
                                    'pop_fields': {...}
                                },
                                'merchant': {
                                      'from': 'merchants',
                                      'local_field': 'merchant',
                                      'foreign_field': '_id',
                                      'as': 'merchant',
                                      'projection': {'_id':1, 'name':1, 'sname':1},
                                      'pop_fields': {...}
                                  }
                            }
        :return:
        """

        # 查询主collection数据
        course = self.find(filter=filter, projection=projection, sort=sort, skip=skip, limit=limit)
        local_data_list = list(course or [])

        for key, value in pop_fields.items():
            local_data_list = self.__populate(local_data_list, value, field_value_filter)

        return local_data_list

    def populate_one(self, filter=None, projection=None, pop_fields=None, field_value_filter=lambda v: v):
        """
        :param filter:
        :param projection:
        :param pop_fields: [
                                {
                                    'from':'users',
                                    'local_field':' support',
                                    'foreign_field':'_id',
                                    'as':'support',
                                    'projection': {'_id':1, 'name':1, 'phone':1},
                                    'pop_fields': {...}
                                },
                            ]
        :return:
        """

        # 查询主collection数据
        result = self.find_one(filter=filter, projection=projection)

        if result:
            result = [result]
            for key, value in pop_fields.items():
                result = self.__populate(result, value, field_value_filter)

            for r in result:
                result = r
                break

        return result

    def __populate(self, local_data_list, pop_fields, field_value_filter=lambda v: v):
        # 获取外键collection，并判断是否存在
        foreign_collection = models.get_collection(pop_fields.get('from'))

        # 主键set，用于存储主键（不重复）
        local_field_set = set()
        local_field = pop_fields.get('local_field')
        _as = pop_fields.get('as') or local_field
        # 收集主键
        for local_data in local_data_list:
            local_field_set.add(field_value_filter(_.get(local_data, local_field)))

        # 无主键，返回主collection数据
        if len(local_field_set) <= 0:
            return local_data_list

        foreign_field = pop_fields.get('foreign_field')
        foreign_data_dict = dict()
        f_filter = {pop_fields.get('foreign_field'): {'$in': list(local_field_set)}}

        # 查询外键collection，并形成dict
        foreign_data_list = list(foreign_collection.find(filter=f_filter, projection=pop_fields.get('projection')))

        # 递归查询populate数据
        recursion_pop_fields = pop_fields.get('pop_fields')
        if recursion_pop_fields:
            for key, value in recursion_pop_fields.items():
                foreign_data_list = self.__populate(foreign_data_list, value, field_value_filter)

        for foreign_data in foreign_data_list:
            if isinstance(_.get(foreign_data, foreign_field), list):
                for arr_id in _.get(foreign_data, foreign_field):
                    foreign_data_dict[field_value_filter(arr_id)] = foreign_data
            else:
                foreign_data_dict[field_value_filter(_.get(foreign_data, foreign_field))] = foreign_data

        # 匹配外键，并将数据添加到主collection
        for local_data in local_data_list:
            local_data[_as] = foreign_data_dict.get(field_value_filter(_.get(local_data, local_field))) or {}

        return local_data_list
