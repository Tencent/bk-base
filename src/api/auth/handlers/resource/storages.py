# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""


import operator
from functools import reduce

import attr
from auth.api.meta import MetaApi
from auth.exceptions import InvalidResourceAttrErr
from auth.handlers.resource.filters import AuthOP
from common.log import logger
from django.db.models import Q
from mako.template import Template


@attr.s
class DgraphAttrFilter:
    func = attr.ib(type=str)
    key = attr.ib(type=str)
    value = attr.ib(type=str)


class BaseStorage:
    def __init__(self, resource_class):
        self.resource_class = resource_class

    def query(self, resource_filter=None, search_filters=None, attr_filters=None, offset=0, limit=100):
        """
        检索入口，支持字段检索、支持分页，一次支持一个 resource_filter 过滤

        @param resource_filter {ResourceFilter} 父级资源过滤条件
        @param attr_filter {ResourceAttrFilter[]} 属性过滤条件，AND 关系
        @param search_filters {ResourceAttrFilter} 检索条件，OR 关系
        """
        raise NotImplementedError

    def get(self, resource_id):
        """
        获取资源实例

        @param {String} resource_id  资源实例 ID
        @return {AuthResource} 返回资源实例
        """
        raise NotImplementedError


class DgraphStorage(BaseStorage):
    QUERY_TEMPLATE = Template(
        """
        {

            % if parent is not UNDEFINED:
            var(func: eq(${parent['resource_identifier_name']}, "${parent['id']}")) {
                raw_uids as ~${parent['name']}
            }
            % else:
            var(func: has(${identifier_name})) {
                raw_uids as uid
            },
            % endif

            % if search_filters is not UNDEFINED:
            var(func: uid(raw_uids))
                    @filter(
                        % for index, filter in enumerate(search_filters):
                        ${filter.func}(${filter.key}, ${filter.value})
                        % if index < len(search_filters) - 1:
                        OR
                        % endif
                        % endfor
                    ) {
                search_uids as uid
            }
            % else:
            var(func: uid(raw_uids)) {
                search_uids as uid
            }
            % endif

            % if attr_filters is not UNDEFINED:
            var(func: uid(search_uids))
                    @filter(
                        % for index, filter in enumerate(attr_filters):
                        ${filter.func}(${filter.key}, ${filter.value})
                        % if index < len(attr_filters) - 1:
                        AND
                        % endif
                        % endfor
                    ) {
                attr_uids as uid
            }
            % else:
            var(func: uid(search_uids)) {
                attr_uids as uid
            }
            % endif

            results(func: uid(attr_uids), offset: ${ offset }, first: ${ first }) {
                % for field in fields:
                    ${ field.name } : ${ field.metadata['dgraph_name']}
                % endfor
            }
            statistics(func: uid(attr_uids)) {
                total: count(uid)
            }
        }
    """
    )

    ONE_TEMPLATE = Template(
        """
        {
            results(func: eq(${identifier_name}, "${resource_id}")) {
                % for field in fields:
                    ${field.name} : ${field.metadata['dgraph_name']}
                % endfor
                % for parent in parents:
                    ${parent.name} : ${parent.metadata['dgraph_name']} {
                        ${parent.type.identifier_attr.name}: ${parent.type.identifier_attr.metadata['dgraph_name']}
                    }
                % endfor
            }
        }
    """
    )

    def query(self, resource_filter=None, search_filters=None, attr_filters=None, offset=0, limit=100):
        resource_class = self.resource_class
        identifier_name = resource_class.identifier_attr.metadata["dgraph_name"]
        context = dict(
            identifier_name=identifier_name,
            fields=list(resource_class.value_attrs_registry.values()),
            offset=offset,
            first=limit,
        )

        # 父级资源属性过滤
        if resource_filter:
            parent_attr = resource_class.parent_resources_registry[resource_filter.resource_type]
            context.update(
                {
                    "parent": {
                        "resource_identifier_name": parent_attr.type.identifier_attr.metadata["dgraph_name"],
                        "id": resource_filter.resource_id,
                        "name": parent_attr.metadata["dgraph_name"],
                    }
                }
            )

        if search_filters:
            search_filters = [self.to_dgraph_filter(f) for f in search_filters]
            context.update(dict(search_filters=search_filters))

        if attr_filters:
            attr_filters = [self.to_dgraph_filter(f) for f in attr_filters]
            context.update(dict(attr_filters=attr_filters))

        statement = self.QUERY_TEMPLATE.render(**context)
        logger.info(f"[DGraph] query statement: {statement}")
        data = MetaApi.entity_complex_search(
            {"statement": statement, "backend_type": "dgraph"}, raise_exception=True
        ).data

        results = data["data"]["results"]
        count = data["data"]["statistics"][0]["total"]

        return {"results": results, "count": count}

    def to_dgraph_filter(self, filter_):
        """
        将定义的 filter 装换为 dgraph 的 filter
        """
        # 装换字段
        if filter_.key not in self.resource_class.value_attrs_registry:
            raise InvalidResourceAttrErr()

        attr_cls = self.resource_class.value_attrs_registry[filter_.key]
        dgraph_key = attr_cls.metadata["dgraph_name"]

        # 根据资源类型定义，规整下过滤属性值，后续即使不适用 dgraph 作为查询后端，这个清洗环节还是有必要的
        if type(filter_.value) is list:
            value = [attr_cls.type(v) for v in filter_.value]
        else:
            value = attr_cls.type(filter_.value)

        # 部分函数格式需要调整
        if filter_.func == AuthOP.LIKE:
            dgraph_func = "regexp"
            dgraph_value = f"/{value}/i"
        elif filter_.func == AuthOP.EQ:
            dgraph_func = "eq"
            dgraph_value = f'"{value}"' if type(value) is str else value
        elif filter_.func == AuthOP.IN:
            dgraph_func = "eq"
            dgraph_value = value
        else:
            raise Exception(f"WTC, Invalid filter.func {filter_.func} ....")

        return DgraphAttrFilter(dgraph_func, dgraph_key, dgraph_value)

    def get(self, resource_id):
        resource_class = self.resource_class
        identifier_name = resource_class.identifier_attr.metadata["dgraph_name"]
        context = dict(
            resource_id=resource_id,
            identifier_name=identifier_name,
            fields=list(resource_class.value_attrs_registry.values()),
            parents=list(resource_class.parent_resources_registry.values()),
        )

        statement = self.ONE_TEMPLATE.render(**context)
        logger.info(f"[DGraph] query statement: {statement}")
        data = MetaApi.entity_complex_search(
            {"statement": statement, "backend_type": "dgraph"}, raise_exception=True
        ).data

        results = data["data"]["results"]
        if len(results) > 0:
            data = results[0]
            for parent in list(resource_class.parent_resources_registry.values()):
                if data.get(parent.name, None) is not None:
                    data[parent.name] = parent.type(**data[parent.name][0])
                else:
                    data[parent.name] = None

            return resource_class(**data)

        return None


class MySQLStorage(BaseStorage):
    def query(self, resource_filter=None, search_filters=None, attr_filters=None, offset=0, limit=100):
        resource_class = self.resource_class
        model_cls = resource_class.storage_model

        qs_arr = []
        if search_filters:
            qs_arr.append(reduce(operator.or_, [self.to_Q(f) for f in search_filters]))

        if attr_filters:
            qs_arr.append(reduce(operator.and_, [self.to_Q(f) for f in attr_filters]))

        if resource_filter:
            parent_attr = resource_class.parent_resources_registry[resource_filter.resource_type]
            qs_arr.append(Q(**{parent_attr.metadata["mysql_name"]: resource_filter.resource_id}))

        if len(qs_arr) > 0:
            query_set = model_cls.objects.filter(reduce(operator.or_, qs_arr))
        else:
            query_set = model_cls.objects.all()

        query_set = query_set[offset : offset + limit]

        return {"results": list(query_set.values()), "count": query_set.count()}

    def get(self, resource_id):
        resource_class = self.resource_class
        model_cls = resource_class.storage_model

        try:
            model_instance = model_cls.objects.get(pk=resource_id)
            return self.convert_to_resource(model_instance)
        except model_cls.DoesNotExist:
            return None

    def to_Q(self, filter_):
        """
        装换为 ORM 查询的 Q 语句
        """
        # 部分函数格式需要调整
        if filter_.func == AuthOP.LIKE:
            key_ = f"{filter_.key}__icontains"
        elif filter_.func == AuthOP.EQ:
            key_ = filter_.key
        elif filter_.func == AuthOP.IN:
            key_ = f"{filter_.key}__in"
        else:
            raise Exception(f"WTC, Invalid filter.func {filter_.func} ....")

        return Q(**{key_: filter_.value})

    def convert_to_resource(self, model_instance):
        """
        将数据库的 Model 实例装换为资源实例
        @param resource_class {AuthResource.Class} 资源类型
        @param model_instance {Django.Model.Instance} 模型实例

        @return {AuthResource} 资源实例
        """
        resource_class = self.resource_class

        data = dict()

        fields = list(resource_class.value_attrs_registry.values())
        parents = list(resource_class.parent_resources_registry.values())

        for f in fields:
            data[f.name] = getattr(model_instance, f.name)

        for p in parents:
            mysql_name = p.metadata["mysql_name"]
            data[p.name] = p.type(**{mysql_name: getattr(model_instance, mysql_name)})

        return resource_class(**data)


class StorageManager:
    def __init__(self):
        self._namespace = dict()

    def register(self, name, storager_cls):
        self._namespace[name] = storager_cls

    def match(self, resource_class):
        """
        通过资源类型，匹配命中合适的 StorageBackend
        """
        storage_type = resource_class.storage_type
        return self._namespace[storage_type](resource_class)


storage_manager = StorageManager()
storage_manager.register("dgraph", DgraphStorage)
storage_manager.register("mysql", MySQLStorage)
