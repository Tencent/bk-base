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


from auth.exceptions import (
    FilterNotToScopeErr,
    InvalidParentResourceTypeErr,
    ObjectNotExistsErr,
    QueryTooManyResourceErr,
)
from auth.handlers.resource.attributes import (
    ConstanceScopeAttribute,
    ResourceScopeAttribute,
)
from auth.handlers.resource.base import default_auth_resources_registry
from auth.handlers.resource.filters import (
    AuthOP,
    GlobalFilter,
    ResourceAttrFilter,
    ResourceFilter,
    ResourceIdFilter,
)
from auth.handlers.resource.storages import storage_manager


class AuthResourceManager:
    @classmethod
    def get_resource_class(cls, resource_type):
        """
        获取资源类
        """
        return default_auth_resources_registry[resource_type]

    @classmethod
    def init(cls, resource_type):
        """
        通过资源类型名称进行初始化
        """
        return cls(cls.get_resource_class(resource_type))

    def __init__(self, resource_class):
        self.resource_class = resource_class

    def list_scope_attributes(self):
        """
        列举作为范围限定的属性选项
        """
        scopes_attrs = list(self.resource_class.scope_attrs_registry.values())
        return [
            {"id": sa.metadata["scope_attribute"].key, "display_name": sa.metadata["scope_attribute"].name}
            for sa in scopes_attrs
        ]

    def query_scope_attribute_value(self, attribute_key, search=None, offset=0, limit=100, ids=None):
        """
        查询范围属性的取值选项

        @param attribute_key {String} 属性键值，指定需要查询的属性
        @param search {String} 关键字，用于检索记录，目前针对 id 或者 display_name 进行检索
        @param offset {Int} 分页配置，偏移量
        @param limit {Int} 分页配置，数量
        @param ids {Int} 根据id批量查询

        @example 返回示例
            [
                {'id': 'public', 'display_name': '公开'}
            ]
        """
        if limit > 1000 or limit == 0:
            raise QueryTooManyResourceErr()

        scope_attribute = self.resource_class.scope_attrs_registry[attribute_key].metadata["scope_attribute"]

        # 一般常量属性，数量不多
        if isinstance(scope_attribute, ConstanceScopeAttribute):
            filter_items = scope_attribute.items
            if ids:
                filter_items = [item for item in filter_items if item["id"] in ids]

            if search:
                filter_items = [item for item in filter_items if search in item["display_name"]]

            return {"results": filter_items[offset : offset + limit], "count": len(filter_items)}

        # 将属性转换为资源选择
        if isinstance(scope_attribute, ResourceScopeAttribute):
            kwargs = dict(offset=offset, limit=limit, only_display=True)

            related_resource = scope_attribute.related_resource

            if search is not None:
                kwargs["search"] = search

            if ids is not None:
                kwargs["ids"] = ids
            scope_manager = AuthResourceManager.init(related_resource)
            return scope_manager.query(**kwargs)

        return {"results": [], "count": 0}

    def query(
        self, search=None, resource_filter=None, attr_filters=None, offset=0, limit=100, only_display=False, ids=None
    ):
        """
        检索入口，支持字段检索、支持分页，一次支持一个 resource_filter 过滤

        @param resource_filter {ResourceFilter} 父级资源过滤条件
        @param attr_filter {ResourceAttrFilter[]} 属性过滤条件
        @param search {String} 快捷检索，支持 ID 匹配或者名称字段模糊检索
        """

        if limit > 1000 or limit == 0:
            raise QueryTooManyResourceErr()

        resource_class = self.resource_class

        context = {"resource_filter": resource_filter, "offset": offset, "limit": limit}
        search_filters = []
        # 模糊搜索属性过滤
        if search:
            search_filters = [ResourceAttrFilter("like", resource_class.display_attr.name, search)]
            # 当主键为数字类型，检索值非数字字符，会出错，此处需要判断后进行规避
            if resource_class.identifier_attr.type is int and search.isdigit():
                search_filters.append(ResourceAttrFilter("eq", resource_class.identifier_attr_name, search))

            context.update(dict(search_filters=search_filters))

        # 父级资源属性过滤
        if resource_filter:
            # 检查是否为合法父类型
            if resource_filter.resource_type not in resource_class.parent_resources_registry:
                raise InvalidParentResourceTypeErr()
            context.update(dict(resource_filter=resource_filter))

        # 常规属性过滤
        if attr_filters is None:
            attr_filters = []

        if ids:
            _filters = [ResourceAttrFilter("in", resource_class.identifier_attr_name, ids)]
            attr_filters.extend(_filters)

        if attr_filters:
            context.update(dict(attr_filters=attr_filters))
        storage = storage_manager.match(self.resource_class)
        data = storage.query(
            resource_filter=resource_filter,
            attr_filters=attr_filters,
            search_filters=search_filters,
            offset=offset,
            limit=limit,
        )

        if only_display:
            data["results"] = self.simplify_for_display(data["results"])

        return data

    def simplify_for_display(self, data):
        """
        将数据结构进行简化，仅保留 id & display 字段
        """
        return [
            {
                "id": d[self.resource_class.identifier_attr.name],
                "display_name": "[{id}] {name}".format(
                    id=d[self.resource_class.identifier_attr.name],
                    name=d.get(self.resource_class.display_attr.name, " "),
                ),
            }
            for d in data
        ]

    def get(self, resource_id, simple=False, raise_exception=False):
        """
        获取资源实例

        @param {String} resource_id  资源实例 ID
        @param {Bool} simple 返回简化版的资源对象，不需要额外属性，避免产生对 Meta 接口调用
        @return {AuthResource} 返回资源实例
        """
        resource_class = self.resource_class

        if simple:
            return resource_class(**{resource_class.identifier_attr_name: resource_id})

        storage = storage_manager.match(self.resource_class)
        resource = storage.get(resource_id)
        if resource is None and raise_exception:
            raise ObjectNotExistsErr(f"{resource_class} instance({resource_id}) not exist")

        return resource

    def to_scopes(self, filter_):
        """
        将资源过滤器，根据 resource_cls 的定义，转换为 scopes 属性范围
        """
        resource_class = self.resource_class

        if isinstance(filter_, ResourceFilter):
            if filter_.resource_type not in resource_class.parent_resources_registry:
                raise InvalidParentResourceTypeErr()

            parent_attr = resource_class.parent_resources_registry[filter_.resource_type]
            return [{parent_attr.type.identifier_attr_name: filter_.resource_id}]

        if isinstance(filter_, ResourceAttrFilter):
            if filter_.func == AuthOP.EQ:
                return [{filter_.key: filter_.value}]

            raise FilterNotToScopeErr()

        if isinstance(filter_, ResourceIdFilter):
            key = resource_class.identifier_attr_name
            if filter_.func == AuthOP.EQ:
                return [{key: filter_.value}]

            if filter_.func == AuthOP.IN:
                return [{key: v} for v in filter_.value]

            raise FilterNotToScopeErr()

        if isinstance(filter_, GlobalFilter):
            return [{"*": "*"}]

        raise FilterNotToScopeErr()
