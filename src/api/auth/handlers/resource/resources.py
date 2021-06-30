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


import attr
from auth.handlers.object_classes import AuthResultTable
from auth.handlers.resource.attributes import (
    bk_biz_id_attribute,
    processing_type_attribute,
    sensitivity_attribute,
)
from auth.handlers.resource.base import AuthMetaAttr, AuthResource, as_attr, as_resource
from auth.models.outer_models import (
    DashboardInfo,
    DataModel,
    DataTokenInfo,
    FlowInfo,
    FunctionInfo,
)


class ResourceMixin(AuthResource):
    @property
    def id(self):
        """
        主键值
        """
        return getattr(self, self.identifier_attr.name)

    @property
    def display_name(self):
        """
        展示名称
        """
        return "[{}] {}".format(self.id, getattr(self, self.display_attr.name))

    @property
    def parents(self):
        """
        将父级属性归类为一个字段，便于获取
        """
        parents = []
        for resource_type, attr_inst in list(self.parent_resources_registry.items()):
            inst = getattr(self, attr_inst.name)
            if inst is not None:
                parents.append((resource_type, inst))

        return parents

    def indirect_parent_resources(self, action_id):
        """
        获取额外的父级资源，非对象属性可以直接获取，有子类来定制化实现
        """
        return []


@as_resource
@attr.s
class ProcessResource(ResourceMixin):
    resource_type = "process_type"
    storage_type = "dgraph"

    processing_type = as_attr(
        type=str, metadata=AuthMetaAttr(identifier=True, dgraph_name="ProcessingTypeConfig.processing_type_name")
    )
    processing_type_alias = as_attr(
        type=str,
        metadata=AuthMetaAttr(dgraph_name="ProcessingTypeConfig.processing_type_alias", is_display=True),
        default=None,
    )


@as_resource
@attr.s
class BizResource(ResourceMixin):
    resource_type = "biz"
    storage_type = "dgraph"

    bk_biz_id = as_attr(type=int, metadata=AuthMetaAttr(identifier=True, dgraph_name="BKBiz.id"))
    bk_biz_name = as_attr(type=str, metadata=AuthMetaAttr(dgraph_name="BKBiz.name", is_display=True), default=None)


@as_resource
@attr.s
class RawDataResource(ResourceMixin):
    resource_type = "raw_data"
    storage_type = "dgraph"

    raw_data_id = as_attr(type=int, metadata=AuthMetaAttr(identifier=True, dgraph_name="AccessRawData.id"))
    raw_data_name = as_attr(
        type=str, metadata=AuthMetaAttr(dgraph_name="AccessRawData.raw_data_name", is_display=True), default=None
    )
    bk_biz_id = as_attr(
        type=int,
        metadata=AuthMetaAttr(dgraph_name="AccessRawData.bk_biz_id", scope_attribute=bk_biz_id_attribute),
        default=None,
    )
    sensitivity = as_attr(
        type=str,
        metadata=AuthMetaAttr(dgraph_name="AccessRawData.sensitivity", scope_attribute=sensitivity_attribute),
        default=None,
    )

    bk_biz = as_attr(
        type=BizResource,
        metadata=AuthMetaAttr(dgraph_name="AccessRawData.bk_biz", is_parent=True, direct_attr="bk_biz_id"),
        default=None,
    )


@as_resource
@attr.s
class ProjectResource(ResourceMixin):
    resource_type = "project"
    storage_type = "dgraph"

    project_id = as_attr(type=int, metadata=AuthMetaAttr(identifier=True, dgraph_name="ProjectInfo.project_id"))
    project_name = as_attr(
        type=str, metadata=AuthMetaAttr(dgraph_name="ProjectInfo.project_name", is_display=True), default=None
    )


@as_resource
@attr.s
class ResultTableResource(ResourceMixin):
    resource_type = "result_table"
    storage_type = "dgraph"

    result_table_id = as_attr(
        type=str, metadata=AuthMetaAttr(identifier=True, dgraph_name="ResultTable.result_table_id")
    )
    result_table_name = as_attr(
        type=str, metadata=AuthMetaAttr(dgraph_name="ResultTable.result_table_name", is_display=True), default=None
    )
    sensitivity = as_attr(
        type=str,
        metadata=AuthMetaAttr(dgraph_name="ResultTable.sensitivity", scope_attribute=sensitivity_attribute),
        default=None,
    )

    processing_type = as_attr(
        type=str,
        metadata=AuthMetaAttr(dgraph_name="ResultTable.processing_type", scope_attribute=processing_type_attribute),
        default=None,
    )

    project = as_attr(
        type=ProjectResource, metadata=AuthMetaAttr(dgraph_name="ResultTable.project", is_parent=True), default=None
    )

    bk_biz = as_attr(
        type=BizResource, metadata=AuthMetaAttr(dgraph_name="ResultTable.bk_biz", is_parent=True), default=None
    )

    raw_data = as_attr(
        type=RawDataResource,
        metadata=AuthMetaAttr(dgraph_name="ResultTable.raw_data", is_indirect=True, is_parent=True),
        default=None,
    )

    def indirect_parent_resources(self, action_id):
        """
        获取额外的父级资源，目前先临时添加在这个位置，后续期望有更好的处理方式
        """
        o_rt = AuthResultTable(self.result_table_id)

        indirect_projects = [
            ProjectResource(project_id=project_id)
            for project_id in o_rt.retrieve_indirect_scopes("project_id", action_id=action_id)
        ]
        indirect_rawdatas = [
            RawDataResource(raw_data_id=raw_data_id)
            for raw_data_id in o_rt.retrieve_indirect_scopes("raw_data_id", action_id=action_id)
        ]

        return indirect_projects + indirect_rawdatas


@as_resource
@attr.s
class FlowResource(ResourceMixin):
    resource_type = "flow"
    storage_type = "mysql"
    storage_model = FlowInfo

    flow_id = as_attr(
        type=int, metadata=AuthMetaAttr(identifier=True, mysql_name="flow_id", dgraph_name="DataflowInfo.flow_id")
    )
    flow_name = as_attr(
        type=str,
        metadata=AuthMetaAttr(dgraph_name="DataflowInfo.flow_name", mysql_name="flow_name", is_display=True),
        default=None,
    )

    project = as_attr(
        type=ProjectResource,
        metadata=AuthMetaAttr(dgraph_name="DataflowInfo.project", mysql_name="project_id", is_parent=True),
        default=None,
    )


@as_resource
@attr.s
class ResourceGroupResource(ResourceMixin):
    resource_type = "resource_group"
    storage_type = "dgraph"

    resource_group_id = as_attr(
        type=int, metadata=AuthMetaAttr(identifier=True, dgraph_name="ResourceGroupInfo.resource_group_id")
    )

    group_name = as_attr(
        type=str, metadata=AuthMetaAttr(dgraph_name="ResourceGroupInfo.group_name", is_display=True), default=None
    )


@as_resource
@attr.s
class DataTokenResource(ResourceMixin):
    resource_type = "data_token"
    storage_type = "mysql"
    storage_model = DataTokenInfo

    data_token_id = as_attr(
        type=int, metadata=AuthMetaAttr(identifier=True, mysql_name="data_token_id", is_display=True)
    )


@as_resource
@attr.s
class FunctionResource(ResourceMixin):
    resource_type = "function"
    storage_type = "mysql"
    storage_model = FunctionInfo

    function_id = as_attr(type=str, metadata=AuthMetaAttr(identifier=True, mysql_name="function_id", is_display=True))


@as_resource
@attr.s
class DashboardResource(ResourceMixin):
    resource_type = "dashboard"
    storage_type = "mysql"
    storage_model = DashboardInfo

    dashboard_id = as_attr(type=int, metadata=AuthMetaAttr(identifier=True, mysql_name="dashboard_id"))
    dashboard_name = as_attr(
        type=str, metadata=AuthMetaAttr(is_display=True, mysql_name="dashboard_name"), default=None
    )
    project = as_attr(
        type=ProjectResource, metadata=AuthMetaAttr(mysql_name="project_id", is_parent=True), default=None
    )


@as_resource
@attr.s
class DataModelResource(ResourceMixin):
    resource_type = "datamodel"
    storage_type = "mysql"
    storage_model = DataModel

    model_id = as_attr(type=int, metadata=AuthMetaAttr(identifier=True, mysql_name="model_id"))

    model_name = as_attr(type=str, metadata=AuthMetaAttr(is_display=True, mysql_name="model_name"))

    project = as_attr(
        type=ProjectResource, metadata=AuthMetaAttr(mysql_name="project_id", is_parent=True), default=None
    )


@as_resource
@attr.s
class ModelResource(ResourceMixin):
    resource_type = "model"
    storage_type = "dgraph"

    model_id = as_attr(type=str, metadata=AuthMetaAttr(identifier=True, dgraph_name="ModelInfo.model_id"))

    model_name = as_attr(
        type=str, metadata=AuthMetaAttr(dgraph_name="ModelInfo.model_name", is_display=True), default=None
    )
    sensitivity = as_attr(
        type=str,
        metadata=AuthMetaAttr(dgraph_name="ModelInfo.sensitivity", scope_attribute=sensitivity_attribute),
        default=None,
    )
    project = as_attr(
        type=ProjectResource, metadata=AuthMetaAttr(dgraph_name="ModelInfo.project", is_parent=True), default=None
    )


@as_resource
@attr.s
class SampleSetResource(ResourceMixin):
    resource_type = "sample_set"
    storage_type = "dgraph"

    sample_set_id = as_attr(type=str, metadata=AuthMetaAttr(identifier=True, dgraph_name="SampleSet.id"))

    sample_set_name = as_attr(
        type=str, metadata=AuthMetaAttr(dgraph_name="SampleSet.sample_set_name", is_display=True), default=None
    )
    sensitivity = as_attr(
        type=str,
        metadata=AuthMetaAttr(dgraph_name="SampleSet.sensitivity", scope_attribute=sensitivity_attribute),
        default=None,
    )
    project = as_attr(
        type=ProjectResource, metadata=AuthMetaAttr(dgraph_name="SampleSet.project", is_parent=True), default=None
    )


@as_resource
@attr.s
class AlgorithmResource(ResourceMixin):
    resource_type = "algorithm"
    storage_type = "dgraph"

    algorithm_name = as_attr(type=str, metadata=AuthMetaAttr(identifier=True, dgraph_name="Algorithm.algorithm_name"))

    algorithm_alias = as_attr(
        type=str, metadata=AuthMetaAttr(dgraph_name="Algorithm.algorithm_alias", is_display=True), default=None
    )
    sensitivity = as_attr(
        type=str,
        metadata=AuthMetaAttr(dgraph_name="Algorithm.sensitivity", scope_attribute=sensitivity_attribute),
        default=None,
    )
    project = as_attr(
        type=ProjectResource, metadata=AuthMetaAttr(dgraph_name="Algorithm.project", is_parent=True), default=None
    )
