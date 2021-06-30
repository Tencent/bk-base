# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from django.utils.translation import ugettext_lazy as _

from apps.api.modules.utils import add_esb_info_before_request
from config.domains import META_APIGATEWAY_ROOT

from ..base import DataAPI, DataDRFAPISet, DRFActionAPI, PassThroughAPI


class _MetaApi(object):
    MODULE = _("数据平台元数据模块")
    URL_PREFIX = META_APIGATEWAY_ROOT

    def __init__(self):
        self.pass_through = PassThroughAPI
        self.projects = DataDRFAPISet(
            url=META_APIGATEWAY_ROOT + "projects/",
            module=self.MODULE,
            primary_key="id",
            description="项目操作",
            default_return_value=None,
            before_request=add_esb_info_before_request,
            custom_config={"mine": DRFActionAPI(method="GET", detail=False)},
        )
        self.result_tables = DataDRFAPISet(
            url=META_APIGATEWAY_ROOT + "result_tables/",
            module=self.MODULE,
            primary_key="result_table_id",
            description="结果表操作",
            default_return_value=None,
            before_request=add_esb_info_before_request,
            custom_config={"storages": DRFActionAPI(method="GET"), "mine": DRFActionAPI(method="GET", detail=False)},
        )

        # 获取全量业务列表
        self.biz_list = DataDRFAPISet(
            url=META_APIGATEWAY_ROOT + "bizs/",
            primary_key="bk_biz_id",
            module="meta",
            before_request=add_esb_info_before_request,
            description="全量业务列表",
            custom_config={},
        )
        # 获取全量项目列表
        self.project_list = DataDRFAPISet(
            url=META_APIGATEWAY_ROOT + "projects/",
            primary_key="project_id",
            module="meta",
            before_request=add_esb_info_before_request,
            description="全量项目列表",
            custom_config={},
        )
        # 获取业务对应的基本信息
        self.biz_info = DataDRFAPISet(
            url=META_APIGATEWAY_ROOT + "bizs/{bk_biz_id}",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            url_keys=["bk_biz_id"],
            primary_key=None,
            description="获取业务对应的基本信息",
            custom_config={},
        )
        # 获取项目对应的基本信息
        self.project_info = DataDRFAPISet(
            url=META_APIGATEWAY_ROOT + "projects/{project_id}/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            url_keys=["project_id"],
            primary_key=None,
            description="获取项目对应的基本信息",
            custom_config={},
        )
        # 查询tag_code对应的tag_alias
        self.tag_info = DataAPI(
            method="GET",
            url=META_APIGATEWAY_ROOT + "tag/tags/{code}/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            url_keys=["code"],
            description="get tag info",
        )
        # 查询rt表对应的标签
        self.target_tag_info = DataAPI(
            method="GET",
            url=META_APIGATEWAY_ROOT + "tag/targets/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="get target tag info",
        )
        # 基于图存储的通用标签查询
        self.target_tag_info_experimental = DataAPI(
            method="GET",
            url=META_APIGATEWAY_ROOT + "tag/targets_experimental/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="get target tag info",
        )
        self.lineage = DataAPI(
            method="GET",
            url=META_APIGATEWAY_ROOT + "lineage/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="get_lineage",
        )
        # 原生后端查询语言搜索
        self.complex_search = DataAPI(
            method="POST",
            url=META_APIGATEWAY_ROOT + "basic/entity/complex_search/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="原生后端查询语言搜索",
        )
        self.query_via_erp = DataAPI(
            method="POST",
            url=META_APIGATEWAY_ROOT + "basic/asset/query_via_erp/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="erp查询元数据",
        )
        self.processing_type_configs = DataAPI(
            method="GET",
            url=META_APIGATEWAY_ROOT + "processing_type_configs/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="数据处理类型",
        )
        # 获取单个数据处理实例的信息
        self.dp_info = DataAPI(
            method="GET",
            url=META_APIGATEWAY_ROOT + "data_processings/{processing_id}/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            url_keys=["processing_id"],
            description="获取单个数据处理实例的信息",
        )
        # 获取TDW表实例的信息
        self.tdw_tables = DataAPI(
            method="GET",
            url=META_APIGATEWAY_ROOT + "tdw/tables/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            description="获取TDW表实例的信息",
        )
        # 编辑rt表字段
        self.rt_field_edit = DataAPI(
            method="PUT",
            url=META_APIGATEWAY_ROOT + "result_tables/{result_table_id}/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            url_keys=["result_table_id"],
            description="编辑rt表字段",
        )
