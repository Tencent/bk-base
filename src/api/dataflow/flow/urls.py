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

from django.conf.urls import include, url
from rest_framework import routers

from dataflow.flow.views import (
    bk_sql_views,
    common_views,
    data_makeup_views,
    flow_views,
    job_views,
    modeling_views,
    node_views,
    task_views,
    tasklog_views,
    udf_views,
    util_views,
    version_views,
)

router = routers.DefaultRouter(trailing_slash=True)
router.register(r"flows", flow_views.FlowViewSet, basename="flow_flows")
router.register(
    r"flows/(?P<flow_id>\d+)/nodes",
    node_views.NodeViewSet,
    basename="flow_flows_nodes",
)
router.register(
    r"flows/(?P<flow_id>\d+)/nodes/(?P<node_id>\d+)/data_makeup",
    data_makeup_views.DataMakeupViewSet,
    basename="flow_flows_nodes_data_makeup",
)
router.register(
    r"flows/(?P<flow_id>\d+)/versions",
    version_views.VersionViewSet,
    basename="flow_flows_versions",
)
router.register(
    r"flows/(?P<flow_id>\d+)/debuggers",
    task_views.DebuggerViewSet,
    basename="flow_flows_debuggers",
)
router.register(
    r"flows/(?P<flow_id>\d+)/custom_calculates",
    task_views.CustomCalcalculateViewSet,
    basename="flow_flows_custom_calculate",
)
router.register(
    r"flows/(?P<flow_id>\d+)/tasks",
    task_views.TaskViewSet,
    basename="flow_flows_tasks",
)
router.register(r"nodes", node_views.ListNodesViewSet, basename="flow_nodes")
router.register(r"bksql", bk_sql_views.BksqlViewSet, basename="flow_bksql")
router.register(r"udfs", udf_views.FunctionViewSet, basename="flow_udfs")
router.register(
    r"udfs/(?P<function_name>\w+)/debuggers",
    udf_views.FunctionDebuggerViewSet,
    basename="flow_udfs_debuggers",
)
router.register(r"jobs", job_views.JobViewSet, basename="flow_jobs")

# 算法模型
# router.register(r'models', flow_views.NodeViewSet, basename='flow_nodes')

# mlsql模型
router.register(r"modeling", modeling_views.ModelViewSet, basename="mlsql_models")

# 迁移任务 & 测试迁移任务
router.register(r"flow_migration", util_views.FlowMigrationTask, basename="flow_migration")

# 扩充partition相关
router.register(r"manage/add_partition", util_views.ManageAddPartition, basename="add_partition")

router.register(r"param_verify", util_views.ParamVerifyUtil, basename="param_verify")

router.register(r"log", tasklog_views.TaskLogViewSet, basename="flow_tasklog")

router.register(
    r"log/blacklist_configs",
    tasklog_views.TaskLogBlacklistViewSet,
    basename="flow_tasklog_blacklist",
)

urlpatterns = [
    url(r"^$", flow_views.index),
    url(r"^", include(router.urls)),
    # 获取DataFlow某个历史版本的图信息
    url(
        r"^flows/(?P<flow_id>\d+)/versions/(?P<version_id>\d+)/graph/",
        flow_views.FlowViewSet.as_view({"get": "get_version_graph"}),
        name="flow_versions_graph",
    ),
    # 系统维护、监控等相关接口
    url(
        r"^list_function_doc/$",
        common_views.CommonViewSet.as_view({"get": "list_function_doc"}),
        name="flow_list_function_doc",
    ),
    # url(r'^node_link_rules/$', common_views.CommonViewSet.as_view({'get': 'node_link_rules'}),
    #     name='flow_node_link_rules'),
    # 获取连线规则 优化
    url(
        r"^node_link_rules_v2/$",
        common_views.CommonViewSet.as_view({"get": "node_link_rules_v2"}),
        name="flow_node_link_rules_v2",
    ),
    # url(r'^node_type_config/$', common_views.CommonViewSet.as_view({'get': 'node_type_config'}),
    #     name='flow_node_type_config'),
    # 原获取画布左侧控件信息 优化
    url(
        r"^node_type_config_v2/$",
        common_views.CommonViewSet.as_view({"get": "node_type_config_v2"}),
        name="flow_node_type_config_v2",
    ),
    url(
        r"^field_type_configs/$",
        common_views.CommonViewSet.as_view({"get": "field_type_configs"}),
        name="flow_field_type_configs",
    ),
    url(
        r"^get_latest_msg/$",
        common_views.CommonViewSet.as_view({"get": "get_latest_msg"}),
        name="flow_get_latest_msg",
    ),
    url(
        r"^code_frame/$",
        common_views.CommonViewSet.as_view({"get": "code_frame"}),
        name="flow_code_frame",
    ),
    url(
        r"^healthz/$",
        flow_views.HealthCheckView.as_view({"get": "healthz"}),
        name="flow_healthz",
    ),
    url(
        r"^hdfs_uploaded_file_clean/$",
        task_views.DeployView.as_view({"post": "hdfs_uploaded_file_clean"}),
        name="flow_hdfs_uploaded_file_clean",
    ),
    url(
        r"^check_custom_calculate/$",
        task_views.DeployView.as_view({"get": "check_custom_calculate"}),
        name="flow_check_custom_calculate",
    ),
    url(r"^exception$", flow_views.TestViewSet.as_view({"get": "test_exception"})),
    url(
        r"^deploy/$",
        task_views.DeployView.as_view({"get": "deploy"}),
        name="flow_deploy",
    ),
    # error code
    url(r"^errorcodes/$", common_views.CommonViewSet.as_view({"get": "get_error_codes"})),
]
