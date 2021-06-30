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

from django.utils.translation import ugettext as _
from rest_framework import serializers


class MigrationCreateFlowSerializer(serializers.Serializer):
    class NodeSerializer(serializers.Serializer):
        id = serializers.IntegerField(label=_("ID"))
        flow_id = serializers.IntegerField(label=_("flow ID"), allow_null=True)
        node_name = serializers.CharField(label=_("node 中文名"))
        node_config = serializers.DictField(label=_("node配置"))
        node_type = serializers.CharField(label=_("node类型"))
        frontend_info = serializers.DictField(label=_("坐标"))
        status = serializers.CharField(label=_("状态"))
        latest_version = serializers.CharField(label=_("最近版本"), allow_null=True, allow_blank=True)
        running_version = serializers.CharField(label=_("运行状态"), allow_null=True, allow_blank=True)
        created_by = serializers.CharField(label=_("创建人"), allow_null=True, allow_blank=True)
        created_at = serializers.CharField(label=_("创建时间"), allow_null=True, allow_blank=True)
        updated_by = serializers.CharField(label=_("更新人"), allow_null=True, allow_blank=True)
        updated_at = serializers.CharField(label=_("更新时间"), allow_null=True, allow_blank=True)
        description = serializers.CharField(label=_("描述"), allow_null=True, allow_blank=True)
        # 计算节点会有job_id和job_type
        job_id = serializers.CharField(label=_("任务ID"), required=False)
        job_type = serializers.CharField(label=_("任务类型"), required=False)

    class LinkSerializer(serializers.Serializer):
        flow_id = serializers.IntegerField(label=_("flow ID"))
        from_node_id = serializers.IntegerField(label=_("头ID"))
        to_node_id = serializers.IntegerField(label=_("尾ID"))
        frontend_info = serializers.DictField(label=_("坐标信息"))

    flow_id = serializers.IntegerField(label=_("flow ID"), allow_null=True)
    flow_name = serializers.CharField(label=_("flow 中文名"))
    project_id = serializers.IntegerField(label=_("项目 ID"))
    status = serializers.CharField(label=_("flow 状态"))
    is_locked = serializers.IntegerField(label=_("是否被锁定"), allow_null=True)
    latest_version = serializers.CharField(label=_("最近版本"), allow_null=True, allow_blank=True)
    bk_app_code = serializers.CharField(label=_("bk code"))
    active = serializers.IntegerField(label=_("是否有效"))
    created_by = serializers.CharField(label=_("创建人"), allow_null=True, allow_blank=True)
    created_at = serializers.CharField(label=_("创建时间"), allow_null=True, allow_blank=True)
    locked_by = serializers.CharField(label=_("锁定人"), allow_null=True, allow_blank=True)
    locked_at = serializers.CharField(label=_("锁定时间"), allow_null=True, allow_blank=True)
    updated_by = serializers.CharField(label=_("更新人"), allow_null=True, allow_blank=True)
    updated_at = serializers.CharField(label=_("更新时间"), allow_null=True, allow_blank=True)
    description = serializers.CharField(label=_("描述"), allow_null=True, allow_blank=True)
    nodes = NodeSerializer(label=_("点的集合"), required=True, many=True)
    links = LinkSerializer(label=_("线的集合"), required=True, many=True)


class MigrationDataflowExecuteLogSerializer(serializers.Serializer):
    class LogSerializer(serializers.Serializer):
        id = serializers.IntegerField(label=_("ID"))
        action = serializers.CharField(label=_("动作"))
        status = serializers.CharField(label=_("状态"))
        start_time = serializers.CharField(label=_("开始时间"), allow_null=True, allow_blank=True)
        created_at = serializers.CharField(label=_("创建时间"), allow_null=True, allow_blank=True)
        end_time = serializers.CharField(label=_("结束时间"), allow_null=True, allow_blank=True)
        created_by = serializers.CharField(label=_("创建人"), allow_null=True, allow_blank=True)
        description = serializers.CharField(label=_("描述"))
        version = serializers.CharField(label=_("版本"), allow_null=True, allow_blank=True)
        context = serializers.CharField(label=_("内容"), allow_null=True, allow_blank=True)
        logs_zh = serializers.CharField(label=_("英文日志"), allow_null=True, allow_blank=True)
        logs_en = serializers.CharField(label=_("中文日志"), allow_null=True, allow_blank=True)

    flow_id = serializers.IntegerField(label=_("flow ID"))
    logs = LogSerializer(label=_("日志列表信息"), required=True, many=True)
