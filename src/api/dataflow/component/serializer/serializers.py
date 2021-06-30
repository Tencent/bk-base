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


class HDFSUtilSerializer(serializers.Serializer):
    path = serializers.CharField(label=_("路径信息"))


class HDFSUploadSerializer(serializers.Serializer):
    path = serializers.CharField(label=_("路径信息"))
    file = serializers.FileField(label=_("文件"))


class HDFSMoveSerializer(serializers.Serializer):
    is_overwrite = serializers.BooleanField(label=_("是否覆盖"))
    from_path = serializers.CharField(label=_("源路径信息"))
    to_path = serializers.CharField(label=_("目标路径信息"))


class HDFSCleanSerializer(serializers.Serializer):
    paths = serializers.ListField(label=_("路径信息列表"), child=serializers.CharField(label=_("path")))
    is_recursive = serializers.BooleanField(label=_("is recursive"), required=False)
    user_name = serializers.CharField(label=_("username"), required=False)


class QueueSerializer(serializers.Serializer):
    name = serializers.CharField(label=_("队列名"))


class RetrieveJobSerializer(serializers.Serializer):
    run_mode = serializers.CharField(label=_("运行模式"))
    job_type = serializers.CharField(label=_("任务类型"))


class RegisterJobSerializer(serializers.Serializer):
    run_mode = serializers.CharField(label=_("运行模式"))
    job_type = serializers.CharField(label=_("任务类型"))


class ClusterConfigSerializer(serializers.Serializer):
    cluster_domain = serializers.CharField(required=False, label=_("对应组件主节点域名"))
    cluster_group = serializers.CharField(required=False, label=_("集群组"))
    cluster_name = serializers.RegexField(
        required=True,
        regex=r"^[a-zA-Z]+(_|[a-zA-Z0-9]|\.)*$",
        label=_("集群名称"),
        error_messages={"invalid": _("由英文字母、数字、下划线、小数点组成，且需以字母开头")},
    )
    cluster_label = serializers.CharField(required=False, label=_("集群标签"))
    priority = serializers.IntegerField(required=False, label=_("集群优先级"))
    version = serializers.CharField(required=False, label=_("集群版本"))
    belong = serializers.CharField(required=False, label=_("组件归属者"))
    component_type = serializers.CharField(required=True, label=_("组件类型"))
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))
    description = serializers.CharField(required=False, label=_("集群描述信息"))


class ClusterConfigOperationSerializer(serializers.Serializer):
    component_type = serializers.CharField(required=True, label=_("组件类型"))
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))
