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


class CreateBulkProcessSerializer(serializers.Serializer):
    """
    多个modeling processing创建校验器

    参数如下：
    {
        "sql_list" : [sql1, sql2],
        "bk_biz_id": 591,
        "project_id": 13105,
        "type": "datalab",
        "bk_username": "xxxx", todo 删除
        "tags": ["inland"],
        "notebook_id": 499,
        "cell_id": 7
    }
    """

    sql_list = serializers.ListField(label="bulk mlsql")
    bk_biz_id = serializers.IntegerField(required=True, label="biz id")
    project_id = serializers.IntegerField(required=True, label="project id")
    type = serializers.CharField(label="type")
    tags = serializers.ListField(label="tags", allow_empty=False)
    notebook_id = serializers.IntegerField(label="notebook id in datahub")
    cell_id = serializers.CharField(label="cell id in datahub")
    component_type = serializers.CharField(label="component_type")


class ModelingProcessingsSerializer(serializers.Serializer):
    bk_username = serializers.CharField(required=True, label=_("user name"))
    dedicated_config = serializers.DictField(required=True, label=_("dedicated_config"))
    processor_logic = serializers.DictField(required=True, label=_("processor_logic"))
    window_info = serializers.ListField(required=True, label=_("window_info"))
    project_id = serializers.CharField(required=True, label=_("project_id"))
    tags = serializers.ListField(required=True, label=_("标签列表"), allow_empty=False)
    use_scenario = serializers.CharField(required=False, label=_("使用场景"), default="dataflow")
    name = serializers.CharField(required=True, label=_("node name"))
    bk_biz_id = serializers.IntegerField(required=True, label=_("biz biz id"))
    component_type = serializers.CharField(required=False, label=_("component type"), default="tensorflow")


class ParseBulkProcessSerializer(serializers.Serializer):
    """
    多个modeling processing创建校验器

    参数如下：
    {
        "sql_list" : [sql1, sql2]
    }
    """

    sql_list = serializers.ListField(label="bulk mlsql")


class CreateDataflowProcessSerializer(serializers.Serializer):
    model_config = serializers.JSONField(label="model_config")
    submit_args = serializers.JSONField(label="submit_args")
    bk_biz_id = serializers.IntegerField(required=True, label="biz id")
    project_id = serializers.IntegerField(required=True, label="project id")
    use_scenario = serializers.CharField(label="use_scenario", required=False)
    tags = serializers.ListField(label="tags", allow_empty=False)


class UpdateDataflowProcessSerializer(serializers.Serializer):
    model_config = serializers.JSONField(label="model_config")
    submit_args = serializers.JSONField(label="submit_args")
    bk_biz_id = serializers.IntegerField(required=True, label="biz id")
    project_id = serializers.IntegerField(required=True, label="project id")
    use_scenario = serializers.CharField(label="use_scenario", required=False)
    tags = serializers.ListField(label="tags", allow_empty=False)
    processing_id = serializers.CharField(label="processing_id", required=False)


class DeleteDataflowProcessSerializer(serializers.Serializer):
    processing_id = serializers.CharField(label="processing_id", required=False)
    with_data = serializers.BooleanField(label="with_data", required=True)


class DeleteProcessSerializer(serializers.Serializer):
    with_data = serializers.BooleanField(label="with_data", required=False, default=True)


class CheckModelUpdateSerializer(serializers.Serializer):
    processing_id = serializers.CharField(label="processing_id", required=True)
    model_id = serializers.CharField(label="model_id", required=True)


class UpdateModelInfoSerializer(serializers.Serializer):
    processing_id = serializers.CharField(label="processing_id", required=True)
    model_id = serializers.CharField(label="model_id", required=True)
