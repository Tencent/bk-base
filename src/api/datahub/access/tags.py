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

from common.exceptions import ValidationError
from common.meta.common import create_tag_to_target, delete_tag_to_target
from common.transaction import auto_meta_sync
from datahub.access.exceptions import GeogAreaError
from datahub.access.handlers.meta import MetaHandler
from datahub.access.settings import (
    DEFAULT_GEOG_AREA_TAG,
    MULTI_GEOG_AREA,
    TARGET_TYPE_ACCESS_RAW_DATA,
    USE_V2_UNIFYTLOGC_TAG,
)
from django.utils.translation import ugettext as _


def create_tags(target_type, target_id, tags):
    # 创建重复tag时会抛异常, 并没有提供相关异常捕获, 需要从msg判断
    try:
        with auto_meta_sync(using="default"):
            create_tag_to_target([(target_type, target_id)], tags)
    except Exception as e:
        if "Duplicate entry" not in str(e):
            raise e


def delete_tags(target_type, target_id, tags):
    # 当不存在时不会抛异常
    with auto_meta_sync(using="default"):
        delete_tag_to_target([(target_type, target_id)], tags)


def get_tags(param):
    """
    提取校验tags, 兼容旧字段data_region
    :param param: 参数
    :return: 地域标签, 全部tags列表(包含地域,数据来源等)
    """
    # 允许多地域时data_region为必填项, 非多地域,按默认值处理
    if MULTI_GEOG_AREA:
        if not param.get("data_region"):
            raise ValidationError(message=_(u"参数校验不通过:允许多区域则数据区域为必填项"), errors={"tags": u"数据区域为必填项"})
    else:
        param["data_region"] = DEFAULT_GEOG_AREA_TAG

    data_region = param["data_region"]
    # 校验数据区域是否合法
    geog_tags = MetaHandler.query_geog_tags()
    data_region_list = [geog_area for geog_area in geog_tags["geog_area"] if geog_area == data_region]
    if not data_region_list:
        raise GeogAreaError(message_kv={"geog_areas": param["tags"]})

    # 兼容tags不存在
    if "tags" in param:
        # unicode to str
        param["tags"] = [str(x) for x in param["tags"]]
    else:
        param["tags"] = list()

    # 兼容数据来源参数, data_source_tags不存在的话,数据来源取data_source
    if "data_source_tags" in param:
        data_source_tags = param["data_source_tags"]
    else:
        data_source_tags = [param["data_source"]]
    param["data_source"] = data_source_tags[0]

    # 所有标签都保存到tags中
    param["tags"].extend(data_source_tags)
    param["tags"].append(data_region)
    param["tags"] = list(set(param["tags"]))  # 去重

    return data_region, param["tags"]


def pop_data_source_tags(tag_list):
    """
    取出数据来源的tags, 数据来源为system下的标签
    :param tag_list: tag list
    :return: (data_source_tag_list, left_tag_list)
    """
    data_source_tag_list = tag_list.pop("system", [])
    tag_list["system"] = list()
    return data_source_tag_list, tag_list


def check_use_v2_unifytlogc(raw_data_id):
    """
    根据data_id 判断是否有use_v2_unifytlogc这个tag
    """
    return MetaHandler.query_target_tags_exist_by_id(raw_data_id, TARGET_TYPE_ACCESS_RAW_DATA, USE_V2_UNIFYTLOGC_TAG)
