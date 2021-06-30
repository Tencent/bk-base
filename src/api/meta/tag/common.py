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


import time
from collections import defaultdict

from common.base_utils import model_to_dict
from common.meta.models import Tag, TagTarget
from django.db.models import Count, F, Q

from meta.tag.models import TagAttributeSchema


def fill_extra_info(results):
    fill_tag_target_counts(results)
    fill_parent_tag_code(results)
    fill_tag_attribute_schemas(results)


def fill_tag_target_counts(results):
    tag_code_list = [item["code"] for item in results]
    tag_target_counts = {}
    if tag_code_list:
        tag_target_counts = {
            item["tag_code"]: item["target_count"]
            for item in TagTarget.objects.filter(tag_code__in=tag_code_list)
            .values("tag_code")
            .annotate(target_count=Count("id"))
        }
    for item in results:
        code = item["code"]
        item["targets_count"] = tag_target_counts.get(code, 0)


def fill_parent_tag_code(results):
    parent_tag_ids = {item["parent_id"] for item in results}
    parent_tag_codes = {
        item["id"]: (item["code"], item["alias"])
        for item in Tag.objects.filter(id__in=parent_tag_ids).values("id", "code", "alias")
    }
    for item in results:
        item["parent_code"], item["parent_alias"] = (
            parent_tag_codes[item["parent_id"]] if item["parent_id"] in parent_tag_codes else (None, None)
        )


def fill_tag_attribute_schemas(results):
    tag_code_list = [item["code"] for item in results]
    tag_attributes = defaultdict(list)
    tag_attributes_info = list(TagAttributeSchema.objects.filter(tag_code__in=tag_code_list).values())
    for item in tag_attributes_info:
        tag_code = item.pop("tag_code")
        tag_attributes[tag_code].append(item)

    for item in results:
        code = item["code"]
        item["attribute_schemas"] = tag_attributes.get(code, [])


def delete_key(item, *args):
    dct = dict(item)
    for k in args:
        dct.pop(k)
    return dct


def search(params):
    page = params.get("page")
    page_size = params.get("page_size")
    actual = params.get("actual")

    # 先过滤tag
    tag_schema_filter = False
    tag_query = Tag.objects.all()
    for k in ["tag_keyword", "tag_code", "tag_type", "parent_id"]:
        v = params.get(k)
        if v:
            if k == "tag_keyword":
                tag_query = tag_query.filter(Q(code__icontains=v) | Q(alias__icontains=v) | Q(description__icontains=v))
            else:
                if k in ("tag_code",):
                    tag_query.filter(code__in=v)
                else:
                    tag_query = tag_query.filter(**{k: v})
            tag_schema_filter = True
    if tag_query.all().count() == 0:
        return {}, {}

    # 再过滤tag_target
    target_query = TagTarget.objects.all()

    for k in [
        "tag_code",
        "tag_type",
        "target_id",
        "target_type",
        "bk_biz_id",
        "project_id",
        "checked",
        "target_keyword",
    ]:
        v = params.get(k)
        if v:
            if k == "target_keyword":
                target_query = target_query.filter(Q(created_by__icontains=v) | Q(target_id__icontains=v))
            elif k == "target_id":
                target_query = target_query.filter(target_id__in=v)
            elif k == "tag_code":
                target_query = target_query.filter(tag_code__in=v)
            else:
                target_query = target_query.filter(**{k: v})

    if actual:
        target_query = target_query.filter(source_tag_code__iexact=F("tag_code"))

    # 若tag有限制，则再次过滤tag_target。
    if tag_schema_filter:
        target_query = target_query.filter(tag_code__in=tag_query.all().values_list("code", flat=True))
    target_query = target_query
    distinct_targets = (
        target_query.all()
        .values("target_type", "target_id")
        .annotate(count=Count("*"))[(page - 1) * page_size : page * page_size]
    )
    if not distinct_targets:
        return {}, {}
    filter_ = None
    for item in distinct_targets:
        if filter_ is None:
            filter_ = Q(target_type=item["target_type"], target_id=item["target_id"])
        else:
            filter_ = filter_ | Q(target_type=item["target_type"], target_id=item["target_id"])

    target_query = target_query.order_by("-id").filter(filter_)

    # 最终获取tag_target。
    tag_targets = list(target_query.all().values())
    tag_targets_dct = {
        "_".join(
            [
                item["tag_code"],
                item["target_type"],
                item["target_id"],
            ]
        ): item
        for item in tag_targets
    }

    # 最终获取tag。
    target_tag_codes = [item["tag_code"] for item in tag_targets]
    tags = list(tag_query.filter(code__in=target_tag_codes).all().values())
    tags_dct = {item["code"]: item for item in tags}
    fill_parent_tag_code(tags)

    return tags_dct, tag_targets_dct


def create_customize_tags(tag_params_list):
    """
    创建自定义标签

    :param tag_params_list:
    :return: dict 创建后的自定义标签实体字典
    """
    tag_dict = {tag_params["alias"]: tag_params for tag_params in tag_params_list}
    exist_tags = Tag.objects.filter(alias__in=list(tag_dict.keys())).filter(~Q(tag_type="manage"))
    exist_tag_dict = {item["alias"]: item for item in exist_tags.values("id", "code", "alias", "tag_type")}
    tag_configs = dict()
    for tag_alias, tag_params in list(tag_dict.items()):
        if tag_alias in exist_tag_dict:
            tag_config = exist_tag_dict[tag_alias]
        else:
            tag_params.pop("id", None)
            tag_params["code"] = "c_tag_{}_{}".format(format(time.time(), ".6f").replace(".", "_"), id(tag_alias))
            tag_params["parent_id"] = 0
            tag_params["tag_type"] = "customize"
            tag_config = Tag.objects.create(**tag_params)
            tag_config = model_to_dict(tag_config)
        tag_configs[tag_alias] = tag_config
    return tag_configs
