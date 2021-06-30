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
import json
from copy import deepcopy
from threading import RLock

from django.conf import settings
from django.forms import model_to_dict

from ..api.modules.meta import MetaApi
from ..exceptions import (
    TagNotExistError,
    TargetNotExistError,
    TargetTypeNotAllowedError,
)
from ..local import get_request_username
from .models import Tag, TagMapping, TagTarget

allowed_target_info = {}
lock = RLock()


def find_related_tag(target_type, target_id):
    return TagTarget.objects.filter(target_type=target_type, target_id=target_id)


def init_allowed_target_info():
    global allowed_target_info
    if not allowed_target_info:
        allowed_target_info = {
            v["target_type"]: v["table_primary_key"] for k, v in getattr(settings, "TAG_RELATED_MODELS", {}).items()
        }
        for k, v in deepcopy(allowed_target_info).items():
            if isinstance(k, (tuple, list)):
                allowed_target_info.pop(k)
                for per_target_type in k:
                    allowed_target_info[per_target_type] = v


def target_validate(target_type, target_id, biz_project_id_dict):
    if not allowed_target_info:
        with lock:
            init_allowed_target_info()
    info = allowed_target_info.get(target_type, None)
    if not info:
        raise TargetTypeNotAllowedError(message_kv={"type": target_type})

    table_name, primary_key = info.split(".")
    sql = "select * from {} where {}={}".format(table_name, primary_key, json.dumps(target_id))
    query_result = MetaApi.entity_complex_search({"statement": sql, "backend_type": "mysql"}, raw=True)
    result = query_result["result"]
    message = query_result["message"]
    if not result:
        raise Exception(message)
    data = query_result["data"]
    if data:
        data_dict = data[0]
        if target_type == "result_table":
            biz_project_id_dict[target_id] = {
                "bk_biz_id": data_dict.get("bk_biz_id"),
                "project_id": data_dict["project_id"],
            }
        elif target_type == "raw_data":
            biz_project_id_dict[target_id] = {"bk_biz_id": data_dict.get("bk_biz_id")}
    return len(data) > 0


def get_inherit_tag(tag_code_list):  # 得到标签的继承关系信息
    tag_result = {}
    for tag_code in tag_code_list:
        tag_list = Tag.objects.filter(active=1, code=tag_code).values("id", "code", "parent_id", "tag_type")
        if not tag_list:
            raise TagNotExistError(message_kv={"code": tag_code})

        while tag_list:
            tag_dict = tag_list[0]
            code = tag_dict["code"]
            tag_type = tag_dict["tag_type"]
            parent_id = tag_dict["parent_id"]
            if tag_code in tag_result:
                tag_result[tag_code].append({"tag_code": code, "tag_type": tag_type})
            else:
                tag_result[tag_code] = [{"tag_code": code, "tag_type": tag_type}]
            if parent_id == 0:
                break
            else:
                tag_list = Tag.objects.filter(active=1, id=parent_id).values("id", "code", "parent_id", "tag_type")
    return tag_result


def map_tags(tag_codes):
    tag_code_set = set(tag_codes)
    mapped_codes = TagMapping.objects.filter(code__in=tag_code_set)
    if mapped_codes:
        for item in mapped_codes:
            tag_code_set.remove(item.code)
            tag_code_set.add(item.mapped_code)
    return list(tag_code_set)


def create_tag_to_target(
    targets, tags, target_exists=True, bk_biz_id=None, project_id=None
):  # 例子:[('result_table', 'battle_info')], ['NA']
    if targets and tags:
        tags = map_tags(tags)
        tag_inherit_result = get_inherit_tag(tags)
        username = get_request_username()
        biz_project_id_dict = {}
        for target in targets:  # 校验参数合法性
            target_type = target[0]
            target_id = target[1]
            if target_exists:  # 说明是给已存在的实体打标签的,若target_type=result_table或者raw_data,则还要拿到实体的bk_biz_id或project_id作为冗余保存
                ret = target_validate(target_type, target_id, biz_project_id_dict)
                if not ret:
                    raise TargetNotExistError(message_kv={"id": target_id, "type": target_type})

        for target in targets:
            target_type = target[0]
            target_id = target[1]
            p_bk_biz_id, p_project_id = bk_biz_id, project_id
            if target_exists:  # 说明是给已存在的实体打标签
                if target_type == "result_table" or target_type == "raw_data":
                    data_dict = biz_project_id_dict[target_id]
                    p_bk_biz_id = data_dict.get("bk_biz_id", None)
                    p_project_id = data_dict.get("project_id", None)

            for tag in tags:
                inherit_list = tag_inherit_result[tag]
                for tag_dict in inherit_list:
                    tag_code = tag_dict["tag_code"]
                    tag_type = tag_dict["tag_type"]
                    item = {
                        "target_id": target_id,
                        "target_type": target_type,
                        "updated_by": username,
                        "tag_code": tag_code,
                        "source_tag_code": tag,
                        "tag_type": tag_type,
                        "created_by": username,
                        "bk_biz_id": p_bk_biz_id,
                        "project_id": p_project_id,
                    }
                    TagTarget.objects.create(**item)


def delete_tag_to_target(targets, tags):
    if targets and tags:
        tags = map_tags(tags)
        for target in targets:
            target_type = target[0]
            target_id = target[1]
            for tag in tags:
                TagTarget.objects.filter(target_id=target_id, target_type=target_type, source_tag_code=tag).delete()


def gen_geog_tags_info(tag_code="geog_area", depth=2):
    codes_map = {}
    codes_info = {}

    start_tag = Tag.objects.filter(code=tag_code).get()
    codes_map[start_tag.code] = {}

    tags_info = [(start_tag, codes_map[start_tag.code])]
    next_tags_info = []
    for i in range(10):
        if not tags_info:
            break
        for tag, storage in tags_info:
            tag_info = model_to_dict(tag)
            if i >= depth:
                codes_info[tag.code] = tag_info
            next_tags = Tag.objects.filter(parent_id=tag.id).all()
            for next_tag in next_tags:
                storage[next_tag.code] = {}
                next_tags_info.append((next_tag, storage[next_tag.code]))
        tags_info = next_tags_info
        next_tags_info = []
    return codes_map, codes_info


def get_default_geog_tag():
    codes_map, codes_info = gen_geog_tags_info()
    if codes_info and isinstance(codes_info, dict):
        return list(codes_info.values())[0]
    return {}
