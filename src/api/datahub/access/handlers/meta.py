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

import datahub.access.settings as settings
from common.log import logger
from datahub.access.api import MetaApi
from datahub.access.exceptions import QueryGeogTagException, QueryTagTargetException


class MetaHandler(object):

    """
    元数据处理器
    """

    @classmethod
    def query_target_tags_by_id_list(cls, target_id_list, target_type):
        """
        :param target_type: 实体类型
        :param target_id_list: 实体id列表
        :return: 实体所有标签
        """
        criterion = [{"k": "id", "func": "eq", "v": target_id} for target_id in target_id_list]
        res = MetaApi.targets.list(
            {
                "limit": settings.TAG_TARGET_LIMIT,
                "target_type": target_type,
                "match_policy": "both",
                "target_filter": json.dumps([{"criterion": criterion, "condition": "OR"}]),
            },
            raise_exception=True,
        )

        try:
            targets = {content["id"]: content["tags"] for content in res.data["content"]}
        except Exception as e:
            raise QueryTagTargetException(message_kv={"message": json.dumps(res.data), "error": str(e)})

        return targets

    @classmethod
    def query_target_tags_by_id(cls, target_id, target_type):
        """
        :param target_type: 实体类型
        :param target_id: 实体id
        :return: 实体所有标签
        """

        res = MetaApi.targets.list(
            {
                "limit": settings.TAG_TARGET_LIMIT,
                "target_type": target_type,
                "target_filter": json.dumps([{"k": "id", "func": "eq", "v": target_id}]),
            },
            raise_exception=True,
        )
        """
        {
            "count":1,
            "content":[
                {
                    "id":123,
                    "_target_type":"AccessRawData",
                    "tags":{
                        "manage":{
                            "geog_area":[
                                {
                                    "code":"inland",
                                    "description":"中国内地（或简称内地）是指除香港、澳门、台湾以外的中华人民共和国主张管辖区，多用于与香港、澳门特别行政区同时出现的语境",
                                    "alias":"中国内地",
                                    "kpath":0,
                                    "sync":0,
                                    "seq_index":0,
                                    "id":123,
                                    "tag_type":"manage"
                                }
                            ]
                        },
                    },
                }
            ]
        }
        """

        try:
            tag_code_dict = res.data["content"][0]["tags"]
        except Exception as e:
            raise QueryTagTargetException(message_kv={"message": json.dumps(res.data), "error": str(e)})

        return tag_code_dict

    @classmethod
    def query_tag_relation_all_target(cls, tag_codes, target_type):
        """
        :param target_type: 实体类型
        :param tag_codes: tag编码列表
        :return: 实体id列表
        """
        criterion = [{"k": "code", "func": "eq", "v": tag} for tag in tag_codes]
        res = MetaApi.targets.list(
            {
                "limit": settings.TAG_TARGET_LIMIT,
                "target_type": target_type,
                "match_policy": "both",
                "tag_filter": json.dumps([{"criterion": criterion, "condition": "OR"}]),
            },
            raise_exception=True,
        )
        """
        {
            "count":1,
            "content":[
                {
                    "id":123,
                    "_target_type":"AccessRawData",
                    "tags":{
                        "manage":{
                            "geog_area":[
                                {
                                    "code":"inland",
                                    "description":"中国内地（或简称内地）是指除香港、澳门、台湾以外的中华人民共和国主张管辖区，多用于与香港、澳门特别行政区同时出现的语境",
                                    "alias":"中国内地",
                                    "kpath":0,
                                    "sync":0,
                                    "seq_index":0,
                                    "id":123,
                                    "tag_type":"manage"
                                }
                            ]
                        },
                    },
                }
            ]
        }
        """
        target_list = list()
        try:
            for content in res.data["content"]:
                target_list.append(str(content["id"]))
        except Exception as e:
            raise QueryTagTargetException(message_kv={"message": json.dumps(res.data), "error": str(e)})

        return target_list

    @classmethod
    def query_geog_tags(cls):
        """
        :return: 所有区域标签
        """
        geog_area_map = dict()

        geog_area_alias = dict()
        geog_area = list()
        res = MetaApi.tag.list()
        if not res.is_success():
            logger.error("query all geog tags happend error, error message: %s" % res.errors)
            raise QueryGeogTagException(message_kv={"message": res.message})

        if res.data.get("supported_areas"):
            for key, val in res.data["supported_areas"].items():
                geog_area_alias[key] = val["alias"]
                geog_area.append(key)
            geog_area_map["geog_area_alias"] = geog_area_alias

        geog_area_map = {
            "geog_area_alias": geog_area_alias,
            "geog_area": geog_area,
        }

        return geog_area_map

    @classmethod
    def query_target_tags_exist_by_id(cls, target_id, target_type, relation_code):
        """
        :param target_type: 实体类型
        :param target_id: 实体id
        :return: 实体所有标签
        """

        res = MetaApi.targets.list(
            {
                "limit": settings.TAG_TARGET_LIMIT,
                "target_type": target_type,
                "target_filter": json.dumps([{"k": "id", "func": "eq", "v": target_id}]),
            },
            raise_exception=True,
        )

        try:
            relations = res.data["content"][0]["relations"]
            for relation in relations:
                if relation["source_tag_code"] == relation_code:
                    return True
        except Exception as e:
            raise QueryTagTargetException(message_kv={"message": json.dumps(res.data), "error": str(e)})

        return False
