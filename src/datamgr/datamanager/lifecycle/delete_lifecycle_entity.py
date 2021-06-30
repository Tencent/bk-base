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
from __future__ import absolute_import, print_function, unicode_literals

import logging
from math import ceil

from api import meta_api
from common.meta import metadata_client
from conf.settings import LIFE_CYCLE_TOKEN_PKEY
from models.meta import Heat, LifeCycle, Range

logger = logging.getLogger(__name__)
COUNT = 100


def get_rt_rd_list():
    """
    查询rt和rd列表
    :return:
    """
    # 查询所有rt
    statement = """{\n  rt_list(func:has(ResultTable.result_table_id)) @filter(lt(ResultTable.bk_biz_id,200000)
    and eq(ResultTable.generate_type,"user")) {\n    ResultTable.result_table_id\n  }\n}"""
    try:
        rt_search_dict = meta_api.complex_search(
            {
                "backend_type": "dgraph",
                "statement": statement,
                "token_pkey": LIFE_CYCLE_TOKEN_PKEY,
            }
        ).data
    except Exception as e:
        logger.error("get_rt_list by complex search error:{}".format(e.message))
        rt_search_dict = {}

    rt_list_tmp = rt_search_dict.get("data").get("rt_list") if rt_search_dict else []
    rt_list = [each["ResultTable.result_table_id"] for each in rt_list_tmp]
    logger.info("rt_list length:[{}], list:[{}]".format(len(rt_list), rt_list))

    # 查询所有的raw_data
    statement = """{\n  rd_list(func:has(AccessRawData.id)) @filter(lt(AccessRawData.bk_biz_id,200000))
            {\n    AccessRawData.id\n  }\n}"""
    try:
        rd_search_dict = meta_api.complex_search(
            {
                "backend_type": "dgraph",
                "statement": statement,
                "token_pkey": LIFE_CYCLE_TOKEN_PKEY,
            }
        ).data
    except Exception as e:
        logger.error("get_rd_list by complex search error:{}".format(e.message))
        rd_search_dict = {}

    rd_list_tmp = rd_search_dict.get("data").get("rd_list") if rd_search_dict else []
    rd_list = [str(each["AccessRawData.id"]) for each in rd_list_tmp]
    logger.info("rd_list length:[{}], list:[{}]".format(len(rd_list), rd_list))
    return rt_list, rd_list


def get_entity_list(dataset_list, key, rt_list, rd_list):
    # 要删除的实体列表
    delete_entity_list = []
    rt_str_len = len("_result_table")
    rd_str_len = len("_raw_data")
    for each_entity in dataset_list:
        if (
            each_entity.get(key).endswith("_result_table")
            and each_entity.get(key)[:-rt_str_len] not in rt_list
        ):
            delete_entity_list.append(each_entity.get(key))
        elif (
            each_entity.get(key).endswith("_raw_data")
            and each_entity.get(key)[:-rd_str_len] not in rd_list
        ):
            delete_entity_list.append(each_entity.get(key))
    logger.info(
        "delete_entity_list length:[{}], list:[{}]".format(
            len(delete_entity_list), delete_entity_list
        )
    )
    return delete_entity_list


def get_dataset_list(key):
    statement = "{{\n  dataset_list(func:has({})){{\n    {}\n  }}\n}}".format(key, key)
    try:
        dataset_search_dict = meta_api.complex_search(
            {
                "backend_type": "dgraph",
                "statement": statement,
                "token_pkey": LIFE_CYCLE_TOKEN_PKEY,
            }
        ).data
    except Exception as e:
        logger.error("get_dataset_list error:{}".format(e.message))
        return []

    dataset_list = dataset_search_dict.get("data").get("dataset_list")
    return dataset_list


def delete_extra_heat_entity():
    """
    删除数据集不存在的热度实体
    :return:
    """
    # 查询所有有热度的数据集列表
    dataset_list_has_heat = get_dataset_list("Heat.id")

    rt_list, rd_list = get_rt_rd_list()

    # 要删除的热度实体列表
    delete_heat_list = get_entity_list(
        dataset_list_has_heat, "Heat.id", rt_list, rd_list
    )
    delete_count = len(delete_heat_list)

    for i in range(int(ceil(delete_count / float(COUNT)))):
        delete_heat_list_tmp = delete_heat_list[i * COUNT : i * COUNT + COUNT]
        with metadata_client.session as se:
            try:
                for each_heat in delete_heat_list_tmp:
                    uniq_id = each_heat
                    se.delete(Heat(id=uniq_id))
                se.commit()
            except Exception as e:
                logger.error(
                    "delete heat entity error:{}, delete_heat_list_tmp:{}, i:{}".format(
                        e, delete_heat_list_tmp, i
                    )
                )
    logger.info("delete heat entity finish")


def delete_extra_range_entity():
    """
    删除数据集不存在的广度实体
    :return:
    """
    # 查询所有有广度的数据集列表
    dataset_list_has_range = get_dataset_list("Range.id")

    rt_list, rd_list = get_rt_rd_list()

    # 要删除的广度实体列表
    delete_range_list = get_entity_list(
        dataset_list_has_range, "Range.id", rt_list, rd_list
    )
    delete_count = len(delete_range_list)

    for i in range(int(ceil(delete_count / float(COUNT)))):
        delete_range_list_tmp = delete_range_list[i * COUNT : i * COUNT + COUNT]
        with metadata_client.session as se:
            try:
                for each_range in delete_range_list_tmp:
                    uniq_id = each_range
                    se.delete(Range(id=uniq_id))
                se.commit()
            except Exception as e:
                logger.error(
                    "delete range entity error:{}, delete_range_list_tmp:{}, i:{}".format(
                        e, delete_range_list_tmp, i
                    )
                )
    logger.info("delete range entity finish")


def delete_extra_lifecycle_entity():
    """
    删除数据集不存在的生命周期实体
    :return:
    """
    # 查询所有有生命周期的数据集列表
    dataset_list_has_lifecycle = get_dataset_list("LifeCycle.id")

    rt_list, rd_list = get_rt_rd_list()

    # 要删除的生命周期实体列表
    delete_lifecycle_list = get_entity_list(
        dataset_list_has_lifecycle, "LifeCycle.id", rt_list, rd_list
    )
    delete_count = len(delete_lifecycle_list)

    for i in range(int(ceil(delete_count / float(COUNT)))):
        delete_lifecycle_list_tmp = delete_lifecycle_list[i * COUNT : i * COUNT + COUNT]
        with metadata_client.session as se:
            try:
                for each_lifecycle in delete_lifecycle_list_tmp:
                    uniq_id = each_lifecycle
                    se.delete(LifeCycle(id=uniq_id))
                se.commit()
            except Exception as e:
                logger.error(
                    "delete lifecycle entity error:{}, delete_lifecycle_list_tmp:{}, i:{}".format(
                        e, delete_lifecycle_list_tmp, i
                    )
                )
    logger.info("delete lifecycle entity finish")
