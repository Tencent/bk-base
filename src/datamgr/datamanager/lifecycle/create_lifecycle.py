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

import copy
import datetime
import logging
from math import ceil

from api import datamanage_api
from common.meta import metadata_client
from conf.settings import DATAMAP_DICT_TOKEN_PKEY
from lifecycle.dataset import DataSet
from lifecycle.metrics.format_metric import format_dict
from lifecycle.metrics.heat import get_day_query_info, get_query_info
from lifecycle.metrics.report_metrics import (
    send_heat_metric_to_kafka,
    send_metric_to_kafka,
    send_rt_day_query_count_to_kafka,
)
from lifecycle.metrics.sort_metric import score_ranking
from models.meta import (
    AssetValue,
    Cost,
    Heat,
    Importance,
    LifeCycle,
    Range,
    StorageCapacity,
)

logger = logging.getLogger(__name__)

COUNT = 50
NORM_IMPORTANCE_SCORE = 18
DATASET_PARAMS = {
    "bk_biz_id": None,
    "project_id": None,
    "tag_ids": [],
    "keyword": "",
    "tag_code": "virtual_data_mart",
    "me_type": "tag",
    "cal_type": ["standard"],
    "data_set_type": "all",
    "page": 1,
    "page_size": COUNT,
    "platform": "bk_data",
    "token_pkey": DATAMAP_DICT_TOKEN_PKEY,
}


def create_importance():
    """
    write importance score to meta
    there are 3 factors which decide the importance score:
    1)dataset attributes(sensitivity & generate type) 2)biz, 3)project
    :return:
    """
    norm_score = NORM_IMPORTANCE_SCORE
    params = copy.deepcopy(DATASET_PARAMS)
    search_dict = datamanage_api.get_data_dict_count(
        params, retry_times=3, raise_exception=True
    ).data
    search_count = search_dict.get("count")
    importance_score_dict = {}
    importance_list = []
    # 1）get importance metric of all dataset
    for i in range(int(ceil(search_count / float(COUNT)))):
        params["page"] = i + 1
        try:
            search_list = datamanage_api.get_data_dict_list(
                params, retry_times=3, raise_exception=True
            ).data
        except Exception as e:
            logger.error(
                "create_importance get_data_dict_list error:{}, page:{}".format(
                    e.message, i + 1
                )
            )
            continue
        for each_ds in search_list:
            dataset = DataSet(each_ds)
            importance_dict = dataset.get_importance_dict(norm_score)
            if importance_dict:
                importance_list.append(importance_dict)
                importance_score_dict[each_ds["data_set_id"]] = importance_dict[
                    "importance_score"
                ]

    # 2）rank importance_score
    importance_df = score_ranking(importance_score_dict)

    for i in range(int(ceil(search_count / float(COUNT)))):
        search_list = importance_list[i * COUNT : (i + 1) * COUNT]
        for each_ds in search_list:
            each_ds["importance_score_ranking"] = round(
                importance_df[importance_df["dataset_id"] == each_ds["data_set_id"]][
                    "ranking_perct"
                ].values[0],
                4,
            )

    # 3）write meta and kafka
    for i in range(int(ceil(search_count / float(COUNT)))):
        search_list = importance_list[i * COUNT : (i + 1) * COUNT]
        with metadata_client.session as se:
            for each_ds in search_list:
                logger.info(
                    "create_importance data_set_id:{}, page:{}".format(
                        each_ds["data_set_id"], i + 1
                    )
                )
                # write kafka topic
                send_metric_to_kafka(
                    each_ds["data_set_id"],
                    each_ds["data_set_type"],
                    each_ds,
                    "importance",
                )
                # wirte meta
                format_dict(each_ds)
                se.create(Importance(**each_ds))
            try:
                se.commit()
            except Exception as e:
                logger.error("importance meta sdk commit error:{}".format(e))
    logger.info("create_importance finish")


def create_asset_value():
    """
    write asset_value to meta
    :return:
    """
    params = copy.deepcopy(DATASET_PARAMS)
    search_dict = datamanage_api.get_data_dict_count(
        params, retry_times=3, raise_exception=True
    ).data
    search_count = search_dict.get("count")
    asset_value_score_dict = {}
    asset_value_list = []
    # 1) get asset_value_dict of all data sets
    for i in range(int(ceil(search_count / float(COUNT)))):
        params["page"] = i + 1
        try:
            search_list = datamanage_api.get_data_dict_list(
                params, retry_times=3, raise_exception=True
            ).data
        except Exception as e:
            logger.error(
                "create_asset_value get_data_dict_list error:{}, page: {}".format(
                    e.message, i + 1
                )
            )
            continue
        for each_ds in search_list:
            dataset = DataSet(each_ds)
            asset_value_dict = dataset.get_asset_value_dict()
            if asset_value_dict:
                asset_value_list.append(asset_value_dict)
                asset_value_score_dict[each_ds["data_set_id"]] = asset_value_dict[
                    "asset_value_score"
                ]

    # 2) rank asset_value_score
    asset_value_df = score_ranking(asset_value_score_dict)

    for i in range(int(ceil(search_count / float(COUNT)))):
        search_list = asset_value_list[i * COUNT : (i + 1) * COUNT]
        for each_ds in search_list:
            each_ds["asset_value_score_ranking"] = round(
                asset_value_df[asset_value_df["dataset_id"] == each_ds["data_set_id"]][
                    "ranking_perct"
                ].values[0],
                4,
            )

    # 3) write kafka and meta
    for i in range(int(ceil(search_count / float(COUNT)))):
        search_list = asset_value_list[i * COUNT : (i + 1) * COUNT]
        with metadata_client.session as se:
            for each_ds in search_list:
                logger.info(
                    "create_asset_value data_set_id:{}, page:{}".format(
                        each_ds["data_set_id"], i + 1
                    )
                )
                # write kafka
                send_metric_to_kafka(
                    each_ds["data_set_id"],
                    each_ds["data_set_type"],
                    each_ds,
                    "asset_value",
                )
                # wirte meta
                format_dict(each_ds)
                se.create(AssetValue(**each_ds))
            try:
                se.commit()
            except Exception as e:
                logger.error("meta sdk commit error:{}".format(e))
    logger.info("create_asset_value finish")


def create_cost():
    """
    write cost and storage_capacity to meta
    :return:
    """
    params = copy.deepcopy(DATASET_PARAMS)
    search_dict = datamanage_api.get_data_dict_count(
        params, retry_times=3, raise_exception=True
    ).data
    search_count = search_dict.get("count")
    for i in range(int(ceil(search_count / float(COUNT)))):
        params["page"] = i + 1
        try:
            search_list = datamanage_api.get_data_dict_list(
                params, retry_times=3, raise_exception=True
            ).data
        except Exception as e:
            logger.error(
                "create_cost get_data_dict_list error:{}, page:{}".format(
                    e.message, i + 1
                )
            )
            continue
        with metadata_client.session as se:
            for each_ds in search_list:
                dataset = DataSet(each_ds)
                storage_capacity_dict = dataset.get_storage_capacity_dict()
                cost_dict = dataset.get_cost_dict()
                # write meta
                se.create(StorageCapacity(**storage_capacity_dict))
                se.create(Cost(**cost_dict))
                # wirte kafka topic
                storage_capacity_dict.update(cost_dict)
                send_metric_to_kafka(
                    each_ds["data_set_id"],
                    each_ds["data_set_type"],
                    storage_capacity_dict,
                    "cost",
                )
            try:
                se.commit()
            except Exception as e:
                logger.error("meta sdk commit error:{}".format(e))
    logger.info("create storage_capacity and create_cost finish")


def create_lifecycle():
    """
    write lifecycle to meta
    :return:
    """
    params = copy.deepcopy(DATASET_PARAMS)
    search_dict = datamanage_api.get_data_dict_count(
        params, retry_times=3, raise_exception=True
    ).data
    search_count = search_dict.get("count")
    assetvalue_to_cost_dict = {}
    lifecycle_list = []
    # 1) get lifecycle_dict of all data sets
    for i in range(int(ceil(search_count / float(COUNT)))):
        params["page"] = i + 1
        try:
            search_list = datamanage_api.get_data_dict_list(
                params, retry_times=3, raise_exception=True
            ).data
        except Exception as e:
            logger.error(
                "create_lifecycle get_data_dict_list error:{}, page:{}".format(
                    e.message, i + 1
                )
            )
            continue
        for each_ds in search_list:
            dataset = DataSet(each_ds)
            lifecycle_dict = dataset.get_lifecycle_dict()
            if lifecycle_dict:
                lifecycle_list.append(lifecycle_dict)
                assetvalue_to_cost_dict[each_ds["data_set_id"]] = lifecycle_dict[
                    "assetvalue_to_cost"
                ]

    # 2) rank assetvalue_to_cost
    lifecycle_df = score_ranking(assetvalue_to_cost_dict)
    for i in range(int(ceil(search_count / float(COUNT)))):
        sub_lifecycle_list = lifecycle_list[i * COUNT : (i + 1) * COUNT]
        for each_ds in sub_lifecycle_list:
            each_ds["assetvalue_to_cost_ranking"] = round(
                lifecycle_df[lifecycle_df["dataset_id"] == each_ds["data_set_id"]][
                    "ranking_perct"
                ].values[0],
                4,
            )

    # 3) write kafka and meta
    for i in range(int(ceil(search_count / float(COUNT)))):
        sub_lifecycle_list = lifecycle_list[i * COUNT : (i + 1) * COUNT]
        with metadata_client.session as se:
            for each_ds in sub_lifecycle_list:
                # wirte kafka topic
                send_metric_to_kafka(
                    each_ds["data_set_id"],
                    each_ds["data_set_type"],
                    each_ds,
                    "lifecycle",
                )
                # write meta
                format_dict(each_ds)
                se.create(LifeCycle(**each_ds))
            try:
                se.commit()
            except Exception as e:
                logger.error("meta sdk commit error:{}".format(e))
    logger.info("create lifecycle finish")


def create_range():
    """
    write range score and metric to kafka topic and meta
    """
    logger.info("create range start, time:{time}".format(time=datetime.datetime.now()))
    params = copy.deepcopy(DATASET_PARAMS)
    search_dict = datamanage_api.get_data_dict_count(
        params, retry_times=3, raise_exception=True
    ).data
    search_count = search_dict.get("count")
    range_score_dict = {}
    range_list = []
    # 1) get range_dict of all data sets
    for i in range(int(ceil(search_count / float(COUNT)))):
        params["page"] = i + 1
        logger.info("create_range page:{}".format(i + 1))
        try:
            search_list = datamanage_api.get_data_dict_list(
                params, retry_times=3, raise_exception=True
            ).data
        except Exception as e:
            logger.error(
                "create_range get_data_dict_list error:{}, page:{}".format(
                    e.message, i + 1
                )
            )
            continue
        for each_ds in search_list:
            dataset = DataSet(each_ds)
            range_dict = dataset.get_range_dict()
            range_list.append(range_dict)
            range_score_dict[each_ds["data_set_id"]] = range_dict[
                "normalized_range_score"
            ]

    # 2) rank range_score
    range_df = score_ranking(range_score_dict)
    for i in range(int(ceil(search_count / float(COUNT)))):
        search_list = range_list[i * COUNT : (i + 1) * COUNT]
        for each_ds in search_list:
            each_ds["range_score_ranking"] = round(
                range_df[range_df["dataset_id"] == each_ds["data_set_id"]][
                    "ranking_perct"
                ].values[0],
                4,
            )

    # 3) write kafka topic and meta
    for i in range(int(ceil(search_count / float(COUNT)))):
        search_list = range_list[i * COUNT : (i + 1) * COUNT]
        with metadata_client.session as se:
            for each_ds in search_list:
                # write kafka topic
                send_metric_to_kafka(
                    each_ds["data_set_id"], each_ds["data_set_type"], each_ds, "range"
                )
                logger.info(
                    "send_range_metric_to_kafka dataset_id: {}, range_metric_dict: {}".format(
                        each_ds["data_set_id"], each_ds
                    )
                )

                # write meta
                format_dict(each_ds)
                se.create(Range(**each_ds))
                logger.info(
                    "create Range dataset_id:{} range_dict:{}".format(
                        each_ds["id"], each_ds
                    )
                )
            try:
                se.commit()
            except Exception as e:
                logger.error("range meta sdk commit error:{}".format(e))
    logger.info("create range finish, time:{time}".format(time=datetime.datetime.now()))


def create_heat():
    """
    write heat score and metric to kafka topic and meta
    """
    query_dataset_dict = get_query_info()

    params = copy.deepcopy(DATASET_PARAMS)
    search_dict = datamanage_api.get_data_dict_count(
        params, retry_times=3, raise_exception=True
    ).data
    search_count = search_dict.get("count")
    heat_score_dict = {}
    heat_list = []
    # 1) get heat_dict of all data sets
    for i in range(int(ceil(search_count / float(COUNT)))):
        params["page"] = i + 1
        logger.info("create_heat page:{}".format(i + 1))
        try:
            search_list = datamanage_api.get_data_dict_list(
                params, retry_times=3, raise_exception=True
            ).data
        except Exception as e:
            logger.error(
                "create_heat get_data_dict_list error:{}, page:{}".format(
                    e.message, i + 1
                )
            )
            continue
        for each_ds in search_list:
            each_ds["query_dataset_dict"] = query_dataset_dict
            dataset = DataSet(each_ds)
            heat_dict = dataset.get_heat_dict()
            heat_list.append(heat_dict)
            heat_score_dict[each_ds["data_set_id"]] = heat_dict["heat_score"]

    # 2) rank heat_score
    heat_df = score_ranking(heat_score_dict)
    for i in range(int(ceil(search_count / float(COUNT)))):
        search_list = heat_list[i * COUNT : (i + 1) * COUNT]
        for each_ds in search_list:
            each_ds["heat_score_ranking"] = round(
                heat_df[heat_df["dataset_id"] == each_ds["data_set_id"]][
                    "ranking_perct"
                ].values[0],
                4,
            )

    # 3) write kafka topic and meta
    for i in range(int(ceil(search_count / float(COUNT)))):
        search_list = heat_list[i * COUNT : (i + 1) * COUNT]
        with metadata_client.session as se:
            for each_ds in search_list:
                # write kafka topic
                send_heat_metric_to_kafka(
                    each_ds["data_set_id"], each_ds["data_set_type"], each_ds
                )

                # write meta
                format_dict(each_ds)
                se.create(Heat(**each_ds))
                logger.info(
                    "create Heat dataset_id:{}, heat_dict:{}".format(
                        each_ds["id"], each_ds
                    )
                )
            try:
                se.commit()
            except Exception as e:
                logger.error("heat meta sdk commit error:{}".format(e))
    logger.info("create heat finish, time:{time}".format(time=datetime.datetime.now()))


def create_heat_related_metric():
    """
    write day query count of all dataset to kafka during latest 7 days
    :return:
    """
    logger.info(
        "create_heat_related_metric start, time:{time}".format(
            time=datetime.datetime.now()
        )
    )
    # get the day query info of product rt during latest 7 days
    day_query_rt_dict, day_query_rt_list = get_day_query_info()

    params = copy.deepcopy(DATASET_PARAMS)
    search_dict = datamanage_api.get_data_dict_count(
        params, retry_times=3, raise_exception=True
    ).data
    search_count = search_dict.get("count")
    for i in range(int(ceil(search_count / float(COUNT)))):
        params["page"] = i + 1
        logger.info("create_heat_related_metric page:{}".format(i + 1))
        try:
            search_list = datamanage_api.get_data_dict_list(
                params, retry_times=3, raise_exception=True
            ).data
        except Exception as e:
            logger.error(
                "create_heat_related_metric get_data_dict_list error:{}, page:{}".format(
                    e.message, i + 1
                )
            )
            continue
        for each_ds in search_list:
            each_ds["day_query_rt_dict"] = day_query_rt_dict
            each_ds["day_query_rt_list"] = day_query_rt_list
            dataset = DataSet(each_ds)
            # write the day query count of all rt to kafka and get the equivalent day query count of all rd(sum of all
            # rts' day query count)
            day_query_count_list = dataset.get_day_query_count_list()
            for each_query in day_query_count_list:
                send_rt_day_query_count_to_kafka(
                    each_ds["data_set_id"],
                    each_ds["data_set_type"],
                    each_query["metric_dict"],
                    each_query["dimension_dict"],
                )
                logger.info(
                    "dataset_id:{}, metric_dict:{}, dimension_dict:{}".format(
                        each_ds["data_set_id"],
                        each_query["metric_dict"],
                        each_query["dimension_dict"],
                    )
                )
    logger.info(
        "create_heat_related_metric end, time:{time}".format(
            time=datetime.datetime.now()
        )
    )
