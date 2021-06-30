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

import math

from lifecycle.metrics.app_code import get_app_code_count
from lifecycle.metrics.cost import (
    get_cost_score,
    get_hdfs_capacity,
    get_tspider_capacity,
)
from lifecycle.metrics.heat import (
    get_heat_score,
    get_query_count,
    get_rd_query_list_group,
)
from lifecycle.metrics.importance import (
    biz_data_correct,
    get_asset_value_score,
    get_biz_info,
    get_dataset_info,
    get_heat_range_importance_score,
    get_project_score,
)
from lifecycle.metrics.range import get_node_info, get_range_info, get_range_score

# 数据敏感度
SENSITIVITY_DICT = {"public": 0, "private": 1, "confidential": 2, "topsecret": 3}
# 存储评分阈值
THRESHOLD = 20.41


class Entity(object):
    def _get_data(self):
        return {}

    def __getattr__(self, item):
        """
        get the attribute of a object when the attribute can not be gotten directly
        :param item:
        :return:
        """
        if item in self._get_data():
            return self._get_data()[item]
        super(Entity, self).__getattr__(item)


class Project(Entity):
    def __init__(self, project_id):
        self.project_id = project_id
        self.get_project_score()

    def get_project_score(self):
        """
        get project score and project active
        :return:
        """
        self.project_score, self.active = get_project_score(self.project_id)


class BkBiz(Entity):
    def __init__(self, biz_config):
        self.biz_config = biz_config
        self.biz_data_correct()

    def _get_data(self):
        return self.biz_config

    def biz_data_correct(self):
        """
        correct biz attributes
        :return:
        """
        if self.biz_config:
            (
                self.bip_grade_id,
                self.oper_state,
                self.app_important_level,
                self.oper_state_name,
                self.bip_grade_name,
                self.app_important_level_name,
            ) = biz_data_correct(self.biz_config)
        else:
            self.bip_grade_id = None
            self.oper_state = None
            self.app_important_level = None
            self.oper_state_name = None
            self.bip_grade_name = None
            self.app_important_level_name = None

    @property
    def is_bip(self):
        """
        is_bip
        :return:
        """
        return True if self.biz_config.get("BizV1.IsBip") == "是" else False

    @property
    def biz_score(self):
        """
        biz_score
        :return:
        """
        if (
            self.oper_state is None
            and self.bip_grade_id is None
            and self.app_important_level is None
        ):
            return 0
        return (
            self.oper_state + self.bip_grade_id + self.app_important_level + 1
            if self.is_bip is True
            else self.oper_state + self.bip_grade_id + self.app_important_level
        )


class Range(Entity):
    def __init__(self, dataset_id, dataset_type):
        self.dataset_id = dataset_id
        self.dataset_type = dataset_type
        self.biz_count = 1
        self.project_count = 1
        self.weighted_node_count = 0.0
        self.node_count_list = "[]"
        self.node_count = 0
        self.depth = 0
        self.range_score = 0.0
        self.normalized_range_score = 13.1
        self.app_code_count = 0
        self.range_related_dict = self.get_range_info()
        self.set_range_dict()

    def get_range_info(self):
        return get_range_info(self.dataset_id, self.dataset_type)

    def get_biz_count(self):
        """
        get the count of bizs which apply the dataset
        if the dataset does not have lineage, biz_count=1 (the biz of itslef)
        :return:
        """
        biz_list = self.range_related_dict.get("data", {}).get("bk_biz_id", [])
        return len(biz_list[0].get("@groupby")) if biz_list else 1

    def get_project_count(self):
        """
        get the count of projects which apply the dataset
        if the dataset does not have lineage, project_count=1 (the project of itslef)
        :return:
        """
        project_list = self.range_related_dict.get("data", {}).get("project_count", [])
        return (
            project_list[0].get("count")
            if project_list and project_list[0].get("count") > 0
            else 1
        )

    def get_node_info(self):
        """
        get the successor node info
        :return:
        """
        return get_node_info(self.dataset_id, self.dataset_type)

    def get_range_score(self):
        """
        get range_score & normalized_range_score
        :return:
        """
        return get_range_score(
            self.weighted_node_count, self.biz_count, self.project_count
        )

    def get_app_code_count(self):
        """
        get the count of apps which query the dataset
        :return:
        """
        return get_app_code_count(self.dataset_id)

    def set_range_dict(self):
        if self.range_related_dict:
            self.biz_count = self.get_biz_count()
            self.project_count = self.get_project_count()
            (
                self.weighted_node_count,
                self.node_count_list,
                self.node_count,
                self.depth,
            ) = self.get_node_info()
            self.range_score, self.normalized_range_score = self.get_range_score()
            self.app_code_count = self.get_app_code_count()


class Heat(Entity):
    def __init__(self, heat_config):
        self.heat_config = heat_config
        self.queue_service_count = 0
        self.heat_score = 0.0

    def _get_data(self):
        return self.heat_config

    def set_heat_score(self):
        self.heat_score = get_heat_score(
            self.heat_config["dataset_id"],
            self.heat_config["dataset_type"],
            self.query_count,
        )


class StorageCapacity(Entity):
    def __init__(self, dataset_id, dataset_type):
        self.dataset_id = dataset_id
        self.dataset_type = dataset_type
        self.set_hdfs_capacity()
        self.set_tspider_capacity()
        self.set_total_capacity()
        self.set_log_capacity()
        self.set_capacity_score(THRESHOLD)

    def set_hdfs_capacity(self):
        if self.dataset_type == "result_table":
            self.hdfs_capacity = get_hdfs_capacity(self.dataset_id)
        else:
            self.hdfs_capacity = 0

    def set_tspider_capacity(self):
        if self.dataset_type == "result_table":
            self.tspider_capacity = get_tspider_capacity(self.dataset_id)
        else:
            self.tspider_capacity = 0

    def set_total_capacity(self):
        """
        get total capacity (hdfs_capacity + tspider_capacity)
        :return:
        """
        self.total_capacity = self.hdfs_capacity + self.tspider_capacity

    def set_log_capacity(self):
        """
        get log capacity
        :return:
        """
        if self.total_capacity:
            self.log_capacity = math.log(self.total_capacity)
        else:
            self.log_capacity = -1

    def set_capacity_score(self, threshold):
        """
        get capacity_score
        :return:
        """
        if self.log_capacity and self.log_capacity >= 0:
            self.capacity_score = (
                99.99
                if self.log_capacity / float(threshold) * 100 > 99.99
                else round(self.log_capacity / float(threshold) * 100, 2)
            )
        else:
            self.capacity_score = -1


class DataSet(Entity):
    def __init__(self, dataset_config):
        self.dataset_config = dataset_config
        self.heat_score = 0
        self.normalized_range_score = 0
        self.importance_score = 0
        self.asset_value_score = 0
        self.query_count = 0

    def _get_data(self):
        return self.dataset_config

    def set_dataset_info(self):
        """
        get the dataset attributes
        :return:
        """
        (
            self.biz_id,
            self.project_id,
            self.sensitivity,
            self.generate_type,
        ) = get_dataset_info(self.data_set_id, self.data_set_type)

    def get_storage_capacity(self):
        return StorageCapacity(
            self.dataset_config["data_set_id"], self.dataset_config["data_set_type"]
        )

    @property
    def storage_capacity(self):
        """
        storage_capacity stucture
        :return:
        """
        if not hasattr(self, "_storage_capacity"):
            self._storage_capacity = self.get_storage_capacity()
        return self._storage_capacity

    def get_biz(self):
        """
        get the biz structure
        :return:
        """
        biz_dict = get_biz_info(self.biz_id)
        return BkBiz(biz_dict)

    @property
    def biz(self):
        """
        biz structure
        :return:
        """
        if not hasattr(self, "_biz"):
            self._biz = self.get_biz()
        return self._biz

    def get_range(self):
        """
        get the range structure
        :return:
        """
        return Range(
            self.dataset_config["data_set_id"], self.dataset_config["data_set_type"]
        )

    @property
    def range(self):
        """
        range structure
        :return:
        """
        if not hasattr(self, "_range"):
            self._range = self.get_range()
        return self._range

    def get_heat(self):
        """
        get the heat structure
        :return:
        """
        return Heat(
            {
                "dataset_id": self.dataset_config["data_set_id"],
                "dataset_type": self.dataset_config["data_set_type"],
                "query_count": self.query_count,
            }
        )

    @property
    def heat(self):
        """
        heat structure
        :return:
        """
        if not hasattr(self, "_heat"):
            self._heat = self.get_heat()
        return self._heat

    def get_project(self):
        """
        get the project structure
        :return:
        """
        return Project(self.project_id)

    @property
    def project(self):
        """
        project structure
        :return:
        """
        if not hasattr(self, "_project"):
            self._project = self.get_project()
        return self._project

    @property
    def dataset_score(self):
        """
        dataset_score
        :return:
        """
        return (
            SENSITIVITY_DICT.get(self.sensitivity, 0) + 1
            if self.generate_type == "user"
            else SENSITIVITY_DICT.get(self.sensitivity, 0)
        )

    @property
    def id(self):
        """
        id
        :return:
        """
        return "{}_{}".format(self.data_set_id, self.data_set_type)

    def get_importance_score(self, norm_score=18):
        """
        get importance_score
        :param norm_score: normalized_score
        :return:
        """
        importance_score = round(
            (self.dataset_score + self.biz.biz_score + self.project.project_score)
            / float(norm_score)
            * 100,
            2,
        )
        importance_score = importance_score if importance_score <= 99.99 else 99.99
        self.importance_score = importance_score
        return importance_score

    def set_heat_range_importance_score(self):
        """
        get heat_score & range_score & importance_score
        :return:
        """
        (
            self.heat_score,
            self.normalized_range_score,
            self.importance_score,
        ) = get_heat_range_importance_score(self.id)

    def get_importance_dict(self, norm_score=18):
        """
        get all properties related to importance
        :param norm_score: normalized_score
        :return:
        """
        self.set_dataset_info()
        if self.biz_id and self.sensitivity and self.generate_type:
            return {
                "id": self.id,
                "data_set_id": self.data_set_id,
                "data_set_type": self.data_set_type,
                "dataset_score": self.dataset_score,
                "biz_score": self.biz.biz_score,
                "is_bip": self.biz.is_bip,
                "oper_state_name": self.biz.oper_state_name,
                "oper_state": self.biz.oper_state,
                "bip_grade_name": self.biz.bip_grade_name,
                "bip_grade_id": self.biz.bip_grade_id,
                "app_important_level_name": self.biz.app_important_level_name,
                "app_important_level": self.biz.app_important_level,
                "project_score": self.project.project_score,
                "active": self.project.active,
                "importance_score": self.get_importance_score(norm_score),
            }
        else:
            return {}

    def get_asset_value_score(self):
        self.asset_value_score = round(
            (self.importance_score + self.heat_score + self.normalized_range_score)
            / 3.0,
            2,
        )

    @property
    def target_id(self):
        return self.dataset_config["data_set_id"]

    @property
    def target_type(self):
        return (
            "access_raw_data"
            if self.dataset_config["data_set_type"] == "raw_data"
            else self.dataset_config["data_set_type"]
        )

    def get_asset_value_dict(self):
        """
        get all properties related to asset_value
        :return:
        """
        self.set_heat_range_importance_score()
        if (
            self.heat_score is not None
            and self.normalized_range_score is not None
            and self.importance_score is not None
        ):
            self.get_asset_value_score()
            return {
                "id": self.id,
                "data_set_id": self.data_set_id,
                "data_set_type": self.data_set_type,
                "range_id": self.id,
                "normalized_range_score": self.normalized_range_score,
                "heat_id": self.id,
                "heat_score": self.heat_score,
                "importance_id": self.id,
                "importance_score": self.importance_score,
                "asset_value_score": self.asset_value_score,
                "target_id": self.target_id,
                "target_type": self.target_type,
            }
        else:
            return {}

    def get_storage_capacity_dict(self):
        """
        get all properties related to storage_capacity
        :return:
        """
        return {
            "id": self.id,
            "hdfs_capacity": self.storage_capacity.hdfs_capacity,
            "tspider_capacity": self.storage_capacity.tspider_capacity,
            "total_capacity": self.storage_capacity.total_capacity,
            "log_capacity": self.storage_capacity.log_capacity,
            "capacity_score": self.storage_capacity.capacity_score,
        }

    def get_cost_dict(self):
        """
        get all properties related to cost
        :return:
        """
        return {
            "id": self.id,
            "target_id": self.target_id,
            "target_type": self.target_type,
            "capacity_id": self.id,
            "capacity_score": self.storage_capacity.capacity_score,
        }

    def set_cost_score(self):
        """
        get cost_score
        :return:
        """
        self.cost_score = get_cost_score(self.id)

    def set_asset_value_score_via_erp(self):
        """
        get asset_value_score
        :return:
        """
        self.asset_value_score = get_asset_value_score(self.id)

    @property
    def assetvalue_to_cost(self):
        if self.cost_score and self.cost_score != -1:
            return (
                round(self.asset_value_score / float(self.cost_score), 2)
                if self.cost_score >= 0
                else -1.0
            )
        else:
            return -1.0

    def get_lifecycle_dict(self):
        """
        get all properties related to lifecycle
        :return:
        """
        self.set_asset_value_score_via_erp()
        self.set_cost_score()
        if self.asset_value_score is not None and self.cost_score is not None:
            return {
                "id": self.id,
                "data_set_id": self.data_set_id,
                "data_set_type": self.data_set_type,
                "target_id": self.target_id,
                "target_type": self.target_type,
                "range_id": self.id,
                "heat_id": self.id,
                "importance_id": self.id,
                "asset_value_id": self.id,
                "cost_id": self.id,
                "assetvalue_to_cost": self.assetvalue_to_cost,
            }
        else:
            return {}

    def get_range_dict(self):
        """
        get all properties related to range
        :return:
        """
        return {
            "id": self.id,
            "data_set_id": self.data_set_id,
            "data_set_type": self.data_set_type,
            "biz_count": self.range.biz_count,
            "project_count": self.range.project_count,
            "depth": self.range.depth,
            "node_count_list": self.range.node_count_list,
            "node_count": self.range.node_count,
            "weighted_node_count": self.range.weighted_node_count,
            "app_code_count": self.range.app_code_count,
            "range_score": self.range.range_score,
            "normalized_range_score": self.range.normalized_range_score,
        }

    def set_query_count(self):
        self.query_count = get_query_count(
            self.data_set_id, self.data_set_type, self.query_dataset_dict
        )

    def get_heat_dict(self):
        """
        get all properties related to heat
        :return:
        """
        self.set_query_count()
        self.heat.set_heat_score()
        return {
            "data_set_id": self.data_set_id,
            "data_set_type": self.data_set_type,
            "id": self.id,
            "query_count": self.heat.query_count,
            "queue_service_count": self.heat.queue_service_count,
            "heat_score": self.heat.heat_score,
        }

    def get_day_query_count_dict(self, query_config_dict):
        """
        get the query_count_list of dataset per day
        :return:
        """
        heat = self.get_heat()
        heat.set_heat_score()
        metric_dict = {
            "queue_service_count": heat.queue_service_count,
            "app_query_count": query_config_dict["app_query_count"],
            "day_query_count": query_config_dict["day_query_count"],
            "query_count": heat.query_count,
            "statistics_timestamp": query_config_dict["timestamp"],
            "heat_score": heat.heat_score,
        }
        dimension_dict = {
            "app_code": query_config_dict["app_code"],
            "statistics_time": query_config_dict["time_str"],
        }
        return metric_dict, dimension_dict

    def get_day_query_count_list(self):
        """
        get the query_count_list of dataset per day, the query_count of rd is gotten by the clean rts of rd
        :return:
        """
        day_query_count_list = []
        if (
            self.data_set_type == "result_table"
            and self.data_set_id in self.day_query_rt_list
        ):
            for each_day in self.day_query_rt_dict[self.data_set_id]["query_list"]:
                self.query_count = self.day_query_rt_dict[self.data_set_id][
                    "query_count"
                ]
                metric_dict, dimension_dict = self.get_day_query_count_dict(each_day)
                day_query_count_list.append(
                    {"metric_dict": metric_dict, "dimension_dict": dimension_dict}
                )

        elif self.data_set_type == "raw_data":
            rd_query_list_group = get_rd_query_list_group(
                self.data_set_id, self.day_query_rt_list, self.day_query_rt_dict
            )
            for key, group in rd_query_list_group:
                sum_query_count = 0
                for g in group:
                    sum_query_count += g.get("day_query_count")
                g["app_query_count"] = 0
                g["day_query_count"] = sum_query_count
                self.query_count = 0
                metric_dict, dimension_dict = self.get_day_query_count_dict(g)
                day_query_count_list.append(
                    {"metric_dict": metric_dict, "dimension_dict": dimension_dict}
                )
        return day_query_count_list
