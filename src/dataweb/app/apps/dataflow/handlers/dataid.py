# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""


from apps import exceptions
from apps.api import AccessApi, DataManageApi
from apps.utils import APIModel
from apps.utils.time_handler import (
    generate_influxdb_time_range,
    get_active_timezone_offset,
    time_format,
)


class DataId(APIModel):
    def __init__(self, data_id):
        super(DataId, self).__init__()
        self.data_id = data_id

    def list_data_count_by_time(self, start_timestamp, end_timestamp, frequency=None):
        # frequency = '3m'  三分钟数据量
        # frequency = '1d'  日数据量
        start_timestamp, end_timestamp = generate_influxdb_time_range(start_timestamp, end_timestamp)
        if frequency:
            group_by = "GROUP BY time({})".format(frequency)
        else:
            group_by = ""
        sql = (
            """SELECT sum("cnt")  as cnt FROM "kafka_topic_message_cnt" WHERE "topic" ='{}'  AND time >= {} """
            """AND time <= {} {} fill(null)"""
        ).format(self.topic, start_timestamp, end_timestamp, group_by)
        params = {"database": "monitor_data_metrics", "sql": sql, "tags": self.geog_area}
        data_counts = DataManageApi.dmonitor_metrics.query(params)
        cnts = [data["cnt"] for data in data_counts.get("series", [])]
        time = [data["time"] for data in data_counts.get("series", [])]

        formated_time = time_format(time)
        return_data = {"cnt": cnts, "time": formated_time, "timezone": get_active_timezone_offset()}
        return return_data

    def is_exist(self):
        try:
            self._data = self._get_data()
            return True
        except exceptions.ApiResultError:
            return False

    def _get_data(self):
        return AccessApi.rawdata.retrieve({"raw_data_id": self.data_id})

    @property
    def topic(self):
        return self.data.get("topic_name", "{}{}".format(self.raw_data_name, self.bk_biz_id))

    @property
    def raw_data_name(self):
        return self.data["raw_data_name"]

    @property
    def bk_biz_id(self):
        return self.data["bk_biz_id"]

    @property
    def geog_area(self):
        """
        区域标签
        """
        try:
            return [tag["code"] for tag in self.data["tags"]["manage"]["geog_area"]]
        except KeyError:
            return []
