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
from datahub.common.const import ROUTE, STREAM_TO


class BaseHandler(object):
    @classmethod
    def get_channel_and_update(cls, data_scenario, raw_data_id, raw_data, cluster_type, queue_route):
        return int(raw_data_id)

    @classmethod
    def get_channel_id(cls, data_scenario, raw_data_id, raw_data):
        return int(raw_data_id)

    @classmethod
    def get_scenario_plat_name(cls, data_scenario):
        return "tgdp"

    @classmethod
    def get_params_by_scenraio(
        cls,
        odm_name,
        data_route_params,
        cluster_type,
        raw_data_name,
        bk_biz_id,
        topic_name,
    ):
        from datahub.access.raw_data.rawdata import add_topic_info_config

        add_topic_info_config(
            data_route_params[ROUTE][0][STREAM_TO],
            cluster_type,
            raw_data_name,
            bk_biz_id,
            topic_name,
        )
        return data_route_params

    @classmethod
    def fill_inner_storages_info(cls, rt_info, storages):
        return rt_info

    @classmethod
    def is_v3_biz(cls, bk_user_name, biz_id):
        return True
