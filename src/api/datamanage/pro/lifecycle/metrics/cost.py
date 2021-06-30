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


from common.log import logger

from datamanage.pro.lifecycle.utils import get_date, get_timestamp, get_last_timestamp, generate_influxdb_time_range
from datamanage.utils.api.dataquery import DataqueryApi
from datamanage.utils.api.storekit import StorekitApi
from datamanage.utils.api.datamanage import DatamanageApi
from datamanage.utils.api.meta import MetaApi

UNIT_LIST = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']


def get_hdfs_capacity(dataset_id):
    """
    get total_hdfs_capacity and incremental_hdfs_capacity during 7 days of rt, unit is byte
    :param dataset_id:
    :return:
    """
    # 获取昨天的日期
    date_1 = get_date(1)
    # 获取8天前的日期
    date_8 = get_date(8)
    prefer_storage = 'tspider'
    sql = (
        "SELECT SUM(rt_capacity_bytes) AS capacity FROM 591_rt_capacity_hdfs_ccid "
        "WHERE (thedate = {} OR thedate = {}) and rt_id = '{}' "
        "GROUP BY thedate ORDER BY thedate DESC".format(date_1, date_8, dataset_id)
    )
    hdfs_cap_dict = DataqueryApi.query(
        {
            'sql': sql,
            'prefer_storage': prefer_storage,
        }
    ).data
    if not hdfs_cap_dict:
        logger.error('get dataset_id:{} hdfs_capacity error'.format(dataset_id))
        return 0, 0
    hdfs_capacity_list = hdfs_cap_dict.get('list', [])
    hdfs_capacity = hdfs_capacity_list[0].get('capacity') if hdfs_capacity_list else 0
    incre_hdfs_capacity = hdfs_capacity - hdfs_capacity_list[1].get('capacity') if len(hdfs_capacity_list) > 1 else 0
    incre_hdfs_capacity = 0 if incre_hdfs_capacity < 0 else incre_hdfs_capacity
    return hdfs_capacity, incre_hdfs_capacity


def get_tspider_capacity(dataset_id):
    """
    get total_tspider_capacity and incremental_tspider_capacity during 7 days of rt, unit is byte
    :param dataset_id:
    :return:
    """
    tspider_cap_list = StorekitApi.tspider_capacity({'result_table_id': dataset_id}).data
    if not tspider_cap_list:
        logger.info('get dataset_id:{} tspider_capacity empty'.format(dataset_id))
        return 0, 0
    # the unit which gotten directly from the api is kb, should be converted to byte
    tspider_capacity = tspider_cap_list[0].get('size', 0) * 1024

    # 获取7天前0时刻的时间戳
    timestamp = get_timestamp(day=7)
    start_time = timestamp - 1
    end_time = timestamp + 1
    past_tspider_cap_list = StorekitApi.tspider_capacity(
        {'result_table_id': dataset_id, 'start_time': start_time, 'end_time': end_time}
    ).data
    if not past_tspider_cap_list:
        logger.info('get dataset_id:{} tspider_capacity empty'.format(dataset_id))
        return tspider_capacity, tspider_capacity
    incre_tspider_capacity = tspider_capacity - past_tspider_cap_list[0].get('size', 0)
    incre_tspider_capacity = 0 if incre_tspider_capacity < 0 else incre_tspider_capacity
    return tspider_capacity, incre_tspider_capacity


def get_rt_count(rt_id, storage_type, storages_dict, day=7):
    """
    rt最新n天新增数据量
    :param rt_id:
    :param storage_type:
    :param storages_dict:
    :param day:
    :return:
    """
    start_timestamp = get_timestamp(day=day)
    end_timestamp = get_last_timestamp()
    start_timestamp, end_timestamp = generate_influxdb_time_range(start_timestamp, end_timestamp)

    # 查meta的result_tables接口拿到storage_cluster_config_id
    cnt = 0
    sql = ''
    rt_condition = "logical_tag = '%s'" % rt_id
    if storage_type == 'kafka':
        sql = """select sum(data_inc) as cnt from data_loss_output_total where {} and storage_cluster_type = 'kafka'
                    and time >= {} and  time <= {}""".format(
            rt_condition, start_timestamp, end_timestamp
        )
    else:
        storage_cluster_config_id = (
            storages_dict[storage_type].get('storage_cluster', {}).get('storage_cluster_config_id')
        )
        if storage_cluster_config_id or storage_cluster_config_id == 0:
            sql = """select sum(data_inc) as cnt from data_loss_output_total where {} and storage = 'storage_{}'
                and time >= {} and  time <= {}""".format(
                rt_condition, storage_cluster_config_id, start_timestamp, end_timestamp
            )
    if sql:
        data_counts = DatamanageApi.dmonitor_metrics_query({'database': 'monitor_data_metrics', 'sql': sql}).data
        cnt = data_counts['series'][0].get('cnt', 0) if data_counts and data_counts.get('series', []) else 0
    return cnt


def get_rd_count_info(raw_data_name, bk_biz_id, day=7):
    """
    rd最新n天新增数据量
    :param day:
    :return:
    """
    start_timestamp = get_timestamp(day=day)
    end_timestamp = get_last_timestamp()
    start_timestamp, end_timestamp = generate_influxdb_time_range(start_timestamp, end_timestamp)
    topic = '{}{}'.format(raw_data_name, bk_biz_id)

    sql = """SELECT sum(cnt) as cnt FROM kafka_topic_message_cnt
            WHERE topic ='{}'  AND time >= {} AND time <= {}""".format(
        topic, start_timestamp, end_timestamp
    )
    has_data = False
    data_counts = DatamanageApi.dmonitor_metrics_query({'database': 'monitor_data_metrics', 'sql': sql}).data
    if not data_counts:
        return has_data
    cnt = data_counts['series'][0].get('cnt', 0) if data_counts.get('series', []) else 0
    # 如果cnt>0的话，有数据，否则无数据
    if cnt > 0:
        has_data = True
    return has_data


def get_storage_info(result_table_id):
    """
    拿到存储列表带存储容量和数据增量
    :param result_table_id:
    :return:
    """
    # 1) 先get存储类型，看该rt有无tspider和hdfs两种存储类型
    rt_dict = MetaApi.result_tables.retrieve({'result_table_id': result_table_id, 'related': ['storages']}).data
    # 存储类型列表
    storages_dict = rt_dict['storages']
    storages_list = list(rt_dict.get('storages', {}).keys())

    # 2) 拿到tspider和hdfs两种存储类型的总存储 & 最近7天新增存储 & 最近7天新增数据量
    has_data = False
    for each_storage in storages_list:
        storages_dict[each_storage]['updated_by'] = (
            storages_dict[each_storage]['created_by']
            if not storages_dict[each_storage]['updated_by']
            else storages_dict[each_storage]['updated_by']
        )
        if each_storage == 'hdfs':
            hdfs_capacity, incre_hdfs_capacity = get_hdfs_capacity(result_table_id)
            storages_dict['hdfs']['capacity'] = hdfs_capacity
            storages_dict['hdfs']['format_capacity'] = hum_storage_unit(hdfs_capacity)
            storages_dict['hdfs']['incre_capacity'] = incre_hdfs_capacity
        if each_storage == 'tspider':
            tspider_capacity, incre_tspider_capacity = get_tspider_capacity(result_table_id)
            storages_dict['tspider']['capacity'] = tspider_capacity
            storages_dict['tspider']['format_capacity'] = hum_storage_unit(tspider_capacity)
            storages_dict['tspider']['incre_capacity'] = incre_tspider_capacity
        storages_dict[each_storage]['incre_count'] = int(
            get_rt_count(result_table_id, each_storage, storages_dict) / 7.0
        )
        if not has_data and storages_dict[each_storage]['incre_count']:
            has_data = True
    return storages_dict, has_data


def hum_storage_unit(num, suffix='B', return_unit=False):
    """
    根据存储大小自适应决定单位
    :param num:
    :param suffix:
    :return:
    """
    for unit in UNIT_LIST:
        if abs(num) < 1024.0:
            if not return_unit:
                return format_capacity("%3.3f%s%s" % (num, unit, suffix))
            else:
                return format_capacity("%3.3f%s%s" % (num, unit, suffix)), '{}B'.format(unit), UNIT_LIST.index(unit)
        num /= 1024.0
    # YB = 1024ZB
    if not return_unit:
        return format_capacity("%.3f%s%s" % (num, 'Y', suffix))
    else:
        return format_capacity("%.3f%s%s" % (num, 'Y', suffix)), 'YB', 8


def format_capacity(cap_str):
    if '0.000' in cap_str:
        cap_str = cap_str[0:5]
    return cap_str
