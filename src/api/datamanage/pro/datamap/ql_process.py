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


def lifecycle_uids_convert(query_statement, data_set_type, type):
    # 生命周期集合，如果集合不存在的话过滤条件就是has(attr)
    lifecycle_uids_name = ''
    cond = ''
    # 集合不存在的话过滤条件就是lifecycle_dict中的attr，has(attr)
    lifecycle_dict = {
        'range': 'Range.normalized_range_score',
        'heat': 'Heat.heat_score',
        'importance': 'Importance.importance_score',
        'asset_value': 'AssetValue.asset_value_score',
        'assetvalue_to_cost': 'LifeCycle.assetvalue_to_cost',
        'storage_capacity': 'StorageCapacity.total_capacity',
    }
    lifecycle_list = list(lifecycle_dict.keys())
    if type in lifecycle_list:
        rt_uids = '{}_rt_uids'.format(type)
        rd_uids = '{}_rd_uids'.format(type)
        if data_set_type == 'all':
            if (rt_uids in query_statement) and (rd_uids in query_statement):
                lifecycle_uids_name = '{}, {}'.format(rt_uids, rd_uids)
            elif (rt_uids in query_statement) and (rd_uids not in query_statement):
                lifecycle_uids_name = rt_uids
            elif (rt_uids not in query_statement) and (rd_uids in query_statement):
                lifecycle_uids_name = rd_uids
        elif data_set_type == 'result_table' and (rt_uids in query_statement):
            lifecycle_uids_name = rt_uids
        elif data_set_type == 'raw_data' and (rd_uids in query_statement):
            lifecycle_uids_name = rd_uids
        if not lifecycle_uids_name:
            cond = 'has({})'.format(lifecycle_dict[type])
    return lifecycle_uids_name, cond


def filter_dataset_by_lifecycle(
    query_statement,
    params,
    final_entites_name,
    range_operate=None,
    range_score=None,
    heat_operate=None,
    heat_score=None,
    importance_operate=None,
    importance_score=None,
    asset_value_operate=None,
    asset_value_score=None,
    assetvalue_to_cost_operate=None,
    assetvalue_to_cost=None,
    storage_capacity_operate=None,
    storage_capacity=None,
):
    # 按照热度、广度等过滤
    lifecycle_uids_name, lifecycle_cond = '', ''
    lifecycle_uids_name_range, lifecycle_cond_range = '', ''
    lifecycle_uids_name_heat, lifecycle_cond_heat = '', ''
    lifecycle_uids_name_importance, lifecycle_cond_importance = '', ''
    lifecycle_uids_name_asset_value, lifecycle_cond_asset_value = '', ''
    lifecycle_uids_name_assetvalue_to_cost, lifecycle_cond_assetvalue_to_cost = '', ''
    lifecycle_uids_name_storage_capacity, lifecycle_cond_storage_capacity = '', ''
    filter_input = '$final_filter_uids_name'
    # 按广度过滤
    if range_operate is not None and range_score is not None:
        query_statement += (
            """
            var(func:uid($lifecycle_uids_name_range))@filter($range_operate(Range.normalized_range_score,$range_score)){
              ~LifeCycle.range{
                data_set_filter_list_range as LifeCycle.target @filter(uid(%s))
              }
            }
        """
            % filter_input
        )
        filter_input = 'data_set_filter_list_range'
        lifecycle_uids_name_range, lifecycle_cond_range = lifecycle_uids_convert(
            query_statement, params.get('data_set_type'), 'range'
        )
    # 按热度度过滤
    if heat_operate is not None and heat_score is not None:
        query_statement += (
            """
            var(func:uid($lifecycle_uids_name_heat))@filter($heat_operate(Heat.heat_score,$heat_score)) {
              ~LifeCycle.heat{
                data_set_filter_list_heat as LifeCycle.target @filter(uid(%s))
              }
            }
        """
            % filter_input
        )
        filter_input = 'data_set_filter_list_heat'
        lifecycle_uids_name_heat, lifecycle_cond_heat = lifecycle_uids_convert(
            query_statement, params.get('data_set_type'), 'heat'
        )
    # 按重要度过滤
    if importance_operate is not None and importance_score is not None:
        query_statement += (
            """
            var(func:uid($lifecycle_uids_name_importance))
            @filter($importance_operate(Importance.importance_score,$importance_score)) {
              ~LifeCycle.importance{
                data_set_filter_list_importance as LifeCycle.target @filter(uid(%s))
              }
            }
        """
            % filter_input
        )
        filter_input = 'data_set_filter_list_importance'
        lifecycle_uids_name_importance, lifecycle_cond_importance = lifecycle_uids_convert(
            query_statement, params.get('data_set_type'), 'importance'
        )
    # 按价值评分过滤
    if asset_value_operate is not None and asset_value_score is not None:
        query_statement += (
            """
            var(func:uid($lifecycle_uids_name_asset_value))
            @filter($asset_value_operate(AssetValue.asset_value_score,$asset_value_score)) {
              ~LifeCycle.asset_value{
                data_set_filter_list_asset_value as LifeCycle.target @filter(uid(%s))
              }
            }
        """
            % filter_input
        )
        filter_input = 'data_set_filter_list_asset_value'
        lifecycle_uids_name_asset_value, lifecycle_cond_asset_value = lifecycle_uids_convert(
            query_statement, params.get('data_set_type'), 'asset_value'
        )
    # 按照收益比过滤
    if assetvalue_to_cost_operate is not None and assetvalue_to_cost is not None:
        query_statement += (
            """
            var(func:uid($lifecycle_uids_name_assetvalue_to_cost))
            @filter($assetvalue_to_cost_operate(LifeCycle.assetvalue_to_cost,$assetvalue_to_cost)) {
                data_set_filter_list_assetvalue_to_cost as LifeCycle.target @filter(uid(%s))
            }
        """
            % filter_input
        )
        filter_input = 'data_set_filter_list_assetvalue_to_cost'
        lifecycle_uids_name_assetvalue_to_cost, lifecycle_cond_assetvalue_to_cost = lifecycle_uids_convert(
            query_statement, params.get('data_set_type'), 'assetvalue_to_cost'
        )
    # 按价值评分过滤
    if storage_capacity_operate is not None and storage_capacity is not None:
        query_statement += (
            """
            var(func:uid($lifecycle_uids_name_storage_capacity))
            @filter($storage_capacity_operate(StorageCapacity.total_capacity,$storage_capacity)) {
              ~Cost.capacity {
                ~LifeCycle.cost{
                  data_set_filter_list_storage_capacity as LifeCycle.target @filter(uid(%s))
                }
              }
            }
        """
            % filter_input
        )
        filter_input = 'data_set_filter_list_storage_capacity'
        lifecycle_uids_name_storage_capacity, lifecycle_cond_storage_capacity = lifecycle_uids_convert(
            query_statement, params.get('data_set_type'), 'storage_capacity'
        )

    if 'data_set_filter_list_storage_capacity' in query_statement:
        query_statement = query_statement.replace('data_set_filter_list_storage_capacity as', 'data_set_filter_list as')
    elif 'data_set_filter_list_assetvalue_to_cost' in query_statement:
        query_statement = query_statement.replace(
            'data_set_filter_list_assetvalue_to_cost as', 'data_set_filter_list as'
        )
    elif 'data_set_filter_list_asset_value' in query_statement:
        query_statement = query_statement.replace('data_set_filter_list_asset_value as', 'data_set_filter_list as')
    elif 'data_set_filter_list_importance' in query_statement:
        query_statement = query_statement.replace('data_set_filter_list_importance as', 'data_set_filter_list as')
    elif 'data_set_filter_list_heat' in query_statement:
        query_statement = query_statement.replace('data_set_filter_list_heat as', 'data_set_filter_list as')
    elif 'data_set_filter_list_range' in query_statement:
        query_statement = query_statement.replace('data_set_filter_list_range as', 'data_set_filter_list as')

    if lifecycle_uids_name:
        query_statement = query_statement.replace('$lifecycle_uids_name', lifecycle_uids_name)
    elif (not lifecycle_uids_name) and lifecycle_cond:
        query_statement = query_statement.replace('uid($lifecycle_uids_name)', lifecycle_cond)

    if lifecycle_uids_name_range:
        query_statement = query_statement.replace('$lifecycle_uids_name_range', lifecycle_uids_name_range)
    elif (not lifecycle_uids_name_range) and lifecycle_cond_range:
        query_statement = query_statement.replace('uid($lifecycle_uids_name_range)', lifecycle_cond_range)

    if lifecycle_uids_name_heat:
        query_statement = query_statement.replace('$lifecycle_uids_name_heat', lifecycle_uids_name_heat)
    elif (not lifecycle_uids_name_heat) and lifecycle_cond_heat:
        query_statement = query_statement.replace('uid($lifecycle_uids_name_heat)', lifecycle_cond_heat)

    if lifecycle_uids_name_importance:
        query_statement = query_statement.replace('$lifecycle_uids_name_importance', lifecycle_uids_name_importance)
    elif (not lifecycle_uids_name_importance) and lifecycle_cond_importance:
        query_statement = query_statement.replace('uid($lifecycle_uids_name_importance)', lifecycle_cond_importance)

    if lifecycle_uids_name_asset_value:
        query_statement = query_statement.replace('$lifecycle_uids_name_asset_value', lifecycle_uids_name_asset_value)
    elif (not lifecycle_uids_name_asset_value) and lifecycle_cond_asset_value:
        query_statement = query_statement.replace('uid($lifecycle_uids_name_asset_value)', lifecycle_cond_asset_value)

    if lifecycle_uids_name_assetvalue_to_cost:
        query_statement = query_statement.replace(
            '$lifecycle_uids_name_assetvalue_to_cost', lifecycle_uids_name_assetvalue_to_cost
        )
    elif (not lifecycle_uids_name_assetvalue_to_cost) and lifecycle_cond_assetvalue_to_cost:
        query_statement = query_statement.replace(
            'uid($lifecycle_uids_name_assetvalue_to_cost)', lifecycle_cond_assetvalue_to_cost
        )

    if lifecycle_uids_name_storage_capacity:
        query_statement = query_statement.replace(
            '$lifecycle_uids_name_storage_capacity', lifecycle_uids_name_storage_capacity
        )
    elif (not lifecycle_uids_name_storage_capacity) and lifecycle_cond_storage_capacity:
        query_statement = query_statement.replace(
            'uid($lifecycle_uids_name_storage_capacity)', lifecycle_cond_storage_capacity
        )

    final_filter_uids_name = 'data_set_filter_list'
    query_statement = query_statement.replace('$final_filter_uids_name', final_entites_name)
    if range_operate is not None and range_score is not None:
        query_statement = query_statement.replace('$range_operate', range_operate)
        query_statement = query_statement.replace('$range_score', str(range_score))
    if heat_operate is not None and heat_score is not None:
        query_statement = query_statement.replace('$heat_operate', heat_operate)
        query_statement = query_statement.replace('$heat_score', str(heat_score))
    if importance_operate is not None and importance_score is not None:
        query_statement = query_statement.replace('$importance_operate', importance_operate)
        query_statement = query_statement.replace('$importance_score', str(importance_score))
    if asset_value_operate is not None and asset_value_score is not None:
        query_statement = query_statement.replace('$asset_value_operate', asset_value_operate)
        query_statement = query_statement.replace('$asset_value_score', str(asset_value_score))
    if assetvalue_to_cost_operate is not None and assetvalue_to_cost is not None:
        query_statement = query_statement.replace('$assetvalue_to_cost_operate', assetvalue_to_cost_operate)
        query_statement = query_statement.replace('$assetvalue_to_cost', str(assetvalue_to_cost))
    if storage_capacity_operate is not None and storage_capacity is not None:
        query_statement = query_statement.replace('$storage_capacity_operate', storage_capacity_operate)
        query_statement = query_statement.replace('$storage_capacity', str(storage_capacity))
    return query_statement, final_filter_uids_name


def lifecycle_order_format(
    order_range,
    order_heat,
    order_assetvalue_to_cost,
    order_importance,
    order_asset_value,
    order_storage_capacity,
    query_statement,
    data_set_type,
):
    lifecycle_uids_name, lifecycle_cond = '', ''
    if (
        order_range == 'asc'
        or order_heat == 'asc'
        or order_assetvalue_to_cost == 'asc'
        or order_importance == 'asc'
        or order_asset_value == 'asc'
        or order_storage_capacity == 'asc'
    ):
        query_statement = query_statement.replace('$asc_or_desc', 'orderasc')
    elif (
        order_range == 'desc'
        or order_heat == 'desc'
        or order_assetvalue_to_cost == 'desc'
        or order_importance == 'desc'
        or order_asset_value == 'desc'
        or order_storage_capacity == 'desc'
    ):
        query_statement = query_statement.replace('$asc_or_desc', 'orderdesc')

    lifecycle_uids_name, lifecycle_cond = '', ''
    if order_range == 'asc' or order_range == 'desc':
        query_statement = query_statement.replace('$score', 'Range.normalized_range_score')
        query_statement = query_statement.replace('$metric', '~LifeCycle.range')
        lifecycle_uids_name, lifecycle_cond = lifecycle_uids_convert(query_statement, data_set_type, 'range')
    elif order_heat == 'asc' or order_heat == 'desc':
        query_statement = query_statement.replace('$score', 'Heat.heat_score')
        query_statement = query_statement.replace('$metric', '~LifeCycle.heat')
        lifecycle_uids_name, lifecycle_cond = lifecycle_uids_convert(query_statement, data_set_type, 'heat')
    elif order_assetvalue_to_cost == 'asc' or order_assetvalue_to_cost == 'desc':
        query_statement = query_statement.replace('$score', 'LifeCycle.assetvalue_to_cost')
        lifecycle_uids_name, lifecycle_cond = lifecycle_uids_convert(
            query_statement, data_set_type, 'assetvalue_to_cost'
        )
    elif order_importance == 'asc' or order_importance == 'desc':
        query_statement = query_statement.replace('$score', 'Importance.importance_score')
        query_statement = query_statement.replace('$metric', '~LifeCycle.importance')
        lifecycle_uids_name, lifecycle_cond = lifecycle_uids_convert(query_statement, data_set_type, 'importance')
    elif order_asset_value == 'asc' or order_asset_value == 'desc':
        query_statement = query_statement.replace('$score', 'AssetValue.asset_value_score')
        query_statement = query_statement.replace('$metric', '~LifeCycle.asset_value')
        lifecycle_uids_name, lifecycle_cond = lifecycle_uids_convert(query_statement, data_set_type, 'asset_value')
    elif order_storage_capacity == 'asc' or order_storage_capacity == 'desc':
        query_statement = query_statement.replace('$score', 'StorageCapacity.total_capacity')
        lifecycle_uids_name, lifecycle_cond = lifecycle_uids_convert(query_statement, data_set_type, 'storage_capacity')

    if lifecycle_uids_name:
        query_statement = query_statement.replace('$lifecycle_uids_name', lifecycle_uids_name)
    elif (not lifecycle_uids_name) and lifecycle_cond:
        query_statement = query_statement.replace('uid($lifecycle_uids_name)', lifecycle_cond)
    return query_statement


def is_filter_by_lifecycle(
    range_operate,
    range_score,
    heat_operate,
    heat_score,
    importance_operate,
    importance_score,
    asset_value_operate,
    asset_value_score,
    assetvalue_to_cost_operate,
    assetvalue_to_cost,
    storage_capacity_operate,
    storage_capacity,
):
    # 判断是否按照生命周期过滤
    return (
        (range_operate is not None and range_score is not None)
        or (heat_operate is not None and heat_score is not None)
        or (importance_operate is not None and importance_score is not None)
        or (asset_value_operate is not None and asset_value_score is not None)
        or (assetvalue_to_cost_operate is not None and assetvalue_to_cost is not None)
        or (storage_capacity_operate is not None and storage_capacity is not None)
    )


def get_lifecycle_type(
    order_range,
    order_heat,
    range_operate,
    range_score,
    heat_operate,
    heat_score,
    importance_operate,
    importance_score,
    asset_value_operate,
    asset_value_score,
    assetvalue_to_cost_operate,
    assetvalue_to_cost,
    storage_capacity_operate,
    storage_capacity,
    order_assetvalue_to_cost=None,
    order_importance=None,
    order_asset_value=None,
    order_storage_capacity=None,
):
    # 判断是否按照生命周期过滤，已经过滤属性
    lifecycle_type = []
    if order_range or (range_operate is not None and range_score is not None):
        if 'range' not in lifecycle_type:
            lifecycle_type.append('range')
    if order_heat or (heat_operate is not None and heat_score is not None):
        if 'heat' not in lifecycle_type:
            lifecycle_type.append('heat')
    if order_assetvalue_to_cost or (assetvalue_to_cost_operate is not None and assetvalue_to_cost is not None):
        if 'assetvalue_to_cost' not in lifecycle_type:
            lifecycle_type.append('assetvalue_to_cost')
    if order_importance or (importance_operate is not None and importance_score is not None):
        if 'importance' not in lifecycle_type:
            lifecycle_type.append('importance')
    if order_asset_value or (asset_value_operate is not None and asset_value_score is not None):
        if 'asset_value' not in lifecycle_type:
            lifecycle_type.append('asset_value')
    if order_storage_capacity or (storage_capacity_operate is not None and storage_capacity is not None):
        if 'storage_capacity' not in lifecycle_type:
            lifecycle_type.append('storage_capacity')

    return lifecycle_type


def filter_dataset_by_storage_type(query_statement, params, final_entites_name, storage_type=None):
    # 按照存储类型过滤
    query_statement += """
        var(func:eq(StorageClusterConfig.cluster_type,$storage_type))  {
          ~StorageResultTable.storage_cluster {
            data_set_filter_list_storage_type as StorageResultTable.result_table @filter(uid($final_filter_uids_name))
          }
        }
    """
    query_statement = query_statement.replace('$storage_type', storage_type)
    query_statement = query_statement.replace('$final_filter_uids_name', final_entites_name)
    final_filter_uids_name = 'data_set_filter_list_storage_type'
    return query_statement, final_filter_uids_name
