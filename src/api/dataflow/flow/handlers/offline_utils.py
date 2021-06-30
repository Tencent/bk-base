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

from django.utils.translation import ugettext as _

from dataflow.flow.api_models import ResultTable
from dataflow.flow.exceptions import NodeError
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.node_types import NodeTypes
from dataflow.shared.log import flow_logger as logger


def valid_start_time(origin_start_time, form_data, is_create):
    """
    校验离线节点启动时间
    @param origin_start_time:
    @param form_data:
    @param is_create:
    @return:
    """
    start_time = None
    if "node_type" in form_data and form_data["node_type"] == "batchv2":
        if "start_time" in form_data["dedicated_config"]["schedule_config"]:
            start_time = form_data["dedicated_config"]["schedule_config"]["start_time"]
    else:
        if form_data["advanced"] and "start_time" in form_data["advanced"] and form_data["advanced"]["start_time"]:
            start_time = form_data["advanced"]["start_time"]
    # 创建操作或在更新操作时将原本有值的 start_time 修改为非空值，start_time 应大于当前时间
    if start_time and (is_create or origin_start_time != start_time):
        time_array = time.strptime(start_time, "%Y-%m-%d %H:%M")
        _start_time = int(time.mktime(time_array))
        if _start_time <= int(round(time.time())):
            raise NodeError(_("高级配置启动时间应大于当前时间"))


def validate_batch_count_freq_and_delay(cur_freq, from_node_handlers):
    """
    校验(离线/tdw)节点上游的统计频率（count_freq）和延迟（delay）配置
    @param from_node_handlers 节点列表
    """
    # 过滤取出离线型节点
    _from_node_handlers = list(
        filter(
            lambda _n: _n.node_type in NodeTypes.BATCH_CATEGORY + NodeTypes.BATCH_SOURCE_CATEGORY,
            from_node_handlers,
        )
    )
    # 上游离线 / TDW 节点列表
    # 1. 过滤获取上游 batch 类型计算节点（batch, tdw, tdw_jar）
    # 2. 对于 batch_source 节点，则查找 rt 对应的源节点（生成该rt的节点）
    # 3. 对于 tdw_source 节点，如果该 rt 的 is_managed 为 true ，则查找该 rt 对应的源节点（生成该rt的节点）
    #    如果该 rt 的 is_managed 为 false，则统计频率通过 rt.get_count_freq()
    batch_nodes = []
    # 父节点统计频率列表
    count_freqs = []

    # 离线/TDW节点的起始节点列表
    batch_origin_nodes = []

    for one_from_node_handler in _from_node_handlers:
        if one_from_node_handler.node_type in NodeTypes.BATCH_CATEGORY:
            batch_nodes.append(one_from_node_handler)
            count_freqs.append(one_from_node_handler.count_freq)

            _origin_node = NodeUtils.get_origin_batch_node(one_from_node_handler)
            batch_origin_nodes.append(
                {
                    "result_table_id": one_from_node_handler.result_table_ids[0],  # 当前这个值仅用于构造界面提示信息
                    "origin_result_table_id": _origin_node.result_table_ids[0],  # 当前这个值仅用于构造界面提示信息
                    "origin_delay": _origin_node.delay_time,
                }
            )

        elif one_from_node_handler.node_type in NodeTypes.BATCH_SOURCE_CATEGORY:
            if one_from_node_handler.node_type == NodeTypes.TDW_SOURCE:
                # tdw数据源
                rt = ResultTable(one_from_node_handler.result_table_id)
                if rt.is_managed:
                    _origin_node = one_from_node_handler.get_rt_generated_node(NodeTypes.BATCH_CATEGORY)
                    if _origin_node is not None:
                        batch_nodes.append(_origin_node)
                        count_freqs.append(_origin_node.count_freq)
                else:
                    count_freqs.append(rt.get_count_freq_in_hour())
            elif one_from_node_handler.node_type == NodeTypes.BATCH_SOURCE:
                # 普通离线数据源
                _origin_node = one_from_node_handler.get_rt_generated_node(NodeTypes.BATCH_CATEGORY)
                if _origin_node is not None:
                    batch_nodes.append(_origin_node)
                    count_freqs.append(_origin_node.count_freq)

    # 处理count_freq校验
    from_freq = None
    for one_count_freq in count_freqs:
        if from_freq is None:
            from_freq = one_count_freq
        if one_count_freq != from_freq:
            raise NodeError(same_freq_err_msg(batch_nodes))

    logger.info(_("【节点校验】nodes:%s") % batch_nodes)
    logger.info(
        _("【节点校验】from_freq:%(from_freq)s, cur_freq:%(cur_freq)s")
        % {"cur_freq": str(cur_freq), "from_freq": str(from_freq)}
    )
    try:
        # 不支持的的连线
        # 周 -> 月
        err_msg = (_("当前节点的统计频率必须大于或者等于上游节点的统计频率，" "当前节点频率=%(cur_freq)s，上游节点频率=%(from_freq)s")) % {
            "cur_freq": str(cur_freq),
            "from_freq": str(from_freq),
        }
        if from_freq is not None:
            if from_freq > cur_freq:
                raise NodeError(err_msg)
            elif from_freq.schedule_period == "week" and cur_freq.schedule_period == "month":
                raise NodeError(_("暂不支持周任务连接至月任务"))
    except Exception as e:
        logger.exception(e)
        raise NodeError(e)

    # 如果离线父节点的数量 < 2，不需要做以下delay校验
    if len(batch_nodes) < 2:
        return

    # 处理delay校验
    from_delay = None
    for _origin in batch_origin_nodes:
        if from_delay is None:
            from_delay = _origin["origin_delay"]
        if from_delay != _origin["origin_delay"]:
            raise NodeError(same_delay_err_msg(batch_origin_nodes))


# def valid_with_from_nodes(cur_freq, batch_nodes):
#     """
#     校验节点配置
#
#     @param {int} cur_freq 当前统计频率
#     @param {BaseNode[]} batch_nodes 父节点列表
#     """
#     # 离线表s所有父表需要保证频率一致，子表频率必须大于或者等于父表
#     from_freq = None
#
#     for _node in batch_nodes:
#         _freq = _node.count_freq
#         if from_freq is None:
#             from_freq = _freq
#         if _freq != from_freq:
#             raise NodeError(same_freq_err_msg(batch_nodes))
#
#     logger.info(_(u"【节点校验】nodes:%s") % batch_nodes)
#     logger.info(_(u"【节点校验】from_freq:%(from_freq)s, cur_freq:%(cur_freq)s") % {
#         'cur_freq': str(cur_freq),
#         'from_freq': str(from_freq)
#     })
#     try:
#         # 不支持的的连线
#         # 周 -> 月
#         err_msg = (
#                       _(u"当前节点的统计频率必须大于或者等于上游节点的统计频率，"
#                         u"当前节点频率=%(cur_freq)s，上游节点频率=%(from_freq)s")
#                   ) % {
#                       'cur_freq': str(cur_freq),
#                       'from_freq': str(from_freq)
#                   }
#         if from_freq is not None:
#             if from_freq > cur_freq:
#                 raise NodeError(err_msg)
#             elif from_freq.schedule_period == 'week' and cur_freq.schedule_period == 'month':
#                 raise NodeError(_(u'暂不支持周任务连接至月任务'))
#     except Exception as e:
#         logger.exception(e)
#         raise NodeError(e.message)
#
#     # 如果离线父节点的数量 < 2，不需要做以下校验
#     if len(batch_nodes) < 2:
#         return
#
#         # 离线表，父表，子表的延迟时间保持一致
#     # TODO: 计算节点需由APP传入父RT
#     batch_origins = []
#     for _node in batch_nodes:
#         _origin_node = NodeUtils.get_origin_batch_node(_node)
#         batch_origins.append({
#             'result_table_id': _node.result_table_ids[0],  # 当前这个值仅用于构造界面提示信息
#             'origin_result_table_id': _origin_node.result_table_ids[0],  # 当前这个值仅用于构造界面提示信息
#             'origin_delay': _origin_node.delay_time
#         })
#
#     from_delay = None
#     for _origin in batch_origins:
#         if from_delay is None:
#             from_delay = _origin['origin_delay']
#         if from_delay != _origin['origin_delay']:
#             raise NodeError(same_delay_err_msg(batch_origins))


def same_delay_err_msg(offline_origins):
    err_msg = _("上游节点结果表延迟时间需要保持一致\n")
    for _origin in offline_origins:
        err_msg += _("\n>>> %(result_table_id)s\n根结果表%(origin_result_table_id)s，延迟%(origin_delay)s小时\n") % {
            "result_table_id": _origin["result_table_id"],
            "origin_result_table_id": _origin["origin_result_table_id"],
            "origin_delay": _origin["origin_delay"],
        }
    return err_msg


def same_freq_err_msg(offline_nodes):
    err_msg = _("上游节点结果表的统计频率需要保证一致")
    for _node in offline_nodes:
        _config = _node.get_config(False)
        _freq_msg = visual_freq(_config["count_freq"], _config["schedule_period"])
        err_msg += "\n{}: {}".format(",".join(_node.result_table_ids), _freq_msg)
    return err_msg


# def visual_freq(freq):
#     day_seconds = 24 * 3600
#     hour_seconds = 3600
#     if freq > day_seconds:
#         return _(u"%s天") % (freq / day_seconds)
#     else:
#         return _(u"%s小时") % (freq / hour_seconds)


def visual_freq(freq_value, freq_unit):
    if freq_unit == "day":
        return _("%s天") % freq_value
    elif freq_unit == "week":
        return _("%s周") % freq_value
    elif freq_unit == "month":
        return _("%s月") % freq_value
    else:
        return _("%s小时") % freq_value
