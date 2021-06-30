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

import ahocorasick
from conf.dataapi_settings import DP_CC_APP_ID

from dataflow.flow.handlers.tasklog_blacklist import TaskLogBlacklistHandler
from dataflow.shared.cc.cc_helper import CCHelper
from dataflow.shared.log import flow_logger as logger


def get_host_list():
    """
    get host list from cc (tgdp's cc app_id 591)
    :return: ip list
    """
    res = CCHelper.get_host_by_app_id({"app_id": DP_CC_APP_ID})
    ret = []
    for one_host in res:
        inner_ip = one_host["InnerIP"]
        outer_ip = one_host["OuterIP"]
        if inner_ip:
            ret.append(str(inner_ip))
        if outer_ip:
            ret.append(str(outer_ip))
    logger.info("get_host_list return ({}) host from cc for app_id({})".format(len(ret), DP_CC_APP_ID))
    return ret


class MaskInfoUtil(object):
    ac_automaton = None
    last_update_time = 0

    @classmethod
    def mask_secret_info(cls, log_content_line):
        """
        对日志文本内容掩盖敏感信息
        1. 对于命中的ip信息，目前基于ip list中的ipv4过滤，掩盖ipv4的前3段地址信息为"*.*.*"
        2. 对于命中黑名单的普通文本信息，则替换成等长度的'*'
        如果没有命中黑名单中的信息，则原样返回
        :param log_content_line: 日志文本内容
        :return: 掩盖敏感信息后的日志文本信息
        """
        now_secs = int(round(time.time()))

        # 每60秒更新敏感机器ip列表和敏感词列表
        if cls.last_update_time + 60 < now_secs:
            new_ac_automaton = ahocorasick.Automaton()

            host_list = get_host_list()
            # 将所有ip列表构建ac自动机
            for word in host_list:
                if word:
                    ips = word.split(",")
                    for one_ip in ips:
                        if one_ip:
                            new_ac_automaton.add_word(one_ip, (one_ip, "ip"))

            # 将所有黑名单text加入ac自动机
            blacklist_words = TaskLogBlacklistHandler.get_blacklist(force_flag=True)
            for one_word in blacklist_words:
                if one_word:
                    new_ac_automaton.add_word(one_word, (one_word, "text"))

            new_ac_automaton.make_automaton()
            # cls.ac_automaton.clear()
            cls.ac_automaton = new_ac_automaton
            cls.last_update_time = now_secs

        if len(cls.ac_automaton) > 0:
            result = []
            last_index = 0
            for item in cls.ac_automaton.iter(str(log_content_line)):
                if item[0] > last_index:
                    result.append(log_content_line[last_index : item[0] - len(item[1][0]) + 1])
                    if item[1][1] == "ip":
                        replace_str = "*.*.*." + item[1][0][item[1][0].rfind(".") + 1]
                    elif item[1][1] == "text":
                        replace_str = "".join(["*" for i in range(len(item[1][0]))])
                    else:
                        # TODO: further type of replace string
                        replace_str = "***"
                    result.append(replace_str)
                    last_index = item[0] + 1
            result.append(log_content_line[last_index:])
            mask_str = "".join(a for a in result)
            return mask_str
        else:
            # do nothing, just return the origin content
            return log_content_line
