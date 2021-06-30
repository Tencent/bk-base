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

from dataflow.flow.models import TaskLogFilterRule


class _TaskLogBlacklistHandler(object):
    blacklist = []

    def __init__(self):
        self.blacklist = self.get_blacklist()

    def get_blacklist(self, group_name="default", force_flag=False):
        """
        获取黑名单列表
        :param group_name: group_name
        :param force_flag: 是否强制从db获取最新的数据
        :return:
        """
        if not self.blacklist or force_flag:
            queryset = TaskLogFilterRule.objects.filter(group_name=group_name)
            self.blacklist = [str(p.blacklist) for p in queryset]
        return self.blacklist

    @staticmethod
    def add_blacklist(blacklist_word, group_name="default"):
        """
        添加黑名单，如果存在则不影响（即db中不存重复的值）
        :param blacklist_word: blacklist_word 多个词以英文分号分隔，如果关键词包含英文分号，只能单个添加
        :param group_name: group_name
        :return:
        """
        all_words = [x for x in blacklist_word.split(",") if x]
        if len(all_words) == 1 and ";" in blacklist_word:
            # add just the origin word including the delimiter
            all_words.clear()
            all_words.append(blacklist_word)
        for one_word in all_words:
            TaskLogFilterRule.objects.update_or_create(group_name=group_name, blacklist=one_word)

    @staticmethod
    def delete_blacklist(blacklist_word, group_name="default"):
        """
        删除黑名单词，删除单个词
        :param blacklist_word: blacklist_word（单个词）
        :param group_name: group_name
        :return:
        """
        TaskLogFilterRule.objects.filter(group_name=group_name, blacklist=blacklist_word).delete()


TaskLogBlacklistHandler = _TaskLogBlacklistHandler()
