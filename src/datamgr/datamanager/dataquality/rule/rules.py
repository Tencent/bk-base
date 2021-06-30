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
import json
import logging

from dataquality.rule.base import Rule, RuleItem
from dataquality.rule.timer import TimerManager


class ResultTableRule(Rule):
    """
    The Audit Rule of the Data System ResultTable

    Attributes:
        _data_set_id: The Data System Result Table ID which the Rule related with.
    """

    def __init__(self, data_set_id, config):
        super(ResultTableRule, self).__init__(config)
        self._bk_biz_id = self.generate_biz(data_set_id)
        self._data_set_id = data_set_id

    @property
    def bk_biz_id(self):
        return self._bk_biz_id

    @property
    def data_set_id(self):
        return self._data_set_id

    def generate_biz(self, data_set_id):
        try:
            tokens = data_set_id.split("_")
            if len(tokens) > 0:
                return int(tokens[0])
            return None
        except Exception as e:
            logging.error(e, exc_info=True)
            return None

    def get_timer(self):
        timer = self._config.get("timer")
        timer_type = timer.get("timer_type")
        timer_config = timer.get("timer_config")
        return TimerManager.generate_timer(timer_type, timer_config)

    def get_rules(self):
        for rule_detail_config in self._config.get("rules", []):
            yield RuleItem(
                input=rule_detail_config.get("input", []),
                rule=rule_detail_config.get("rule", {}),
                output=rule_detail_config.get("output", {}),
            )

    def as_origin(self):
        return json.dumps(self._config)
