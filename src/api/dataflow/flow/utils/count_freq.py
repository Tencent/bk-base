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

from django.utils.translation import ugettext as _


class CountFreq(object):
    def __init__(self, count_freq, schedule_period):
        self.count_freq = count_freq
        self.schedule_period = schedule_period

    def __str__(self):
        if self.schedule_period == "day":
            return _("%s天") % self.count_freq
        elif self.schedule_period == "week":
            return _("%s周") % self.count_freq
        elif self.schedule_period == "month":
            return _("%s月") % self.count_freq
        else:
            return _("%s小时") % self.count_freq

    def __ne__(self, other):
        """
        !=
        @return:
        """
        if self.schedule_period == "month" and other.schedule_period == "month":
            return self.count_freq != other.count_freq
        else:
            # 统一转换为小时
            return self.hours != other.hours

    def __gt__(self, other):
        """
        >
        @param other:
        @return:
        """
        if self.schedule_period == "month" and other.schedule_period == "month":
            return self.count_freq > other.count_freq
        else:
            # 统一转换为小时
            return self.hours > other.hours

    @property
    def hours(self):
        if self.schedule_period == "hour":
            return self.count_freq
        elif self.schedule_period == "day":
            return self.count_freq * 24
        elif self.schedule_period == "week":
            return self.count_freq * 24 * 7
        elif self.schedule_period == "month":
            return self.count_freq * 24 * 7 * 28
        raise Exception("Schedule period for hour is not support for calculate %s" % self.schedule_period)
