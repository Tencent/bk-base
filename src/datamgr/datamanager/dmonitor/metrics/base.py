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
import copy
import uuid

from dmonitor.exceptions import MetricMessageNotCorrectError
from utils.time import timetostr


class MetricEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, BaseMetric):
            return obj.as_message()
        return json.JSONEncoder.default(self, obj)


class Metric(object):
    """
    具体的某一个指标
    职责： 快速查询这个指标某时间，某维度的时序value
    """

    def __init__(self, database, measurement, retention_policy="autogen", **kwargs):
        super(Metric, self).__init__()
        self._database = database
        self._retention_policy = retention_policy
        self._measurement = measurement
        self._producer = False
        self._err_message = ""
        self._err_code = "00"

    def get_errcode(self):
        return self._err_code

    def get_err_msg(self):
        return self._err_message

    @property
    def db(self):
        return self._database

    @db.setter
    def db(self, database):
        self._database = database
        return self

    @property
    def measurement(self):
        return self._measurement

    @measurement.setter
    def measurement(self, measurement):
        self._measurement = measurement
        return self

    @property
    def retention_policy(self):
        return self._retention_policy

    @retention_policy.setter
    def retention_policy(self, retention_policy):
        self._retention_policy = retention_policy


class BaseMetric(Metric):
    @classmethod
    def from_message(cls, message):
        if cls.MEASUREMENT not in message:
            raise MetricMessageNotCorrectError(
                u"Message with data(%s) does't have correct format for measurement(%s)"
                % (json.dumps(message), cls.MEASUREMENT)
            )
        message_data = message
        try:
            message_data["timestamp"] = message_data.pop("time")
            message_info = message_data.pop(cls.MEASUREMENT, {})
            message_data.update(message_info)
            return cls(**message_data)
        except Exception:
            import traceback

            traceback.print_exc()
            raise MetricMessageNotCorrectError(
                u"Message with data(%s) does't have correct format for measurement(%s)"
                % (json.dumps(message), cls.MEASUREMENT)
            )

    def __init__(self, timestamp, database, measurement, metrics, tags=None):
        super(BaseMetric, self).__init__(database, measurement)
        self._timestamp = timestamp
        self._metrics = metrics or {}
        self._tags = tags or {}

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def time_str(self):
        return timetostr(self._timestamp)

    @property
    def tags(self):
        return self._tags

    @property
    def metrics(self):
        return self._metrics

    def set_metric(self, key, value):
        self._metrics[key] = value

    def get_metric(self, key, default_value=None):
        return self._metrics.get(key, default_value)

    def update_tags(self, new_tags):
        self._tags.update(new_tags)

    def set_tag(self, tag, value):
        self._tags[tag] = value

    def get_tag(self, tag, default_value=None):
        return self._tags.get(tag, default_value)

    def as_dict(self):
        obj = {
            "time": self._timestamp,
            "database": self._database,
            self.MEASUREMENT: copy.copy(self._metrics),
        }
        obj[self.MEASUREMENT].update({"tags": self._tags})
        return obj

    def as_message(self):
        return json.dumps(self.as_dict())

    def __gt__(self, other):
        return self.timestamp > other.timestamp

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def __ge__(self, other):
        return self.timestamp >= other.timestamp

    def __le__(self, other):
        return self.timestamp <= other.timestamp

    def __eq__(self, other):
        return self.timestamp == other.timestamp


class DataLossOutputTotal(BaseMetric):
    MEASUREMENT = "data_loss_output_total"

    def __init__(
        self,
        timestamp,
        database,
        data_cnt,
        data_inc,
        ckp_drop_cnt=0,
        tags=None,
        **kwargs
    ):
        super(DataLossOutputTotal, self).__init__(
            timestamp=timestamp,
            database=database,
            measurement=self.MEASUREMENT,
            metrics={
                "data_cnt": data_cnt,
                "data_inc": data_inc,
                "ckp_drop_cnt": ckp_drop_cnt,
            },
            tags=tags,
        )


class DataLossInputTotal(BaseMetric):
    MEASUREMENT = "data_loss_input_total"

    def __init__(self, timestamp, database, data_cnt, data_inc, tags=None, **kwargs):
        super(DataLossInputTotal, self).__init__(
            timestamp=timestamp,
            database=database,
            measurement=self.MEASUREMENT,
            metrics={"data_cnt": data_cnt, "data_inc": data_inc},
            tags=tags,
        )


class DataLossAudit(BaseMetric):
    MEASUREMENT = "data_loss_audit"

    def __init__(
        self,
        timestamp,
        database,
        output_cnt,
        consume_cnt,
        loss_cnt,
        tags=None,
        **kwargs
    ):
        super(DataLossAudit, self).__init__(
            timestamp=timestamp,
            database=database,
            measurement=self.MEASUREMENT,
            metrics={
                "output_cnt": output_cnt,
                "consume_cnt": consume_cnt,
                "loss_cnt": loss_cnt,
            },
            tags=tags,
        )


class DataRelativeDelay(BaseMetric):
    MEASUREMENT = "data_relative_delay"

    def __init__(
        self,
        timestamp,
        database,
        ab_delay,
        window_time,
        waiting_time,
        relative_delay,
        fore_delay,
        tags=None,
        **kwargs
    ):
        super(DataRelativeDelay, self).__init__(
            timestamp=timestamp,
            database=database,
            measurement=self.MEASUREMENT,
            metrics={
                "ab_delay": ab_delay,
                "window_time": window_time,
                "waiting_time": waiting_time,
                "relative_delay": relative_delay,
                "fore_delay": fore_delay,
            },
            tags=tags,
        )


class DataDelayMax(BaseMetric):
    MEASUREMENT = "data_delay_max"

    def __init__(
        self,
        timestamp,
        database,
        waiting_time,
        data_time,
        delay_time,
        output_time,
        tags=None,
        **kwargs
    ):
        super(DataDelayMax, self).__init__(
            timestamp=timestamp,
            database=database,
            measurement=self.MEASUREMENT,
            metrics={
                "waiting_time": waiting_time,
                "data_time": data_time,
                "delay_time": delay_time,
                "output_time": output_time,
            },
            tags=tags,
        )


class DataLossDropRate(BaseMetric):
    MEASUREMENT = "data_loss_drop_rate"

    def __init__(
        self, timestamp, database, drop_cnt, drop_rate, message_cnt, tags=None, **kwargs
    ):
        super(DataLossDropRate, self).__init__(
            timestamp=timestamp,
            database=database,
            measurement=self.MEASUREMENT,
            metrics={
                "drop_cnt": drop_cnt,
                "drop_rate": drop_rate,
                "message_cnt": message_cnt,
            },
            tags=tags,
        )


class DmonitorAlerts(BaseMetric):
    MEASUREMENT = "dmonitor_alerts"

    def __init__(
        self,
        timestamp,
        database,
        message,
        message_en,
        full_message,
        full_message_en,
        alert_status,
        recover_time=0,
        tags=None,
        **kwargs
    ):
        super(DmonitorAlerts, self).__init__(
            timestamp=timestamp,
            database=database,
            measurement=self.MEASUREMENT,
            metrics={
                "message": message,
                "message_en": message_en,
                "full_message": full_message,
                "full_message_en": full_message_en,
                "alert_status": alert_status,
                "recover_time": recover_time,
            },
            tags=tags,
        )
        self.alert_id = uuid.uuid4().hex


class KafkaTopicMessageCnt(BaseMetric):
    MEASUREMENT = "kafka_topic_message_cnt"

    def __init__(self, timestamp, database, cnt, msg_inc_cnt, tags=None, **kwargs):
        super(KafkaTopicMessageCnt, self).__init__(
            timestamp=timestamp,
            database=database,
            measurement=self.MEASUREMENT,
            metrics={"cnt": cnt, "msg_inc_cnt": msg_inc_cnt},
            tags=tags,
        )
