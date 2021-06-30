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
import os
import time

import gevent
from gevent import Greenlet
from sqlalchemy.orm.exc import ObjectDeletedError

from api import datamanage_api
from common.kafka import clusters
from dataquality import session
from dataquality.audit.context import AuditTaskContext
from dataquality.event.manager import EventManager
from dataquality.exceptions import StartAuditTaskTimeoutError
from dataquality.metric.manager import MetricManager
from dataquality.model.base import AuditTaskStatus, EventLevel, InputType
from dataquality.model.core import AuditTask
from dataquality.rule.functions.manager import FunctionManager
from dataquality.rule.rules import ResultTableRule
from dataquality.settings import (
    AUDIT_TASK_START_TIMEOUT,
    AUDIT_TASK_WAITING_INTERVAL,
    BKDATA_BIZ_ID,
    EVENT_RAW_DATA_NAME,
    EVENT_REPORT_TOPIC,
    METRIC_REPORT_TOPIC,
)
from utils.rawdata_producer import RawDataProducer
from utils.tools import get_host_ip


class ResultTableAuditTaskGreenlet(Greenlet):
    def __init__(self, audit_task_config=None):
        super(ResultTableAuditTaskGreenlet, self).__init__()
        self._data_set_id = audit_task_config.get("data_set_id")
        self._rule_id = audit_task_config.get("rule_id")
        self._rule_config = json.loads(audit_task_config.get("rule_config") or "{}")
        self._rule = ResultTableRule(self._data_set_id, self._rule_config)

        self._metrics = MetricManager()
        self._events = EventManager()
        self._functions = FunctionManager()

        self.init_audit_task()
        self.init_schedule_timer()
        self.init_producer()

        logging.info(
            "Init the task for the audit rule({rule_id}) about dataset({data_set_id}) success.".format(
                rule_id=self._rule_id,
                data_set_id=self._data_set_id,
            )
        )

    def init_audit_task(self):
        start_time = time.time()
        while True:
            audit_task = (
                session.query(AuditTask)
                .filter(AuditTask.data_set_id == self._data_set_id)
                .filter(AuditTask.rule_id == self._rule_id)
                .first()
            )

            if audit_task is not None:
                break

            gevent.sleep(AUDIT_TASK_WAITING_INTERVAL)
            if time.time() - start_time > AUDIT_TASK_START_TIMEOUT:
                raise StartAuditTaskTimeoutError(
                    message_kv={
                        "data_set_id": self._data_set_id,
                        "rule_id": self._rule_id,
                    }
                )

        self._audit_task = audit_task
        self._audit_task.add_runtime("host", get_host_ip())
        self._audit_task.add_runtime("pid", os.getpid())
        session.commit()

    def init_schedule_timer(self):
        self._timer = self._rule.get_timer()

    def init_producer(self):
        self._raw_data_producer = RawDataProducer(BKDATA_BIZ_ID, EVENT_RAW_DATA_NAME)
        self._producer = clusters.get_producer("op", EVENT_REPORT_TOPIC)

    def refresh_task_status(self):
        session.refresh(self._audit_task)
        session.commit()

    def _run(self):
        self._audit_task.status = AuditTaskStatus.RUNNING.value
        session.commit()

        while True:
            try:
                seconds = self._timer.next()
                gevent.sleep(seconds)

                logging.info(
                    "Start to execute audit rule({rule_id}) about data_set({data_set_id})".format(
                        rule_id=self._rule_id,
                        data_set_id=self._data_set_id,
                    )
                )
                self._timer.tick()
                self.refresh_task_status()

                try:
                    if self._audit_task.status == AuditTaskStatus.WAITING.value:
                        logging.info(
                            "Stop audit rule({rule_id}) about data_set({data_set_id})".format(
                                rule_id=self._rule_id,
                                data_set_id=self._data_set_id,
                            )
                        )
                        break
                except ObjectDeletedError:
                    logging.info(
                        "Stop audit rule({rule_id}) about data_set({data_set_id})".format(
                            rule_id=self._rule_id,
                            data_set_id=self._data_set_id,
                        )
                    )
                    break

                context = AuditTaskContext(functions=self._functions)
                for rule_item in self._rule.get_rules():
                    input_context = self.prepare_input(rule_item.input)
                    input_context.update(context)
                    event_happening = self.execute(input_context, rule_item.rule)

                    if event_happening:
                        event = self.output_event(rule_item.output)
                        context.add_event(event.id, event)
                        logging.info(
                            "Generate event({event_id}) by rule({rule_id}) about data_set({data_set_id})".format(
                                event_id=event.id,
                                rule_id=self._rule_id,
                                data_set_id=self._data_set_id,
                            )
                        )

                logging.info(
                    "Finish to execute audit rule({rule_id}) about data_set({data_set_id})".format(
                        rule_id=self._rule_id,
                        data_set_id=self._data_set_id,
                    )
                )

                if self._timer.finished():
                    break
            except Exception as e:
                logging.error(
                    "Failed to execute audit task for the reason: {}".format(e),
                    exc_info=True,
                )
                self._audit_task.status = AuditTaskStatus.FAILED.value
                session.commit()
                break

    def prepare_input(self, input_configs):
        """
        Prepare the input metric and input event by the audit rule.
        """
        input_context = AuditTaskContext(functions=self._functions)

        prepare_tasks = []
        for input_config in input_configs:
            prepare_tasks.append(
                gevent.spawn(self.fetch_input_data, input_context, input_config)
            )
        gevent.joinall(prepare_tasks)

        return input_context

    def fetch_input_data(self, input_context, input_config):
        input_type = input_config.get("input_type")
        if input_type == InputType.EVENT.value:
            event_id = input_config.get("event_id")
            event = self._events.get_event(event_id).from_event_libraries()
            input_context.add_event(event_id, event)
        elif input_type == InputType.METRIC.value:
            metric_name = input_config.get("metric_name")
            dimensions = {
                "data_set_id": self._data_set_id,
            }
            if "metric_field" in input_config:
                dimensions["field"] = input_config["metric_field"]
            metric = self._metrics.get_metric(metric_name).generate_metric(dimensions)
            input_context.add_metric(metric_name, metric)

    def execute(self, input_context, rule_detail):
        return input_context.get_function_result(rule_detail.get("function", {}))

    def output_event(self, output_config):
        event_id = output_config.get("event_id")
        event = self._events.get_event(event_id).generate_event(
            event_time=time.time(),
            event_level=EventLevel.COMMON.value,
            event_status_alias=self._audit_task.rule_config_alias,
            platform="bkdata",
            generate_type="user",
            origin=self._rule.as_origin(),
            dimensions={
                "data_set_id": self._data_set_id,
                "rule_id": self._rule_id,
                "bk_biz_id": self._rule.bk_biz_id,
            },
        )
        event_message = event.as_message()
        self._raw_data_producer.produce_message(event_message)
        self.produce_message(event_message)
        try:
            datamanage_api.influx_report(
                {
                    "message": event.as_tsdb_data(),
                    "kafka_topic": METRIC_REPORT_TOPIC,
                }
            )
        except Exception as e:
            logging.error("Report event to tsdb error: {}".format(e), exc_info=True)
        return event

    def produce_message(self, value, key=None, partition=None, timeout=10):
        max_timeout = timeout
        while True:
            try:
                self._producer.produce(
                    message=bytes(value, encoding="utf-8"), partition_key=key
                )
                return True
            except Exception:
                max_timeout -= 0.1
                gevent.sleep(0.1)
            if max_timeout < 0:
                return False
        return True

    def flush(self):
        self.producer._wait_all()
