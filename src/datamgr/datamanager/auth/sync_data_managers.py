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
from __future__ import absolute_import, print_function, unicode_literals

import json
import logging
import time

import attr
import confluent_kafka

from api import meta_api
from auth.core.data_managers import DataManagerController
from auth.event import (
    AddManagersEvent,
    BaseManagersEvent,
    CreateDataProcessingRelationEvent,
    DeleteDataProcessingRelationEvent,
    DeleteManagersEvent,
    EventController,
    UpdateManagersEvent,
)
from auth.handlers.lineage import (
    DataNode,
    DataSetType,
    NodeManager,
    load_meta_lineage_graph,
    transform_to_graph_relation,
)
from common.db import connections
from common.redis import connections as redis_connections
from conf.settings import HAS_TDW_TABLE, KAFKA_ADDR, TIMEZONE
from utils.time import timestamp_to_arrow

logger = logging.getLogger(__name__)


class TransmitMetaRecordsTask(object):
    """
    元数据操作记录搬运任务，生成事件
    """

    def __init__(self):
        self.event_controller = EventController(redis_connections["default"])
        self.consumer = None

        self.init_consumer()

    def init_consumer(self):
        topics = ["transactional_operation_records"]
        config = {
            "bootstrap.servers": KAFKA_ADDR,
            "group.id": "auth_datamanager_module",
            "enable.auto.commit": True,
            "fetch.min.bytes": 10240,
            "fetch.max.bytes": 1024000,
            "enable.partition.eof": False,
        }

        c = confluent_kafka.Consumer(config)
        c.subscribe(topics)
        logger.info("Start to subscribe Topics({}), config({})".format(topics, config))

        self.consumer = c

    def main(self):
        while True:
            messages = self.consumer.consume(num_messages=5000, timeout=0)
            if len(messages) == 0:
                time.sleep(0.1)
                continue

            for msg in messages:
                logger.info(
                    "Fetch {}th message from partition({})".format(
                        msg.offset(), msg.partition()
                    )
                )
                raw_content = msg.value()
                try:
                    self.handler(raw_content)
                except Exception as err:
                    logger.info(
                        "Fail to transalate message({}), {}".format(raw_content, err)
                    )

    def handler(self, raw_content):
        content = json.loads(raw_content)

        if content["state"] != "Applied":
            return

        events = []
        for operation in content["operations"]:
            event = None
            change_time = operation["change_time"]
            identifier_value = operation["identifier_value"]
            if (
                operation["type_name"] == "DataProcessingRelation"
                and operation["method"] == "CREATE"
            ):
                changed_data = json.loads(operation["changed_data"])
                change_time = operation["change_time"]
                event = CreateDataProcessingRelationEvent(
                    id=identifier_value,
                    data_directing=changed_data["data_directing"],
                    data_set_type=changed_data["data_set_type"],
                    data_set_id=changed_data["data_set_id"],
                    processing_id=changed_data["processing_id"],
                    change_time=change_time,
                )
            elif (
                operation["type_name"] == "DataProcessingRelation"
                and operation["method"] == "DELETE"
            ):
                changed_data = json.loads(operation["changed_data"])

                if "data_directing" in changed_data:
                    event = DeleteDataProcessingRelationEvent(
                        id=identifier_value,
                        data_directing=changed_data["data_directing"],
                        data_set_type=changed_data["data_set_type"],
                        data_set_id=changed_data["data_set_id"],
                        processing_id=changed_data["processing_id"],
                        change_time=change_time,
                    )
                else:
                    logger.warning(
                        "DeleteDataProcessingRelationEvent has no changed_data: {}".format(
                            operation
                        )
                    )
                    event = DeleteDataProcessingRelationEvent(
                        id=identifier_value, change_time=change_time
                    )

            if event is not None:
                events.append(event)

        events = self.reduce_opposition_events(events)
        for event in events:
            self.event_controller.push_event(event)
            logger.info(
                "Fetch and generate event to inner event_controller: {}".format(event)
            )

    @staticmethod
    def reduce_opposition_events(events):
        """
        很多时候更新操作会涉及到先删后加，这种场景下会产生两个相悖的事件，可以抵消，无需产生事件。
        比如 CreateDataProcessingRelationEvent 与 DeleteDataProcessingRelationEvent
        即为相悖的事件，如果操作内容一致，则可抵消
        """

        def is_opposition(event1, event2):
            opposition_pair = [
                CreateDataProcessingRelationEvent,
                DeleteDataProcessingRelationEvent,
            ]

            index1 = index2 = -1
            for index, EventCls in enumerate(opposition_pair):
                if isinstance(event1, EventCls):
                    index1 = index
                if isinstance(event2, EventCls):
                    index2 = index

            if index1 >= 0 and index2 >= 0 and index1 != index2:
                return all(
                    [
                        event1.data_directing == event2.data_directing,
                        event1.data_set_type == event2.data_set_type,
                        event1.data_set_id == event2.data_set_id,
                        event1.processing_id == event2.processing_id,
                    ]
                )
            return False

        removed_events = []
        for index, event1 in enumerate(events):
            if event1 in removed_events:
                continue

            for event2 in events[index + 1 :]:
                if is_opposition(event1, event2):
                    removed_events.append(event1)
                    removed_events.append(event2)

        return [e for e in events if e not in removed_events]


def update_manager(node, raw_data_managers):
    """
    目前仅会更新 RESULT_TABLE 的管理员
    """
    if node.data_set_type != DataSetType.RESULT_TABLE:
        return

    datamanager_controller = DataManagerController()
    try:
        with connections["basic"].session() as session:
            managers = datamanager_controller.get_managers_interset(
                session, node.source_identifiers, cache_managers=raw_data_managers
            )
            updated_num = datamanager_controller.update_managers(
                session, node.identifier, managers
            )
            if updated_num > 0:
                logger.info(
                    "[Note] Update {} managers using sources({}, {}), "
                    "updated number={}".format(
                        node.identifier, node.source_identifiers, managers, updated_num
                    )
                )
                session.commit()
    except Exception as err:
        logger.exception("Error in the update_node_managers: {}".format(err))


class SyncTask(object):
    def __init__(self):
        self.event_controller = None
        self.graph = None
        self.raw_data_managers = None

        self.init()

    def init(self):
        self.event_controller = EventController(redis_connections["default"])

        self.graph = load_meta_lineage_graph()
        logger.info("Succeed to init lineage graph, {}".format(self.graph))

        with connections["basic"].session() as session:
            # 仅加载一次，需要作为常驻变量维护起来，需要重点关注
            self.raw_data_managers = DataManagerController().get_raw_data_managers(
                session
            )
            logger.info("Succeed to load raw_data managers and cache it.")

    def main(self):
        hearttime = time.time()
        while True:
            event = self.event_controller.pop_event()
            try:
                if event is not None:
                    self.handler(event)
            except Exception as err:
                logger.exception("Fail to update event {}, err={}".format(event, err))

            time.sleep(0.1)
            curtime = time.time()
            if curtime - hearttime > 10:
                hearttime = curtime
                logger.info(
                    "Hearttime: {}".format(timestamp_to_arrow(hearttime, TIMEZONE))
                )

    def handler(self, event):
        graph = self.graph
        raw_data_managers = self.raw_data_managers

        logger.info("Receive one event: {}".format(event))

        start_time = time.time()
        if isinstance(event, CreateDataProcessingRelationEvent):
            relation = transform_to_graph_relation(attr.asdict(event))
            if relation is not None:
                graph.add_relation(
                    relation, recalc_source=True, recalc_func=self.update_node_managers
                )
                logger.info("Add relation({}) to the graph, {}".format(relation, graph))
        elif isinstance(event, DeleteDataProcessingRelationEvent):
            relation = graph.remove_relation(
                event.id, recalc_source=True, recalc_func=self.update_node_managers
            )
            logger.info(
                "Remove relation({}) from the graph, {}".format(relation, graph)
            )
        elif isinstance(event, BaseManagersEvent):
            # update & create raw_data.managers
            node = DataNode(event.data_set_type, event.data_set_id)

            logger.info("Update raw_data.manager({}) in the cache".format(node))

            _managers = raw_data_managers.get(node.identifier, [])
            if isinstance(event, AddManagersEvent):
                for m in event.managers:
                    if m not in _managers:
                        _managers.append(m)
            elif isinstance(event, DeleteManagersEvent):
                for m in event.managers:
                    try:
                        _managers.remove(m)
                    except ValueError:
                        logger.warning(
                            "Fail remove {} from {}, current managers={}".format(
                                m, node.identifier, _managers
                            )
                        )

            elif isinstance(event, UpdateManagersEvent):
                _managers = event.managers

            raw_data_managers[node.identifier] = _managers

            logger.info(
                "Strat traverse and reassign RT.datamanagers from {}".format(node)
            )
            graph.traverse(node, self.update_node_manager_from_origin)

        end_time = time.time()
        logger.info(
            "Succeed to handle the event: {}, cost time: {}".format(
                event, end_time - start_time
            )
        )

    def update_node_managers(self, node, **kawrgs):
        is_changed = NodeManager.calc_node_sources(node)

        if is_changed and not node.is_source:
            update_manager(node, self.raw_data_managers)

        return is_changed

    def update_node_manager_from_origin(self, node, **kawrgs):
        traversed_nodes = kawrgs["traversed_nodes"]

        if node in traversed_nodes:
            return False

        if not node.is_source:
            update_manager(node, self.raw_data_managers)

        return True


def init():
    graph = load_meta_lineage_graph()

    if HAS_TDW_TABLE:
        response = meta_api.query_tdw_tables(
            {"associated_with_rt": True}, raise_exception=True
        )
        tdw_result_table_ids = [d["result_table_id"] for d in response.data]
    else:
        tdw_result_table_ids = []

    logger.info("Current rts with tdw relation: {}".format(tdw_result_table_ids))

    logger.info("Succeed to init lineage graph, {}".format(graph))

    datamanager_controller = DataManagerController()

    with connections["basic"].session() as session:
        raw_data_managers = datamanager_controller.get_raw_data_managers(session)

    count = 0
    for node in graph.nodes.values():
        count += 1
        if count % 1000 == 0:
            logger.info("Traverse and arrive to {} nodes".format(count))

        # TDW 表不进行操作
        if node.data_set_id in tdw_result_table_ids:
            continue

        update_manager(node, raw_data_managers)

    logger.info(
        "Done, init data managers for result tables in the lineage, total={}".format(
            count
        )
    )


def sync():
    task = SyncTask()
    task.main()


def transmit_meta_operation_record():
    task = TransmitMetaRecordsTask()
    task.main()
