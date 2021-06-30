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
from datetime import datetime, timedelta

from common.base_utils import model_to_dict
from common.decorators import detail_route, list_route
from common.local import get_request_username
from common.log import logger
from common.views import APIViewSet
from datahub.databus.exceptions import (
    ImportHdfsCreateError,
    ImportHdfsEarliestGetError,
    ImportHdfsHistoryCleanError,
    ImportHdfsNotNotFound,
    ImportHdfsTaskCheckError,
    ImportHdfsTaskUpdateError,
)
from datahub.databus.serializers import (
    HdfsImportCheckSerializer,
    HdfsImportCleanSerializer,
    HdfsImportSearchSerializer,
    HdfsImportSerializer,
    HdfsImportUpdateCompatibleSerializer,
    HdfsImportUpdateSerializer,
)
from datahub.databus.task.pulsar import topic as pulsar_topic
from datahub.storekit.iceberg import construct_hdfs_conf
from django.db import IntegrityError
from django.db.models import Q
from rest_framework.response import Response

from datahub.databus import import_hdfs, settings

from ..common.const import (
    BOOTSTRAP_SERVERS,
    BROKER_SERVICE_URL,
    CHANNEL_TYPE,
    CONNECTION_INFO,
    DATA_DIR,
    DEFAULT,
    DESCRIPTION,
    GEOG_AREA,
    HDFS,
    HDFS_CLUSTER_NAME,
    HDFS_DATA_TYPE,
    ICEBERG,
    PULSAR,
    PULSAR_CHANNEL_TOKEN,
    RESULT_TABLE_ID,
    TABLE,
)
from . import rt
from .models import DatabusHdfsImportTask
from .task.transport_task import build_hdfs_custom_conf_from_conn


class ImportHdfsViewSet(APIViewSet):
    """
    离线hdfs导入任务相关接口，包括添加、修改、删除
    """

    # 任务的id，用于关联相应的离线hdfs任务列表
    lookup_field = "task_id"

    def create(self, request):
        """
        @api {POST} /v3/databus/import_hdfs/ 创建总线离线导入配置
        @apiGroup ImportHdfs
        @apiDescription 创建一个总线的离线导入配置（当前只支持数据源是hdfs），用于将离线hdfs的数据导入kafka用于后续的数据分发
        @apiParam {string{小于255字符}} result_table_id result_table表的result_table_id，结果表标识。
        @apiParam {string{小于512字符}} data_dir 离线hdfs数据源目录。
        @apiParam {string{小于128字符}} [description] 备注信息

        @apiError (错误码) 1570001 hdfs离线导入任务创建失败.

        @apiParamExample {json} 参数样例:
        {
            "result_table_id": "inner",
            "data_dir": "kafka",
            "description": "测试创建import_hdfs离线导入任务"
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1570200",
            "result": true,
            "data": {
                "id": 1,
                "result_table_id": "105_xxx_test",
                "data_dir": "/test/data_dir",
                "hdfs_conf_dir": "/test/hdfs_con_dif",
                "kafka_bs": "localhost:2181",
                "finished": "0",
                "status": testStatus,
                "created_by": "xxx",
                "created_at": "2018-10-22 10:10:05",
                "updated_by": "xxx",
                "updated_at": "2018-10-22 10:10:05"
                "description": "测试创建import_hdfs离线导入任务"
            }
        }
        """
        # 解析参数
        params = self.params_valid(serializer=HdfsImportSerializer)  # type: dict
        result_table_id = params[RESULT_TABLE_ID]
        try:
            # 获取rt详情
            rt_info = rt.get_databus_rt_info(result_table_id)
            if rt_info:
                if HDFS in rt_info:
                    storage_conn = json.loads(rt_info[HDFS][CONNECTION_INFO])
                else:
                    # 使用默认的hdfs集群（default）
                    storage_conf = rt.get_storage_cluster(HDFS, DEFAULT)
                    storage_conn = json.loads(storage_conf[CONNECTION_INFO])

                if rt_info[HDFS_DATA_TYPE] == ICEBERG:
                    custom_conf = construct_hdfs_conf(rt_info[HDFS][CONNECTION_INFO], rt_info[GEOG_AREA], ICEBERG)
                else:
                    custom_conf = build_hdfs_custom_conf_from_conn(storage_conn)

                channel_url = (
                    rt_info[BROKER_SERVICE_URL] if rt_info[CHANNEL_TYPE] == PULSAR else rt_info[BOOTSTRAP_SERVERS]
                )
                # ensure pulsar partition topic
                if rt_info[CHANNEL_TYPE] == settings.TYPE_PULSAR:
                    topic = "{}_{}".format(TABLE, params[RESULT_TABLE_ID])
                    if not pulsar_topic.exist_partition_topic(
                        rt_info[BOOTSTRAP_SERVERS],
                        topic,
                        rt_info.get(PULSAR_CHANNEL_TOKEN, None),
                    ):
                        pulsar_topic.create_partition_topic(
                            rt_info[BOOTSTRAP_SERVERS],
                            topic,
                            settings.PULSAR_DEFAULT_PARTITION,
                            rt_info.get(PULSAR_CHANNEL_TOKEN, None),
                        )

                obj = DatabusHdfsImportTask.objects.create(
                    result_table_id=params[RESULT_TABLE_ID],
                    geog_area=rt_info[GEOG_AREA],
                    data_dir=params[DATA_DIR],
                    hdfs_conf_dir=storage_conn[HDFS_CLUSTER_NAME],
                    hdfs_custom_property=json.dumps(custom_conf),
                    kafka_bs=channel_url,
                    created_by=get_request_username(),
                    updated_by=get_request_username(),
                    description=params.get(DESCRIPTION, ""),
                    finished=0,
                    databus_type=rt_info[CHANNEL_TYPE],
                )
                return Response(model_to_dict(obj))
            else:
                logger.error(
                    u"databus DatabusHdfsImportTask create failed! ===> caused by rt:%s not found" % result_table_id
                )
                raise ImportHdfsCreateError(message=u"离线导入任务创建失败，result_table_id:%s 对应的结果表不存在！" % result_table_id)
        except IntegrityError:
            logger.error("databus DatabusHdfsImportTask create error!", exc_info=True)
            raise ImportHdfsCreateError()

    def retrieve(self, request, task_id):
        """
        @api {GET} /v3/databus/import_hdfs/:task_id/ 获取总线离线导入配置
        @apiGroup ImportHdfs
        @apiDescription 获取总线的离线hdfs导入任务配置信息
        @apiError (错误码) 1570002 查询的数据不存在

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "id": 1,
                "result_table_id": "105_xxx_test",
                "data_dir": "/test/data_dir",
                "hdfs_conf_dir": "/test/hdfs_con_dif",
                "kafka_bs": "localhost:2181",
                "finished": "0",
                "status": "testStatus",
                "created_by": "xxx",
                "created_at": "2018-10-22 10:10:05",
                "updated_by": "xxx",
                "updated_at": "2018-10-22 10:10:05"
                "description": "测试创建import_hdfs离线导入任务"
            }
        }
        """
        try:
            obj = DatabusHdfsImportTask.objects.get(id=task_id)
            return Response(model_to_dict(obj))
        except DatabusHdfsImportTask.DoesNotExist:
            logger.error(u"databus DatabusHdfsImportTask retrieve error! ===> caused by id:%s DoesNotExist" % task_id)
            raise ImportHdfsNotNotFound(message_kv={"task_id": task_id})

    @list_route(methods=["post"], url_path="clean_history")
    def clean_history(self, request):
        """
        @api {DELETE} /v3/databus/import_hdfs/clean_history/ 删除历史离线导入配置
        @apiGroup ImportHdfs
        @apiDescription 删除N天前finish=1已完成的离线hdfs导入任务记录
        @apiParam {int{天数}} days 要清除days天之前的记录，days>0,默认值=30
        @apiError (错误码) 1570003 history_clean api操作异常

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "del_count": 5, #成功删除的历史记录数量
                "days": 2
            }
        }
        """
        params = self.params_valid(serializer=HdfsImportCleanSerializer)  # type: dict
        days = params["days"]
        try:
            now_date = datetime.now().date()
            del_date = now_date - timedelta(int(days))
            obj = DatabusHdfsImportTask.objects.filter(Q(created_at__lt=del_date), Q(finished=1))
            if obj:
                count = obj.count()
                obj.delete()
                return Response({"del_count": count, "days": days})
            else:
                return Response({"del_count": 0, "days": days})
        except Exception:
            logger.error("databus DatabusHdfsImportTask history clean error!", exc_info=True)
            raise ImportHdfsHistoryCleanError(message_kv={"days": days})

    @list_route(methods=["get"], url_path="get_earliest")
    def get_earliest(self, request):
        """
        @api {GET} /v3/databus/import_hdfs/get_earliest/ 获取最近离线导入配置列表
        @apiGroup ImportHdfs
        @apiDescription 获取最近未完成的任务列表
        @apiParam {int{限定记录数}} limit 指定获取limit条最近未处理finish=0的任务记录
        @apiError (错误码) 1570004 earliest_get api操作异常
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": [{
                "id": 1,
                "result_table_id": "105_xxx_test",
                "rt_id": "105_xxx_test",
                "data_dir": "/test/data_dir",
                "hdfs_conf_dir": "/test/hdfs_con_dif",
                "kafka_bs": "localhost:2181",
                "finished": "0",
                "status": "testStatus",
                "created_by": "xxx",
                "created_at": "2018-10-22 10:10:05",
                "updated_by": "xxx",
                "updated_at": "2018-10-22 10:10:05"
                "description": "测试创建import_hdfs离线导入任务"
            },
            {
                "id": 2,
                "result_table_id": "106_xxx_test",
                "rt_id": "106_xxx_test",
                "data_dir": "/test/data_dir2",
                "hdfs_conf_dir": "/test/hdfs_con_dif2",
                "kafka_bs": "localhost:2181",
                "finished": "0",
                "status": "testStatus",
                "created_by": "xxx",
                "created_at": "2018-10-22 11:10:05",
                "updated_by": "xxx",
                "updated_at": "2018-10-22 11:10:05"
                "description": "测试创建import_hdfs离线导入任务"
            }

        }
        """
        params = self.params_valid(serializer=HdfsImportSearchSerializer)  # type: dict
        limit = params["limit"]
        geog_area = params[GEOG_AREA]
        databus_type = params["databus_type"]
        try:
            obj = DatabusHdfsImportTask.objects.filter(finished=0, databus_type=databus_type)
            if geog_area != "":
                obj = obj.filter(geog_area__exact=geog_area)
            obj = obj.extra(select={"rt_id": RESULT_TABLE_ID})[0:limit]
            return Response(obj.values())
        except Exception:
            logger.error("databus DatabusHdfsImportTask get_earliest error!", exc_info=True)
            raise ImportHdfsEarliestGetError()

    @detail_route(methods=["POST"], url_path="update")
    def update_task(self, request, task_id):
        """
        @api {POST} /v3/databus/import_hdfs/:task_id/update/ 更新总线离线导入配置
        @apiGroup ImportHdfs
        @apiDescription 更新离线hdfs导入任务状态信息（新接口）
        @apiParam {string{小于255字符}} result_table_id result_table表的result_table_id，结果表标识。
        @apiParam {int{主键id}} task_id databus_hdfs_import_tasks表的id，任务id
        @apiParam {string{小于512字符}} data_dir 离线hdfs数据源目录。
        @apiParam {tinyint{任务状态标识}} finished 离线hdfs导入任务状态 0：未完成 1：已完成
        @apiParam {string{小于512字符}} status 离线hdfs导入任务处理状态


        @apiError (错误码) 1570005 task_update api操作异常

        @apiParamExample {json} 参数样例:
        {
            "result_table_id": "105_xxx_test",
            "data_dir": "/test/data_dir",
            "finished": 1,
            "status": "test_status"
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1570200",
            "result": true,
            "data": {
                "id": 1,
                "result_table_id": "105_xxx_test",
                "data_dir": "/test/data_dir",
                "hdfs_conf_dir": "/test/hdfs_con_dif",
                "kafka_bs": "localhost:2181",
                "finished": "1",
                "status": "test_status",
                "created_by": "xxx",
                "created_at": "2018-10-22 10:10:05",
                "updated_by": "xxx",
                "updated_at": "2018-10-22 10:10:05"
                "description": "测试创建import_hdfs离线导入任务"
            }
        }
        """

        params = self.params_valid(serializer=HdfsImportUpdateSerializer)  # type: dict
        try:
            finished = params["finished"]
            finished = int(finished)
            if finished:
                obj = DatabusHdfsImportTask.objects.get(
                    Q(id=task_id),
                    Q(result_table_id=params[RESULT_TABLE_ID]),
                    Q(data_dir=params[DATA_DIR]),
                )
                if obj:
                    obj.updated_by = get_request_username()
                    obj.finished = 1
                    obj.status = params["status"]
                    obj.save()

            else:
                obj = DatabusHdfsImportTask.objects.get(
                    id=task_id,
                    result_table_id=params[RESULT_TABLE_ID],
                    data_dir=params[DATA_DIR],
                    finished=0,
                )
                if obj:
                    obj.status = params["status"]
                    obj.updated_by = get_request_username()
                    obj.save()

            return Response(model_to_dict(obj))
        except DatabusHdfsImportTask.DoesNotExist:
            logger.error("databus DatabusHdfsImportTask task_update error!", exc_info=True)
            raise ImportHdfsTaskUpdateError(
                message_kv={
                    "task_id": task_id,
                    RESULT_TABLE_ID: params[RESULT_TABLE_ID],
                    DATA_DIR: params[DATA_DIR],
                    "finished": params["finished"],
                }
            )

    @list_route(methods=["POST"], url_path="update_hdfs_import_task")
    def update_hdfs_import_task(self, request):
        """
        @api {POST} /v3/databus/import_hdfs/update_hdfs_import_task/ 更新离线导入配置(老接口)
        @apiGroup ImportHdfs
        @apiDescription 更新离线hdfs导入任务状态信息(兼容老接口)
        @apiParam {string{小于255字符}} id databus_hdfs_import_tasks表的id，任务id
        @apiParam {string{小于255字符}} rt_id result_table表的result_table_id，结果表标识
        @apiParam {string{小于512字符}} data_dir 离线hdfs数据源目录
        @apiParam {tinyint{任务状态标识}} finish 离线hdfs导入任务状态 true：未完成 false：已完成
        @apiParam {string{小于512字符}} status_update 离线hdfs导入任务处理状态


        @apiError (错误码) 1570005 task_update api操作异常

        @apiParamExample {json} 参数样例:
        {
            "id": "1"
            "rt_id": "105_xxx_test",
            "data_dir": "/test/data_dir",
            "finish": 'true',
            "status_update": "test_status"
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1570200",
            "result": true,
            "data": {
                "id": 1,
                "rt_id": "105_xxx_test",
                "data_dir": "/test/data_dir",
                "hdfs_conf_dir": "/test/hdfs_con_dif",
                "kafka_bs": "localhost:2181",
                "finished": "1",
                "status": "test_status",
                "created_by": "xxx",
                "created_at": "2018-10-22 10:10:05",
                "updated_by": "xxx",
                "updated_at": "2018-10-22 10:10:05"
                "description": "测试创建import_hdfs离线导入任务"
            }
        }
        """

        params = self.params_valid(serializer=HdfsImportUpdateCompatibleSerializer)  # type: dict
        try:
            finish = params["finish"]
            finished = finish.lower() == "true"
            if finished:
                obj = DatabusHdfsImportTask.objects.get(
                    Q(id=params["id"]),
                    Q(result_table_id=params["rt_id"]),
                    Q(data_dir=params[DATA_DIR]),
                )
                if obj:
                    obj.updated_by = get_request_username()
                    obj.finished = 1
                    obj.status = params["status_update"]
                    obj.save()

            else:
                obj = DatabusHdfsImportTask.objects.get(
                    id=params["id"],
                    result_table_id=params["rt_id"],
                    data_dir=params[DATA_DIR],
                    finished=0,
                )
                if obj:
                    obj.status = params["status_update"]
                    obj.updated_by = get_request_username()
                    obj.save()
            return Response(model_to_dict(obj))
        except DatabusHdfsImportTask.DoesNotExist:
            logger.error("databus DatabusHdfsImportTask task_update error!", exc_info=True)
            raise ImportHdfsTaskUpdateError(
                message_kv={
                    "task_id": params["id"],
                    RESULT_TABLE_ID: params["rt_id"],
                    DATA_DIR: params[DATA_DIR],
                    "finished": params["finish"],
                }
            )

    @list_route(methods=["get"], url_path="check")
    def check_tasks(self, request):
        """
        @api {GET} /v3/databus/import_hdfs/check/ 检查离线导入任务状态
        @apiGroup ImportHdfs
        @apiDescription 检查hdfs离线数据导入任务运行状况
        @apiParam {int{限定时间间隔,单位:分钟}} interval_min 检查间隔（距离当前事件多少分钟前创建的任务）
        @apiError (错误码) 1570020 hdfs_import_tasks_check api操作异常
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data":

        }
        """
        params = self.params_valid(serializer=HdfsImportCheckSerializer)  # type: dict
        try:
            check_interval_min = params["interval_min"]
            obj = import_hdfs.check_tasks(check_interval_min)
            return Response(obj)
        except Exception:
            logger.error(
                "databus DatabusHdfsImportTask hdfs_import_tasks_check error!",
                exc_info=True,
            )
            raise ImportHdfsTaskCheckError()
