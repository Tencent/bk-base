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

from common.base_utils import model_to_dict
from common.decorators import list_route
from common.local import get_request_username
from common.log import logger
from common.views import APIViewSet
from datahub.databus.constants import StorageType
from datahub.databus.exceptions import (
    NotSupportEventStorageError,
    StorageEventCreateError,
    StorageEventNotifyJobError,
)
from datahub.databus.models import DatabusStorageEvent
from datahub.databus.serializers import JobNotifySerializer, StorageEventSerializer
from django.db import IntegrityError
from rest_framework.response import Response

from datahub.databus import event


class EventViewset(APIViewSet):
    """
    总线事件相关的接口，存储事件、通知jobnavi等
    """

    def create(self, request):
        """
        @api {post} /v3/databus/events/ 创建总线事件
        @apiGroup Event
        @apiDescription 存储一个总线事件，同时根据条件判断是否需要启动离线任务
        @apiParam {string{小于255字符}} result_table_id result_table表的result_table_id，结果表标识。
        @apiParam {string{小于32字符}} storage 存储类型，目前只支持hdfs
        @apiParam {string{小于32字符}} event_type 事件类型
        @apiParam {string{小于255}} event_value 事件值
        @apiParam {string{小于128字符}} [description] 备注信息
        @apiError (错误码) 1570008 总线事件创建失败

        @apiParamExample {json} 参数样例:
        {
            "result_table_id": "105_xxx_test",
            "storage": "hdfs",
            "event_type": "tdwFinishDir",
            "event_value": "20180330",
            "description": "创建一个测试总线事件"
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
                "storage": "hdfs",
                "event_type": tdwFinishDir,
                "event_value": "20180331",
                "created_by": "xxx",
                "created_at": "2018-10-22 10:10:05",
                "updated_by": "xxx",
                "updated_at": "2018-10-22 10:10:05"
                "description": "创建一个测试总线事件"
            }
        }
        """
        params = self.params_valid(serializer=StorageEventSerializer)  # type: dict
        result_table_id = params["result_table_id"]
        try:
            obj = DatabusStorageEvent.objects.create(
                result_table_id=result_table_id,
                storage=params.get("storage", ""),
                event_type=params.get("event_type", ""),
                event_value=params.get("event_value", ""),
                created_by=get_request_username(),
                updated_by=get_request_username(),
                description=params.get("description", ""),
            )
            storage = obj.storage
            event_type = obj.event_type
            event_value = obj.event_value
            if storage == StorageType.HDFS.value:
                event.check_hdfs_event_and_notify(result_table_id, event_type, event_value)
                return Response(model_to_dict(obj))
            else:
                logger.error(
                    u"databus DatabusStorageEvent create failed! ===> caused by storage:%s is not support " % storage
                )
                raise NotSupportEventStorageError(message=u"保存总线事件失败, %s 存储类型当前不支持" % storage)
        except IntegrityError:
            logger.error("databus DatabusHdfsImportTask create error!", exc_info=True)
            raise StorageEventCreateError(message_kv={"result_table_id": result_table_id})

    @list_route(methods=["post"], url_path="notify_jobnavi")
    def notify_jobnavi(self, request):
        """
        @api {post} /v3/databus/events/notify_jobnavi/ 启动离线任务
        @apiGroup Event
        @apiDescription 通过调用jobnavi离线任务接口来启动一个数据已经准备好的离线任务
        @apiParam {string{小于255字符}} result_table_id result_table表的result_table_id，结果表标识。
        @apiParam {string{小于32字符}} date_time hdfs数据文件名 YYYYMMDD格式。
        @apiError (错误码) 1570009 notify_jobnavi 接口调用异常


        @apiParamExample {json} 参数样例:
        {
            "result_table_id": "105_xxx_test",
            "date_time": "20180830"
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1570200",
            "result": true,
            "data": null
        }
        """
        params = self.params_valid(serializer=JobNotifySerializer)  # type: dict
        result_table_id = params["result_table_id"]
        date_time = params["date_time"]
        date_valid = event.validate_date_time(date_time)
        if date_valid:
            call_result = event.call_dataflow_batch_job(result_table_id, date_time)
            if call_result[0]:
                return Response({"result_table_id": result_table_id, "date_time": date_time})
            else:
                logger.error(u"notify_dataflow batch job failed! ===> caused by dataflow-batch-api call failed ")
                raise StorageEventNotifyJobError(message_kv={"result_table_id": result_table_id})
        else:
            logger.error(u"notify_dataflow batch job failed！===>  caused by date_time:%s is not valid" % date_time)
            raise StorageEventNotifyJobError(message=u"总线调用离线任务失败,日期参数校验失败,date_time:%s" % date_time)
