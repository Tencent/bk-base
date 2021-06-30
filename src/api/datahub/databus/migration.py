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

import datetime
import json
import time

import requests
from common.local import get_request_username
from common.log import logger
from common.transaction import auto_meta_sync
from datahub.common.const import (
    BK_BIZ_ID,
    CLUSTER_TYPE_JAVA,
    COLUMNS,
    CONNECTION_INFO,
    CONNECTION_PASSWORD,
    CONNECTION_URL,
    CONNECTION_USER,
    DEFAULT,
    DEST,
    DTEVENTTIME,
    DTEVENTTIMESTAMP,
    FIELDS,
    GEOG_AREA,
    HOST,
    INCREMENTING,
    INCREMENTING_COLUMN_NAME,
    INT,
    ITERARION_IDX,
    KAFKA_METRIC_CONFIG,
    LOCALTIME,
    LONG,
    MAPLELEAF,
    MIGRATION,
    MODE,
    MONITOR_COMPONENT_NAME,
    MONITOR_MODULE_NAME,
    MONITOR_PRODUCER_BOOTSTRAP_SERVERS,
    MONITOR_PRODUCER_TOPIC,
    NAME,
    NAMESPACE,
    OFFSET,
    PASSWORD,
    PHYSICAL_TABLE_NAME,
    POLL_INTERVAL_MS,
    PORT,
    PRODUCER_TOPIC,
    RECORD,
    RESULT_TABLE_ID,
    RTID,
    SCHEMA,
    SOURCE,
    STRING,
    TABLE,
    TABLE_POLL_INTERVAL_MS,
    TABLE_WHITE_LIST,
    TABLES,
    TEST_NAMESPACE,
    TEXT,
    THEDATE,
    TIMESTAMP,
    TSPIDER,
    TYPE,
    USER,
)
from datahub.databus.api import meta
from datahub.databus.exceptions import (
    MigrationCannotOperatError,
    MigrationNotFoundError,
    MigrationOperatFailError,
    MigrationRunningError,
    MigrationTimeError,
    MigrationTooLongError,
    StorageNotSupportedError,
)
from datahub.databus.task.transport_task import build_hdfs_custom_conf_from_conn
from requests_toolbelt import MultipartEncoder

from datahub.databus import exceptions, models, settings


def get_task(id):
    """
    获取任务并检查任务类型
    :param id: 任务id
    :return 任务详情
    """
    try:
        migrate_task = models.DatabusMigrateTask.objects.get(id=id)
    except models.DatabusMigrateTask.DoesNotExist as e:
        logger.error("get task error, %s" % str(e))
        raise MigrationNotFoundError()

    if migrate_task.task_type not in ["subjob"]:
        raise MigrationCannotOperatError(message_kv={"type": migrate_task.task_type})

    return migrate_task


def create_task(rt_info, config):
    """
    创建迁移任务，该方法下生产的db任务信息拥有同一个任务标签
    :param rt_info: rt详情
    :param config: 任务参数
    :return: 任务标签
    """
    start = datetime.datetime.strptime(config["start"], "%Y-%m-%d %H:%M:%S")
    end = datetime.datetime.strptime(config["end"], "%Y-%m-%d %H:%M:%S")
    limit_time = start + datetime.timedelta(days=7)
    if limit_time < end:
        # 迁移任务时间长度超过7天
        raise MigrationTooLongError(message_kv={"days": 7})
    elif end <= start:
        # 起始时间 >= 结束时间
        raise MigrationTimeError()

    obj = models.DatabusMigrateTask.objects.filter(
        result_table_id=config["result_table_id"],
        source=config["source"],
        dest=config["dest"],
    ).exclude(status__in=["finish"])
    if len(obj) > 0:
        # 已有相同rt，相同迁移路径的任务在运行中
        raise MigrationRunningError()

    src_config = {}
    if config["source"] == "tspider":
        # tspider 源存储配置任务
        src_config = _build_source_tspider_conf(config, rt_info)

    dest_config = {}
    if config["dest"] == "hdfs":
        dest_config = _build_sink_hdfs_conf(config, rt_info)

    # 生成基本任务信息
    task_list = []
    task_label = "migrate-{}-{}".format(config["result_table_id"], time.time())
    task_list.append(
        models.DatabusMigrateTask(
            task_type="overall",
            task_label=task_label,
            result_table_id=config["result_table_id"],
            geog_area=rt_info["geog_area"],
            parallelism=config["parallelism"],
            overwrite=config["overwrite"],
            start=config["start"],
            end=config["end"],
            source=config["source"],
            source_config=json.dumps(src_config),
            source_name="",
            dest=config["dest"],
            dest_config=json.dumps(dest_config),
            dest_name="",
            status="init",
            created_by=get_request_username(),
        )
    )

    time_delta = 60
    # 按小时切分生成子任务
    while start < end:
        new_time = start + datetime.timedelta(minutes=time_delta)
        str_start = datetime.datetime.strftime(start, "%Y-%m-%d %H:%M:%S")
        str_end = config["end"] if new_time >= end else datetime.datetime.strftime(new_time, "%Y-%m-%d %H:%M:%S")

        if config["source"] == "tspider":
            src_config["start"] = str(int(time.mktime(start.timetuple()))) + "000"  # 转换为毫秒
            if new_time < end:
                src_config["end"] = str(int(time.mktime(new_time.timetuple()))) + "000"
            else:
                src_config["end"] = str(int(time.mktime(end.timetuple()))) + "000"

        start = new_time
        task_list.append(
            models.DatabusMigrateTask(
                task_type="subjob",
                task_label=task_label,
                result_table_id=config["result_table_id"],
                geog_area=rt_info["geog_area"],
                parallelism=config["parallelism"],
                overwrite=config["overwrite"],
                start=str_start,
                end=str_end,
                source=config["source"],
                source_config=json.dumps(src_config),
                source_name="",
                dest=config["dest"],
                dest_config=json.dumps(dest_config),
                dest_name="",
                status="init",
                created_by=get_request_username(),
            )
        )

    # 创建datatransfer
    _create_transfer(config, rt_info)
    # 写入DB，并同步给meta
    for one_task in task_list:
        with auto_meta_sync(using=DEFAULT):
            one_task.save()

    return task_label


def _create_transfer(config, rt_info):
    """
    创建data transfer：已存在跳过，否则创建
    :param config: 任务配置参数
    :param rt_info: rt
    :return:
    """
    param = {
        "bk_username": get_request_username(),
        "project_id": rt_info["project_id"],
        "transferring_id": "{}_{}_{}".format(config["result_table_id"], config["source"], config["dest"]),
        "transferring_alias": u"{}从{}迁移到{}".format(config["result_table_id"], config["source"], config["dest"]),
        "transferring_type": "migration",
        "generate_type": "system",
        "description": u"数据迁移",
        "tags": [rt_info["geog_area"]],
        "inputs": [
            {
                "data_set_type": "result_table",
                "data_set_id": config["result_table_id"],
                "storage_cluster_config_id": rt_info[config["source"]]["id"],
                "channel_cluster_config_id": None,
                "storage_type": "storage",
            }
        ],
        "outputs": [
            {
                "data_set_type": "result_table",
                "data_set_id": config["result_table_id"],
                "storage_cluster_config_id": rt_info[config["dest"]]["id"],
                "channel_cluster_config_id": None,
                "storage_type": "storage",
            }
        ],
    }
    respond = meta.MetaApi.data_transferrings.retrieve({"transferring_id": param["transferring_id"]})
    if not respond.is_success:
        raise exceptions.MigrationCreateError(message_kv={"message": "查询data_transfering失败"})
    if respond.data:
        respond = meta.MetaApi.data_transferrings.create(param)
        if not respond.is_success:
            raise exceptions.MigrationCreateError(message_kv={"message": "创建data_transfering失败"})


def _build_sink_hdfs_conf(config, rt_info):
    """
    生成pulsar中hdfs sink任务配置参数
    :param config: 任务参数
    :param rt_info: rt详细信息
    :return: 配置map
    """
    dest_config = {}
    # hdfs 目的存储配置任务
    try:
        storage_conn = json.loads(rt_info["hdfs"]["connection_info"])
    except Exception:
        raise exceptions.TaskStorageConnConfigErr()
    # 生成sink任务配置
    physical_table_name = rt_info["hdfs"]["physical_table_name"]
    dest_config["bizId"] = rt_info["bk_biz_id"]
    dest_config["rtId"] = config["result_table_id"]
    dest_config["clusterType"] = config["dest"]
    dest_config["tableName"] = physical_table_name.split("/")[-1]
    dest_config["cluster"] = "pulsar"
    dest_config["hdfsUrl"] = storage_conn["hdfs_url"]
    dest_config["hdfsConfDir"] = ""
    dest_config["topicsDir"] = "/kafka/data/"
    dest_config["hdfsCustomProperty"] = json.dumps(build_hdfs_custom_conf_from_conn(storage_conn))
    dest_config["flushSize"] = 1000000
    dest_config["rotateIntervalMs"] = 60000
    dest_config["kafkaMetricConfig"] = {
        "monitor.producer.bootstrap.servers": settings.KAFKA_OP_HOST[rt_info["geog_area"]],
        "monitor.producer.topic": settings.DATABUS_MONITOR_CONF["producer.topic"],
        "monitor.module.name": "migration",
        "monitor.component.name": config["source"],
    }
    return dest_config


def _build_source_tspider_conf(config, rt_info):
    """
    生成pulsar中tspider source任务配置参数
    :param config: 任务参数
    :param rt_info: rt详细信息
    :return: 配置map
    """
    src_config = {}
    try:
        storage_conn = json.loads(rt_info[TSPIDER][CONNECTION_INFO])
    except Exception:
        raise exceptions.TaskStorageConnConfigErr()
    physical_table_name = rt_info[TSPIDER][PHYSICAL_TABLE_NAME]
    # physical_table_name 格式 "mapleleaf_591.new_test_591"
    arr = physical_table_name.split(".")
    if len(arr) == 1:
        db_name = "{}_{}".format(MAPLELEAF, rt_info[BK_BIZ_ID])
        table_name = physical_table_name
    else:
        db_name = arr[0]
        table_name = arr[1]
    # source任务配置生成
    src_config[RTID] = config[RESULT_TABLE_ID]
    src_config[CLUSTER_TYPE_JAVA] = config[DEST]
    src_config[CONNECTION_URL] = "jdbc:mysql://{}:{}/{}".format(
        storage_conn[HOST],
        storage_conn[PORT],
        db_name,
    )
    src_config[CONNECTION_USER] = storage_conn[USER]
    src_config[CONNECTION_PASSWORD] = storage_conn[PASSWORD]
    src_config[MODE] = INCREMENTING
    src_config[TABLES] = table_name
    src_config[TABLE_WHITE_LIST] = table_name
    src_config[INCREMENTING_COLUMN_NAME] = DTEVENTTIMESTAMP
    src_config[POLL_INTERVAL_MS] = "10"
    src_config[TABLE_POLL_INTERVAL_MS] = "60000"
    src_config[KAFKA_METRIC_CONFIG] = {
        MONITOR_PRODUCER_BOOTSTRAP_SERVERS: settings.KAFKA_OP_HOST[rt_info[GEOG_AREA]],
        MONITOR_PRODUCER_TOPIC: settings.DATABUS_MONITOR_CONF[PRODUCER_TOPIC],
        MONITOR_MODULE_NAME: MIGRATION,
        MONITOR_COMPONENT_NAME: config[SOURCE],
    }
    # 生成列信息
    use_fields = []
    for f in rt_info[COLUMNS].split(","):
        arr = f.split("=")
        if arr[0] in [TIMESTAMP, OFFSET, ITERARION_IDX]:
            continue
        field_type = STRING if arr[1] == TEXT else arr[1]
        use_fields.append({NAME: arr[0], TYPE: field_type})
    use_fields.append({NAME: THEDATE, TYPE: INT})
    use_fields.append({NAME: DTEVENTTIME, TYPE: STRING})
    use_fields.append({NAME: DTEVENTTIMESTAMP, TYPE: LONG})
    use_fields.append({NAME: LOCALTIME, TYPE: STRING})
    src_config[SCHEMA] = json.dumps(
        {
            NAMESPACE: TEST_NAMESPACE,
            TYPE: RECORD,
            NAME: "{}_{}".format(TABLE, config[RESULT_TABLE_ID]),
            FIELDS: use_fields,
        }
    )
    return src_config


def start_task(task, type):
    """
    启动迁移任务
    :param task: 任务详情
    :param type: 启动类型，sink，source，all
    :return: 成功返回True
    """
    sink_task_name = "{}-{}-{}".format(task.result_table_id, task.dest, task.id)
    source_task_name = "{}-{}-{}".format(task.result_table_id, task.source, task.id)

    # 先删除topic，保证数据干净
    if type in ["all", "source"]:
        topic = "{}-{}".format(task.task_label, task.id)
        url = "http://{}/admin/v2/persistent/{}/{}/{}/".format(
            settings.pulsar_admin_url[task.geog_area],
            settings.pulsar_tenant,
            settings.pulsar_namespace,
            topic,
        )
        res = requests.delete(url)
        logger.info("delete pulsar topic: url={}, code={}, text={}".format(url, res.status_code, res.text))

    # 起sink任务
    if type in ["all", "sink"]:
        _start_pulsar_task(task, "sink", sink_task_name)
        # 任务启动成功，更新db状态，即便下面操作失败了，调度进程会调度source任务
        with auto_meta_sync(using=DEFAULT):
            task.dest_name = sink_task_name
            task.source_name = source_task_name
            task.status = "running"
            task.save()

    time.sleep(settings.task_start_interval)

    # 再起source任务
    if type in ["all", "source"]:
        _start_pulsar_task(task, "source", source_task_name)

    return True


def _start_pulsar_task(task, type, task_name):
    """
    启动pulsar实例任务
    :param task: 任务详情
    :param type: 任务类型，source，sink
    :param task_name: 任务名称
    """
    task_config = {}
    if type == "sink":
        task_config = json.loads(task.dest_config)
        task_config["connector"] = task_name
        task_config["logsDir"] = "/kafka/logs/pulsar_hdfs/m_{}_{}_logs".format(
            task_config["rtId"],
            task.id,
        )
    elif type == "source":
        task_config = json.loads(task.source_config)
        task_config["start"] = str(int(time.mktime(time.strptime(task.start, "%Y-%m-%d %H:%M:%S")))) + "000"  # 转换为毫秒
        task_config["end"] = str(int(time.mktime(time.strptime(task.end, "%Y-%m-%d %H:%M:%S")))) + "000"
        task_config["basicInfo"] = {"task_id": task.id}
        task_config["connectionPasswordKey"] = settings.CRYPT_INSTANCE_KEY

    task_config["metricReportUrl"] = "%smigrations/update_task_status/" % (settings.DATABUS_API_URL)

    config = {
        "tenant": settings.pulsar_tenant,
        "namespace": settings.pulsar_namespace,
        "name": task_name,
        "parallelism": 1,
        "configs": task_config,
    }

    if type == "sink":
        config["inputs"] = [
            "persistent://%s/%s/%s-%s"
            % (
                settings.pulsar_tenant,
                settings.pulsar_namespace,
                task.task_label,
                task.id,
            )
        ]
        config["processingGuarantees"] = "EFFECTIVELY_ONCE"
        if task.dest == "hdfs":
            config["className"] = "com.tencent.bkdata.io.hdfs.BizHdfsSink"
            config["archive"] = "builtin://bkhdfs2"
        else:
            raise StorageNotSupportedError(message_kv={"cluster_type": task.dest})
    elif type == "source":
        config["topicName"] = "persistent://{}/{}/{}-{}".format(
            settings.pulsar_tenant,
            settings.pulsar_namespace,
            task.task_label,
            task.id,
        )
        if task.source == "tspider":
            config["className"] = "io.confluent.connect.jdbc.source.JdbcSource"
            config["archive"] = "builtin://bkjdbc"
        else:
            raise StorageNotSupportedError(message_kv={"cluster_type": task.source})

    mp_encoder = MultipartEncoder([("%sConfig" % type, (None, json.dumps(config), "application/json"))])
    url = "http://{}/admin/v3/{}s/{}/{}/{}".format(
        settings.pulsar_io_url[task.geog_area],
        type,
        settings.pulsar_tenant,
        settings.pulsar_namespace,
        task_name,
    )
    res = requests.post(url, data=mp_encoder, headers={"Content-Type": mp_encoder.content_type})
    logger.info(
        "create pulsar {}: url={}, param={} code={}, text={}".format(type, url, mp_encoder, res.status_code, res.text)
    )
    if res.status_code in [200, 204, 400]:
        if res.status_code == 400 and "already exists" not in res.text:
            raise MigrationOperatFailError(message_kv={"message": res.text})
    else:
        raise MigrationOperatFailError(message_kv={"message": res.text})


def stop_task(task, type):
    """
    停止迁移任务
    :param task: 任务详情
    :param type: 停止任务类型，source，sink，all
    """
    # 先停source任务
    if type in ["all", "source"]:
        url = "http://{}/admin/v3/{}/{}/{}/{}".format(
            settings.pulsar_io_url[task.geog_area],
            "sources",
            settings.pulsar_tenant,
            settings.pulsar_namespace,
            task.source_name,
        )
        res = requests.delete(url)
        logger.info("stop pulsar source: url={}, code={}, text={}".format(url, res.status_code, res.text))
        if res.status_code not in [200, 204, 404]:
            raise MigrationOperatFailError(message_kv={"message": res.text})

    # 再停sink任务
    if type in ["all", "sink"]:
        url = "http://{}/admin/v3/{}/{}/{}/{}".format(
            settings.pulsar_io_url[task.geog_area],
            "sinks",
            settings.pulsar_tenant,
            settings.pulsar_namespace,
            task.dest_name,
        )
        res = requests.delete(url)
        logger.info("stop pulsar sink: url={}, code={}, text={}".format(url, res.status_code, res.text))
        if res.status_code not in [200, 204, 404]:
            raise MigrationOperatFailError(message_kv={"message": res.text})


def get_task_status(task, type):
    """
    获取pulsar任务运行状态
    :param task: 任务详情
    :param type: 任务类型，source，sink，all
    :return: 任务状态map
    """
    result = {}
    # 先查source任务
    if type in ["all", "source"]:
        url = "http://{}/admin/v3/sources/{}/{}/{}/status".format(
            settings.pulsar_io_url[task.geog_area],
            settings.pulsar_tenant,
            settings.pulsar_namespace,
            task.source_name,
        )
        res = requests.get(url)
        logger.info("get source result: code={}, rep={}".format(res.status_code, res.text))
        result["source"] = res.json()
    # 再查sink任务
    if type in ["all", "sink"]:
        url = "http://{}/admin/v3/sinks/{}/{}/{}/status".format(
            settings.pulsar_io_url[task.geog_area],
            settings.pulsar_tenant,
            settings.pulsar_namespace,
            task.dest_name,
        )
        res = requests.get(url)
        logger.info("get sink result: code={}, rep={}".format(res.status_code, res.text))
        result["sink"] = res.json()
    return result
