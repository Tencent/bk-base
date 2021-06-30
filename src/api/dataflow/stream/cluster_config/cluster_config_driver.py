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
import time

import requests
from conf.dataapi_settings import SYSTEM_ADMINISTRATORS
from django.db import transaction

from dataflow import pizza_settings
from dataflow.pizza_settings import (
    FLINK_ADVANCED_CONTAINERS,
    FLINK_ADVANCED_JOB_MANAGER_MEMORY,
    FLINK_ADVANCED_SLOTS,
    FLINK_ADVANCED_TASK_MANAGER_MEMORY,
    FLINK_STANDARD_CONTAINERS,
    FLINK_STANDARD_JOB_MANAGER_MEMORY,
    FLINK_STANDARD_SLOTS,
    FLINK_STANDARD_TASK_MANAGER_MEMORY,
    FLINK_VERSION,
    MAX_FLINK_YARN_SESSION_SLOTS,
)
from dataflow.shared.datamanage.datamanage_helper import DatamanageHelper
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.lock import Lock
from dataflow.shared.log import stream_logger as logger
from dataflow.shared.meta.cluster_group_config.cluster_group_config_helper import ClusterGroupConfigHelper
from dataflow.shared.resourcecenter.resourcecenter_helper import ResourceCenterHelper
from dataflow.shared.send_message import send_message
from dataflow.shared.stream.utils.resource_util import (
    add_flink_session_blacklist,
    check_storm_cluster_exist,
    delete_flink_yarn_session_standard,
    get_flink_session_std_cluster,
    get_flink_yarn_session_id,
    get_flink_yarn_session_queue_name,
    get_storm_standalone_std_cluster,
    remove_flink_session_blacklist,
    update_flink_yarn_session_priority,
    update_storm_standalone_priority,
)
from dataflow.shared.utils.concurrency import concurrent_call_func
from dataflow.stream.api import stream_jobnavi_helper
from dataflow.stream.cluster_config.util.ScheduleHandler import ScheduleHandler
from dataflow.stream.exceptions.comp_execptions import ClusterNameIllegalError, FlinkClusterTypeError
from dataflow.stream.handlers import processing_stream_job
from dataflow.stream.job.job_for_flink import force_kill_flink_job
from dataflow.stream.settings import COMMON_MAX_COROUTINE_NUM, DEFAULT_GEOG_AREA_CODE, FLINK_VERSIONS, DeployMode


def create_cluster_config(args):
    if args["component_type"] == "storm":
        create_storm_cluster(args)
    elif args["component_type"] == "flink":
        # 只支持创建管理session类型集群，其它类型集群由资源系统负责初始化和创建
        if args.get("cluster_type", "") == "yarn-session":
            apply_flink_yarn_session(args)
        else:
            raise FlinkClusterTypeError()


def update_cluster_config(args, cluster_name):
    if args["component_type"] == "storm":
        update_storm_cluster(args, cluster_name)


def delete_cluster_config(args, cluster_name):
    geog_area_code = args["geog_area_code"]
    if args["component_type"] == "flink":
        # debug集群不注册到资源系统
        if not cluster_name.startswith("debug"):
            delete_flink_yarn_session_standard(geog_area_code, cluster_name)
        cluster_id = JobNaviHelper.get_jobnavi_cluster("stream")
        force_kill_flink_job(geog_area_code, cluster_id, cluster_name)


def create_storm_cluster(args):
    cluster_name = args["cluster_name"]
    cluster_group = args["cluster_group"]
    cluster_domain = args["cluster_domain"]
    geog_area_code = args["geog_area_code"]
    check_storm_cluster_exist(cluster_name)
    # 注册到资源系统
    src_cluster_id = "{cluster_group}__{cluster_name}".format(cluster_group=cluster_domain, cluster_name=cluster_name)
    capacity = {"connection_info": json.dumps({"cluster_domain": cluster_domain})}
    ResourceCenterHelper.create_or_update_cluster(
        cluster_group=cluster_group,
        geog_area_code=geog_area_code,
        cluster_name=cluster_name,
        src_cluster_id=src_cluster_id,
        resource_type="processing",
        service_type="stream",
        component_type="storm",
        cluster_type="standalone",
        update_type="full",
        active=1,
        priority=1,
        **capacity
    )


def update_storm_cluster(args, cluster_name):
    cluster_group = args["cluster_group"]
    cluster_domain = args["cluster_domain"]
    geog_area_code = args["geog_area_code"]
    src_cluster_id = "{cluster_group}__{cluster_name}".format(cluster_group=cluster_domain, cluster_name=cluster_name)
    capacity = {"connection_info": json.dumps({"cluster_domain": cluster_domain})}
    ResourceCenterHelper.create_or_update_cluster(
        cluster_group=cluster_group,
        geog_area_code=geog_area_code,
        cluster_name=cluster_name,
        src_cluster_id=src_cluster_id,
        resource_type="processing",
        service_type="stream",
        component_type="storm",
        cluster_type="standalone",
        update_type="full",
        active=1,
        **capacity
    )


def validate_flink_version(type_id, version):
    # 校验配置
    if not FLINK_VERSIONS.get(type_id):
        raise Exception("type_id({}) is invalid, support {}".format(type_id, ",".join(list(FLINK_VERSIONS.keys()))))
    if FLINK_VERSIONS.get(type_id) != version:
        raise Exception(
            "type_id(%s) and version(%s) are not match, need version of %s."
            % (type_id, version, FLINK_VERSIONS.get(type_id))
        )


def apply_flink_yarn_session(params):
    operate_meta = params.get("operate_meta", "add")
    cluster_group = params.get("cluster_group")
    if ClusterGroupConfigHelper.is_default_cluster_group(cluster_group):
        cluster_group = "default"
    geog_area_code = params.get("geog_area_code")
    cluster_name = params.get("cluster_name")
    extra_info = params.get("extra_info")
    version = params.get("version")
    type_id = params.get("type_id", "stream")
    # 校验配置
    validate_flink_version(type_id, version)
    # 从资源系统获取cluster_group对应的session队列
    session_queue_name = get_flink_yarn_session_queue_name(geog_area_code, cluster_group)
    extra_info["yarn_queue"] = session_queue_name
    # jobnavi使用资源组确定队列
    extra_info["cluster_group_id"] = cluster_group
    extra_info["service_type"] = "stream"
    extra_info["component_type"] = "flink"
    if "standard" not in cluster_name:
        raise ClusterNameIllegalError()

    cluster_id = JobNaviHelper.get_jobnavi_cluster("stream")
    jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(geog_area_code, cluster_id)
    # 注册jobnavi元数据
    register_params = {
        "schedule_id": cluster_name,
        "type_id": type_id,
        "description": type_id,
        "extra_info": str(json.dumps(extra_info)),
        "operate": operate_meta,
        "recovery": {"enable": True},
        "max_running_task": 1,
    }
    # 增加 jobnavi runner 标签信息
    stream_job_node_label = getattr(pizza_settings, "STREAM_JOB_NODE_LABEL", None)
    if stream_job_node_label:
        register_params["node_label"] = stream_job_node_label
    jobnavi_stream_helper.execute_schedule_meta_data(cluster_name, register_params)
    # 假如execute id存在，则判断yarn session存在
    execute_id = jobnavi_stream_helper.get_yarn_session_execute_id(cluster_name)
    if not execute_id:
        # 执行注册的元数据
        execute_id = jobnavi_stream_helper.execute_yarn_schedule(cluster_name)
        # 查看jobnavi对应的execute id是否执行
        data = jobnavi_stream_helper.get_execute_status(execute_id)
        if data["status"] == "skipped":
            raise Exception("The execute_id(%s) is duplicated. " % execute_id)
        # 等待yarn session部署成功
        jobnavi_stream_helper.get_yarn_application_status(execute_id)

    # 开始注册集群到资源系统
    capacity = {}
    if params.get("cpu", -1) > 0.0:
        capacity["cpu"] = params["cpu"]
    if params.get("memory", -1) > 0.0:
        capacity["memory"] = params["memory"]
    if params.get("disk", -1) > 0.0:
        capacity["disk"] = params["disk"]

    _register_flink_session_to_resource(cluster_group, geog_area_code, cluster_name, **capacity)


def _register_flink_session_to_resource(cluster_group, geog_area_code, cluster_name, **capacity):
    """
    注册实时flink session集群到资源系统
    :param cluster_group: 集群组
    :param geog_area_code: 地域
    :param cluster_name: 集群名称如：default_standard2
    :param capacity:
    :return:
    """
    # debug集群不需要注册
    if "debug" != cluster_group:
        # 注册到资源系统
        src_cluster_id = "{cluster_group}__{cluster_name}".format(
            cluster_group=cluster_group, cluster_name=cluster_name
        )
        belongs_to = get_flink_yarn_session_id(geog_area_code, cluster_group)
        ResourceCenterHelper.create_or_update_cluster(
            cluster_group=cluster_group,
            geog_area_code=geog_area_code,
            cluster_name=cluster_name,
            src_cluster_id=src_cluster_id,
            resource_type="processing",
            service_type="stream",
            component_type="flink",
            cluster_type="yarn-session",
            update_type="full",
            belongs_to=belongs_to,
            active=1,
            priority=1,
            **capacity
        )


# 通过storm原生api，获取集群的空闲资源比率
def get_cluser_free_slot_ratio(nimbus_ip):
    try:
        summary = json.loads(requests.get("http://%s:8080/api/v1/cluster/summary" % nimbus_ip).content)
        if summary["slotsUsed"] >= 750:
            return float(-1)
        free_slot_ratio = summary["slotsFree"] / float(summary["slotsTotal"])
        return free_slot_ratio
    except Exception:
        return float(-1)


@transaction.atomic()
def election_storm_cluster(cluster_group):
    result_list = get_storm_standalone_std_cluster(cluster_group)
    try:
        if result_list:
            cluster_free_ratios = {}
            for result in result_list:
                cluster_name = result["cluster_name"]
                connection_info = json.loads(result["connection_info"])
                nimbus_domain = connection_info["cluster_domain"]
                free_ratio = get_cluser_free_slot_ratio(nimbus_domain)
                cluster_free_ratios[cluster_name] = free_ratio
            # 获取空闲率最大的集群domain对应的cluster_name
            priority_cluster_name = max(list(cluster_free_ratios.items()), key=lambda x: x[1])[0]
            # 将选举后的nimbus的优先级置为1,其他为0
            update_storm_standalone_priority(cluster_group, priority_cluster_name)
            logger.info("当前cluster_group({})，选举集群({})".format(cluster_group, priority_cluster_name))
        else:
            raise Exception("Don't have this cluster set %s" % cluster_group, "0E.0E.5020")
    except Exception as e:
        code = "0E.0E.5021"
        logger.exception(e, code)
        raise Exception("Election priority storm cluster failed.", "0E.0E.5021")


def _get_flink_cluster_overview(geog_area_code, jobnavi_cluster_id, cluster_name):
    """
    获取 flink yarn-session 集群 overview 信息
    @param geog_area_code:
    @param jobnavi_cluster_id:
    @param cluster_name:
    @return:
    """
    jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(geog_area_code, jobnavi_cluster_id)
    logger.info("[_get_flink_cluster_overview] start to get execute_id")
    execute_id = jobnavi_stream_helper.get_execute_id(cluster_name)
    # 调用获取 job 状态的接口
    logger.info("[_get_flink_cluster_overview] start to send event of cluster_overview")
    event_id = jobnavi_stream_helper.send_event(execute_id, "cluster_overview")
    # 根据 event_id 获取 overview 信息
    logger.info("[_get_flink_cluster_overview] start to list event result of cluster_overview")
    overview = json.loads(jobnavi_stream_helper.list_jobs_status(event_id))
    return overview


def get_flink_cluster_load_state(overview):
    """
    根据 overview 分析集群负载情况
    @param overview:
        {
            'slots-total': 1
            'jobs-running': 1
            ...
        }
    @return:
        {
            'value': 100
            'available': false
        }
    """
    return {
        "value": overview["slots-total"],
        "available": overview["slots-total"] < MAX_FLINK_YARN_SESSION_SLOTS,
    }


@transaction.atomic()
def _update_flink_cluster_priority(max_priority_clusters):
    """
    将指定集群配置的优先级置为1，其它的置为0(基于 cluster_group, cluster_label, version, geog_area_code)
    更新时跳过黑名单内的集群（优先级为-1的即为黑名单内的集群）
    @param max_priority_clusters:
        [
            {
                'cluster_group': '',
                'cluster_label': '',
                'cluster_name': '',
                'version': '',
                'geog_area_code': ''
            }
        ]
    @return:
    """
    for max_priority_cluster in max_priority_clusters:
        geog_area_code = max_priority_cluster["geog_area_code"]
        cluster_group = max_priority_cluster["cluster_group"]
        # cluster_label = max_priority_cluster["cluster_label"]
        # version = max_priority_cluster["version"]
        cluster_name = max_priority_cluster["cluster_name"]
        # 更新flink session集群优先级，将资源组内选举出来的集群优先级升高，将其余集群优先级降低
        update_flink_yarn_session_priority(geog_area_code, cluster_group, cluster_name)


@transaction.atomic()
def add_flink_cluster_blacklist(geog_area_code, cluster_name):
    """
    基于 geog_area_code, cluster_name
    将集群加入黑名单内（优先级置为-1，不参与选举）
    @param:
        {
            'geog_area_code': '',
            'cluster_name': ''
        }
    @return:
    """
    add_flink_session_blacklist(geog_area_code, cluster_name)


@transaction.atomic()
def remove_flink_cluster_blacklist(geog_area_code, cluster_name):
    """
    基于 geog_area_code, cluster_name
    将集群从黑名单内移除（优先级置为0，参与下一轮的选举）
    @param:
        {
            'geog_area_code': '',
            'cluster_name': ''
        }
    @return:
    """
    remove_flink_session_blacklist(geog_area_code, cluster_name)


def _apply_yarn_session(cluster_configs):
    """
    申请 yarn-session 集群流程，注意，若发送消息组件都发生异常，可能管理员会在不收到信息的情况下成功/失败创建集群
    @param cluster_configs:
        [{
            'cluster_group: 'xxx',
            'cluster_label': 'xxx',
            'cluster_name': 'xxx',
            'version': 'xxx',
            'geog_area_code': 'inland'
        }]
    @return:
    """
    if not cluster_configs:
        return
    # 需要申请 yarn-session 前发消息通知
    title = "《创建 yarn-session 集群》"
    message_template = (
        "集群组: %(cluster_group)s, 配置: %(cluster_label)s," " 集群名称: %(cluster_name)s, 版本: %(version)s, 状态: %(status)s"
    )
    try:
        content = "<br>".join(
            map(
                lambda _cluster_config: message_template
                % {
                    "cluster_group": _cluster_config["cluster_group"],
                    "cluster_label": _cluster_config["cluster_label"],
                    "cluster_name": _cluster_config["cluster_name"],
                    "version": _cluster_config["version"],
                    "status": "pending",
                },
                cluster_configs,
            )
        )
        send_message(SYSTEM_ADMINISTRATORS, title, content, raise_exception=False)
        params = {
            "component_type": "flink",
            "cluster_type": "yarn-session",
            "cluster_group": None,
            "cluster_name": None,
            "version": None,
            "extra_info": {
                "slot": 0,
                "job_manager_memory": 0,
                "task_manager_memory": 0,
                "container": 0,
            },
            "geog_area_code": None,
        }
        for cluster_config in cluster_configs:
            if cluster_config["cluster_label"] == "standard":
                params["version"] = cluster_config["version"]
                params["extra_info"]["slot"] = FLINK_STANDARD_SLOTS
                params["extra_info"]["job_manager_memory"] = FLINK_STANDARD_JOB_MANAGER_MEMORY
                params["extra_info"]["task_manager_memory"] = FLINK_STANDARD_TASK_MANAGER_MEMORY
                params["extra_info"]["container"] = FLINK_STANDARD_CONTAINERS
            else:
                params["version"] = cluster_config["version"]
                params["extra_info"]["slot"] = FLINK_ADVANCED_SLOTS
                params["extra_info"]["job_manager_memory"] = FLINK_ADVANCED_JOB_MANAGER_MEMORY
                params["extra_info"]["task_manager_memory"] = FLINK_ADVANCED_TASK_MANAGER_MEMORY
                params["extra_info"]["container"] = FLINK_ADVANCED_CONTAINERS
            params.update(cluster_config)
            apply_flink_yarn_session(params)
    except Exception as e:
        logger.exception(e)
        content = "申请 yarn-session 集群失败，原因：{}".format(e)
        send_message(SYSTEM_ADMINISTRATORS, title, content, raise_exception=False)
        raise e
    else:
        # 成功申请之后发消息通知
        content = "<br>".join(
            map(
                lambda _cluster_config: message_template
                % {
                    "cluster_group": _cluster_config["cluster_group"],
                    "cluster_label": _cluster_config["cluster_label"],
                    "cluster_name": _cluster_config["cluster_name"],
                    "version": _cluster_config["version"],
                    "status": "finished",
                },
                cluster_configs,
            )
        )
        send_message(SYSTEM_ADMINISTRATORS, title, content, raise_exception=False)


def _get_yarn_session_num(cluster_group, cluster_label, cluster_name):
    """
    获取 yarn session 集群编号，未按标准格式命名的均返回0
    @param cluster_group:
    @param cluster_label:
    @param cluster_name:
    @return:
    """
    version_prefix = ""
    cluster_name_prefix = "{}_{}{}".format(cluster_group, cluster_label, version_prefix)
    _cluster_name_split = cluster_name.split(cluster_name_prefix)
    if not _cluster_name_split or len(_cluster_name_split) < 2:
        return 0
    try:
        return int(_cluster_name_split[1])
    except ValueError:
        return 0


@Lock.func_lock(Lock.LockTypes.ELECTION_FLINK_CLUSTER)
def election_flink_cluster(cluster_group=None):
    """
    根据 cluster_group 的取值，选择单个选举或全部选举
    当优先级为-1时表示该集群被加入黑名单，不参与选举。选举结果也不更新黑名单的集群
    只有手动将集群的优先级从-1更改为0时，集群方可再次参与选举
    debug集群不注册到资源系统
    @param cluster_group:
    @return:
    """
    cluster_label = "standard"
    result_list = get_flink_session_std_cluster(resource_group_id=cluster_group, active=1)
    # 根据 cluster_group, cluster_label, version 进行选举或另起新集群
    flink_cluster_overview = []
    func_info = []
    jobnavi_cluster_info = {}
    for result in result_list:
        resource_group_id = result["resource_group_id"]
        cluster_name = result["cluster_name"]
        geog_area_code = result["geog_area_code"]
        flink_cluster_overview.append(
            {
                "cluster_group": resource_group_id,
                "cluster_label": cluster_label,
                "cluster_name": cluster_name,
                "geog_area_code": geog_area_code,
                "version": FLINK_VERSION,
                "overview": {},
            }
        )
        if geog_area_code not in jobnavi_cluster_info:
            jobnavi_cluster_id = JobNaviHelper.get_jobnavi_cluster("stream")
            jobnavi_cluster_info[geog_area_code] = jobnavi_cluster_id
        jobnavi_cluster_id = jobnavi_cluster_info[geog_area_code]
        params = {
            "geog_area_code": geog_area_code,
            "jobnavi_cluster_id": jobnavi_cluster_id,
            "cluster_name": cluster_name,
        }
        func_info.append([_get_flink_cluster_overview, params])
    # 控制最大并发为 COMMON_MAX_COROUTINE_NUM
    threads_res = []
    for segment in [
        func_info[i : i + COMMON_MAX_COROUTINE_NUM] for i in range(0, len(func_info), COMMON_MAX_COROUTINE_NUM)
    ]:
        threads_res.extend(concurrent_call_func(segment))
    # 根据 cluster_group, cluster_label, version 进行分组，每一个组中的值为一个选举集合
    grouped_flink_cluster_info = {}
    error_clusters = []
    for idx, flink_cluster in enumerate(flink_cluster_overview):
        # 若线程调用异常返回，threads_res[idx] 为 None
        overview = threads_res[idx] or {}
        if not overview:
            # 获取当前集群信息失败
            error_clusters.append(
                {
                    "cluster_group": flink_cluster["resource_group_id"],
                    "cluster_label": cluster_label,
                    "cluster_name": flink_cluster["cluster_name"],
                    "version": FLINK_VERSION,
                }
            )
            continue
        flink_clusters_to_elect = grouped_flink_cluster_info.setdefault(
            (
                flink_cluster["cluster_group"],
                flink_cluster["cluster_label"],
                flink_cluster["version"],
            ),
            [],
        )
        flink_clusters_to_elect.append(
            {
                "cluster_name": flink_cluster["cluster_name"],
                "geog_area_code": flink_cluster["geog_area_code"],
                "overview": overview,
            }
        )
    # 打印获取信息异常的 yarn-session 集群
    if error_clusters:
        content = "<br>".join(
            map(
                lambda _cluster: "资源组: {}, 集群名称: {}".format(_cluster["cluster_group"], _cluster["cluster_name"]),
                error_clusters,
            )
        )
        logger.exception("get yarn-session schedule info error: %s\n" % content)
        send_message(SYSTEM_ADMINISTRATORS, "《获取 yarn-session 集群信息失败》", content)
    max_priority_clusters = []
    apply_flink_clusters = []
    for group_key, _flink_clusters_info in list(grouped_flink_cluster_info.items()):
        # 在 _flink_clusters_info 中将负载最低的集群的优先级赋值为1，其它为0
        # 若 _flink_clusters_info 中所有集群都是非正常(非低负载)状态，需要根据 group_key 申请新集群
        max_yarn_session_num = -1
        max_priority_cluster_name = None
        min_cluster_load_state = -1
        # 标记当前 cluster_group、cluster_label 是否需要启动集群
        has_available_cluster = False
        # 一个 group_key 必定对应一个 geog_area_code
        geog_area_code = None
        for _flink_cluster_info in _flink_clusters_info:
            _load_state = get_flink_cluster_load_state(_flink_cluster_info["overview"])
            _yarn_session_num = _get_yarn_session_num(group_key[0], group_key[1], _flink_cluster_info["cluster_name"])
            if _yarn_session_num > max_yarn_session_num:
                max_yarn_session_num = _yarn_session_num
            # 该标配(高配)集群组存在可用资源，不申请新集群
            has_available_cluster = _load_state["available"] | has_available_cluster
            if min_cluster_load_state == -1 or _load_state["value"] <= min_cluster_load_state:
                max_priority_cluster_name = _flink_cluster_info["cluster_name"]
                min_cluster_load_state = _load_state["value"]
            geog_area_code = _flink_cluster_info["geog_area_code"]
        if not has_available_cluster:
            apply_flink_clusters.append(
                {
                    "cluster_group": group_key[0],
                    "cluster_label": group_key[1],
                    "version": group_key[2],
                    "cluster_name": "{}_{}{}".format(group_key[0], group_key[1], max_yarn_session_num + 1),
                    "geog_area_code": geog_area_code,
                }
            )
        # 对于当前的 group_key, 需要将 cluster_name 对应的记录的优先级置为最高
        max_priority_clusters.append(
            {
                "cluster_group": group_key[0],
                "cluster_label": group_key[1],
                "version": group_key[2],
                "cluster_name": max_priority_cluster_name,
                "geog_area_code": geog_area_code,
            }
        )
    # 重置优先级
    _update_flink_cluster_priority(max_priority_clusters)
    # 申请集群
    if apply_flink_clusters:
        _apply_yarn_session(apply_flink_clusters)


@Lock.func_lock(Lock.LockTypes.MONITOR_FLINK_JOB)
def monitor_flink_job():
    schedule_ids = []
    # 当前监控功能只针对 SQL 实时计算任务
    running_flink_job_info = processing_stream_job.filter(
        component_type="flink", status="running", implement_type="sql"
    )
    for running_flink_job in running_flink_job_info:
        if running_flink_job.deploy_mode == DeployMode.YARN_SESSION.value:
            if running_flink_job.cluster_name not in schedule_ids:
                schedule_ids.append(running_flink_job.cluster_name)
        elif running_flink_job.deploy_mode == DeployMode.YARN_CLUSTER.value:
            schedule_ids.append(running_flink_job.stream_id)
    func_info = []
    for schedule_id in schedule_ids:
        # TODO: 仅用于内部版自我监控，暂不考虑多地域
        func_info.append([ScheduleHandler(schedule_id, DEFAULT_GEOG_AREA_CODE).list_status, {}])
    failing_schedule = {}
    error_list_application_status = []
    # 并发获取集群上失败或者异常的apps
    fetch_error_apps(error_list_application_status, failing_schedule, func_info, schedule_ids)
    if error_list_application_status:
        send_message(
            SYSTEM_ADMINISTRATORS,
            "《获取 yarn application 概要信息失败》",
            "<br>".join(error_list_application_status[:5]),
            raise_exception=False,
        )
    func_info = []
    for status, schedule_list in list(failing_schedule.items()):
        for schedule_id in schedule_list:
            message = {
                "time": int(time.time()),
                "database": "monitor_custom_metrics",
                "stream_flink_failed_job": {
                    "status": status,
                    "schedule_id": schedule_id,
                    "tags": {"schedule_id": schedule_id, "status": status},
                },
            }
            func_info.append(
                [
                    DatamanageHelper.op_metric_report,
                    {
                        "message": message,
                        "kafka_topic": getattr(pizza_settings, "BK_MONITOR_METRIC_TOPIC"),
                        "tags": [DEFAULT_GEOG_AREA_CODE],
                    },
                ]
            )
    for segment in [
        func_info[i : i + COMMON_MAX_COROUTINE_NUM] for i in range(0, len(func_info), COMMON_MAX_COROUTINE_NUM)
    ]:
        threads_res = concurrent_call_func(segment)
        for index, res in enumerate(threads_res):
            if res is None:
                logger.exception("Error when sending %s" % segment[index][1].get("message"))


def fetch_error_apps(error_list_application_status, failing_schedule, func_info, schedule_ids):
    start_index = 0
    for segment in [
        func_info[i : i + COMMON_MAX_COROUTINE_NUM] for i in range(0, len(func_info), COMMON_MAX_COROUTINE_NUM)
    ]:
        threads_res = concurrent_call_func(segment)
        for segment_index, application_info in enumerate(threads_res):
            if application_info:
                for per_job_info in application_info["jobs"]:
                    # 如果状态非 running、持续时间超过 5 分钟、task 中存在正在 canceling 的任务
                    if (
                        per_job_info.get("state").lower() not in ["running"]
                        and per_job_info.get("duration", 0) > 300000
                        and per_job_info.get("tasks", {}).get("canceling") > 0
                    ):
                        schedule_list = failing_schedule.setdefault(per_job_info.get("state"), [])
                        schedule_list.append(schedule_ids[start_index + segment_index])
                        # 任意任务异常则表明 application 任务异常
                        break
            else:
                error_list_application_status.append(schedule_ids[start_index + segment_index])
        start_index = start_index + len(segment)
