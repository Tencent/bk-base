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
from urllib.parse import quote

import requests
from common.log import logger
from datahub.common.const import (
    CLUSTER,
    CLUSTER_NAME,
    CLUSTERINFO,
    CONNECTION_INFO,
    DATA_TOKEN,
    EXPIRED,
    GRANTED,
    GROUPID,
    HOST,
    ID,
    KAFKA,
    NAMESPACE,
    NOAUTH,
    PASSWORD,
    PORT,
    PULSAR,
    QUEUE,
    QUEUE_PASSWORD,
    QUEUE_PULSAR,
    QUEUE_USER,
    REVOKED,
    TENANT,
    TOPIC,
    TYPE,
    USERNAME,
    VERSION,
)
from datahub.databus.api import AuthApi, StoreKitApi

from datahub.databus import channel, cmd_helper, exceptions, model_manager, rt, settings


def add_queue_user(user, password):
    """
    通过调用命令行工具添加队列服务的用户，用于访问队列服务的kafka集群
    :param user: 用户名
    :param password: 密码
    :return: True/False，是否添加用户成功
    """
    succ = True
    for zk_addr in get_queue_zk_addrs():
        add_user_succ, output = cmd_helper.get_cmd_response(
            [
                settings.KAFKA_CONFIG_SCRIPT,
                "--zookeeper",
                zk_addr,
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                user,
                "--add-config",
                "SCRAM-SHA-256=[password={}],SCRAM-SHA-512=[password={}]".format(password, password),
            ]
        )
        model_manager.add_databus_oplog("add_queue_user", "", zk_addr, user, output)
        succ = succ and add_user_succ

    return succ


def remove_queue_user(user):
    """
    通过调用命令行工具删除队列服务的用户
    :param user: 用户名
    :return: True/False，是否删除用户成功
    """
    succ = True
    for zk_addr in get_queue_zk_addrs():
        remove_user_succ, output = cmd_helper.get_cmd_response(
            [
                settings.KAFKA_CONFIG_SCRIPT,
                "--zookeeper",
                zk_addr,
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                user,
                "--delete-config",
                "SCRAM-SHA-256,SCRAM-SHA-512",
            ]
        )
        model_manager.add_databus_oplog("remove_queue_user", "", zk_addr, user, output)
        succ = succ and remove_user_succ

    return succ


def list_queue_user():
    """
    通过调用命令行工具获取队列服务账户列表
    :return: 访问队列服务的账户列表
    """
    result = {}
    for zk_addr in get_queue_zk_addrs():
        succ, output = cmd_helper.get_cmd_response(
            [
                settings.KAFKA_CONFIG_SCRIPT,
                "--zookeeper",
                zk_addr,
                "--describe",
                "--entity-type",
                "users",
            ]
        )
        if succ:
            users = []
            for line in output.split("\n"):
                if line.startswith("Configs for user-principal '"):
                    user = line.replace("Configs for user-principal '", "").split("'")[0]
                    users.append(user)
                else:
                    logger.warning("bad cmd output line for list queue users: %s" % line)

            result[zk_addr] = users
        else:
            result[zk_addr] = {}

    return result


def add_auth_for_queue(rt_ids, user):
    """
    通过调用命令行工具添加消费组和topic的授权（可重复添加，需要kafka版本0.10.2.0或以上版本）
    :param rt_ids: 访问的rt_id的列表
    :param user: 用户名
    :return: True/False，是否授权成功
    """

    for rt_id in rt_ids:
        rt_info = rt.get_rt_fields_storages(rt_id)
        storage_dict = rt_info.get("storages")
        if QUEUE not in storage_dict.keys() and QUEUE_PULSAR not in storage_dict.keys():
            raise exceptions.AuthError()
    succ = True
    # 先pulsar授权
    rst = StoreKitApi.cluster_config.list({"cluster_type": "queue_pulsar"})
    if rst.is_success():
        pulsar_user = quote(user)
        for cluster in rst.data:
            config = cluster["connection"]
            for rt_id in rt_ids:
                succ = _grant_pulsar_permissions(
                    config["host"],
                    config["http_port"],
                    config["tenant"],
                    config["namespace"],
                    "queue_%s" % rt_id,
                    pulsar_user,
                )
                if succ is False:
                    return False
    else:
        return False

    # 其次kafka授权
    for zk_addr in get_queue_zk_addrs():
        cmd_args = [
            settings.KAFKA_ACL_SCRIPT,
            "--authorizer",
            "kafka.security.auth.SimpleAclAuthorizer",
            "--authorizer-properties",
            "zookeeper.connect=%s" % zk_addr,
            "--add",
            "--allow-principal",
            "User:%s" % user,
            "--consumer",
            "--group",
            "queue-%s" % user,
        ]
        for rt_id in rt_ids:
            cmd_args.append("--topic")
            cmd_args.append("queue_%s" % rt_id)

        read_group_succ, output = cmd_helper.get_cmd_response(cmd_args)
        model_manager.add_databus_oplog("add_auth_for_queue", "", zk_addr, "{} -> {}".format(user, rt_ids), output)
        succ = succ and read_group_succ

    if succ is False:
        return False

    return _register_to_queue(rt_ids, user)


def remove_auth_for_queue(rt_ids, user):
    """
    通过调用命令行工具删除用户对指定的队列服务上的topic的访问权限
    :param rt_ids: ResultTable的id列表
    :param user: 用户名
    :return: True/False，是否删除权限成功
    """
    succ = True
    # 先pulsar删除权限
    rst = StoreKitApi.cluster_config.list({"cluster_type": "queue_pulsar"})
    if rst.is_success():
        pulsar_user = quote(user)
        for cluster in rst.data:
            config = cluster["connection"]
            for rt_id in rt_ids:
                succ = _revoke_pulsar_permissions(
                    config["host"],
                    config["http_port"],
                    config["tenant"],
                    config["namespace"],
                    "queue_%s" % rt_id,
                    pulsar_user,
                )
                if succ is False:
                    return False
    else:
        return False

    for zk_addr in get_queue_zk_addrs():
        cmd_args = [
            settings.KAFKA_ACL_SCRIPT,
            "--authorizer",
            "kafka.security.auth.SimpleAclAuthorizer",
            "--authorizer-properties",
            "zookeeper.connect=%s" % zk_addr,
            "--remove",
            "--allow-principal",
            "User:%s" % user,
            "--operation",
            "Read",
            "--operation",
            "Describe",
            "--force",
        ]
        for rt_id in rt_ids:
            cmd_args.append("--topic")
            cmd_args.append("queue_%s" % rt_id)

        remove_succ, output = cmd_helper.get_cmd_response(cmd_args)
        model_manager.add_databus_oplog(
            "remove_auth_for_queue",
            "",
            zk_addr,
            "{} -> {}".format(user, rt_ids),
            remove_succ,
        )
        succ = succ and remove_succ

    if succ is False:
        return succ

    return _deregister_to_queue(rt_ids, user)


def _grant_pulsar_permissions(host, port, tenant, namespace, topic, user):
    url = "http://{}:{}/admin/v2/persistent/{}/{}/{}/permissions/{}".format(
        host,
        port,
        tenant,
        namespace,
        topic,
        user,
    )
    headers = {
        "Authorization": "Bearer %s" % settings.pulsar_queue_token,
        "Content-type": "application/json",
    }
    respond = requests.post(url, data=json.dumps(["consume"]), headers=headers)

    if respond.status_code in [204, 200]:
        return True
    else:
        logger.error("failed to grant a permission on a topic: url={}, respond={}".format(url, respond.text))
        return False


def _revoke_pulsar_permissions(host, port, tenant, namespace, topic, user):
    url = "http://{}:{}/admin/v2/persistent/{}/{}/{}/permissions/{}".format(
        host,
        port,
        tenant,
        namespace,
        topic,
        user,
    )
    headers = {"Authorization": "Bearer %s" % settings.pulsar_queue_token}
    respond = requests.delete(url, headers=headers)

    if respond.status_code in [204, 200, 412]:
        return True
    else:
        logger.error("failed to revoke permissions on a topic: url={}, respond={}".format(url, respond.text))
        return False


def _deregister_to_queue(rt_ids, user):
    """
    向队列服务器注销信息
    :param rt_ids: result_table_id列表
    :param user: 用户
    """
    group_id = _get_queue_group_name(user)
    for rt_id in rt_ids:
        topic = _get_queue_topic_name(rt_id)
        result = model_manager.get_queue_consumer_config_by_topic(group_id, topic, user)
        if result is not None:
            result.status = EXPIRED
            result.save()
            model_manager.create_queue_consumer_config(
                result.type,
                user,
                group_id,
                topic,
                result.cluster_id,
                "admin",
                "admin",
                REVOKED,
            )


def _register_to_queue(rt_ids, user):
    """
    向队列服务器注册信息
    :param rt_ids: result_table_id列表
    :param user: 用户
    """
    group_id = _get_queue_group_name(user)
    for rt_id in rt_ids:
        topic = _get_queue_topic_name(rt_id)
        queue_cluster = _get_queue_cluster_for_rt(rt_id)
        if queue_cluster is None:
            raise exceptions.AuthError()
        result = model_manager.get_queue_consumer_conf_by_cluster(group_id, topic, user, queue_cluster[ID])
        if result is None:  # 如果没授权，则给当前rt 授权
            model_manager.create_queue_consumer_config(
                queue_cluster[TYPE],
                user,
                group_id,
                topic,
                queue_cluster[ID],
                "admin",
                "admin",
            )


def register_to_custom_queue(rt_id, storage):
    """
    同步当前rt已经授权的信息到指定的storage 类型存储上
    :param rt_id: result_table_id列表
    :param storage: 存储类型
    """
    # 如果当前rt 没有已经授权的信息则不处理
    topic = _get_queue_topic_name(rt_id)
    cluster_type = KAFKA if storage == QUEUE else PULSAR
    # 找出除 cluster_type 以外的授权记录，并将此类型的授权同步到 cluster_type 上
    exist_grants_clusters = model_manager.get_queue_consumer_config_exclude(topic, cluster_type)
    if exist_grants_clusters is None or len(exist_grants_clusters) == 0:
        return

    queue_cluster = _get_queue_cluster_for_rt(rt_id)
    if queue_cluster is None:
        raise exceptions.AuthError()
    # 添加未授权的队列集群类型
    for queue_consumer_config in exist_grants_clusters:
        exist_grant_consumer = model_manager.get_queue_consumer_by_cluster_type(
            queue_consumer_config.group_id,
            topic,
            queue_consumer_config.user_name,
            cluster_type,
        )
        if exist_grant_consumer is None:
            model_manager.create_queue_consumer_config(
                cluster_type,
                queue_consumer_config.user_name,
                queue_consumer_config.group_id,
                topic,
                queue_cluster[ID],
                "admin",
                "admin",
            )


def _get_queue_topic_name(rt_id):
    return QUEUE + "_" + rt_id


def _get_queue_group_name(user):
    return QUEUE + "-" + user


def _get_queue_cluster_for_rt(rt_id):
    """
    根据rt 获取当前rt所关联的queue
    :param rt_id: result_table_id
    :return: queue集群信息
    """
    rt_info = rt.get_databus_rt_info(rt_id)
    queue_cluster = None
    if QUEUE in rt_info.keys() or QUEUE_PULSAR in rt_info.keys():
        if QUEUE in rt_info.keys():
            cluster_type = KAFKA
            storage = QUEUE
        else:
            cluster_type = PULSAR
            storage = QUEUE_PULSAR

        # 从rt 的meta信息中获取关联的queue集群信息
        cluster_name = rt_info[storage][CLUSTER_NAME]
        connection_info = json.loads(rt_info[storage][CONNECTION_INFO])
        tenant = connection_info[TENANT] if TENANT in connection_info else ""
        namespace = connection_info[NAMESPACE] if NAMESPACE in connection_info else ""
        queue_cluster_config = model_manager.get_queue_cluster_config(cluster_name, cluster_type)
        if queue_cluster_config is None:  # 如果队列服务中没记录该队列集群信息则往数据库记录该集群信息
            queue_cluster_config = model_manager.create_queue_cluster_config(
                cluster_name,
                cluster_type,
                connection_info[HOST],
                connection_info[PORT],
                tenant,
                namespace,
            )
        queue_cluster = {ID: queue_cluster_config.id, TYPE: cluster_type}
    return queue_cluster


def get_token_init_info(token):
    """
    获取授权码信息
    :param token: token
    :return:
    """
    queue_user_res = AuthApi.exchange_queue_user.create({DATA_TOKEN: token})
    if not queue_user_res.is_success() or not queue_user_res.data:
        raise Exception("exchange auth failed, try again later")
    result = {
        USERNAME: queue_user_res.data[QUEUE_USER],
        PASSWORD: queue_user_res.data[QUEUE_PASSWORD],
        GROUPID: "{}-{}".format(QUEUE, queue_user_res.data[QUEUE_USER]),
    }
    return result


def get_producer_info(user, topics, version):
    """
    获取producer 授权信息
    :param user: user name
    :param topics: 已授权的topic列表
    :param version: 版本号
    :return:
    {
       "clusterInfo": [ {"topic": "topic1","cluster": "x.x.x.x:9092","version": 1}],                ],
        "noAuth": [{"topic": "topic2","cluster": "x.x.x.x:9092","version": 2}],
    }
    """
    revoke_list = []
    grant_list = []
    for topic in topics.split(","):
        producer_configs = model_manager.get_queue_producer_config(user, topic, version)
        if producer_configs is not None and len(producer_configs) > 0:
            newest_producer_config = producer_configs[0]
            cluster_info = model_manager.get_queue_cluster_config_by_id(newest_producer_config.cluster_id, KAFKA)
            if cluster_info is None:
                continue
            producer_grant_info = {
                TOPIC: topic,
                CLUSTER: "{}:{}".format(cluster_info.cluster_domain, cluster_info.cluster_port),
                VERSION: newest_producer_config.id,
            }
            grant_list.append(producer_grant_info) if newest_producer_config.status == GRANTED else revoke_list.append(
                producer_grant_info
            )
        else:
            if version == 0:
                revoke_list.append({TOPIC: topic, CLUSTER: ""})

    return {CLUSTERINFO: grant_list, NOAUTH: revoke_list}


def get_consumer_info(topics, group_id, version, cluster_type, is_pattern):
    """
    获取producer 授权信息
    :param group_id: group_id
    :param topics: 已授权的topic列表
    :param version: 版本号
    :param cluster_type 集群类型
    :param is_pattern 是否为正则表示的topic
    :return:
    {
       "clusterInfo": [ {"topic": "topic1","cluster": "x.x.x.x:9092","version": 1}],                ],
        "noAuth": [{"topic": "topic2","cluster": "x.x.x.x:9092","version": 2}],
    }
    """

    revoke_list = []
    grant_list = []
    for topic in topics.split(","):
        consumer_configs = model_manager.get_granted_queue_consumer_config(
            group_id, topic, version, cluster_type, is_pattern
        )
        if consumer_configs is not None and len(consumer_configs) > 0:
            newest_consumer_config = consumer_configs[0]
            cluster_info = model_manager.get_queue_cluster_config_by_id(newest_consumer_config.cluster_id, cluster_type)
            if cluster_info is None:
                continue

            topic = (
                "persistent://{}/{}/{}".format(cluster_info.tenant, cluster_info.namespace, topic)
                if cluster_type == settings.COMPONENT_PULSAR_QUEUE
                else topic
            )
            cluster_patten = "pulsar://%s:%s" if cluster_type == settings.COMPONENT_PULSAR_QUEUE else "%s:%s"
            consumer_grant_info = {
                TOPIC: topic,
                CLUSTER: cluster_patten % (cluster_info.cluster_domain, cluster_info.cluster_port),
                VERSION: newest_consumer_config.id,
            }
            grant_list.append(consumer_grant_info) if newest_consumer_config.status == GRANTED else revoke_list.append(
                consumer_grant_info
            )
        else:
            if version == 0:
                revoke_list.append({TOPIC: topic, CLUSTER: "", VERSION: 1})

    return {CLUSTERINFO: grant_list, NOAUTH: revoke_list}


def transfer_topic(topic, src_cluster, dest_cluster):
    """
    将topic 从src_cluster 队列集群迁移至dest_cluster队列集群
    :param topic: 需要迁移的topic
    :param src_cluster: 原来的队列集群
    :param dest_cluster: 目标队列集群
    """
    src_granted_producer_clusters = model_manager.get_granted_queue_producer_by_id(topic, src_cluster)
    if len(src_granted_producer_clusters) > 1:
        raise Exception("too many user to send data to topic %s" % topic)

    src_granted_consumer_clusters = model_manager.get_granted_queue_consumer_by_id(topic, src_cluster)
    if len(src_granted_producer_clusters) == 0 and len(src_granted_consumer_clusters) == 0:
        logger.warning("transfer topic failed! No granted consumer/producer for topic: %s" % topic)
        return

    for granted_producer in src_granted_producer_clusters:
        granted_producer.status = EXPIRED
        granted_producer.save()
        model_manager.create_queue_producer_config(granted_producer.user_name, topic, dest_cluster, "admin", "admin")

    model_manager.create_queue_change_event(topic, src_cluster, dest_cluster)
    for granted_consumer in src_granted_consumer_clusters:
        granted_consumer.status = EXPIRED
        granted_consumer.save()
        model_manager.create_queue_consumer_config(
            granted_consumer.type,
            granted_consumer.user_name,
            granted_consumer.group_id,
            topic,
            dest_cluster,
            "admin",
            "admin",
            GRANTED,
        )


def list_queue_auth():
    """
    通过调用命令行工具查看当前队列服务授权信息列表
    :return: 访问队列服务的授权信息列表
    """
    result = {}
    for zk_addr in get_queue_zk_addrs():
        succ, output = cmd_helper.get_cmd_response(
            [
                settings.KAFKA_ACL_SCRIPT,
                "--authorizer-properties",
                "zookeeper.connect=%s" % zk_addr,
                "--list",
            ]
        )
        if succ:
            # 按照group和topic两个维度获取队列服务的权限信息
            group_auths = {}
            topic_auths = {}
            resource = ""
            auths = []
            for line in output.split("\n"):
                if line.startswith("Current ACLs for resource"):
                    resource = line.replace("Current ACLs for resource `", "").replace("`:", "").strip()
                elif "has Allow permission for operations:" in line:
                    auth = line.replace("has Allow permission for operations: ", "").strip()
                    auths.append(auth)
                    # 暂时只处理Topic和Group的权限信息
                    if resource.startswith("Topic"):
                        topic_auths[resource] = auths
                    elif resource.startswith("Group"):
                        group_auths[resource] = auths
                else:
                    resource = ""
                    auths = []
            result[zk_addr] = {"group": group_auths, "topic": topic_auths}
        else:
            result[zk_addr] = {}

    return result


def get_queue_zk_addrs():
    """
    获取kafka队列服务的zk地址的列表
    :return: kafka队列服务的zk地址的列表
    """
    queues = channel.get_kafka_queues()
    result = []
    for queue in queues:
        result.append("{}:{}{}".format(queue["zk_domain"], queue["zk_port"], queue["zk_root_path"]))

    return result
