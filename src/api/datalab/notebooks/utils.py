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
import re
import uuid
from datetime import datetime
from time import sleep

from common.local import get_request_username
from common.log import sys_logger
from conf.dataapi_settings import (
    DEFAULT_GEOG_AREA_TAG,
    HDFS_DATA_TYPE,
    HTTP_SCHEMA,
    SUPPORT_ASSOCIATED_STORAGE,
)
from datalab.api.dataflow import DataFlowApi
from datalab.api.jupyterhub import (
    create_notebook,
    create_user,
    create_ws,
    delete_notebook,
    start_kernel,
    start_server,
    stop_kernel,
)
from datalab.api.meta import MetaApi
from datalab.api.queryengine import QueryEngineApi
from datalab.api.storekit import StoreKitApi
from datalab.constants import (
    API_OPERATE_LIST,
    BK_BIZ_ID,
    BK_USERNAME,
    BKDATA,
    CANCELED,
    CELL_ID,
    CHANNEL_CLUSTER_CONFIG_ID,
    CLICKHOUSE,
    CLUSTER_NAME,
    CLUSTER_TYPE,
    COMMON,
    CONTENT_NAME,
    CREATE,
    CREATED_BY_NOTEBOOK,
    DATA_PROCESSING,
    DATA_SET_ID,
    DATA_SET_TYPE,
    DATA_TYPE,
    DATALAB,
    DESCRIPTION,
    DESTROY,
    DROP_MODEL,
    ENABLE,
    EXPIRES,
    FAILED,
    FIELDS,
    FINISHED,
    GENERATE_TYPE,
    HDFS,
    ID,
    INPUTS,
    MESSAGE,
    MODEL,
    NOTEBOOK_HOST,
    NOTEBOOK_ID,
    NOTEBOOK_NAME,
    NOTEBOOK_PATTERN,
    OPERATE_OBJECT,
    OPERATE_PARAMS,
    OPERATE_TYPE,
    ORDER_BY,
    OUTPUT_FOREIGN_ID,
    OUTPUTS,
    PARAM,
    PENDING,
    PERSONAL,
    PLATFORM,
    PRIORITY,
    PRIVATE,
    PROCESSING_ALIAS,
    PROCESSING_ID,
    PROCESSING_TYPE,
    PROJECT_ID,
    QUERYSET,
    RESULT_TABLE,
    RESULT_TABLE_ID,
    RESULT_TABLE_NAME,
    RESULT_TABLE_NAME_ALIAS,
    RESULT_TABLES,
    SENSITIVITY,
    SNAPSHOT,
    SQL_LIST,
    STORAGE,
    STORAGE_CLUSTER_CONFIG_ID,
    STORAGE_CONFIG,
    STORAGE_TYPE,
    STORAGES,
    TAGS,
    TEMPLATE_NAMES,
    TOKEN,
    TYPE,
    WITH_DATA,
    WITH_HIVE_META,
)
from datalab.exceptions import (
    ClearDataError,
    CreateStorageError,
    DeleteStorageError,
    MetaTransactError,
    StorageNotSupportError,
    StoragePrepareError,
)
from datalab.notebooks.models import (
    DatalabNotebookExecuteInfoModel,
    DatalabNotebookOutputModel,
    DatalabNotebookTaskModel,
    JupyterHubSpawnersModel,
    JupyterHubUsersModel,
)
from datalab.projects.models import DatalabProjectModel
from datalab.utils import replace_name_prefix, retry_deco
from django.utils.translation import ugettext as _


def create_hub_user(jupyter_username):
    """
    判断jupyterhub中有无此用户，没有的话新增用户

    :param jupyter_username: 用户名
    :return: 执行结果
    """
    is_exists = JupyterHubUsersModel.objects.filter(name=jupyter_username).exists()
    if not is_exists:
        create_user(jupyter_username)


def start_user_notebook_server(jupyter_username):
    """
    判断此用户的jupyter服务有没有启动，没有的话启动服务

    :param jupyter_username: 用户名
    :return: 执行结果
    """
    try:
        user_id = JupyterHubUsersModel.objects.get(name=jupyter_username).id
        server_id = JupyterHubSpawnersModel.objects.get(user_id=user_id).server_id
        if not server_id:
            while True:
                status = start_server(jupyter_username)
                if status == FINISHED:
                    break
                else:
                    sleep(1)
    except JupyterHubUsersModel.DoesNotExist:
        sys_logger.error("failed to find user by jupyter_username %s" % jupyter_username)


def extract_jupyter_username(project_id, project_type):
    """
    提取笔记账号名，笔记的启动账号分两种："项目"是以project_id作为启动账号，"个人"是以转换后的bk_username作为启动账号

    :param project_id: 项目id
    :param project_type: 项目类型
    :return: 笔记账号名
    """
    if project_type == COMMON:
        return project_id
    else:
        bk_username = get_request_username()
        return replace_name_prefix(bk_username)


def create_notebook_name(objs):
    """
    创建笔记任务名
    判断有没有满足 notebook_x 形式的任务名，如果有的话，新创建的笔记为 notebook_max(x)+1，否则为notebook_1

    :param objs: db obj
    :return: 笔记任务名
    """
    max_task_num = 0
    for obj in objs:
        if re.match(NOTEBOOK_PATTERN, obj[NOTEBOOK_NAME]):
            max_task_num = max(max_task_num, int(obj[NOTEBOOK_NAME].split("_")[1]))
    return "notebook_" + str(max_task_num + 1) if max_task_num else "notebook_1"


def notebook_name_is_exists(project_id, project_type, notebook_name):
    """
    判断此项目下是否存在同名笔记任务

    :param project_id: 项目id
    :param project_type: 项目类型
    :param notebook_name: 笔记任务名
    :return:
    """
    task_objs = list(DatalabNotebookTaskModel.objects.filter(project_id=project_id, project_type=project_type).values())
    if any(notebook_name == obj[NOTEBOOK_NAME] for obj in task_objs):
        raise Exception(_("此项目下已存在同名笔记任务"))


def get_library(library_map):
    """
    获取笔记库集合

    :param library_map: 笔记内核库
    :return: 笔记库集合
    """
    return {"libs_order": list(library_map.keys()), "libs": library_map}


def extract_storage(result_tables):
    """
    抽取存储配置

    :param result_tables: 结果表信息
    :return: 存储配置
    """
    if result_tables and result_tables[0].get(STORAGES):
        storages = result_tables[0].get(STORAGES)
        if not isinstance(storages, dict) or len(storages) > 1:
            raise Exception(_("当前仅支持关联一种存储"))

        cluster_type, storage = list(storages.items())[0]
        if cluster_type not in SUPPORT_ASSOCIATED_STORAGE:
            raise StorageNotSupportError(message_kv={PARAM: cluster_type})

        if cluster_type == CLICKHOUSE:
            storage_config = storage.get(STORAGE_CONFIG)
            if ORDER_BY not in json.loads(storage_config):
                raise Exception(_("clickhouse存储必须指定主键"))

        storage[CLUSTER_TYPE] = cluster_type
    else:
        storage = {CLUSTER_TYPE: HDFS}
    generate_cluster_config(storage)
    return storage


def retrieve_result_table_info(result_tables):
    """
    获取结果表信息

    :param result_tables: 结果表列表
    :return: 结果表信息
    """
    result_tables_info = MetaApi.result_tables.list(
        {
            "result_table_ids": result_tables,
            "related_filter": """{
                "type": "processing_type",
                "attr_name": "show",
                "attr_value": "queryset"
            }""",
        }
    )
    return result_tables_info.data


def generate_cluster_config(storage):
    """
    获取数据探索默认的存储集群

    :param storage: 存储信息
    """
    cluster_type = storage.get(CLUSTER_TYPE)
    cluster_name = storage.get(CLUSTER_NAME)
    if not cluster_name:
        # 获取数据探索对应的存储集群
        cluster_res = StoreKitApi.datalab_cluster({CLUSTER_TYPE: cluster_type, TAGS: [DATALAB, DEFAULT_GEOG_AREA_TAG]})
        if cluster_res.data:
            cluster_info = cluster_res.data[0]
            storage_cluster_config_id = cluster_info.get(ID)
            cluster_name = cluster_info.get(CLUSTER_NAME)
        else:
            sys_logger.error(
                "retrieve_datalab_cluster_error|cluster_type: {}|message: {}".format(cluster_type, cluster_res.response)
            )
            raise Exception(_("获取存储集群信息失败"))
    else:
        cluster_info = StoreKitApi.cluster_info(
            {CLUSTER_TYPE: cluster_type, CLUSTER_NAME: cluster_name, TAGS: [DEFAULT_GEOG_AREA_TAG, ENABLE]}
        )
        storage_cluster_config_id = cluster_info.data.get(ID)
    storage[CLUSTER_NAME] = cluster_name
    storage[STORAGE_CLUSTER_CONFIG_ID] = storage_cluster_config_id


class ApiHelper(object):
    @staticmethod
    def meta_transact(bk_username, api_operate_list):
        """
        集合事务操作

        :param bk_username: 用户名
        :param api_operate_list: 集合事务列表
        :return: 操作结果
        """
        request_data = {BK_USERNAME: bk_username, API_OPERATE_LIST: api_operate_list}
        res = MetaApi.transaction(request_data)
        if not res.is_success():
            sys_logger.error(
                "meta_transact_error|api_operate_list:{}|response:{}".format(api_operate_list, res.response)
            )
            message = res.errors[0].get(MESSAGE) if isinstance(res.errors, list) else res.message
            raise MetaTransactError(message_kv={MESSAGE: message}, errors=res.errors)

    @staticmethod
    def destroy_transact(bk_username, result_table_ids, processing_ids):
        """
        集合事务删除rt和dp

        :param bk_username: 用户名
        :param result_table_ids: rt列表
        :param processing_ids: 数据处理id
        :return: 操作结果
        """
        dp_operate_list = DefineOperateHelper.define_destroy_operate(
            processing_ids, DATA_PROCESSING, PROCESSING_ID, bk_username
        )
        rt_operate_list = DefineOperateHelper.define_destroy_operate(
            result_table_ids, RESULT_TABLE, RESULT_TABLE_ID, bk_username
        )
        dp_operate_list.extend(rt_operate_list)
        return ApiHelper.meta_transact(bk_username, dp_operate_list)

    @staticmethod
    def create_rt_storage(result_table_id, storage, processing_type, generate_type):
        """
        rt关联存储

        :param result_table_id: 结果表id
        :param storage: 存储信息
        :param processing_type: rt数据处理类型
        :param generate_type: 产生类型
        :return: 执行结果
        """
        # 如果是SNAPSHOT类型rt，数据永久保存
        expires = "-1" if processing_type == SNAPSHOT else "7d"
        request_data = {
            CLUSTER_NAME: storage[CLUSTER_NAME],
            CLUSTER_TYPE: storage[CLUSTER_TYPE],
            EXPIRES: expires,
            STORAGE_CONFIG: storage.get(STORAGE_CONFIG, "{}"),
            DESCRIPTION: "",
            PRIORITY: 0,
            GENERATE_TYPE: generate_type,
        }
        if storage[CLUSTER_TYPE] == HDFS:
            request_data[DATA_TYPE] = storage.get(DATA_TYPE, HDFS_DATA_TYPE)
        # 更新url中的参数
        request_data.update({RESULT_TABLE_ID: result_table_id})
        res = StoreKitApi.create_storage(request_data)
        if not res.is_success():
            sys_logger.error(
                "create_rt_storage_error|result_table_id:{}|response:{}".format(result_table_id, str(res.response))
            )
            raise CreateStorageError(message_kv={MESSAGE: res.message})

    @staticmethod
    @retry_deco(3)
    def prepare(result_table_id, storage=None):
        """
        为存储准备元信息、数据目录等

        :param result_table_id: 结果表id
        :param storage: 存储信息
        :return: 执行结果
        """
        if storage is None:
            storage = {CLUSTER_TYPE: HDFS}
        res = StoreKitApi.prepare({CLUSTER_TYPE: storage[CLUSTER_TYPE], RESULT_TABLE_ID: result_table_id})
        if not (res.is_success() and res.data):
            sys_logger.error(
                "prepare_storage_error|result_table_id:{}|response:{}".format(result_table_id, res.response)
            )
            raise StoragePrepareError(message_kv={MESSAGE: res.message})

    @staticmethod
    def delete_storage(result_table_id, storage=None):
        """
        删除rt关联的存储

        :param result_table_id: 结果表id
        :param storage: 存储信息
        :return: 执行结果
        """
        if storage is None:
            storage = {CLUSTER_TYPE: HDFS}
        res = StoreKitApi.delete_storage({RESULT_TABLE_ID: result_table_id, CLUSTER_TYPE: storage[CLUSTER_TYPE]})
        if not res.is_success():
            sys_logger.error(
                "delete_storage_error|result_table_id:{}|response:{}".format(result_table_id, res.response)
            )
            raise DeleteStorageError(message_kv={MESSAGE: res.message})

    @staticmethod
    def clear_data(result_table_id):
        """
        清理数据

        :param result_table_id: 结果表id
        :return: 执行结果
        """
        res = StoreKitApi.clear_data({RESULT_TABLE_ID: result_table_id, WITH_DATA: True, WITH_HIVE_META: True})
        if not res.is_success():
            sys_logger.error("clear_data_error|result_table_id:{}|response:{}".format(result_table_id, res.response))
            raise ClearDataError(message_kv={MESSAGE: res.message})


class DefineOperateHelper(object):
    @staticmethod
    def define_dp_operate(result_tables, processing_elements, bk_username, project_id, generate_type):
        """
        定义数据处理操作

        :param result_tables: 结果表列表
        :param processing_elements: 数据处理元素
        :param bk_username: 用户名
        :param project_id: 项目id
        :param generate_type: 产生类型
        :return: 执行结果
        """
        operate_params = {
            BK_USERNAME: bk_username,
            PROJECT_ID: project_id,
            PROCESSING_ID: processing_elements[PROCESSING_ID],
            PROCESSING_ALIAS: processing_elements[PROCESSING_ID],
            PROCESSING_TYPE: processing_elements[PROCESSING_TYPE],
            PLATFORM: BKDATA,
            GENERATE_TYPE: generate_type,
            DESCRIPTION: CREATED_BY_NOTEBOOK,
            RESULT_TABLES: result_tables,
            INPUTS: processing_elements[INPUTS],
            OUTPUTS: processing_elements[OUTPUTS],
        }
        dp_operate = {OPERATE_OBJECT: DATA_PROCESSING, OPERATE_TYPE: CREATE, OPERATE_PARAMS: operate_params}
        return dp_operate

    @staticmethod
    def define_destroy_operate(operate_ids, operate_object, operate_id_type, bk_username):
        """
        定义删除操作

        :param operate_ids: 操作id列表
        :param operate_object: 操作类型
        :param operate_id_type: 操作id类型
        :param bk_username: 用户名
        :return: 操作列表
        """
        operate_list = (
            [
                {
                    OPERATE_OBJECT: operate_object,
                    OPERATE_TYPE: DESTROY,
                    OPERATE_PARAMS: {BK_USERNAME: bk_username, operate_id_type: operate_id},
                }
                for operate_id in operate_ids
                if operate_id
            ]
            if operate_ids and isinstance(operate_ids, list)
            else [
                {
                    OPERATE_OBJECT: operate_object,
                    OPERATE_TYPE: DESTROY,
                    OPERATE_PARAMS: {BK_USERNAME: bk_username, operate_id_type: operate_ids},
                }
            ]
            if operate_ids
            else []
        )
        return operate_list

    @staticmethod
    def define_result_table(rt_dict, bk_username, project_id, processing_type, generate_type):
        """
        定义结果表

        :param rt_dict: rt元素集合
        :param bk_username: 用户名
        :param project_id: 项目id
        :param processing_type: 处理类型
        :param generate_type: 产生类型
        :return: meta定义结果
        """
        result_table_id = rt_dict[RESULT_TABLE_ID]
        biz_and_rt_name = result_table_id.split("_", 1)
        if len(biz_and_rt_name) < 2:
            raise Exception(_('结果表名定义不符合规范，请使用"业务id_表名"的形式'))
        bk_biz_id = biz_and_rt_name[0]
        result_table_name = biz_and_rt_name[1]
        result_table_elements = {
            RESULT_TABLE_ID: result_table_id,
            FIELDS: rt_dict[FIELDS],
            PROJECT_ID: project_id,
            BK_USERNAME: bk_username,
            BK_BIZ_ID: bk_biz_id,
            RESULT_TABLE_NAME: result_table_name,
            RESULT_TABLE_NAME_ALIAS: result_table_name,
            PROCESSING_TYPE: processing_type,
            GENERATE_TYPE: generate_type,
            SENSITIVITY: PRIVATE,
            DESCRIPTION: CREATED_BY_NOTEBOOK,
            TAGS: [DEFAULT_GEOG_AREA_TAG],
        }
        return result_table_elements

    @staticmethod
    def define_dp_element(result_table_ids, storage):
        """
        定义数据处理元素

        :param result_table_ids: 结果表列表
        :param storage: 存储信息
        :return: 数据处理元素
        """
        dp_elements = [
            {
                DATA_SET_TYPE: RESULT_TABLE,
                DATA_SET_ID: result_table,
                STORAGE_CLUSTER_CONFIG_ID: storage[STORAGE_CLUSTER_CONFIG_ID],
                CHANNEL_CLUSTER_CONFIG_ID: None,
                STORAGE_TYPE: STORAGE,
                TAGS: [DEFAULT_GEOG_AREA_TAG],
            }
            for result_table in result_table_ids
        ]
        return dp_elements


def create_notebook_task(project_id, project_type, notebook_name, bk_username):
    """
    生成笔记任务

    :param project_id: 项目id
    :param project_type: 项目类型
    :param notebook_name: 笔记任务名
    :param bk_username: 用户名
    :return: obj
    """
    jupyter_username = extract_jupyter_username(project_id, project_type)
    # 判断此用户名有没有在hub中注册，没有的话进行注册
    create_hub_user(jupyter_username)
    # 判断此notebook服务有没有启动，没有的话启动服务
    start_user_notebook_server(jupyter_username)
    # 创建笔记
    res = create_notebook(jupyter_username)
    content_name = res[CONTENT_NAME]
    notebook_url = "{}://{}/user/{}/notebooks/{}?token={}".format(
        HTTP_SCHEMA,
        NOTEBOOK_HOST,
        jupyter_username,
        content_name,
        res[TOKEN],
    )
    if not notebook_name:
        notebook_name = content_name.split(".")[0]
    if project_type == PERSONAL:
        project_id = DatalabProjectModel.objects.get(bind_to=bk_username).project_id
    obj = DatalabNotebookTaskModel.objects.create(
        project_id=project_id,
        project_type=project_type,
        notebook_name=notebook_name,
        content_name=content_name,
        notebook_url=notebook_url,
        lock_user=bk_username,
        lock_time=datetime.now(),
        created_by=bk_username,
    )
    return obj


def destroy_notebook(notebook_task_obj, bk_username):
    """
    删除笔记和笔记关联的rt、dp

    :param notebook_task_obj: 笔记任务对象
    :param bk_username: 用户名
    """
    # 删除笔记关联的queryset类型rt和dp
    output_objs = DatalabNotebookOutputModel.objects.filter(notebook_id=notebook_task_obj.notebook_id)
    # 删除模型
    model_objs = output_objs.filter(output_type=MODEL)
    if model_objs:
        sql_list = [DROP_MODEL % model_obj[OUTPUT_FOREIGN_ID] for model_obj in list(model_objs.values())]
        delete_res = DataFlowApi.start_modeling(
            {
                SQL_LIST: sql_list,
                PROJECT_ID: notebook_task_obj.project_id,
                TYPE: DATALAB,
                BK_USERNAME: bk_username,
                NOTEBOOK_ID: notebook_task_obj.notebook_id,
                CELL_ID: "",
            },
            raise_exception=True,
        )
        sys_logger.info(
            "delete_model|notebook_id: {}|result: {}".format(notebook_task_obj.notebook_id, delete_res.response)
        )
        model_objs.delete()

    # 过滤结果表
    result_table_values = list(output_objs.filter(output_type=RESULT_TABLE).values())
    if result_table_values:
        rt_ids = [result_table_value[OUTPUT_FOREIGN_ID] for result_table_value in result_table_values]
        result_tables_info = retrieve_result_table_info(rt_ids)
        # 获取queryset类型rt
        queryset_rt_ids = [_rt[RESULT_TABLE_ID] for _rt in result_tables_info if _rt[PROCESSING_TYPE] == QUERYSET]
        for _rt in queryset_rt_ids:
            res = QueryEngineApi.delete_job_conf(
                {
                    RESULT_TABLE_ID: _rt,
                }
            )
            sys_logger.info("delete_job_conf|rt: {}|result: {}".format(_rt, res.response))

        # dp和rt同名
        destroy_rt_and_dp(notebook_task_obj.notebook_id, bk_username, queryset_rt_ids, queryset_rt_ids)
        output_objs.filter(output_type=RESULT_TABLE, output_foreign_id__in=queryset_rt_ids).delete()
    # 删除笔记任务，如果笔记任务属于快速入门，此时把active置为0，便于判断快速入门被用户主动删除，不再给项目创建快速入门
    if notebook_task_obj.content_name in list(TEMPLATE_NAMES.keys()):
        notebook_task_obj.active = 0
        notebook_task_obj.save()
    else:
        notebook_task_obj.delete()
    # 删除notebook
    jupyter_username = extract_jupyter_username(notebook_task_obj.project_id, notebook_task_obj.project_type)
    delete_notebook(jupyter_username, notebook_task_obj.content_name)


def destroy_rt_and_dp(notebook_id, bk_username, result_table_ids, processing_ids):
    """
    删除rt和dp

    :param notebook_id: 笔记id
    :param bk_username: 用户名
    :param result_table_ids: 结果表id列表
    :param processing_ids: 数据处理id列表
    :return: 删除结果
    """
    for result_table_id in result_table_ids:
        ApiHelper.clear_data(result_table_id)
        ApiHelper.delete_storage(result_table_id)

    # 删除rt和dp
    ApiHelper.destroy_transact(bk_username, result_table_ids, processing_ids)
    DatalabNotebookOutputModel.objects.filter(output_type=RESULT_TABLE, output_foreign_id__in=result_table_ids).delete()


def execute_cell(jupyter_username, bk_username, notebook_id, content_name, cell_codes):
    """
    执行cell

    :param jupyter_username: 笔记用户名
    :param bk_username: 平台用户名
    :param notebook_id: 笔记id
    :param content_name: 笔记名
    :param cell_codes: cell代码列表
    """
    execute_objs = [
        DatalabNotebookExecuteInfoModel(
            notebook_id=notebook_id, cell_id=index, code_text=code, stage_status=PENDING, created_by=bk_username
        )
        for index, code in enumerate(cell_codes)
    ]
    DatalabNotebookExecuteInfoModel.objects.bulk_create(execute_objs)

    kernel = start_kernel(jupyter_username, content_name)
    ws = create_ws(jupyter_username, kernel[ID])
    output = []

    for index, code in enumerate(cell_codes):
        stage_start_time = datetime.now()
        stage_status = FINISHED
        error_message = ""

        ws.send(json.dumps(define_execute_request(code, bk_username)))
        try:
            while True:
                rsp = json.loads(ws.recv())
                msg_type = rsp["msg_type"]
                if msg_type == "stream":
                    output.append(rsp["content"]["text"])
                elif msg_type == "execute_result":
                    if "image/png" in (list(rsp["content"]["data"].keys())):
                        output.append(rsp["content"]["data"]["image/png"])
                    else:
                        result = rsp["content"]["data"]["text/plain"]
                        output.append(result)
                        if any(error_flag in result for error_flag in ["失败", "异常", "错误"]):
                            stage_status = FAILED
                            error_message = eval(result)
                elif msg_type == "display_data":
                    output.append(rsp["content"]["data"]["image/png"])
                elif msg_type == "error":
                    stage_status = FAILED
                    output.append(rsp["content"]["traceback"])
                elif msg_type == "status" and rsp["content"]["execution_state"] == "idle":
                    break
        except Exception as e:
            sys_logger.error("execute_cell_ws_error|ex_name:{}|message:{}".format(type(e).__name__, str(e)))
            stage_status = FAILED
            error_message = str(e)
            ws.close()
        finally:
            DatalabNotebookExecuteInfoModel.objects.filter(notebook_id=notebook_id, cell_id=index).update(
                stage_start_time=stage_start_time,
                stage_end_time=datetime.now(),
                stage_status=stage_status,
                error_message=error_message,
                created_by=bk_username,
            )
        if stage_status == FAILED:
            DatalabNotebookExecuteInfoModel.objects.filter(notebook_id=notebook_id, stage_status=PENDING).update(
                stage_status=CANCELED
            )
            break
    ws.close()
    stop_kernel(jupyter_username, kernel[ID])


def define_execute_request(code, bk_username):
    """
    定义执行请求

    :param code: 代码
    :param bk_username: 用户名
    :return: 执行请求
    """
    msg_type = "execute_request"
    content = {"code": code, "silent": False}
    header = {
        "msg_id": uuid.uuid1().hex,
        "cell_id": uuid.uuid1().hex,
        "username": bk_username,
        "session": uuid.uuid1().hex,
        "data": datetime.now().isoformat(),
        "msg_type": msg_type,
        "version": "5.0",
    }
    msg = {"header": header, "parent_header": header, "metadata": {}, "content": content}
    return msg
