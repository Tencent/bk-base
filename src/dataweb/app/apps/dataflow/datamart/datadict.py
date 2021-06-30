# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
import json
import operator

import arrow
from django.conf import settings
from django.core.cache import cache
from django.utils.translation import ugettext as _

from apps.api import AuthApi, DataManageApi, MetaApi
from apps.common.log import logger
from apps.dataflow.handlers.business import Business
from apps.dataflow.utils import async_func, subdigit
from apps.exceptions import DataError
from apps.utils import cmp_to_key
from apps.utils.local import activate_request

# 测试环境和正式环境清洗项目id，无法直接从数据源详情拿到项目
clean_project_id = 4
clean_project_name_product = _("数据平台清洗项目")
clean_project_name_dev = _("测试项目")

processing_type_dict = {
    "batch_model": _("ModelFlow模型"),
    "stream": _("实时计算"),
    "stream_model": _("ModelFlow模型"),
    "batch": _("离线计算"),
    "transform": _("转换"),
    "clean": _("清洗"),
    "model": _("场景化模型"),
    "storage": _("存储类计算"),
    "view": _("视图"),
}
meta_status_dict = {"success": _("元数据同步正常"), "error": _("元数据同步异常，请联系TGDP(IEG数据平台助手)"), "processing": _("元数据同步中")}
meta_success_list = ["OK"]
meta_error_list = ["SyncInconsistencyError", "SyncAuthError", "CountFreqError", "TdwOriginalError"]
meta_processing_list = ["Init", "WaitForVerify", "Grab"]


def list_dataset_in_apply(username):
    """获取一个用户正在申请权限的数据集列表"""
    tickets_search = AuthApi.tickets.list(
        {"is_creator": 1, "bk_username": username, "status": "processing", "ticket_type": "apply_role"}
    )
    tickets = []
    for each_ticket in tickets_search:
        permissions = each_ticket.get("permissions", [])
        if permissions:
            role_id = permissions[0].get("role_id", "")
            scope_id = permissions[0].get("scope_id", "")
            if (role_id == "result_table.viewer" or role_id == "raw_data.viewer") and scope_id:
                tickets.append(scope_id)
    return tickets


def cmp_by_tag_type(left, right):
    """
    tag按照类型排序函数，业务标签 来源标签 应用标签 描述标签 管理标签（地域）
    :param left: tag1
    :param right: tag2
    :return:-1,1,0
    """
    left = left["tag_type"]
    right = right["tag_type"]

    tags_weight_list = ["business", "system", "application", "desc", "manage"]
    tags_score_dict = {}
    for i, value in enumerate(tags_weight_list):
        tags_score_dict[value] = i
    try:
        left_temp_tag = tags_score_dict.get(left, -1)
        right_temp_tag = tags_score_dict.get(right, -1)
        temp_mock_cmp = operator.eq(left_temp_tag, right_temp_tag)
        if temp_mock_cmp is True:
            return 0
        else:
            temp_mock_cmp = operator.gt(left_temp_tag, right_temp_tag)
            if temp_mock_cmp is True:
                return 1
            else:
                return -1
    except Exception:
        pass


def get_sort_tag_list(tag_list):
    """tag_list根据tag类型排序，业务标签 来源标签 应用标签 描述标签 管理标签（地域）"""
    tag_list.sort(key=cmp_to_key(cmp_by_tag_type))
    return tag_list


def async_add_result_table_attr(item, request):
    """
    并发给结果表添加属性
    :param item_list:
    :return:
    """
    # 激活除了主线程以外的request
    activate_request(request)
    batch_add_result_table_attr(item, request)


def add_data_manager_and_viewer(rt_dict, dataset_id, dataset_type):
    """
    增加 数据管理员 & 数据观察员 属性
    :param rt_dict: dict，rt/数据源详情
    :param dataset_id: 数据源id/结果表id
    :param dataset_type: 数据集类型
    :return:
    """
    manager = AuthApi.roles_users.list({"role_id": "%s.manager" % dataset_type, "scope_id": dataset_id})
    rt_dict["manager"] = manager

    viewer = AuthApi.roles_users.list({"role_id": "%s.viewer" % dataset_type, "scope_id": dataset_id})
    rt_dict["viewer"] = viewer


def get_dataset_list_info(data_set_type, request, search_list):
    """
    获取rt表/数据源的详情
    :param data_set_type: 数据集type
    :param dataset_list:
    :param request:
    :param search_list:
    :return:
    """
    if not search_list:
        return []
    if data_set_type == "result_table":
        res = search_list

        # 批量查询用户有无结果表的查询权限
        dataset_list = [each_rt.get("data_set_id") for each_rt in search_list]
        has_permission_list = batch_check_user_perm(dataset_list, "result_table", request)
        has_permission_dict = {each_perm.get("object_id"): each_perm.get("result") for each_perm in has_permission_list}

        for each_rt in res:
            tag_list = []
            for each_tag in each_rt.get("~Tag.targets", ""):
                tag_list.append(
                    {
                        "tag_type": each_tag.get("tag_type"),
                        "tag_code": each_tag.get("code"),
                        "tag_alias": each_tag.get("alias"),
                    }
                )
            tag_list = get_sort_tag_list(tag_list)
            each_rt["tag_list"] = tag_list

        for each_rt in res:
            if each_rt.get("~DmTaskDetail.data_set") and each_rt.get("~DmTaskDetail.data_set")[0].get(
                "standard_version"
            ):
                each_rt["standard_id"] = (
                    each_rt.get("~DmTaskDetail.data_set")[0].get("standard_version")[0].get("standard_id")
                )
            each_rt["project_name"] = each_rt.get("project")[0].get("project_name") if each_rt.get("project") else ""
            each_rt["processing_type_alias"] = (
                each_rt.get("processing_type_obj")[0].get("processing_type_alias", "")
                if each_rt.get("processing_type_obj")
                else ""
            )
            each_rt["has_permission"] = has_permission_dict.get(each_rt.get("result_table_id"))
            each_rt["updated_at"] = utc_to_local(each_rt.get("updated_at", ""))

            # 生命周期相关属性
            add_lifecycle_attr(each_rt)

            each_rt["processing_type_alias"] = (
                each_rt.get("processing_type_obj")[0].get("processing_type_alias")
                if each_rt.get("processing_type_obj")
                else ""
            )

        # 给结果表添加 业务名称&processing_type（并发)
        async_func(async_add_result_table_attr, res, request)

    elif data_set_type == "raw_data":
        async_func(async_add_raw_data_attr, search_list, request)
        res = search_list
        data_ids = [each_rawdata.get("data_set_id") for each_rawdata in search_list]

        # 批量查询用户有无结果表的查询权限
        has_permission_list = batch_check_user_perm(data_ids, "raw_data", request)
        has_permission_dict = {each_perm.get("object_id"): each_perm.get("result") for each_perm in has_permission_list}

        for each_rawdata in res:
            each_rawdata["has_permission"] = has_permission_dict.get(each_rawdata.get("data_set_id"))
            each_rawdata["updated_at"] = utc_to_local(each_rawdata.get("updated_at", ""))

            # 生命周期相关属性
            add_lifecycle_attr(each_rawdata)

    else:
        res = search_list
        # 批量查询用户有无结果表的查询权限
        dataset_list = [each_tdw_rt.get("data_set_id") for each_tdw_rt in search_list]
        has_permission_list = batch_check_user_perm(dataset_list, "tdw_table", request, "select")
        has_permission_dict = {each_perm.get("object_id"): each_perm.get("result") for each_perm in has_permission_list}
        for each_tdw_rt in res:
            each_tdw_rt["has_permission"] = has_permission_dict.get(each_tdw_rt.get("table_id"))
            each_tdw_rt["sensitivity"] = "private"
            each_tdw_rt["bk_biz_name"] = _("腾讯数据仓库TDW")
            each_tdw_rt["processing_type"] = "TDW"
            each_tdw_rt["processing_type_alias"] = "TDW"
            each_tdw_rt["description"] = each_tdw_rt.get("table_comment", "")
            each_tdw_rt["updated_at"] = utc_to_local(each_tdw_rt.get("updated_at", ""))
            each_tdw_rt["heat_metric"] = {}
            each_tdw_rt["range_metric"] = {}
    return res


def hum_storage_unit(num, suffix="B"):
    """
    根据存储大小自适应决定单位
    :param num:
    :param suffix:
    :return:
    """
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return format_capacity("{:3.3f}{}{}".format(num, unit, suffix))
        num /= 1024.0
    return format_capacity("{:.3f}{}{}".format(num, "Y", suffix))


def format_capacity(cap_str):
    if "0.000" in cap_str:
        cap_str = cap_str[0:8]
    return cap_str


def add_lifecycle_attr(each_ds):
    """
    补充生命周期相关属性
    :param each_rt:
    :return:
    """
    lifecycle_list = each_ds.get("~LifeCycle.target")
    each_ds["heat_metric"] = (
        lifecycle_list[0]["heat"][0]
        if lifecycle_list and lifecycle_list[0].get("heat")
        else {"query_count": 0, "heat_score": 0.0}
    )
    each_ds["range_metric"] = (
        lifecycle_list[0]["range"][0]
        if lifecycle_list and lifecycle_list[0].get("range")
        else {"project_count": 1, "normalized_range_score": 13.1, "node_count": 0, "biz_count": 1, "app_code_count": 0}
    )
    each_ds["importance_metric"] = (
        lifecycle_list[0]["importance"][0]
        if lifecycle_list and lifecycle_list[0].get("importance")
        else {"importance_score": 0.0}
    )
    each_ds["asset_value_metric"] = (
        lifecycle_list[0]["asset_value"][0]
        if lifecycle_list and lifecycle_list[0].get("asset_value")
        else {"asset_value_score": 4.37}
    )
    each_ds["cost_metric"] = (
        lifecycle_list[0]["cost"][0]["capacity"][0]
        if lifecycle_list and lifecycle_list[0].get("cost") and lifecycle_list[0]["cost"][0].get("capacity")
        else {"total_capacity": 0}
    )
    each_ds["cost_metric"]["format_capacity"] = hum_storage_unit(each_ds["cost_metric"]["total_capacity"])
    each_ds["assetvalue_to_cost"] = lifecycle_list[0].get("assetvalue_to_cost", -1) if lifecycle_list else -1


def batch_check_user_perm(dataset_list, object_type, request, action_type="query_data"):
    """批量校验用户与对象权限"""
    username = request.user.username
    object_type = "{}.{}".format(object_type, action_type)
    permissions_list = [
        {"user_id": username, "action_id": object_type, "object_id": each_dataset} for each_dataset in dataset_list
    ]
    has_permission_list = AuthApi.batch_check_user_perm(
        {
            "permissions": permissions_list,
        }
    )
    return has_permission_list


def add_result_table_attr(item, o_rt, request):
    """
    给结果表添加业务名称、项目管理员&标签
    :param item:dict,结果表详情
    :return:
    """
    username = request.user.username

    item["bk_biz_name"] = o_rt.biz_name

    # 查询分类,不支持列表查询,数据分类在页面弃用
    # item['data_category_alias'] = o_rt.data_category_alias()

    # 查询rt表对应的标签
    item["tag_list"] = o_rt.tag_list()
    # get_entity_tag(item, item['result_table_id'], 'result_table')
    # 查询用户有无结果表的查询权限&写权限
    item["has_permission"], item["no_pers_reason"] = o_rt.judge_permission(username)
    item["has_write_permission"], item["no_write_pers_reason"] = o_rt.judge_permission(username, "update")

    # 查询rt表对应的类型
    item["processing_type_alias"] = o_rt.result_table_type_alias


def add_standard_info(res, standard_id, result_table_id):
    """给标准结果表添加标准相关属性

    """
    res["standard_id"] = standard_id
    # 查询该标准数据关联的指标
    query_erp = [
        {
            "?:typed": "DmStandardConfig",
            "?:start_filter": "id = {standard_id}",
            "standard_name": True,
            "description": True,
            "?:cascade": True,
            "~DmStandardVersionConfig.standard": {
                "standard_version": True,
                "?:filter": "standard_version_status = \"online\""
            }
        },
        {
            "?:typed": "DmTaskDetail",
            "?:start_filter": "data_set_id = \"{result_table_id}\"",
            "standard_content_id": True,
            "?:cascade": True,
            "standard_content": {
                "standard_content_name": True
            }
        }
    ]
    query_erp[0]['?:start_filter'] = query_erp[0]['?:start_filter'].format(
        standard_id=standard_id)
    query_erp[1]['?:start_filter'] = query_erp[1]['?:start_filter'].format(
        result_table_id=result_table_id)
    meta_data_dict = MetaApi.query_via_erp({
        "retrieve_args": query_erp
    })
    if meta_data_dict:
        dm_standard_config_list = meta_data_dict.get('DmStandardConfig')
        if dm_standard_config_list:
            dm_standard_config_dict = dm_standard_config_list[0]
            res["standard_name"] = dm_standard_config_dict.get('standard_name')
            res["standard_description"] = dm_standard_config_dict.get('description')
            res["standard_version"] = (
                dm_standard_config_dict["~DmStandardVersionConfig.standard"][0].get('standard_version')
                if dm_standard_config_dict.get("~DmStandardVersionConfig.standard")
                else None
            )
        dm_task_detail_list = meta_data_dict.get('DmTaskDetail')
        if dm_task_detail_list:
            dm_task_detail_dict = dm_task_detail_list[0]
            res["standard_content_id"] = dm_task_detail_dict.get('standard_content_id')
            res["standard_content_name"] = (
                dm_task_detail_dict["standard_content"][0].get('standard_content_name')
                if dm_task_detail_dict.get("standard_content")
                else None
            )


def batch_add_result_table_attr(item, request):
    """
    用于并发给结果表添加业务名称、项目管理员&分类名称&标签
    :param item:dict,结果表详情
    :return:
    """
    all_biz_dict = Business.get_name_dict()

    # 增加 业务名称 属性
    if item.get("bk_biz_id", ""):
        item["bk_biz_name"] = all_biz_dict.get(item["bk_biz_id"], "")

    # 查询rt表对应的类型
    # if item.get('processing_type', ''):
    #     # processing_type_dict = get_processing_type_dict()
    #     item['processing_type_alias'] = processing_type_dict.get(item['processing_type']) \
    #         if item['processing_type'] not in ['storage', 'view'] else ''


def get_processing_type_dict():
    """
    获取数据处理类型列表
    {
        "batch_model":"离线模型",
        "stream":"流处理",
        "stream_model":"实时模型",
        "storage":"存储",
        "batch":"批处理",
        "transform":"固化节点",
        "clean":"清洗",
        "model":"模型",
        "view":"视图"
    }
    """
    processing_type_dict = cache.get("processing_type_dict")
    if processing_type_dict is None:
        processing_type_list = MetaApi.processing_type_configs({})
        processing_type_dict = {
            each_processing["processing_type_name"]: each_processing["processing_type_alias"]
            for each_processing in processing_type_list
        }
        cache.set("processing_type_dict", processing_type_dict, 3600)

    return processing_type_dict


def add_raw_data_attr(item, request):
    """
    给数据源添加属性
    :param each_data_id:
    :param res:
    :return:
    """
    all_biz_dict = Business.get_name_dict()

    # 增加 业务名称 属性
    if item.get("bk_biz_id", ""):
        item["bk_biz_name"] = all_biz_dict.get(item["bk_biz_id"], "")

    # 给数据源添加项目名称
    if settings.RUN_MODE == "PRODUCT":
        item["project_name"] = clean_project_name_product
    else:
        item["project_name"] = clean_project_name_dev
    item["project_id"] = clean_project_id

    tag_list = []
    for each_tag in item.get("~Tag.targets", ""):
        tag_list.append(
            {"tag_type": each_tag.get("tag_type"), "tag_code": each_tag.get("code"), "tag_alias": each_tag.get("alias")}
        )
    tag_list = get_sort_tag_list(tag_list)
    item["tag_list"] = tag_list


def async_add_raw_data_attr(item, request):
    """
    异步给数据源添加属性
    :param res:
    :param item_list:
    :return:
    """
    # 激活除了主线程以外的request
    activate_request(request)
    add_raw_data_attr(item, request)


def get_entity_tag(item, target_id, target_type):
    """查询rt表对应的标签"""
    # 正式环境target_type传table，测试传result_table
    tag_search = MetaApi.target_tag_info(
        {"target_id": target_id, "target_type": target_type, "page": 1, "page_size": 10}
    )

    if tag_search:
        tag_search = tag_search[0].get("tags", {})
        # tag_list = format_tag_list(tag_search)
        # tag_list = get_sort_tag_list(tag_list)
        # 对实体查到的标签进行格式化处理和排序
        tag_list = get_format_tag_list(tag_search)
    item["tag_list"] = tag_list


def get_format_tag_list(tag_search):
    """对实体查到的标签进行格式化处理和排序"""
    tag_list = format_tag_list(tag_search)
    tag_list = get_sort_tag_list(tag_list)
    return tag_list


def batch_get_entity_tag(target_id, target_type, page=1, page_size=10):
    """排量查询查询rt表对应的标签"""
    target_filter = {"condition": "OR"}
    criterion = [{"k": "result_table_id", "v": each_rtid, "func": "eq"} for each_rtid in target_id]
    target_filter["criterion"] = criterion
    params = {
        "page": page,
        "page_size": page_size,
        "target_type": target_type,
        "target_filter": json.dumps([target_filter]),
    }
    tag_search = MetaApi.target_tag_info(params).get("content", [])

    tag_search_dict = {}
    for each_tag in tag_search:
        tag_search_dict_tmp = each_tag.get("tags", {})
        # 对标签进行格式化处理
        tag_list = get_format_tag_list(tag_search_dict_tmp)
        tag_search_dict[each_tag.get("result_table_id")] = tag_list
    return tag_search_dict


def batch_get_heat_and_range_score(target_id):
    """ "批量查询数据的热度和广度评分"""
    try:
        heat_list = DataManageApi.heat_metric_by_influxdb({"dataset_id": target_id})
    except Exception as e:
        logger.error("dataset: [{}] get heat score error: [{}]".format(target_id, e.message))
        heat_list = []
    heat_dict = {}
    for each_dataset in heat_list:
        heat_dict[each_dataset["dataset_id"]] = each_dataset

    try:
        range_list = DataManageApi.range_metric_by_influxdb({"dataset_id": target_id})
    except Exception as e:
        logger.error("dataset: [{}] get range score error: [{}]".format(target_id, e.message))
        range_list = []
    range_dict = {}
    for each_dataset in range_list:
        range_dict[each_dataset["dataset_id"]] = each_dataset
    return heat_dict, range_dict


def get_heat_and_range_score(dataset_id, res):
    try:
        heat_list = DataManageApi.heat_metric_by_influxdb({"dataset_id": dataset_id})
    except Exception as e:
        logger.error("dataset: [{}] get range score error: [{}]".format(dataset_id, e.message))
        heat_list = []
    res["heat_score"] = heat_list[0].get("heat_score") if heat_list else None
    res["heat_metric"] = heat_list[0] if heat_list else None

    try:
        range_list = DataManageApi.range_metric_by_influxdb({"dataset_id": dataset_id})
    except Exception as e:
        logger.error("dataset: [{}] get range score error: [{}]".format(dataset_id, e.message))
        range_list = []
    res["range_score"] = range_list[0].get("normalized_range_score") if range_list else None
    res["range_metric"] = range_list[0] if range_list else None
    if res["range_metric"]:
        node_count_list = json.loads(res["range_metric"].get("node_count", []))
        node_count = 0
        for each_level in node_count_list:
            node_count += each_level
        res["range_metric"]["node_count"] = node_count


def format_tag_list(tag_dict):
    """拿到格式化的标签列表"""
    tag_list = []
    for key, value in tag_dict.items():
        if key == "manage":
            for manage_key, manage_value in value.items():
                for each_tag in manage_value:
                    tag_list.append(
                        {
                            "tag_code": each_tag.get("code", ""),
                            "tag_alias": each_tag.get("alias", ""),
                            "tag_type": each_tag.get("tag_type", ""),
                        }
                    )
        else:
            for each_tag in value:
                tag_list.append(
                    {
                        "tag_code": each_tag.get("code", ""),
                        "tag_alias": each_tag.get("alias", ""),
                        "tag_type": each_tag.get("tag_type", ""),
                    }
                )
    return tag_list


def judge_permission(item, object_id, object_type, username, action_type="query_data"):
    """判断用户有无数据查询权限"""
    object_type = "{}.{}".format(object_type, action_type)
    try:
        permission_dict = AuthApi.check_user_perm(
            {"user_id": username, "action_id": object_type, "object_id": object_id, "display_detail": True}
        )
        has_permission = permission_dict.get("result", False)
    except DataError:
        has_permission = False

    if action_type == "query_data" or action_type == "select":
        item["has_permission"] = has_permission
        if not has_permission:
            item["no_pers_reason"] = permission_dict.get("message", "")
    elif action_type == "alter" or action_type == "update":
        item["has_write_permission"] = has_permission
        if not has_permission:
            item["no_write_pers_reason"] = permission_dict.get("message", "")


def get_expires(item):
    """获取rt表的存储天数"""
    # 查询结果表的推荐存储
    storage_types_dict = {}
    for key, value in item.get("storages", {}).items():
        storage_types_dict[key] = subdigit(value.get("expires", ""))
    item["storage_types"] = storage_types_dict


def add_result_table_list_attr(each_rt, result_table_dict):
    """
    将rt列表中补充pizza列表接口已有的属性
    """
    if each_rt.get("platform", "") == "tdw":
        rt_id = each_rt.get("table_id", "")
    else:
        rt_id = each_rt.get("result_table_id", "")

    each_rt["index"] = result_table_dict[rt_id].get("index", "")


def add_dataset_list_ticket_status(username, dataset_list):
    """
    添加数据集权限是否正在申请中属性
    """
    # 获取用户正在申请中的数据集列表
    tickets = list_dataset_in_apply(username)
    for each_dataset in dataset_list:
        each_dataset["ticket_status"] = "processing" if each_dataset.get("data_set_id", "") in tickets else ""


def utc_to_local(time_str):
    """
    utc时间转化为当地时间
    :param time_str: 各种各式的utc时间字符串
    :return: "%Y-%m-%d %H:%M:%S"
    """
    try:
        res = arrow.get(time_str).to("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.warning("time_str: [{}] convert utc time to local time error: [{}]".format(time_str, e.message))
        res = time_str
    return res


def add_meta_status(res):
    if res.get("usability", "") in meta_success_list:
        res["meta_status"] = "success"
        res["meta_status_message"] = meta_status_dict["success"]
    elif res.get("usability", "") in meta_error_list:
        res["meta_status"] = "error"
        res["meta_status_message"] = meta_status_dict["error"]
    elif res.get("usability", "") in meta_processing_list:
        res["meta_status"] = "processing"
        res["meta_status_message"] = meta_status_dict["processing"]
    else:
        res["meta_status"] = "error"
        res["meta_status_message"] = meta_status_dict["error"]
