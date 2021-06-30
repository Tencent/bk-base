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
import operator
import re
import time
import traceback

import requests
from common.base_utils import model_to_dict
from common.exceptions import ApiRequestError, ApiResultError, ValidationError
from common.meta.models import Tag
from common.transaction.meta_sync import (
    _current_frame,
    _pop_frame,
    _push_frame,
    add_to_meta_sync_wait_list,
    meta_sync_register,
    sync_model_data,
)
from conf import dataapi_settings
from conf.tag_settings import TAG_LIST
from django.conf import settings
from django.core.management import call_command
from django.db import connections, models
from django.utils.six import StringIO
from more_itertools import chunked

SYNC_TABLES = [
    # 整体配置表
    {"database": "bkdata_basic", "table": "belongs_to_config"},
    {"database": "bkdata_basic", "table": "cluster_group_config"},
    {"database": "bkdata_basic", "table": "content_language_config"},
    {"database": "bkdata_basic", "table": "data_category_config"},
    {"database": "bkdata_basic", "table": "encoding_config"},
    {"database": "bkdata_basic", "table": "field_type_config"},
    {"database": "bkdata_basic", "table": "job_status_config"},
    # {'database': 'bkdata_basic', 'table': 'operation_config'},
    {"database": "bkdata_basic", "table": "time_format_config"},
    {"database": "bkdata_basic", "table": "result_table_type_config"},
    {"database": "bkdata_basic", "table": "processing_type_config"},
    {"database": "bkdata_basic", "table": "transferring_type_config"},
    {"database": "bkdata_basic", "table": "tag_type_config"},
    # 数据实体
    {"database": "bkdata_basic", "table": "project_info"},
    {"database": "bkdata_basic", "table": "storage_cluster_config"},
    {"database": "bkdata_basic", "table": "databus_channel_cluster_config"},
    {"database": "bkdata_basic", "table": "databus_connector_cluster_config"},
    {"database": "bkdata_basic", "table": "result_table"},
    {"database": "bkdata_basic", "table": "result_table_field"},
    {"database": "bkdata_basic", "table": "storage_result_table"},
    {"database": "bkdata_basic", "table": "access_raw_data"},
    {"database": "bkdata_basic", "table": "data_processing"},
    {"database": "bkdata_basic", "table": "data_processing_relation"},
    {"database": "bkdata_basic", "table": "data_transferring"},
    {"database": "bkdata_basic", "table": "data_transferring_relation"},
]


def database_sync_to_meta():
    databases = connections.databases
    database_names = {item["NAME"]: key for key, item in databases.items()}
    error_list = []
    print("### [RE-SYNC] re-sync exists data from meta db: total {} tables".format(len(SYNC_TABLES)))
    time.sleep(3)
    for table_config in SYNC_TABLES:
        print("### [START] syncing table: <{}.{}> ...".format(table_config["database"], table_config["table"]))
        try:
            io = StringIO()
            call_command(
                "inspectdb",
                "--database={}".format(database_names.get(table_config["database"], "default")),
                table_config["table"],
                stdout=io,
            )
            model_text = io.getvalue()
            model_text = re.sub(r"(class\ Meta\:\s)", r"\1        app_label = 'public'\n", model_text)
            model = define_model(model_text)
            if model is not None:
                sync_by_model(model, error_list)
            else:
                pattern = re.compile(r"The error was.+(?=\n)")
                try:
                    print
                    pattern.search(model_text).group()
                except Exception:
                    print
                    model_text
        except Exception:
            traceback.print_exc()
        print(
            "### [SUCCESS] re-sync table: <{}.{}> succeed !!!\n".format(table_config["database"], table_config["table"])
        )
    return error_list


def define_model(model_text):
    exec(model_text)
    for key, value in locals().iteritems():
        if isinstance(value, type) and issubclass(value, models.Model):
            return value


def sync_by_model(model, error_list, batch=50):
    meta_sync_register(model)
    item_list = model.objects.all()
    all_cnt = len(item_list)
    process = 0
    item_slice_list = [item_list[i : i + batch] for i in range(0, len(item_list), batch)]

    for item_slice in item_slice_list:
        process += len(item_slice)
        print("syncing: {} / {} ...".format(process, all_cnt))
        _push_frame(False)
        for item in item_slice:
            add_to_meta_sync_wait_list(item, "CREATE")
        current_frame = _current_frame()
        try:
            retries = 3
            while True:
                try:
                    sync_model_data(current_frame.contexts)
                    break
                except Exception as e:
                    retries -= 1
                    if retries == 0:
                        raise e
        except Exception as e:
            error_list.append(
                {
                    "model": model,
                    "instance": item,
                    "error": e,
                }
            )
        _pop_frame()
        time.sleep(0.5)


# ========= sync tags ========== #
def sync_tags_to_meta(platform=settings.RUN_VERSION, geog=settings.GEOG_AREA_CODE):
    """
    同步标签配置到元数据
    :param platform: 版本类型 tgdp-海外版; ee-企业版(默认)
    :param geog: 地域 inland-大陆; SEA-东南亚; NA-北美; ...
    :return: boolean 同步结果
    """
    if not TAG_LIST:
        print("no content to sync, check TAG_LIST")
        return False
    if not geog:
        print("no geog area set, check input param `geog`")
        return False
    legality = check_tag_legality(TAG_LIST)
    if legality:
        tag_relations = tag_serializer(TAG_LIST)
        sorted_tag_genes, tag_id_gene_map = sort_by_dependence(tag_relations)
        filter_by_platform(sorted_tag_genes, tag_id_gene_map, [geog], platform)
        try:
            gen_and_sync_via_bridge(sorted_tag_genes, platform=platform, geog=geog)
            print("sync tags completed successfully!")
            return True
        except Exception as ee:
            traceback.print_exc()
            print("exception occur, detail: {}".format(ee))
            return False
    else:
        print("tag conf is illegal, check TAG_LIST")
        return False


def tag_serializer(tag_list):
    """格式化tag配置"""
    tag_relations = {}
    for tag_item in tag_list:
        tag_id = str(tag_item["id"])
        tag_code = str(tag_item["code"])
        parent_id = str(tag_item["parent_id"])
        gene = 0 if int(parent_id) == 0 else None
        tag_relations[tag_id] = dict(
            id=tag_id, parent_id=parent_id, code=tag_code, tag_item=tag_item, gene=gene, filtered=False
        )
    return tag_relations


def filter_by_platform(sorted_tag_genes, tag_id_gene_map, enable_geog_list, platform=settings.RUN_VERSION):
    """
    根据发布版本平台，过滤不需要的标签(和子标签)
    :param sorted_tag_genes: dict 标签列表(按代排序)
    :param tag_id_gene_map: dict 标签id_代数对应关系
    :param platform: string 平台类型
    :param enable_geog_list: list 当前环境有效的地域列表
    :return: dict 过滤后的标签列表(按代排序)
    """
    filter_dict = dict(tgdp=set(), ee={"terminal_ops", "game"}, tencent=set())

    geog_area_root = dict(root=dict(tags=[], ids=[]), mid=dict(tags=[], ids=[]), leaf=dict(tags=[], ids=[]))
    for gene, tag_relations in sorted_tag_genes.items():
        for tag_relation_item in tag_relations.values():
            if tag_relation_item["code"] == "geog_area":
                geog_area_root["root"]["tags"].append(tag_relation_item)
                geog_area_root["root"]["ids"].append(tag_relation_item["id"])
            elif tag_relation_item["parent_id"] in geog_area_root["root"]["ids"]:
                geog_area_root["mid"]["tags"].append(tag_relation_item)
                geog_area_root["mid"]["ids"].append(tag_relation_item["id"])
            elif tag_relation_item["parent_id"] in geog_area_root["mid"]["ids"]:
                geog_area_root["leaf"]["tags"].append(tag_relation_item)
                geog_area_root["leaf"]["ids"].append(tag_relation_item["id"])
    filter_geog_area_tag = [
        geog_item["code"] for geog_item in geog_area_root["leaf"]["tags"] if geog_item["code"] not in enable_geog_list
    ]
    filter_dict[platform] = filter_dict[platform] | set(filter_geog_area_tag)

    print("filter_dict is {}".format(filter_dict))

    def check_tag_need_filtered(tag, depth=0):
        """
        递归检查标签是否应该被过滤,一直检查到顶层标签
        :param tag: tag节点信息
        :param depth: 查询深度
        :return: boolean 是否需要被过滤
        """
        if depth >= 20:
            return False
        if tag.get("code", None) in filter_dict.get(platform, set()):
            return True
        pid = tag["parent_id"]
        if int(pid) == 0:
            return tag["filtered"]
        else:
            parent_tag_relation_item = sorted_tag_genes[tag_id_gene_map.get(pid, 0)][pid]
            return check_tag_need_filtered(parent_tag_relation_item, depth + 1)

    for gene, tag_relations in sorted_tag_genes.items():
        for tag_id, tag_relation_item in tag_relations.items():
            tag_relations[tag_id]["filtered"] = check_tag_need_filtered(tag_relation_item)


def check_tag_legality(tags):
    """
    检查标签列表合法性: 标签id域，是否重复，父子关系

    :param tags: list 标签配置列表
    :return: boolean 是否合法
    """
    max_id = 0
    tag_code_set = set()
    tag_relation_dict = dict()
    for tag_item in tags:
        tag_id = str(tag_item["id"])
        tag_code = str(tag_item["code"])
        parent_id = str(tag_item["parent_id"])
        if int(tag_id) > max_id:
            max_id = int(tag_id)
        tag_code_set.add(tag_code)
        tag_relation_dict[tag_id] = parent_id
    if max_id > 10000:
        print("[ERROR] tag_id bigger than 10000: {}".format(max_id))
        return False
    if len(tag_code_set) != len(tags):
        print("[ERROR] duplicate tag codes exist, has {} ids but only {} codes".format(len(tags), len(tag_code_set)))
        return False
    for parent_id in tag_relation_dict.values():
        if int(parent_id) != 0 and parent_id not in tag_relation_dict:
            print("[ERROR] parent code id {} is not exists".format(parent_id))
            return False
    return True


def sort_by_dependence(tag_relations):
    """
    根据父子关系、代数，对标签列表进行分批和排序

    :param tag_relations: list 标签配置列表
    :return: list 排序后的标签列表
    """
    tag_id_gene_map = dict()

    def get_tag_gene_from_parent(pid, depth=0):
        """
        递归从父标签获取当前标签子代数
        :param pid: 父标签id
        :param depth: 深度
        :return: 子代数
        """
        if depth >= 20:
            return -999
        if int(pid) == 0:
            return depth
        else:
            parent = tag_relations[pid]
            return get_tag_gene_from_parent(parent["parent_id"], depth + 1)

    for tag_id, tag_info in tag_relations.items():
        if tag_info["gene"] is not None:
            continue
        gene = get_tag_gene_from_parent(tag_info["parent_id"], 0)
        if gene < 0:
            print("[ERROR] get gene of {} failed, get {}".format(tag_id, gene))
            return False
        tag_info["gene"] = gene
        tag_id_gene_map[tag_id] = gene
    sorted_tags_generation = {}
    for tag_gene in sorted(tag_relations.values(), key=lambda x: int(x["gene"])):
        if tag_gene["gene"] not in sorted_tags_generation:
            sorted_tags_generation[tag_gene["gene"]] = {}
        sorted_tags_generation[tag_gene["gene"]][str(tag_gene["id"])] = tag_gene

    return sorted_tags_generation, tag_id_gene_map


def gen_and_sync_via_bridge(sync_tags, platform, geog):
    """
    创建tag同步用的增量ms文件
    :param sync_tags: tag配置
    :param platform: 环境
    :param geog: 地域
    :return: boolean 文件创建结果
    """
    changed_info_dict = {gene: dict(CREATE=[], UPDATE=[]) for gene in sync_tags.keys()}
    full_tags = Tag.objects.all()
    online_tags = {str(tag.id): model_to_dict(tag) for tag in full_tags}
    for gene, tag_map in sync_tags.items():
        for tag_id, tag_relation_item in tag_map.items():
            if tag_relation_item.get("filtered", True) is True:
                print("Because of env[{}-{}], discard tag[{}]\n".format(platform, geog, tag_relation_item["code"]))
                continue
            tag_item = tag_relation_item.get("tag_item", {})
            if tag_id not in online_tags:
                changed_info_dict[gene]["CREATE"].append(tag_item)
                # 打印详情
                print("Create New Tag: [{}]{}".format(tag_id, tag_relation_item.get("code")))
                for key in sorted(tag_item.keys()):
                    print("    {}: {}".format(key, json.dumps(tag_item[key])))
                print("\n")

            else:
                online_tag_item = online_tags[tag_id]
                online_tag_item["created_at"] = str(online_tag_item["created_at"])
                online_tag_item["updated_at"] = str(online_tag_item["created_at"])
                check_tag = {
                    key: val
                    for key, val in tag_item.items()
                    if key not in ("created_by", "created_at", "updated_by", "updated_at")
                }
                check_online_tag = {
                    key: val.encode("utf-8") if isinstance(val, str) else val
                    for key, val in online_tag_item.items()
                    if key not in ("created_by", "created_at", "updated_by", "updated_at")
                }
                if operator.ne(check_tag, check_online_tag):
                    check_diff_item = {
                        key: val
                        for key, val in tag_item.items()
                        if check_tag.get(key, None) != check_online_tag.get(key, None)
                    }
                    update_tag_item = tag_item
                    update_tag_item["id"] = tag_item["id"]
                    update_tag_item["updated_at"] = str(datetime.datetime.now())
                    update_tag_item["updated_by"] = "admin"
                    changed_info_dict[gene]["UPDATE"].append(update_tag_item)
                    # 打印详情
                    print("Update Exist Tag: [{}]{}".format(tag_id, online_tag_item["code"]))
                    for key in sorted(check_diff_item.keys()):
                        print(
                            "    {}: {} --> {}".format(
                                key, json.dumps(online_tag_item[key]), json.dumps(update_tag_item[key])
                            )
                        )
                    print("\n")

    # 针对差异生成ms文件
    for gene, changed_info_item in changed_info_dict.items():
        ms_content_list = []
        for method, changed_info_list in changed_info_item.items():
            for changed_info in changed_info_list:
                ms_content_list.append(
                    {
                        "changed_data": json.dumps(changed_info),
                        "change_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                        "primary_key_value": int(changed_info["id"]),
                        "db_name": "bkdata_basic",
                        "table_name": "tag",
                        "method": method,
                    }
                )
        if ms_content_list:
            print("===> ms content for gene {}:\n{}\n".format(gene, json.dumps(ms_content_list)))
            print("you have 3s to check ms file contents")
            print('if you want to STOP sync process, use "CTRL + C"\n')
            time.sleep(3)
            send_sync_requests(ms_content_list)


def send_sync_requests(ms_content_list, batch_cnt=20):
    """
    发送tag同步请求
    :param ms_content_list: 同步tag内容
    :param batch_cnt: 批量同步条数
    :return: boolean 同步结果
    """
    header = {"content-type": "application/json;charset=UTF-8"}
    meta_sync_url = "http://{host}:{port}/v3/meta/sync/".format(
        host=getattr(dataapi_settings, "META_API_HOST"), port=getattr(dataapi_settings, "META_API_PORT")
    )
    ms_chunked_lst = chunked(ms_content_list, batch_cnt)
    for chunk_piece in ms_chunked_lst:
        body = {
            "content_mode": "id",
            "bk_username": "admin",
            "batch": False,
            "affect_original": True,
            "db_operations_list": chunk_piece,
        }
        response = requests.post(meta_sync_url, data=json.dumps(body), headers=header)
        if response.status_code != 200:
            message = "sync status_code error，status_code: {}".format(response.status_code)
            raise ApiRequestError(message)
        ret_obj = response.json()
        if ret_obj["errors"] is not None or ret_obj["code"] != "1500200":
            message = "sync error，detail: {}".format(ret_obj)
            raise ApiResultError(message)
        print("sync success! ret is {}".format(ret_obj))


def init_default_cluster_group(
    platform=settings.RUN_VERSION, geog=settings.GEOG_AREA_CODE, multi_geog=settings.MULTI_GEOG_AREA
):
    """
    初始化默认集群组

    :param platform: string 平台[ee/tgdp/tencent]
    :param geog: string (默认)地域[inland/SEA/NA/...]
    :param multi_geog: boolean True/False
    :return: None
    """

    default_cluster_group_mapping = dict(tgdp=set(), ee=set(), tencent=set())

    header = {"content-type": "application/json;charset=UTF-8"}
    init_url = "http://{host}:{port}/v3/meta/cluster_group_configs/".format(
        host=getattr(dataapi_settings, "META_API_HOST"), port=getattr(dataapi_settings, "META_API_PORT")
    )

    failed_config_list = []
    for platform_name, default_geog_set in default_cluster_group_mapping.items():
        if platform != platform_name:
            continue
        if geog is None and not default_geog_set:
            message = "default geog area not found, please set param `geog`"
            raise ValidationError(message)
        geog = geog if geog is not None else list(default_geog_set)[0]
        # 非多地域以输入地域为准
        if not multi_geog:
            default_geog_set = {geog}
        # 多地域将输入地域和默认已知地域合并注册
        else:
            default_geog_set.add(geog)

        for geog_val in default_geog_set:
            geog_val = geog_val.lower() if geog_val else ""
            con = "_" if geog_val else ""
            cluster_group_id = "default{}{}".format(con, geog_val)
            body = {
                "bk_username": "stream-admin",
                "cluster_group_id": cluster_group_id,
                "cluster_group_name": "{}_group".format(cluster_group_id),
                "cluster_group_alias": "default_{}_group".format(str(geog_val)),
                "scope": "public",
                "tags": [geog_val, "default_cluster_group_config"],
            }
            response = requests.post(init_url, data=json.dumps(body), headers=header)
            if response.status_code != 200:
                message = "sync status_code error，status_code: {}".format(response.status_code)
                print(message)
                failed_config_list.append(cluster_group_id)
                continue
            ret_obj = response.json()
            if ret_obj["errors"] is not None or ret_obj["code"] != "1500200":
                message = "sync error，detail: {}".format(ret_obj)
                if isinstance(ret_obj["errors"], dict) and "cluster_group_id" in ret_obj["errors"]:
                    continue
                print(message)
                failed_config_list.append(cluster_group_id)
                continue
            print("now init cluster {} as default config cluster".format(cluster_group_id))
    if failed_config_list:
        print("init default cluster [{}] config failed! please retry!".format(", ".join(failed_config_list)))
    else:
        print("init default cluster config success.")
