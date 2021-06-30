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

import copy
import functools
import hashlib
import json
import operator
import threading
import time
from collections import OrderedDict

from django import forms
from django.core.cache import cache
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from apps import forms as data_forms
from apps.api import DataManageApi
from apps.common.log import logger
from apps.common.views import list_route
from apps.dataflow.datamart.datamap_tag_selector import (
    format_row,
    format_row_top_zero,
    format_tag_selector,
    get_tag_sort_count,
)
from apps.dataflow.datamart.datamap_tree import (
    filter_count_zero,
    label_tree,
    parse_tree_json,
)
from apps.dataflow.utils import format_date_string, fun_dup_remove
from apps.generic import APIViewSet
from apps.utils.local import activate_request

CACHE_TIME = 60 * 60


class TagViewSet(APIViewSet):
    @list_route(methods=["get"], url_path="sort_count")
    def sort_count(self, request):
        """
        @api {get} /datamart/datamap/tag/sort_count/ 获取数据地图标签展示列表: top_n标签 & 层级结构
        @apiName sort_count
        @apiGroup TagViewSet
        @apiParam {Int} top 默认展示top个标签
        @apiSuccessExample {json} 成功返回
        {
            "message":"",
            "code":"00",
            "data":[
                {
                    "description":"主机（包含物理机/虚拟机/容器）系统性能数据（主要是CPU、磁盘、内存等指标）",
                    "parent_id":1,
                    "tag_code":"operating_system",
                    "tag_alias":"操作系统",
                    "seq_index":3,
                    "tag_id":5,
                    "sub_list":Array[0],
                    "is_more_show":false,
                    "dataset_count":135,
                    "tooltip":"操作系统（主机（包含物理机/虚拟机/容器）系统性能数据（主要是CPU、磁盘、内存等指标）",
                    "is_flat":true,
                    "me_dataset_count":0,
                    "is_inrow_more_show":false,
                    "sub_top_list":[
                        {
                            "description":"",
                            "parent_id":322,
                            "tag_code":"cpu",
                            "tag_alias":"CPU使用",
                            "tag_id":50,
                            "seq_index":1,
                            "dataset_count":26,
                            "be_category_alias":"系统性能",
                            "tooltip":"CPU使用",
                            "is_click":false,
                            "be_category_id":322,
                            "be_category_code":"sys_performance",
                            "me_dataset_count":26,
                            "is_top10":true,
                            "sub_list":[

                            ],
                            "tag_type":"business"
                        }
                    ],
                    "tag_type":"business"
                },
            ],
            "result":true
        }
        """

        class TagForm(data_forms.BaseForm):
            top = forms.IntegerField(label="默认展示的标签数", required=True)
            delete_biz_tag = forms.IntegerField(label="是否删除业务标签", required=False)
            is_overall_topn_shown = forms.IntegerField(label="是否显示整体热门标签", required=False)
            overall_top = forms.IntegerField(label="整体热门标签数", required=False)
            is_latest_api = forms.IntegerField(label="是否使用最新接口的结构", required=False)

        _params = self.valid(TagForm)
        top = _params.get("top")
        delete_biz_tag = _params.get("delete_biz_tag")

        search_res_tmp = DataManageApi.tag_sort_count({"top": top})
        search_res = search_res_tmp if isinstance(search_res_tmp, list) else search_res_tmp.get("tag_list", [])
        list(map(format_row, search_res))

        if delete_biz_tag:
            for each_row in search_res[:]:
                if each_row.get("tag_type", "") == "business" or each_row.get("tag_type", "") == "datamap_other":
                    search_res.remove(each_row)

        return Response(search_res)

    @list_route(methods=["get"], url_path="sort_count_for_access")
    def sort_count_for_access(self, request):
        """
        @api {get} /datamart/datamap/tag/sort_count_for_access/ 获取数据接入页面标签展示列表: top_n标签 & 层级结构
        @apiName sort_count
        @apiGroup TagViewSet
        @apiParam {Int} top 默认展示top个标签
        @apiSuccessExample {json} 成功返回
        {
            "message":"",
            "code":"00",
            "data":[
                {
                    "description":"主机（包含物理机/虚拟机/容器）系统性能数据（主要是CPU、磁盘、内存等指标）",
                    "parent_id":1,
                    "tag_code":"operating_system",
                    "tag_alias":"操作系统",
                    "seq_index":3,
                    "tag_id":5,
                    "sub_list":Array[0],
                    "is_more_show":false,
                    "dataset_count":135,
                    "tooltip":"操作系统（主机（包含物理机/虚拟机/容器）系统性能数据（主要是CPU、磁盘、内存等指标）",
                    "is_flat":true,
                    "me_dataset_count":0,
                    "is_inrow_more_show":false,
                    "sub_top_list":[
                        {
                            "description":"",
                            "parent_id":322,
                            "tag_code":"cpu",
                            "tag_alias":"CPU使用",
                            "tag_id":50,
                            "seq_index":1,
                            "dataset_count":26,
                            "be_category_alias":"系统性能",
                            "tooltip":"CPU使用",
                            "is_click":false,
                            "be_category_id":322,
                            "be_category_code":"sys_performance",
                            "me_dataset_count":26,
                            "is_top10":true,
                            "sub_list":[

                            ],
                            "tag_type":"business"
                        }
                    ],
                    "tag_type":"business"
                },
            ],
            "result":true
        }
        """

        class TagForm(data_forms.BaseForm):
            top = forms.IntegerField(label="默认展示的标签数", required=True)
            is_overall_topn_shown = forms.IntegerField(label="是否显示整体热门标签", required=False)
            overall_top = forms.IntegerField(label="整体热门标签数", required=False)
            used_in_data_dict = forms.IntegerField(label="是否使用在数据字典", required=False)
            is_latest_api = forms.IntegerField(label="是否使用最新接口的结构", required=False)

        _params = self.valid(TagForm)
        top = _params.get("top")
        is_overall_topn_shown = _params.get("is_overall_topn_shown", 0)
        overall_top = _params.get("overall_top", 0)
        used_in_data_dict = _params.get("used_in_data_dict", 0)
        is_latest_api = _params.get("is_latest_api", 0)

        search_res_dict, search_res_zero_dict = get_tag_sort_count(
            used_in_data_dict, top, is_overall_topn_shown, overall_top
        )
        # if not used_in_data_dict:
        #     search_res_dict = DataManageApi.tag_sort_count(
        #         {'top': top, 'exclude_source': 1, 'is_overall_topn_shown': is_overall_topn_shown,
        #          'overall_top': overall_top})
        #     search_res_zero_dict = DataManageApi.tag_sort_count({'top': 0, 'exclude_source': 1})
        # else:
        #     search_res_dict = DataManageApi.tag_sort_count(
        #         {'top': top, 'is_overall_topn_shown': is_overall_topn_shown,
        #          'overall_top': overall_top})
        #     search_res_zero_dict = DataManageApi.tag_sort_count({'top': 0})

        search_res = search_res_dict if isinstance(search_res_dict, list) else search_res_dict.get("tag_list", [])
        search_res_zero = (
            search_res_zero_dict if isinstance(search_res_zero_dict, list) else search_res_zero_dict.get("tag_list", [])
        )

        format_row_top_zero(search_res_zero)

        # 不需要来源系统标签
        if not used_in_data_dict:
            for each_row in search_res:
                if each_row.get("tag_code", "") == "datamap_source":
                    search_res.remove(each_row)
                    break
            for each_row in search_res_zero:
                if each_row.get("tag_code", "") == "datamap_source":
                    search_res_zero.remove(each_row)
                    break

        # 将search_res_zero处理过后的sub_list赋值给search_res
        sub_tag_dict = {}
        for each_row in search_res_zero:
            sub_tag_dict[each_row.get("tag_code")] = each_row.get("sub_list")
        for each_row in search_res:
            # if each_row['tag_code'] == 'datamap_source':
            #     each_row['tag_alias'] = '来源系统'
            #     each_row['tooltip'] = '来源系统'
            del each_row["sub_list"]
            each_row["sub_list"] = sub_tag_dict[each_row.get("tag_code")]
        list(map(format_row, search_res))
        for each_row in search_res:
            if each_row["tag_code"] == "data_type":
                each_row["is_more_show"] = False
            else:
                each_row["is_more_show"] = True
        # 如果要展示全部热门标签
        overall_top_tag_list = []
        if is_overall_topn_shown:
            overall_top_tag_list = search_res_dict.get("overall_top_tag_list", [])
        if is_latest_api:
            ret_dict = format_tag_selector(search_res, used_in_data_dict, overall_top_tag_list)
            return Response(ret_dict)
        else:
            return Response(search_res)


CACHE_DICT = {}

DATAMAP_DICT_TOKEN_PKEY = "datamap_dict"
CACHE_TARGET = {
    "tree": json.dumps(
        OrderedDict(
            {
                "bk_biz_id": None,
                "project_id": None,
                "keyword": "",
                "tag_ids": [],
                "is_show_count_zero": True,
                "bk_biz_name": None,
                "cal_type": ["standard"],
            }
        )
    ),
    "_statistic_data": json.dumps(
        OrderedDict({"bk_biz_id": None, "project_id": None, "keyword": "", "tag_ids": [], "cal_type": ["standard"]})
    ),
    "_overall_statistic": json.dumps(
        OrderedDict(
            {
                "bk_biz_id": None,
                "project_id": None,
                "platform": "bk_data",
                "keyword": "",
                "tag_ids": [],
                "cal_type": ["standard"],
                "page": 1,
                "page_size": 10,
                "data_set_type": "all",
                "me_type": "tag",
                "has_standard": 1,
                "tag_code": "virtual_data_mart",
            }
        )
    ),
    "_data_set_list": json.dumps(
        OrderedDict(
            {
                "bk_biz_id": None,
                "project_id": None,
                "tag_ids": [],
                "keyword": "",
                "tag_code": "virtual_data_mart",
                "me_type": "tag",
                "has_standard": 1,
                "cal_type": ["standard"],
                "data_set_type": "all",
                "page": 1,
                "page_size": 10,
                "platform": "bk_data",
                "is_cache_used": 1,
            }
        )
    ),
    "_popular_query": json.dumps({}),
}


CACHE_TIME_DICT = {
    "tree": 60 * 60,
    "_statistic_data": 60 * 60,
    "_overall_statistic": 60 * 60,
    "_data_set_list": 24 * 60 * 60,
    "_popular_query": 24 * 60 * 60,
}

CACHE_FUNC_LAST_RUN_TIME = {
    "tree": 0,
    "_statistic_data": 0,
    "_overall_statistic": 0,
    "_data_set_list": 0,
    "_popular_query": 0,
}

ORDER_PARAMS_LIST = [
    "order_range",
    "order_heat",
    "order_importance",
    "order_storage_capacity",
    "order_assetvalue_to_cost",
]


def update_target_cache():
    """
    更新缓存
    :return:
    """
    is_succeed = False
    logger.info("update_target_cache, cache_dict size: [%s]" % len(CACHE_DICT))
    for func_name in list(CACHE_DICT.keys()):
        for cache_target, func_config in list(CACHE_DICT[func_name].items()):
            key = None
            try:
                # 1  判断当前时间与 cache_func_last_run_time之间的差值 是否大于
                if int(time.time()) - CACHE_FUNC_LAST_RUN_TIME[func_name] >= CACHE_TIME_DICT[func_name]:
                    func = func_config["func"]
                    self = func_config.get("self", None)
                    request = func_config["request"]
                    args = func_config["params"]["args"]
                    kwargs = func_config["params"]["kwargs"]
                    activate_request(request)

                    if request.method == "GET":
                        request.GET["token_pkey"] = DATAMAP_DICT_TOKEN_PKEY
                    else:
                        request_params = json.loads(request.body)
                        request_params["token_pkey"] = DATAMAP_DICT_TOKEN_PKEY
                        request.body = json.dumps(request_params)

                    key = get_md5_key(cache_target + func_name)
                    if cache.get(key):
                        return True
                    result = func(self, request, *args, **kwargs)
                    if result:
                        cache.set(key, result, CACHE_TIME_DICT[func_name] * 2)
                        logger.info(
                            "key: {}, update_target_cache func: [{}] successfully, cache_target_size: [{}]".format(
                                key, func.__name__, len(CACHE_DICT[func_name])
                            )
                        )
                        is_succeed = True
                    CACHE_FUNC_LAST_RUN_TIME[func_name] = int(time.time())
            except Exception as e:
                logger.exception("key: {}, update_target_cache error: {}".format(key, e.message))
    return is_succeed


class UpdateTargetCache(threading.Thread):
    def __init__(self):
        super(UpdateTargetCache, self).__init__()
        self.setDaemon(True)
        self._stop_event = threading.Event()
        self.running = False

    def stopped(self):
        """

        :return:
        """
        try:
            return self._stop_event.isSet()
        except Exception as e:
            logger.warning(
                "stopped method, {} has no attribute _stop_event, error: {}".format(self.__class__.__name__, e.message)
            )
            return True

    def stop(self):
        """

        :return:
        """
        try:
            self._stop_event.set()
            self.running = False
        except Exception as e:
            logger.warning(
                "stop method {} has no attribute _stop_event, error: {}".format(self.__class__.__name__, e.message)
            )

    def run(self):
        """

        :return:
        """
        self.running = True
        while not self.stopped():
            import random

            try:
                update_target_cache()
                time.sleep(CACHE_TIME + random.randint(0, 300) - 150)

            except Exception as e:
                logger.exception("UpdateTargetCache run error: [%s]" % e.message)


update_target_cache_thread = UpdateTargetCache()


def get_md5_key(key):
    """
    生成用于cache的key
    :param key:
    :return:
    """
    m = hashlib.md5()
    m.update(str(key).encode("utf-8"))
    return m.hexdigest()


def judge_cache_active(cache_target, request_params, func_name):
    # 判断请求参数和缓存的key是否一致
    res = False
    if operator.eq(json.loads(cache_target), request_params):
        res = True
    elif func_name == "_data_set_list":
        for each_param in ORDER_PARAMS_LIST:
            if each_param in request_params:
                params_dict = copy.deepcopy(request_params)
                params_dict.pop(each_param)
                if operator.eq(json.loads(cache_target), params_dict):
                    res = True
                break
    elif func_name == "_popular_query":
        res = True
    return res


def datamap_cache(func):
    # add thread cache update task

    @functools.wraps(func)
    def wrapper(self, request, *args, **kwargs):

        # 判断是否目标缓存参数，并取出目标缓存参数
        func_name = func.__name__
        cache_target = CACHE_TARGET[func_name]
        if func_name not in CACHE_DICT:
            CACHE_DICT[func_name] = {}

        if not update_target_cache_thread.running:
            update_target_cache_thread.start()

        # check whether result in cache
        if request.method == "GET":
            request_params = dict(request.query_params.iterlists())
        else:
            request_params = json.loads(request.body)
        is_cache_active = judge_cache_active(cache_target, request_params, func_name)

        if func_name == "_data_set_list":
            for each_param in ORDER_PARAMS_LIST:
                if each_param in request_params:
                    cache_target_dict = json.loads(cache_target)
                    cache_target_dict[each_param] = "desc"
                    cache_target = json.dumps(cache_target_dict)
                    break

        key = get_md5_key(cache_target + func_name)
        logger.info("datamap_cache key: {}, func_name: {}, cache_target: {}".format(key, func_name, cache_target))
        if is_cache_active:
            logger.info(
                "may datamap_cache set CACHE_DICT, func: [{}], params {}".format(func_name, json.dumps(request_params))
            )
            CACHE_DICT[func_name][cache_target] = {
                "func": func,
                "self": self,
                "request": request,
                "params": {"args": args, "kwargs": kwargs},
            }

        if is_cache_active and cache.get(key):
            return cache.get(key)

        response = func(self, request, *args, **kwargs)
        if is_cache_active and not cache.get(key) and response:
            # 把需要定时更新的函数参数记录起来
            logger.info(
                "may datamap_cache set cache {}, key: {}, func_name: {}".format(
                    json.dumps(request_params), key, func_name
                )
            )
            cache.set(key, response, 2 * CACHE_TIME_DICT[func_name])
        return response

    return wrapper


class TreeViewSet(APIViewSet):
    @datamap_cache
    def tree(self, request):
        """
        :param request:
        :return:
        """
        request_params = json.loads(request.body)
        is_show_count_zero = request_params.get("is_show_count_zero", True)
        tree = DataManageApi.data_map_tree(request_params)
        if tree:
            tree[0]["category_alias"] = (
                request_params.get("bk_biz_name") if request_params.get("bk_biz_name") else _("数据地图")
            )
        for each_tree in tree:
            each_tree["is_selected"] = 0
            each_tree["parent_zero"] = 0

        for node in tree:
            # 首先根据is_show_count_zero判断是否要过滤掉count=0的节点，如果is_show_count_zero=False，则过滤
            if not is_show_count_zero:
                filter_count_zero(node)
            label_tree(node)
        relations = []
        entity_attribute_value = []
        parse_tree_json(tree, relations, entity_attribute_value, {}, 0)
        # 对relations和entity_attribute_value做去重
        relations = functools.reduce(
            fun_dup_remove,
            [
                [],
            ]
            + relations,
        )
        entity_attribute_value = functools.reduce(
            fun_dup_remove,
            [
                [],
            ]
            + entity_attribute_value,
        )

        res = {
            "tree_structure": tree,
            "baseEntityGuid": "techops",
            "relations": relations,
            "entity_attribute_value": entity_attribute_value,
        }
        return res

    def create(self, request):
        """
        @api {post} /datamart/datamap/tree/ 获取数据地图map树形结构
        @apiName tree
        @apiGroup TreeViewSet
        @apiParam {Int} bk_biz_id 业务id
        @apiParam {Int} project_id 项目id
        @apiParam {String} keyword 查询关键字 "tag"/"standard"
        @apiParam {List} cal_type 是否显示标准数据&是否仅显示标准数据 ["standard""]/[standard", "only_standard"]
        @apiParam {String} tag_ids 选中的标签
        @apiParam {Boolean} is_show_count_zero 是否显示空分类
        @apiParamExample {json} 参数样例:
        {
            "message":"",
            "code":"00",
            "data":{
                "entity_attribute_value":[
                    {
                        "child_show":-1,
                        "direction":"left",
                        "code":"game",
                        "is_selected":0,
                        "id":"game",
                        "row":0,
                        "count":92,
                        "parent_zero":0,
                        "has_standard":1,
                        "name":"游戏",
                        "column":1,
                        "tptGroup":"default",
                        "type":"hasstandard",
                        "parent_code":"bissness"
                    },
                    {
                        "child_show":-1,
                        "direction":"right",
                        "code":"virtual_data_mart",
                        "is_selected":0,
                        "id":"virtual_data_mart",
                        "row":0,
                        "count":4536,
                        "parent_zero":0,
                        "has_standard":0,
                        "name":"数据集市",
                        "column":0,
                        "tptGroup":"root",
                        "type":"root",
                        "parent_code":"virtual_data_node"
                    }
                ],
                "tree_structure":[
                    {
                        "loc":-1,
                        "parent_zero":0,
                        "category_alias":"数据集市",
                        "description":"数据集市",
                        "is_selected":0,
                        "tag_type":"business",
                        "kpath":1,
                        "sync":1,
                        "category_id":-1,
                        "seq_index":1,
                        "dataset_count":4536,
                        "parent_code":"virtual_data_node",
                        "parent_id":-1,
                        "me_type":"tag",
                        "type":"root",
                        "icon":null,
                        "dm_pos":0,
                        "sub_list":[],
                        "category_name":"virtual_data_mart"
                    }
                ],
                "relations":[
                    {
                        "toEntityId":"game",
                        "fromEntityId":"virtual_data_mart"
                    }
                ],
                "baseEntityGuid":"techops"
            },
            "result":true
        }
        """
        # res = cache.get('tree_structure')
        # if res is None:
        return Response(self.tree(request))


class StatisticDataViewSet(APIViewSet):
    @datamap_cache
    def _statistic_data(self, request):
        """
        :param request:
        :return:
        """
        request_params = json.loads(request.body)
        cal_type = request_params.get("cal_type", [])
        search_res = DataManageApi.statistic_data(request_params)

        recent_dataset_format = {"x": [], "y": []}
        for each_date in search_res.get("recent_dataset_count_details", []):
            recent_dataset_format["x"].append(format_date_string(each_date.get("day", "")))
            recent_dataset_format["y"].append(each_date.get("dataset_count", 0))
        search_res["recent_dataset_format"] = recent_dataset_format

        recent_data_source_format = {"x": [], "y": []}
        for each_date in search_res.get("recent_data_source_count_details", []):
            recent_data_source_format["x"].append(format_date_string(each_date.get("day", "")))
            recent_data_source_format["y"].append(each_date.get("data_source_count", ""))
        search_res["recent_data_source_format"] = recent_data_source_format

        if cal_type:
            recent_standard_dataset_format = {"x": [], "y": []}
            for each_date in search_res.get("recent_standard_dataset_count_details", []):
                recent_standard_dataset_format["x"].append(format_date_string(each_date.get("day", "")))
                recent_standard_dataset_format["y"].append(each_date.get("standard_dataset_count", ""))
            search_res["recent_standard_dataset_format"] = recent_standard_dataset_format
        return search_res

    @list_route(methods=["post"], url_path="statistic_data")
    def statistic_data(self, request):
        """
        @api {post} /datamart/datamap/statistic/statistic_data/ 获取数据地图右侧的统计数据
        @apiName statistic_data
        @apiGroup StatisticDataViewSet
        @apiParam {Int} bk_biz_id 业务id
        @apiParam {Int} project_id 项目id
        @apiParam {String} keyword 查询关键字 "tag"/"standard"
        @apiParam {List} cal_type 是否显示标准数据&是否仅显示标准数据 ["standard""]/[standard", "only_standard"]
        @apiParam {String} tag_ids 选中的标签
        @apiParamExample {json} 参数样例:
        {
            "message":"",
            "code":"00",
            "data":{
                "project_count":128,
                "recent_dataset_count_details":[
                    {
                        "dataset_count":3,
                        "day":"20190903"
                    },
                    {
                        "dataset_count":8,
                        "day":"20190904"
                    }
                ],
                "dataset_count":3362,
                "recent_dataset_count_sum":24,
                "recent_data_source_format":{
                    "y":[
                        0,
                        0
                    ],
                    "x":[
                        "09/03",
                        "09/04"
                    ]
                },
                "bk_biz_count":100,
                "recent_standard_dataset_count_details":[
                    {
                        "standard_dataset_count":0,
                        "day":"20190903"
                    },
                    {
                        "standard_dataset_count":0,
                        "day":"20190904"
                    }
                ],
                "data_source_count":1174,
                "recent_data_source_count_details":[
                    {
                        "day":"20190903",
                        "data_source_count":0
                    },
                    {
                        "day":"20190904",
                        "data_source_count":0
                    }
                ],
                "standard_dataset_count":35,
                "recent_standard_dataset_format":{
                    "y":[
                        0,
                        0
                    ],
                    "x":[
                        "09/03",
                        "09/04"
                    ]
                },
                "recent_dataset_format":{
                    "y":[
                        3,
                        8
                    ],
                    "x":[
                        "09/03",
                        "09/04"
                    ]
                },
                "recent_standard_dataset_count_sum":0,
                "recent_data_source_count_sum":15
            },
            "result":true
        }
        """

        return Response(self._statistic_data(request))
