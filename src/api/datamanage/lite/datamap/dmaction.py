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

from django.utils.translation import ugettext_lazy as _
from django.conf import settings

from common.local import get_local_param

from datamanage.utils.api.meta import MetaApi


DGRAPH_CACHE_KEY = 'd_cache_key'
DGRAPH_TIME_OUT = 60 * 60 * 2
OTHER_STR = _('其他')
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
processing_type_list = list(processing_type_dict.keys())
RUN_MODE = getattr(settings, 'RUN_MODE', 'DEVELOP')


def dgraph_exclude_bk_biz_id_cond(table_pre=''):
    """
    判断是都要在查询语句中过滤掉海外业务
    :param table_pre:
    :return:
    """
    appenv = get_local_param('appenv', 'ieod')
    cond = ''
    if table_pre:
        table_pre += '.'
    if appenv == 'ieod':
        cond = 'lt({table_pre}bk_biz_id,200000)'
    elif appenv == 'clouds':
        cond = 'gt({table_pre}bk_biz_id,200000)'
    return cond.format(table_pre=table_pre)


def get_single_tag_query(tag_code, final_entites_name, need_me_count=False, need_processing_type=False):
    query_str = """\nmy_{tag_code} as var(func:eq(Tag.code,"{tag_code}")) @recurse{
      parentids_{tag_code} as ~Tag.parent_tag
    }
    var(func:uid(parentids_{tag_code},my_{tag_code})){
      dataset_ids_{tag_code} as Tag.targets @filter(uid({supported_entitys}))
    }
    c_{tag_code}(func:uid(dataset_ids_{tag_code})){
      count(uid)
    }"""
    if need_me_count:
        query_str += """
        var(func:uid(my_{tag_code})){
           dataset_ids_me_{tag_code} as Tag.targets @filter(uid({supported_entitys}))
          }

         me_{tag_code}(func:uid(dataset_ids_me_{tag_code})){
          count(uid)
            }
        """
    if need_processing_type:
        for each_processing_type in processing_type_list:
            if RUN_MODE != 'PRODUCT':
                query_str += """
                c_{tag_code}_{processing_type}(func:eq(ResultTable.processing_type,"{processing_type}"))
                @filter(uid(dataset_ids_{tag_code})){
                  count(uid)
                }
                """
            else:
                query_str += """
                c_{tag_code}_{processing_type}(func:eq(ProcessingTypeConfig.processing_type_name, "{processing_type}")){
                    ~ResultTable.processing_type_obj @filter(uid(dataset_ids_{tag_code})){
                        count(uid)
                    }
                }"""
            query_str = query_str.replace('{processing_type}', each_processing_type)
    query_str = query_str.replace('{tag_code}', tag_code)
    query_str = query_str.replace('{supported_entitys}', final_entites_name)
    return query_str


def meta_dgraph_complex_search(statement, result_pre='', return_original=False, token_pkey=None, token_msg=None):
    if token_pkey or token_msg:
        com_params = {"backend_type": "dgraph", "statement": statement}
        if token_pkey:
            com_params['token_pkey'] = token_pkey
        if token_msg:
            com_params['token_msg'] = token_msg
        query_result = MetaApi.complex_search(com_params, raw=True)
    else:
        query_result = MetaApi.complex_search({"backend_type": "dgraph", "statement": statement}, raw=True)
    result = query_result['result']
    message = query_result['message']
    if not result:
        raise Exception(message)
    data = query_result['data']
    dgraph_result_dict = data['data']
    if return_original:
        return dgraph_result_dict
    count_result = {}
    if dgraph_result_dict:
        for k, v in list(dgraph_result_dict.items()):
            if result_pre:
                k = k.replace(result_pre, '', 1)
            count_result[k] = v[0]['count']
    return count_result


def build_tree(ret_result, id_name):  # 拿传递的全部数据构建一棵完整的树
    parse_result = []
    tmp_result = []  # 找出有效可展示的数据
    not_visible_dict = {}  # visible=0的id与parent_id,在数据地图不过滤
    if ret_result:
        ret_result.sort(key=lambda l: (l['parent_id'], l['seq_index']), reverse=False)
        for ret_dict in ret_result:
            tmp_result.append(ret_dict)

    if tmp_result:
        for ret_dict in tmp_result:
            parent_id = ret_dict['parent_id']
            if parent_id in not_visible_dict:
                p_parent_id = not_visible_dict[parent_id]
                while p_parent_id in not_visible_dict:
                    p_parent_id = not_visible_dict[p_parent_id]
                ret_dict['parent_id'] = p_parent_id

    if tmp_result:
        parent_id_dict = {}  # parent_id:value_list

        for ret_dict in tmp_result:
            parent_id = ret_dict['parent_id']
            if parent_id in parent_id_dict:
                parent_id_dict[parent_id].append(ret_dict)
            else:
                parent_id_dict[parent_id] = [ret_dict]

        for ret_dict in tmp_result:
            id_val = ret_dict[id_name]
            parent_id = ret_dict['parent_id']
            ret_dict['sub_list'] = parent_id_dict[id_val] if id_val in parent_id_dict else []
            if parent_id == 0:
                parse_result.append(ret_dict)

    return parse_result


def sum_parent_node_dataset_counts(node_dict, need_dict, is_accumulate, id_name):  # 子节点累加
    sum_count = 0
    sub_list = node_dict.get('sub_list')
    dataset_count = node_dict['dataset_count']
    tag_id = node_dict[id_name]
    if tag_id in need_dict:
        need_dict[tag_id] = node_dict
    if sub_list:
        for sub_node_dict in sub_list:
            sum_count += sum_parent_node_dataset_counts(sub_node_dict, need_dict, is_accumulate, id_name)
    sum_count += dataset_count
    return sum_count


def overall_handler(result1_list, tree_list, is_accumulate, id_name):  # 累计各个父节点的值并找到需要的节点
    need_dict = {}
    if result1_list:
        for tag1_obj in result1_list:
            tag_id = tag1_obj['tag_id']
            need_dict[tag_id] = {}

    for node_dict in tree_list:  # 累计各个节点的dataset_count的值
        sum_parent_node_dataset_counts(node_dict, need_dict, is_accumulate, id_name)  # 顺便找出需要的数据

    del result1_list[:]  # 清空
    for dict_val in list(need_dict.values()):
        result1_list.append(dict_val)

    result1_list.sort(key=lambda l: (l['dataset_count']), reverse=True)
    return result1_list


def handler_sub_leaf(node_obj, result_sub_leaf):  # 处理一个节点下的所有的子节点的数据
    sub_list = node_obj['sub_list']
    if sub_list:
        for sub_obj in sub_list:
            result_sub_leaf.append(sub_obj)
            handler_sub_leaf(sub_obj, result_sub_leaf)


def tile_all_sub_leaf(node_obj, result_sub_leaf):  # 平铺一个节点下的所有的子节点的数据,不包括它自己
    sub_list = node_obj['sub_list']
    if sub_list:
        for sub_obj in sub_list:
            result_sub_leaf.append(sub_obj)
            tile_all_sub_leaf(sub_obj, result_sub_leaf)
