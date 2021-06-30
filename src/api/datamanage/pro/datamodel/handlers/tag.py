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

from django.utils.translation import ugettext as _

from datamanage.utils.api.meta import MetaApi
from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.utils import list_diff


def tag_target(target_id, target_type, tags, bk_username):
    """
    给实体打标签 (如果有不存在的自定义标签调用接口的时候会创建自定义标签，并给接口打自定义标签和系统内置标签)
    :param target_id: {Int} 实体id
    :param target_type: {String} 实体类型
    :param tags: {List} 标签列表[{'tag_code':'xx', 'tag_alias':'yy'}]
    :param bk_username: {String} 用户名
    :return: {List} 标签列表,例如[{"alias": "公共维度", "code": "common_dimension"}]
    """
    param_dict = {
        'tag_targets': [
            {
                'target_id': target_id,
                'target_type': target_type,
                'tags': [{'tag_code': tag_dict['tag_code'], 'tag_alias': tag_dict['tag_alias']} for tag_dict in tags],
            }
        ],
        'ret_detail': True,
        'bk_username': bk_username,
    }
    # 接口成功返回标签列表，例如[{"alias": "公共维度", "code": "common_dimension"}]
    ret_list = MetaApi.tagged(param_dict, raise_exception=True).data

    return ret_list


def untag_target(target_id, target_type, tags, bk_username):
    """
    删除实体上打的标签
    :param target_id: {Int} 实体id
    :param target_type: {String} 实体类型
    :param tags: {List}标签code列表
    :return: {Boolean}是否成功
    """
    param_dict = {
        'tag_targets': [
            {
                'target_id': target_id,
                'target_type': target_type,
                'tags': [{'tag_code': tag_dict['tag_code']} for tag_dict in tags],
            }
        ],
        'bk_username': bk_username,
    }
    ret = MetaApi.untagged(param_dict, raise_exception=True).data

    if ret == 'Success':
        return True
    return False


def get_datamodel_tags(model_ids, with_tag_alias=False):
    """
    批量获取数据模型的标签
    :param model_ids: {List}模型ids
    :param with_tag_alias: {Boolean}是否返回标签别名
    :return: {Dict}模型对应的标签
    """
    data_dict = MetaApi.get_target_tag(
        {
            'target_type': ['data_model'],
            'target_filter': json.dumps(
                [
                    {
                        "criterion": [{"k": "model_id", "func": "eq", "v": model_id} for model_id in model_ids],
                        'condition': 'OR',
                    }
                ]
            ),
        },
        raise_exception=True,
    ).data

    if data_dict['count'] == 0:
        return {}

    categories = ['business', 'system', 'application', 'desc', 'customize']

    tag_target_dict = {}
    for tag_dict in data_dict['content']:
        tags = tag_dict['tags']
        tag_codes = []
        tag_codes_alias = []
        for cate in categories:
            tag_codes.extend([tag['code'] for tag in tags[cate]])
            tag_codes_alias.extend([{'tag_code': tag['code'], 'tag_alias': tag['alias']} for tag in tags[cate]])
        # 返回标签别名
        if with_tag_alias:
            tag_target_dict[tag_dict['model_id']] = tag_codes_alias
        else:
            tag_target_dict[tag_dict['model_id']] = tag_codes

    return tag_target_dict


def get_datamodels_by_tag(keyword):
    """
    按照关键字(标签名称/标签别名)获取标签对应的数据模型
    :param keyword: {String}关键字(标签名称/标签别名)
    :return: model_ids: {List}模型ids
    """
    data_dict = MetaApi.query_tag_targets(
        {
            'target_type': 'data_model',
            'tag_filter': json.dumps(
                [
                    {
                        "criterion": [
                            {"k": "code", "func": "eq", "v": keyword},
                            {"k": "alias", "func": "eq", "v": keyword},
                        ],
                        'condition': 'OR',
                    }
                ]
            ),
        },
        raise_exception=True,
    ).data
    if data_dict['count'] == 0:
        return {}
    model_ids = []
    for target_dict in data_dict['content']:
        if target_dict['model_id'] not in model_ids:
            model_ids.append(target_dict['model_id'])
    return model_ids


def tags_diff(orig_tags, new_tags):
    """
    对比新传入标签列表和模型已有标签列表的差异
    :param orig_tags: {List} 已有标签列表，例如[{"tag_code":"x","tag_alias":"y"}]
    :param new_tags: {List} 新传入标签列表，例如[{"tag_code":"x","tag_alias":"y"}]
    :return:add_tags:{List} 待新增标签列表, delete_tags:{List}待删除标签列表
    """

    class HashDict(dict):
        def __hash__(self):
            return hash('{}-{}'.format(self['tag_code'], self['tag_alias']))

    orig_hash_tags = [HashDict(tag_dict) for tag_dict in orig_tags]
    new_hash_tags = [HashDict(tag_dict) for tag_dict in new_tags]
    # 新传入标签唯一值列表 和 已有标签唯一值列表 的 差异
    add_hash_tags, delete_hash_tags, _ = list_diff(orig_hash_tags, new_hash_tags)
    return add_hash_tags, delete_hash_tags


def update_datamodel_tag(model_id, new_tags, bk_username):
    """
    更新模型标签
    :param model_id: {Int}模型ID
    :param new_tags: {List}实体编辑时标签列表, 例如[{'tag_code':'xx', 'tag_alias':'yy'}]
    :return: {Boolean}是否有标签变更
    """
    # 1）拿到模型已经打上的标签列表
    orig_tag_target_dict = get_datamodel_tags([model_id], with_tag_alias=True)
    orig_tags = orig_tag_target_dict.get(model_id, [])

    # 2) 对比新传入标签列表和模型已有标签列表的差异
    has_tags_updated = False
    add_tags, delete_tags = tags_diff(orig_tags, new_tags)

    # 3) 删除标签
    if delete_tags:
        has_tags_updated = True
        delete_ret = untag_target(model_id, 'data_model', delete_tags, bk_username)
        if not delete_ret:
            raise dm_pro_errors.DataModelUNTAGTARGETError(message=_('删除模型标签失败'))

    # 4) 新增标签
    if add_tags:
        has_tags_updated = True
        add_ret_list = tag_target(model_id, 'data_model', add_tags, bk_username)
        if not add_ret_list:
            raise dm_pro_errors.DataModelTAGTARGETError(message=_('修改模型标签失败'))

    return has_tags_updated


def delete_model_tag(model_id, bk_username):
    """
    删除模型上已有的标签
    :param model_id: {Int} 模型id
    :return: {Boolean} 是否成功删除模型
    """
    # 1) 拿到模型已经打上的标签列表
    tag_target_dict = get_datamodel_tags([model_id], with_tag_alias=True)
    tags = tag_target_dict.get(model_id, [])
    # 2) 删除标签
    if tags:
        delete_ret = untag_target(model_id, 'data_model', tags, bk_username)
        if not delete_ret:
            raise dm_pro_errors.DataModelUNTAGTARGETError(message=_('修改模型标签失败'))
    return True
