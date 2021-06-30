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
from __future__ import absolute_import, print_function, unicode_literals

import logging

from api import meta_api
from common.meta import metadata_client
from conf.settings import LIFE_CYCLE_TOKEN_PKEY

logger = logging.getLogger(__name__)

BIP_GRADE_CORRECT_DICT = {
    "准三星": 2.5,
    "预备三星": 2.5,
    "准四星": 3.5,
    "预备四星": 3.5,
    "准五星": 4.5,
    "预备五星": 4.5,
    "准六星": 5.5,
    "预备六星": 5.5,
    "退市": 0.0,
}

OPER_STATE_CORRECT_DICT = {
    "前期接触": 0.5,
    "接入评估": 1,
    "接入准备": 1.5,
    "接入中": 2,
    "技术封测": 2.5,
    "测试阶段": 3,
    "封测": 3,
    "内测": 3.5,
    "不删档": 4,
    "公测": 4.5,
    "正式运行": 5,
    "停运": 2,
    "取消": 1,
    "退市": 1,
}

IM_LEVEL_CORRECT_DICT = {
    "关键": 2,
    "重要": 1,
    "普通": 0,
}


def biz_data_correct(biz_dict):
    """
    data correction of biz info
    :return:
    """
    # a) fill OperStateName according to BipGradeName
    oper_state_name = oper_state_name_correct(biz_dict)

    # b) correct BipGradeId according to BipGradeName
    bip_grade_id, bip_grade_name = bip_grade_id_correct(biz_dict)

    # c) correct OperState according to OperStateName
    oper_state = oper_state_correct(biz_dict, oper_state_name)

    # d) correct AppImportantLevel according to AppImportantLevelName
    im_level, im_level_name = app_important_level_correct(biz_dict)
    return (
        bip_grade_id,
        oper_state,
        im_level,
        oper_state_name,
        bip_grade_name,
        im_level_name,
    )


def oper_state_name_correct(biz_dict):
    """
    fill OperStateName according to BipGradeName
    :param biz_dict:
    :return:
    """
    bip_grade_name = biz_dict["BizV1.BipGradeName"]
    oper_state_name = biz_dict["BizV1.OperStateName"]
    oper_state_name = (
        "退市"
        if oper_state_name == -1 and (bip_grade_name == "退市" or bip_grade_name == "已下架")
        else oper_state_name
    )
    return oper_state_name


def bip_grade_id_correct(biz_dict):
    """
    correct BipGradeId according to BipGradeName
    :param biz_dict:
    :return:
    """
    bip_grade_id = biz_dict["BizV1.BipGradeId"]
    bip_grade_name = biz_dict["BizV1.BipGradeName"]
    bip_grade_id = (
        BIP_GRADE_CORRECT_DICT[bip_grade_name]
        if bip_grade_name in list(BIP_GRADE_CORRECT_DICT.keys())
        else bip_grade_id
    )
    bip_grade_id = 0.0 if bip_grade_id == -1 or (not bip_grade_id) else bip_grade_id
    bip_grade_id = float(bip_grade_id) if is_float(bip_grade_id) else 0.0
    return bip_grade_id, bip_grade_name


def oper_state_correct(biz_dict, oper_state_name):
    """
    correct OperState according to OperStateName
    :param biz_dict:
    :return:
    """
    oper_state = biz_dict["BizV1.OperState"]
    oper_state = (
        OPER_STATE_CORRECT_DICT[oper_state_name]
        if oper_state_name in list(OPER_STATE_CORRECT_DICT.keys())
        else oper_state
    )
    oper_state = (
        0.0
        if oper_state == -1 or (not oper_state) or (not oper_state_name)
        else float(oper_state)
    )
    oper_state = float(oper_state) if is_float(oper_state) else 0.0
    return oper_state


def app_important_level_correct(biz_dict):
    """
    correct AppImportantLevel according to AppImportantLevelName
    :param biz_dict:
    :return:
    """
    im_level_name = biz_dict["BizV1.AppImportantLevelName"]
    im_level = (
        IM_LEVEL_CORRECT_DICT[im_level_name]
        if im_level_name in list(IM_LEVEL_CORRECT_DICT.keys())
        else 0
    )
    return im_level, im_level_name


def get_biz_info(biz_id):
    """
    get the basic biz info
    :param biz_id:
    :return:
    """
    statement = (
        """
    {
      biz(func: eq(BizV1.ApplicationID, %s)){
        BizV1.IsBip,
        BizV1.OperStateName
        BizV1.OperState
        BizV1.BipGradeId
        BizV1.BipGradeName
        BizV1.AppImportantLevel
        BizV1.AppImportantLevelName
      }
    }
    """
        % biz_id
    )
    try:
        search_dict = meta_api.complex_search(
            {
                "statement": statement,
                "backend_type": "dgraph",
                "token_pkey": LIFE_CYCLE_TOKEN_PKEY,
            },
            retry_times=3,
            raise_exception=True,
        ).data
    except Exception as e:
        logger.error("Complex search error: {error}".format(error=e.message))
        search_dict = {}
    biz_dict = (
        search_dict["data"]["biz"][0]
        if search_dict and search_dict.get("data", {}).get("biz", [])
        else {}
    )
    if not biz_dict:
        logger.info("can not get the detail info of {}".format(biz_id))
    return biz_dict


def get_project_score(proj_id):
    """
    get the importance score due to project
    :param proj_id: project_id
    :return:
    """
    if not proj_id:
        return 0, False
    statement = (
        """
    {
      proj(func: eq(ProjectInfo.project_id, %s)){
        active
      }
    }
    """
        % proj_id
    )
    try:
        search_dict = meta_api.complex_search(
            {
                "statement": statement,
                "backend_type": "dgraph",
                "token_pkey": LIFE_CYCLE_TOKEN_PKEY,
            },
            retry_times=3,
            raise_exception=True,
        ).data
    except Exception as e:
        logger.error("Complex search error: {error}".format(error=e.message))
        search_dict = {}
    proj_dict = (
        search_dict["data"]["proj"][0]
        if search_dict and search_dict.get("data", {}).get("proj", [])
        else {}
    )
    active = proj_dict.get("active", False)
    proj_score = 1 if proj_dict.get("active", 0) else 0
    return proj_score, active


def get_dataset_info(dataset_id, dataset_type):
    """
    get the basic info of dataset
    :param dataset_id:
    :return:
    """
    if dataset_type == "result_table":
        rt_dict = meta_api.result_tables.retrieve(
            {"result_table_id": dataset_id}, retry_times=3, raise_exception=True
        ).data
        if not rt_dict:
            logger.info("can not get detail info of {}".format(dataset_id))
            return None, None, None, None
        biz_id = rt_dict.get("bk_biz_id", None)
        proj_id = rt_dict.get("project_id", None)
        sensitivity = rt_dict.get("sensitivity", None)
        generate_type = rt_dict.get("generate_type", None)
    else:
        statement = (
            """
        {
          raw_data(func: eq(AccessRawData.id, %s)){
            AccessRawData.bk_biz_id,
            AccessRawData.sensitivity
          }
        }
        """
            % dataset_id
        )
        try:
            search_dict = meta_api.complex_search(
                {
                    "statement": statement,
                    "backend_type": "dgraph",
                    "token_pkey": LIFE_CYCLE_TOKEN_PKEY,
                },
                retry_times=3,
                raise_exception=True,
            ).data
        except Exception as e:
            logger.error("Complex search error: {error}".format(error=e.message))
            search_dict = {}
        rd_dict = (
            search_dict["data"]["raw_data"][0]
            if search_dict and search_dict.get("data", {}).get("raw_data", [])
            else {}
        )
        biz_id = rd_dict.get("AccessRawData.bk_biz_id")
        proj_id = None
        sensitivity = rd_dict.get("AccessRawData.sensitivity")
        generate_type = "user"
    return biz_id, proj_id, sensitivity, generate_type


def is_float(x):
    """
    determine whether a str can be converted into float
    :param x:
    :return:
    """
    try:
        float(x)
        return True
    except Exception as e:
        logger.info("{} is not float error:{}".format(x, e.message))
        return False


def get_heat_range_importance_score(uniq_id):
    """
    get heat_score & range_score & importance_score
    :param uniq_id:
    :return:
    """
    erp_param = {
        "Range": {"starts": [uniq_id], "expression": {"*": True}},
        "Heat": {"starts": [uniq_id], "expression": {"*": True}},
        "Importance": {"starts": [uniq_id], "expression": {"*": True}},
    }
    try:
        ret_dict = metadata_client.query_via_erp(erp_param)
    except Exception as e:
        logger.error("query_via_erp error:{}".format(e))
        return None, None, None
    if ret_dict:
        heat_list = ret_dict.get("Heat", [])
        range_list = ret_dict.get("Range", [])
        im_list = ret_dict.get("Importance", [])
        if heat_list and range_list and im_list:
            importance_score = im_list[0].get("importance_score", 0)
            heat_score = heat_list[0].get("heat_score", 0)
            normalized_range_score = range_list[0].get("normalized_range_score", 13.1)
            return heat_score, normalized_range_score, importance_score
    return None, None, None


def get_asset_value_score(uniq_id):
    """
    get asset_value_score
    :param uniq_id:
    :return:
    """
    erp_param = {"AssetValue": {"starts": [uniq_id], "expression": {"*": True}}}
    try:
        ret_dict = metadata_client.query_via_erp(erp_param)
    except Exception as e:
        logger.error("query_via_erp error:{}".format(e))
        return None
    if ret_dict:
        asset_value_list = ret_dict.get("AssetValue", [])
        if asset_value_list:
            asset_value_score = asset_value_list[0].get("asset_value_score", 4.37)
            return asset_value_score
    return None
