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


from datamanage.utils.api.meta import MetaApi
from datamanage.utils.api.datamanage import DatamanageApi
from datamanage.utils.api.access import AccessApi
from datamanage.pro.lifecycle.metrics.cost import get_storage_info, get_rd_count_info


def get_importance_dict(dataset_id, dataset_type):
    """
    get importance metric, including biz metric and project metric
    :return:
    """
    if dataset_type == 'result_table':
        statement = (
            """
        {
          importance(func:eq(ResultTable.result_table_id, "%s")) {
            ResultTable.bk_biz {
              BKBiz.biz {
                BizV1.BipGradeName
                BizV1.OperStateName
                BizV1.IsBip
                BizV1.AppImportantLevelName
                }
            }
            ResultTable.project {
              active
            }
          }
        }
        """
            % dataset_id
        )
    else:
        statement = (
            """
        {
          importance(func:eq(AccessRawData.id, %s)) {
            AccessRawData.bk_biz {
              BKBiz.biz {
                BizV1.BipGradeName
                BizV1.OperStateName
                BizV1.IsBip
                BizV1.AppImportantLevelName
              }
            }
          }
        }
        """
            % dataset_id
        )
    search_dict = MetaApi.complex_search({'statement': statement, 'backend_type': 'dgraph'}).data
    tmp_dict = search_dict['data']['importance'][0] if search_dict.get('data', {}).get('importance') else {}
    if tmp_dict:
        biz_list = (
            tmp_dict.get('ResultTable.bk_biz', [])
            if dataset_type == 'result_table'
            else tmp_dict.get('AccessRawData.bk_biz', [])
        )
        biz_dict = biz_list[0]['BKBiz.biz'][0] if biz_list and biz_list[0].get("BKBiz.biz") else {}
        # active = False if dataset_type == 'raw_data'
        project_dict = (
            tmp_dict['ResultTable.project'][0]
            if dataset_type == 'result_table' and tmp_dict.get('ResultTable.project')
            else {}
        )
    else:
        biz_dict = {}
        project_dict = {}
    importance_dict = {
        "bip_grade_name": biz_dict.get("BizV1.BipGradeName", None),
        "oper_state_name": biz_dict.get("BizV1.OperStateName", None),
        "is_bip": biz_dict.get("BizV1.IsBip", None),
        "app_important_level_name": biz_dict.get("BizV1.AppImportantLevelName", None),
        "active": project_dict.get("active", False),
    }
    return importance_dict


def get_alert_info(dataset_id, bk_username):
    """
    数据是否有告警，以及告警数量
    :param dataset_id:
    :param bk_username:
    :return:
    """
    alert_list = DatamanageApi.alert_count(
        {
            "data_set_ids": dataset_id,
            "format": "value",
            "start_time": "now()-1d",
            "end_time": "now()",
            "bk_username": bk_username,
        }
    ).data
    # 告警数量
    alert_count = 0
    has_alert = False
    if alert_list:
        has_alert = True
        for each_alert in alert_list:
            alert_count += each_alert.get('value', {}).get('alert_count', 0)
    return has_alert, alert_count


def get_data_count_info(dataset_id, dataset_type):
    """
    判断最近1天是否有数据
    :return:
    """
    if dataset_type == 'result_table':
        storages_dict, has_data = get_storage_info(dataset_id)
    else:
        rd_dict = AccessApi.rawdata.retrieve({'raw_data_id': dataset_id}).data
        raw_data_name = rd_dict['raw_data_name']
        bk_biz_id = rd_dict['bk_biz_id']
        has_data = get_rd_count_info(raw_data_name, bk_biz_id)
    return has_data
