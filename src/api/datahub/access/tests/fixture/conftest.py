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
from __future__ import absolute_import

import copy
import json

import httpretty
from common.api.modules.auth import AUTH_API_URL
from conf.dataapi_settings import (
    CC_API_URL,
    COLLECTOR_HUB_API_URL,
    JOB_API_URL,
    TOF_API_URL,
    TQOS_API_URL,
)
from datahub.access.api.bk_node import BK_NODE_API_URL
from datahub.pizza_settings import (
    ACCESS_API_URL,
    DATABUS_API_URL,
    DATAMANAGE_API_URL,
    META_API_URL,
)
from mock import Mock, patch

# mock func


def mock_dmonitor_alert_create():
    httpretty.register_uri(
        httpretty.POST,
        DATAMANAGE_API_URL + "dmonitor/alert_configs/rawdata/",
        body=mock_common_response,
    )


def mock_dmonitor_alert_delete(raw_data_id):
    httpretty.register_uri(
        httpretty.DELETE,
        DATAMANAGE_API_URL + "dmonitor/alert_configs/rawdata/%d/" % raw_data_id,
        body=mock_common_response,
    )


def mock_sycn_auth():
    httpretty.register_uri(httpretty.POST, AUTH_API_URL + "sync/", body=mock_common_response)


def mock_collector_hub_start(raw_data_id):
    httpretty.register_uri(
        httpretty.POST,
        COLLECTOR_HUB_API_URL + "deploy_plan/" + raw_data_id + "/start/",
        body=mock_common_response,
    )


def mock_collector_hub_stop(raw_data_id):
    httpretty.register_uri(
        httpretty.POST,
        COLLECTOR_HUB_API_URL + "deploy_plan/" + raw_data_id + "/stop/",
        body=mock_common_response,
    )


def mock_collector_hub_summary(raw_data_id, status):
    mock_collector_hub_summary_response_dict = json.loads(mock_collector_hub_summary_response)
    mock_collector_hub_summary_response_dict["data"]["deploy_status"] = status

    httpretty.register_uri(
        httpretty.GET,
        COLLECTOR_HUB_API_URL + "deploy_plan/" + raw_data_id + "/status/summary/",
        body=json.dumps(mock_collector_hub_summary_response_dict),
    )


def mock_databus_clean_list(raw_data_id):
    httpretty.register_uri(
        httpretty.GET,
        DATABUS_API_URL + "cleans/",
        body=mock_databus_clean_list_response,
    )


def mock_databus_task_start(result_table_id):
    httpretty.register_uri(httpretty.POST, DATABUS_API_URL + "tasks/", body=mock_common_response)


def mock_databus_task_stop(result_table_id):
    httpretty.register_uri(
        httpretty.DELETE,
        DATABUS_API_URL + "tasks/" + result_table_id + "/",
        body=mock_common_response,
    )


def mock_open_file():
    with patch("access.collectors.script_collector.access.ScriptAccess.check") as mock_open:
        mock_open.return_value.__enter__ = mock_open
        mock_open.return_value.__iter__ = Mock(return_value=iter(["first line", "second line"]))


def mock_meta_category_info():
    httpretty.register_uri(
        httpretty.POST,
        META_API_URL + "get_query_info/",
        body=json.dumps(
            {
                "message": u"成功",
                "code": 0,
                "data": [{"InnerIP": "x.x.x.x", "OSName": ""}],
                "result": True,
                "request_id": "xxx",
            }
        ),
    )


def mock_get_host_info_by_ips(data):
    httpretty.register_uri(
        httpretty.POST,
        CC_API_URL + "get_query_info/",
        body=json.dumps(
            {
                "message": u"成功",
                "code": 0,
                "data": [{"InnerIP": "x.x.x.x", "OSName": data}],
                "result": True,
                "request_id": "xxx",
            }
        ),
    )


def mock_get_applist():
    """下发配置"""
    httpretty.register_uri(
        httpretty.GET,
        CC_API_URL + "get_application/",
        body=json.dumps(
            {
                "message": u"成功",
                "code": 0,
                "data": [{"ApplicationID": 2, "ApplicationName": u"蓝鲸"}],
                "result": True,
                "request_id": "xxx",
            }
        ),
    )


def mock_user_perm(bk_username):
    httpretty.register_uri(
        httpretty.POST,
        AUTH_API_URL + "users/%s/check/" % bk_username,
        body=json.dumps({"result": True, "data": True, "message": "ok"}),
    )


def mock_app_perm(app_code):
    httpretty.register_uri(
        httpretty.POST,
        AUTH_API_URL + "app_perm/%s/check/" % app_code,
        body=json.dumps({"result": True, "data": True, "message": "ok"}),
    )


def mock_databus_task_puller():
    httpretty.register_uri(
        httpretty.POST,
        DATABUS_API_URL + "tasks/puller/",
        body=json.dumps({"result": True, "code": "1500200", "data": [], "message": "ok"}),
    )


def mock_tqos_info():
    httpretty.register_uri(httpretty.GET, TQOS_API_URL + "tqosid/ccid_to_qosid", body=mock_tqos_response)


def mock_create_data_id():
    httpretty.register_uri(httpretty.POST, ACCESS_API_URL + "rawdata/", body=mock_create_raw_data_response)


def mock_create_data_auth():
    httpretty.register_uri(
        httpretty.PUT,
        AUTH_API_URL + "raw_data/123/role_users/",
        body=json.dumps({"result": True}),
    )


def mock_get_data_auth():
    httpretty.register_uri(
        httpretty.GET,
        AUTH_API_URL + "raw_data/123/role_users/",
        body=json.dumps(
            {
                "result": True,
                "data": [
                    {
                        "role_name": "数据管理员",
                        "users": ["admin"],
                        "role_id": "raw_data.manager",
                    },
                    {"role_name": "数据清洗员", "users": [], "role_id": "raw_data.cleaner"},
                    {"role_name": "数据观察员", "users": [], "role_id": "raw_data.viewer"},
                ],
            }
        ),
    )


def mock_create_alert():
    httpretty.register_uri(
        httpretty.POST,
        DATAMANAGE_API_URL + "dmonitor/alert_configs/rawdata/",
        body=json.dumps({"result": True, "data": []}),
    )


def mock_update_data_id(data_scenario):
    param = copy.deepcopy(mock_data_id)
    param["data"]["data_scenario"] = data_scenario
    httpretty.register_uri(
        httpretty.PATCH,
        ACCESS_API_URL + "rawdata/123/",
        body=mock_patch_raw_data_response,
    )


def mock_get_data_id(data_scenario):
    param = copy.deepcopy(mock_data_id)
    param["data"]["data_scenario"] = data_scenario
    httpretty.register_uri(httpretty.GET, ACCESS_API_URL + "rawdata/123/", body=json.dumps(param))


def mock_get_mysql_type():
    # 1: mysql
    httpretty.register_uri(httpretty.GET, ACCESS_API_URL + "db_type/1/", body=mock_db_type_mysql_response)


def mock_get_mysql_type_error():
    # 1: mysql
    httpretty.register_uri(
        httpretty.GET,
        ACCESS_API_URL + "db_type/1/",
        body=mock_db_type_mysql_error_response,
    )


def mock_collector_hub_deploy_plan():
    httpretty.register_uri(
        httpretty.POST,
        COLLECTOR_HUB_API_URL + "deploy_plan/",
        body=mock_common_response,
    )


def mock_node_man_create():
    node_man_create_response = json.dumps({"result": True, "message": "", "code": 0, "data": {"subscription_id": 2100}})
    httpretty.register_uri(
        httpretty.POST,
        BK_NODE_API_URL + "subscription/create/",
        body=node_man_create_response,
    )


def mock_node_man_run():
    node_man_create_response = json.dumps({"result": True, "message": "", "code": 0, "data": {"task_id": 250000}})
    httpretty.register_uri(
        httpretty.POST,
        BK_NODE_API_URL + "subscription/run/",
        body=node_man_create_response,
    )


def mock_node_man_switch():
    node_man_create_response = json.dumps({"result": True, "message": "", "code": 0, "data": {"task_id": 250000}})
    httpretty.register_uri(
        httpretty.POST,
        BK_NODE_API_URL + "subscription/switch/",
        body=node_man_create_response,
    )


def mock_node_man_delete():
    node_man_create_response = json.dumps({"result": True, "message": "", "code": 0, "data": {"task_id": 250000}})
    httpretty.register_uri(
        httpretty.POST,
        BK_NODE_API_URL + "subscription/delete/",
        body=node_man_create_response,
    )


def mock_app_info():
    httpretty.register_uri(httpretty.POST, CC_API_URL + "get_query_info/", body=mock_app_info_response)


def mock_send_email():
    httpretty.register_uri(httpretty.POST, TOF_API_URL + "send_mail/", body=mock_common_response)


def mock_hub_update_deploy_plan():
    httpretty.register_uri(
        httpretty.PUT,
        COLLECTOR_HUB_API_URL + "deploy_plan/123/",
        body=mock_common_response,
    )


def mock_hub_deploy_plan_error():
    httpretty.register_uri(
        httpretty.POST,
        COLLECTOR_HUB_API_URL + "deploy_plan/",
        body=mock_collector_hub_result_false_response,
    )


def mock_hub_deploy_plan_format_error():
    httpretty.register_uri(
        httpretty.POST,
        COLLECTOR_HUB_API_URL + "deploy_plan/",
        body=mock_collector_hub_format_error_response,
    )


def mock_fast_execute_script():
    httpretty.register_uri(
        httpretty.POST,
        JOB_API_URL + "fast_execute_script",
        body=mock_job_fast_execute_script_res,
    )


def mock_get_task_result():
    httpretty.register_uri(
        httpretty.GET,
        JOB_API_URL + "get_task_result",
        body=mock_job_get_task_result_res,
    )


def mock_get_task_result_err():
    """
    比如执行job失败
    :return:
    """
    httpretty.register_uri(
        httpretty.GET,
        JOB_API_URL + "get_task_result",
        body=mock_job_get_task_result_err_res,
    )


def mock_get_task_result_404():
    # get_task_result should use GET, POST will call failed
    httpretty.register_uri(
        httpretty.POST,
        JOB_API_URL + "get_task_result",
        body=mock_job_get_task_result_res,
    )


def mock_get_task_ip_log(content="test content"):
    ret = copy.deepcopy(mock_job_get_task_ip_log_res)
    if content != "test content":
        tmp = ret["data"][0]["stepAnalyseResult"][0]["ipLogContent"][0]["logContent"]
        ret["data"][0]["stepAnalyseResult"][0]["ipLogContent"][0]["logContent"] = tmp.replace("@@", content)
    httpretty.register_uri(httpretty.POST, JOB_API_URL + "get_task_ip_log", body=json.dumps(ret))


def mock_db_get_task_ip_log(content="[]"):
    ret = copy.deepcopy(mock_job_get_task_ip_log_res)
    if content != "test content":
        ret["data"][0]["stepAnalyseResult"][0]["ipLogContent"][0]["logContent"] = content
    httpretty.register_uri(httpretty.POST, JOB_API_URL + "get_task_ip_log", body=json.dumps(ret))


def mock_db_data_get_task_ip_log(content="[]"):
    ret = copy.deepcopy(mock_job_get_task_ip_log_res)
    if content != "test content":
        ret["data"][0]["stepAnalyseResult"][0]["ipLogContent"][0]["logContent"] = content
    httpretty.register_uri(httpretty.POST, JOB_API_URL + "get_task_ip_log", body=json.dumps(ret))


def mock_get_task_ip_log_err():
    ret = copy.deepcopy(mock_job_get_task_ip_log_res)
    ret["data"][0]["stepAnalyseResult"][0]["ipLogContent"][0]["status"] = 11
    httpretty.register_uri(httpretty.POST, JOB_API_URL + "get_task_ip_log", body=json.dumps(ret))


def mock_gse_push_file():
    httpretty.register_uri(httpretty.POST, JOB_API_URL + "gse_push_file", body=mock_job_gse_push_file_res)


def mock_get_file_result(content="test content"):
    ret = copy.deepcopy(mock_job_get_file_result)
    if content != "test content":
        tmp = ret["data"]["data"][0]["content"]
        ret["data"]["data"][0]["content"] = tmp.replace("@@", content)
    httpretty.register_uri(httpretty.POST, JOB_API_URL + "get_file_result", body=json.dumps(ret))


def mock_gse_proc_operate():
    httpretty.register_uri(
        httpretty.POST,
        JOB_API_URL + "gse_proc_operate",
        body=mock_job_gse_proc_operate_res,
    )


def mock_get_proc_result():
    httpretty.register_uri(
        httpretty.GET,
        JOB_API_URL + "get_proc_result",
        body=mock_job_get_proc_result_res,
    )


def mock_get_proc_result_error():
    httpretty.register_uri(
        httpretty.GET,
        JOB_API_URL + "get_proc_result",
        body=mock_job_get_proc_result_err_res,
    )


def mock_get_meta_lang_conf():
    mock_common_response = json.dumps({"result": True, "message": "", "code": 0, "data": []})
    httpretty.register_uri(
        httpretty.GET,
        META_API_URL + "content_language_configs/",
        body=mock_common_response,
    )


# mock databus response
mock_databus_clean_list_response = json.dumps(
    {
        "code": "1500200",
        "data": [
            {
                "created_at": "2018-10-30T18:59:14",
                "created_by": "admin",
                "description": "清洗测试",
                "id": 4,
                "json_config": "",
                "pe_config": "",
                "processing_id": "123",
                "raw_data_id": 42,
                "status": "started",
                "status_display": "started",
                "updated_at": "2018-10-31T10:40:24",
                "updated_by": "",
            }
        ],
        "errors": None,
        "message": "ok",
        "result": True,
    }
)
# mock access response

mock_tqos_response = json.dumps(
    {
        "message": "ok",
        "code": "1500200",
        "data": [
            {
                "ccid": "2",
                "zoneid": 6,
                "bu_name": "downloadtools",
                "tqosid": 1002,
                "zone_info": None,
                "id_info": "downloadtools",
            }
        ],
        "result": True,
    }
)

mock_create_raw_data_response = json.dumps(
    {
        "message": "ok",
        "code": "1500200",
        "data": {"raw_data_id": 123},
        "result": True,
    }
)

mock_patch_raw_data_response = json.dumps(
    {
        "message": "ok",
        "code": "1500200",
        "data": {"id": 123},
        "result": True,
    }
)

mock_data_id = {
    "errors": None,
    "message": "ok",
    "code": "1500200",
    "data": {
        "bk_biz_id": 2,
        "created_at": "2018-11-06T20:51:17",
        "data_source": "svr",
        "maintainer": "admin",
        "updated_by": None,
        "raw_data_name": "mayi22",
        "storage_partitions": 1,
        "sensitivity": "private",
        "storage_channel_id": 11,
        "data_encoding": "UTF-8",
        "raw_data_alias": "",
        "updated_at": None,
        "bk_app_code": "bk_dataweb",
        "data_scenario": "",
        "created_by": "admin",
        "id": 123,
        "description": "中文描述",
    },
    "result": True,
}

mock_db_type_mysql_response = json.dumps(
    {
        "errors": None,
        "message": "ok",
        "code": "1500200",
        "data": {
            "updated_by": None,
            "created_at": "2018-11-06T12:09:05",
            "updated_at": None,
            "created_by": None,
            "db_type_alias": "mysql",
            "db_type_name": "mysql",
            "active": 1,
            "id": 1,
            "description": "mysql",
        },
        "result": True,
    }
)

mock_db_type_mysql_error_response = json.dumps(
    {
        "errors": None,
        "message": "AccessDbTypeConfig matching query does not exist.",
        "code": "1500500",
        "data": None,
        "result": False,
    }
)

# mock job response
mock_job_fast_execute_script_res = json.dumps(
    {
        "result": True,
        "code": "00",
        "message": "",
        "data": {"taskInstanceName": "API执行脚本1456715609220", "taskInstanceId": 123},
    }
)

mock_job_fast_execute_script_err_res = json.dumps(
    {
        "code": "20100",
        "request_id": "c98dcbbc322142019288c6116abe4053",
        "result": False,
        "error": {
            "message": "APP认证失败, 参数 app_secret 不正确，请进行检查",
            "code": "20100",
            "error_data": {},
        },
        "message": "APP认证失败, 参数 app_secret 不正确，请进行检查",
        "data": None,
    }
)

mock_job_gse_push_file_res = json.dumps(
    {
        "result": True,
        "code": "00",
        "message": "",
        "data": {"taskInstanceName": "测试", "taskInstanceId": 123},
    }
)

mock_job_get_task_result_res = json.dumps(
    {
        "message": "",
        "code": "00",
        "data": {
            "isFinished": True,
            "taskInstance": {
                "status": 3,
                "totalTime": 1.134,
                "currentStepId": 123,
                "name": "",
                "appCode": None,
                "startTime": "2018-11-13 11:13:00",
                "operationList": [],
                "tagId": "1",
                "startWay": 2,
                "endTime": "2018-11-13 11:13:02",
                "taskId": -1,
                "appId": 2,
                "operator": "admin",
                "taskInstanceId": 123,
                "type": 0,
                "createTime": "2018-11-13 11:13:00",
            },
            "blocks": [
                {
                    "type": 2,
                    "stepInstances": [
                        {
                            "totalTime": 1.134,
                            "isParamSensive": 0,
                            "failIPNum": 0,
                            "text": None,
                            "successIPNum": 1,
                            "dbAccount": None,
                            "isPause": 0,
                            "operator": "admin",
                            "stepInstanceId": 123,
                            "companyId": 0,
                            "execResult": 1,
                            "isIgnoreError": 0,
                            "taskInstanceId": 123,
                            "type": 2,
                            "badIPNum": 0,
                            "status": 3,
                            "stepId": -1,
                            "blockName": "",
                            "dbType": 0,
                            "operationList": [],
                            "startTime": "2018-11-13 11:13:00",
                            "appId": 2,
                            "totalIPNum": 1,
                            "ord": 1,
                            "createTime": "2018-11-13 11:13:00",
                            "dbPort": 0,
                            "account": "root",
                            "dbAccountId": 0,
                            "name": "",
                            "isUseCCFileParam": 0,
                            "isAutoCreateDir": 1,
                            "dbPass": None,
                            "stepIpResult": [
                                {
                                    "resultTypeText": "执行成功",
                                    "ip": "x.x.x.x",
                                    "tag": "",
                                    "status": 9,
                                    "source": 1,
                                }
                            ],
                            "blockOrd": 1,
                            "dbAlias": None,
                            "retryCount": 0,
                            "endTime": "2018-11-13 11:13:02",
                            "runIPNum": 1,
                        }
                    ],
                    "blockOrd": 1,
                    "blockName": "",
                }
            ],
        },
        "result": True,
        "request_id": "xxx",
    }
)

mock_job_get_task_result_err_res = json.dumps(
    {
        "message": "",
        "code": "00",
        "data": {
            "isFinished": True,
            "taskInstance": {
                "status": 4,
                "totalTime": 1.142,
                "currentStepId": 123,
                "name": "xxx执行脚本1542164108700",
                "appCode": "data",
                "startTime": "2018-11-14 10:55:09",
                "operationList": [{"operationName": "从头执行作业", "operationCode": 1}],
                "tagId": "1",
                "startWay": 2,
                "endTime": "2018-11-14 10:55:10",
                "taskId": -1,
                "appId": 2,
                "operator": "admin",
                "taskInstanceId": 123,
                "type": 1,
                "createTime": "2018-11-14 10:55:09",
            },
            "blocks": [
                {
                    "type": 1,
                    "stepInstances": [
                        {
                            "totalTime": 1.142,
                            "isParamSensive": 0,
                            "failIPNum": 1,
                            "text": None,
                            "successIPNum": 0,
                            "dbAccount": None,
                            "isPause": 0,
                            "operator": "admin",
                            "stepInstanceId": 1215702930,
                            "companyId": 0,
                            "execResult": 1,
                            "isIgnoreError": 0,
                            "taskInstanceId": 1207298895,
                            "type": 1,
                            "badIPNum": 0,
                            "status": 4,
                            "stepId": -1,
                            "blockName": "API执行脚本1542164108700",
                            "dbType": 0,
                            "operationList": [
                                {"operationName": "忽略错误", "operationCode": 3},
                                {"operationName": "失败IP重做", "operationCode": 2},
                            ],
                            "startTime": "2018-11-14 10:55:09",
                            "appId": 2,
                            "totalIPNum": 1,
                            "ord": 1,
                            "createTime": "2018-11-14 10:55:09",
                            "dbPort": 0,
                            "account": "root",
                            "dbAccountId": 0,
                            "name": "API执行脚本1542164108700",
                            "isUseCCFileParam": 0,
                            "isAutoCreateDir": 1,
                            "dbPass": None,
                            "stepIpResult": [
                                {
                                    "resultTypeText": "脚本返回码非零",
                                    "ip": "x.x.x.x",
                                    "tag": "",
                                    "status": 104,
                                    "source": 1,
                                }
                            ],
                            "blockOrd": 1,
                            "dbAlias": None,
                            "retryCount": 0,
                            "endTime": "2018-11-14 10:55:10",
                            "runIPNum": 1,
                        }
                    ],
                    "blockOrd": 1,
                    "blockName": "API执行脚本1542164108700",
                }
            ],
        },
        "result": True,
        "request_id": "xxx",
    }
)

mock_job_get_task_ip_log_res = {
    "message": "",
    "code": "00",
    "data": [
        {
            "isFinished": True,
            "stepInstanceName": "",
            "stepAnalyseResult": [
                {
                    "count": 1,
                    "ipLogContent": [
                        {
                            "totalTime": 0.0010000000474974513,
                            "isJobIp": 1,
                            "stepInstanceId": 123,
                            "tag": "",
                            "ip": "x.x.x.x",
                            "displayIp": "",
                            "errCode": 0,
                            "source": 1,
                            "logContent": "@@",
                            "status": 9,
                            "startTime": "2018-11-13 11:13:01",
                            "offset": 0,
                            "retryCount": 0,
                            "endTime": "2018-11-13 11:13:01",
                            "exitCode": 0,
                        }
                    ],
                    "resultTypeText": "执行成功",
                    "resultType": 9,
                    "tag": "",
                    "resultTypeTextEn": "Success ",
                }
            ],
            "stepInstanceId": 123,
            "stepInstanceStatus": 3,
        }
    ],
    "result": True,
    "request_id": "xxx",
}

mock_job_gse_proc_operate_res = json.dumps(
    {
        "result": True,
        "code": "00",
        "message": "",
        "data": {
            "errorCode": 0,
            "gse_task_id": "GSETASK:20170413215154:8239",
            "errorMessage": "success",
        },
    }
)

mock_job_gse_proc_operate_format_err_res = json.dumps(
    {
        "result": True,
        "code": "00",
        "message": "",
        "data": {
            "errorCode": 0,
            "gseTaskId": "GSETASK:20170413215154:8239",  # field error, want gse_task_id
            "errorMessage": "success",
        },
    }
)

mock_job_get_proc_result_res = json.dumps(
    {
        "message": "",
        "code": "00",
        "data": {
            "m_rsp": {
                "/data/xxx:0:x.x.x.x": {
                    "content": '{"ip":"x.x.x.x","process":[{"instance":[{"cmdline":"","cpuUsage":0,"cpuUsageAve":0,'
                    '"diskSize":-1,"elapsedTime":0,"freeVMem":"0","phyMemUsage":0,"pid":123,"processName":'
                    '"xxx","startTime":"","stat":"","stime":"0","threadCount":0,"usePhyMem":0,"utime":"0"}],'
                    '"procname":"unifyTlogc_common_log"}],"timezone":8,"utctime":"2018-11-13 14:47:38",'
                    '"utctime2":"2018-11-13 06:47:38"}\n',
                    "errcode": 0,
                    "errmsg": "success",
                }
            },
            "m_errcode": 0,
            "m_errmsg": "",
            "setM_errmsg": True,
            "m_rspSize": 1,
            "setM_errcode": True,
            "setM_rsp": True,
        },
        "result": True,
        "request_id": "xxx",
    }
)

mock_job_get_proc_result_err_res = json.dumps(
    {
        "result": True,
        "code": "00",
        "message": "",
        "data": {
            "m_rsp": {
                "key_test:0:x.x.x.x": {
                    "errcode": 117,
                    "errmsg": "can not find connection by ip x.x.x.x",
                }
            },
            "m_errcode": 0,
            "m_errmsg": "",
            "setM_errmsg": True,
            "m_rspSize": 1,
            "setM_errcode": True,
            "setM_rsp": True,
        },
    }
)


mock_job_get_file_result = {
    "result": True,
    "code": "00",
    "message": "",
    "data": {
        "data": [
            {
                "content": '{"content" : "@@", "errcode" : 0, "errmsg" : "SUCC"}',
                "ip": "x.x.x.x",
                "source": 1,
                "filePath": "/data/a.txt",
            }
        ],
        "errorMessage": "",
        "errorCode": 0,
    },
}

mock_common_response = json.dumps({"result": True, "message": "", "code": 0, "data": None})

# mock collector hub response
mock_collector_hub_summary_response = json.dumps(
    {
        "result": True,
        "message": "",
        "code": 0,
        "data": {
            "deploy_plans": [
                {
                    "deploy_plan_id": 11,
                    "deploy_status": "success",
                    "deploy_status_display": "成功",
                    "module": [
                        {
                            "bk_inst_name_list": [
                                "testdev",
                                "testdev-test-collecotr-server",
                            ],
                            "bk_object_id": "module",
                            "deploy_status_display": "成功",
                            "summary": {"total": 2, "success": 2},
                            "deploy_status": "success",
                            "bk_instance_id": 399534,
                        }
                    ],
                    "summary": {"total": 2, "success": 2},
                },
                {
                    "deploy_plan_id": 12,
                    "deploy_status": "running",
                    "deploy_status_display": "正在运行",
                    "summary": {"running": 1, "total": 1},
                },
            ],
            "deploy_status": "failure",
            "deploy_status_display": "失败",
            "summary": {"failure": 1, "running": 1, "total": 2, "success": 2},
        },
    }
)

mock_app_info_response = json.dumps(
    {
        "result": True,
        "message": "",
        "code": 0,
        "data": [
            {
                "ApplicationID": "2",
                "Maintainers": "xxx",
                "DisplayName": "yyy",
                "OperationPlanning": "xxx",
                "MobileQQAppID": "",
            }
        ],
    }
)

mock_collector_hub_result_false_response = json.dumps(
    {"result": False, "message": "xxx", "code": "1578201", "data": None}
)

mock_collector_hub_format_error_response = json.dumps({"result": False, "message": "xxx", "code": 123, "data": None})
