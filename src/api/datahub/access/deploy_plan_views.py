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

from common.auth import perm_check
from common.decorators import detail_route
from common.exceptions import ValidationError
from common.log import logger
from common.views import APIViewSet
from datahub.access import settings
from datahub.access.models import AccessOperationLog, AccessRawData, DatabusChannel
from datahub.access.raw_data import rawdata
from datahub.access.serializers import (
    BaseSerializer,
    CollectDeleteSerializer,
    DeployPlanListByParamSerializer,
    RetriveSerializer,
    UpdateBaseSerializer,
)
from datahub.access.utils import kafka_tool
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from ..common.const import BK_BIZ_ID, DATA_SCENARIO, ID, OFFLINEFILE, RAW_DATA_IDS
from .collectors.factory import CollectorFactory


class CollectorDeployPlanViewSet(APIViewSet):
    lookup_field = "raw_data_id"

    def list(self, request):
        """
        @api {get} v3/access/deploy_plan/?bk_biz_id=&data_scenario=log&raw_data_ids=XX&raw_data_ids=XX 获取源数据列表
        @apiGroup RawData
        @apiDescription  获取部署计划详情列表
        @apiParam {int} [raw_data_ids] 源数据id列表
        @apiParam {int{CMDB合法的业务ID}} [bk_biz_id] 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{接入场景}}                [data_scenario] 接入场景
        @apiParam {list{data_id列表}}        [raw_data_ids] 数据分类

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/deploy_plan/?bk_biz_id=&data_scenario=log&raw_data_ids=XX&raw_data_ids=XX
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "bk_biz_id": 591,
                    "created_at": "2018-11-07T10:04:57",
                    "data_source": "svr",
                    "maintainer": "admin",
                    "updated_by": null,
                    "data_category_alias": "",
                    "raw_data_name": "http_test",
                    "topic": "http_test591",
                    "storage_partitions": 1,
                    "sensitivity": "private",
                    "storage_channel_id": 11,
                    "data_encoding": "UTF-8",
                    "raw_data_alias": "中文名",
                    "updated_at": "2018-11-07T14:21:28",
                    "bk_app_code": "bk_dataweb",
                    "data_scenario": "http",
                    "created_by": "admin",
                    "data_category": "",
                    "data_scenario_alias": null,
                    "data_source_alias": "svr",
                    "id": 287,
                    "description": "这里填写描述"
                }
            ],
            "result": true
        }
        """
        collector_factory = CollectorFactory.get_collector_factory()
        param = self.params_valid(serializer=DeployPlanListByParamSerializer)
        raw_data_ids = param.get(RAW_DATA_IDS)
        data_scenario = param.get(DATA_SCENARIO, None)
        bk_biz_id = param.get(BK_BIZ_ID, None)
        res_list = []

        if bk_biz_id and data_scenario:
            filterd_raw_data_ids = AccessRawData.objects.filter(
                id__in=raw_data_ids, data_scenario=data_scenario, bk_biz_id=bk_biz_id
            ).values_list(ID, flat=True)
        elif bk_biz_id:
            filterd_raw_data_ids = AccessRawData.objects.filter(id__in=raw_data_ids, bk_biz_id=bk_biz_id).values_list(
                ID, flat=True
            )
        elif data_scenario:
            filterd_raw_data_ids = AccessRawData.objects.filter(
                id__in=raw_data_ids, data_scenario=data_scenario
            ).values_list(ID, flat=True)
        else:
            filterd_raw_data_ids = raw_data_ids

        # 针对文件场景优化返回速度
        if data_scenario and data_scenario == OFFLINEFILE:
            collector = collector_factory.get_collector_by_data_scenario(data_scenario)
            deploy_plan_list = collector(raw_data_id=filterd_raw_data_ids[0], show_display=1).get_by_list(
                filterd_raw_data_ids
            )
            return Response(deploy_plan_list)

        for raw_data_id in filterd_raw_data_ids:
            try:
                if data_scenario:
                    collector = collector_factory.get_collector_by_data_scenario(data_scenario)
                else:
                    collector = collector_factory.get_collector_by_data_id(int(raw_data_id))

                deploy_plan = collector(raw_data_id=raw_data_id, show_display=1).get()
                res_list.append(deploy_plan)
            except Exception as e:
                logger.warning("raw_data_id {} get deploy_plan failed, {}".format(raw_data_id, e))

        return Response(res_list)

    def create(self, request):
        """
        @api {post} v3/access/deploy_plan/ log提交接入部署计划
        @apiName create_deploy_plan_log
        @apiGroup CollectorDeployPlan
        @apiDescription log提交接入部署计划
        @apiParam {string{合法蓝鲸APP标识}} bk_app_code 蓝鲸APP标识
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。用户名需要具有<code>bk_biz_id</code>业务的权限。
        @apiParam {int{CMDB合法的业务ID}} bk_biz_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{合法接入场景}} data_scenario 接入场景
        @apiParam {string} [description] 接入数据备注

        @apiParam {dict} access_raw_data 接入源数据信息
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} raw_data_name 数据英文标识。英文标识在业务下唯一，
        重复创建会报错。这个字段会用来在消息队列中创建对应的channel。
        @apiParam {string{小于15字符,中文}} raw_data_alias 数据别名（中文名）。别名会用在数据展示，请填写可读性强的名字。
        @apiParam {string{合法数据来源}} data_source 数据来源
        @apiParam {string{合法字符编码来源}} data_encoding 字符编码
        @apiParam {string{合法敏感度标识}} sensitivity 敏感度。作为数据使用权限审核的依据
        @apiParam {string{合法数据维护人}} maintainer  数据维护者
        @apiParam {string{小于100字符}} [description] 源数据数据描述

        @apiParam {dict} access_conf_info 接入配置信息

        @apiParam {dict} collection_model 接入方式
        @apiParam {string{"incr":增量，"all":全量，暂时不支持}} collection_type 接入类型
        @apiParam {int} start_at 起始位置,采集周期为实时时,0是表示接入存量数据
        @apiParam {int{-1:实时,0:一次性,n:周期性}} period 采集周期,单位s

        @apiParam {dict} filters 接入过滤条件
        @apiParam {string{合法分隔符}} delimiter 分隔符
        @apiParam {dict{必须为合法的字典,请参考请求参数实例}} fields 过滤条件

        @apiParam {dict} resource 接入对象资源
        @apiParam {array{必须为合法的数组,支持多个接入对象。请参考请求参数实例}} scope 接入对象

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .
        @apiParamExample {json} 日志接入参数样例
        {
            "bk_app_secret": "xxx",
            "bk_app_code": "bk_dataweb",
            "bk_username": "xxxx",
            "data_scenario": "log",
            "bk_biz_id": 591,
            "description": "xx",

            "access_raw_data": {
                "raw_data_name": "log_new_00011",
                "maintainer": "xxxx",
                "raw_data_alias": "asdfsaf",
                "data_source": "svr",
                "data_encoding": "UTF-8",
                "sensitivity": "private",
                "description": "xx"
            },
            "access_conf_info": {
                "collection_model": {
                    "collection_type": "incr",
                    "start_at": 1,
                    "period": 0
                },
                "filters": {
                    "delimiter": "|",
                    "fields": [{
                        "index": 1,
                        "op": "=",
                        "logic_op": "and",
                        "value": "111"
                    }]
                },
                "resource": {
                    "scope": [
                        {
                        "module_scope": [{
                            "bk_obj_id": "set",
                            "bk_inst_id": 123
                        }],
                        "host_scope": [{
                            "bk_cloud_id": 1,
                            "ip": "x.x.x.x"
                        }],
                        "scope_config": {
                            "paths": [
                                {
                                    "system":"linux",
                                    "path":[
                                            "/tmp/*.aaaz",
                                            "/tmp/*.l"
                                            ]
                                },
                                {
                                    "system":"windows",
                                    "path":[
                                            "c:/tmp/*.aaaz",
                                            "c:/tmp/*.l"
                                            ]
                                }

                            ]
                        }
                    }]
                }
            }
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "raw_data_id": 263
            },
            "result": true
        }
        """
        """
        @api {post} v3/access/deploy_plan/ http提交接入部署计划
        @apiName create_deploy_plan_http
        @apiGroup CollectorDeployPlan
        @apiDescription http提交接入部署计划
        @apiParam {string{合法蓝鲸APP标识}} bk_app_code 蓝鲸APP标识
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。用户名需要具有<code>bk_biz_id</code>业务的权限。
        @apiParam {int{CMDB合法的业务ID}} bk_biz_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{合法接入场景}} data_scenario 接入场景
        @apiParam {string} [description] 接入数据备注

        @apiParam {dict} access_raw_data 接入源数据信息
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} raw_data_name 数据英文标识。英文标识在业务下唯一，
        重复创建会报错。这个字段会用来在消息队列中创建对应的channel。
        @apiParam {string{小于15字符,中文}} raw_data_alias 数据别名（中文名）。别名会用在数据展示，请填写可读性强的名字。
        @apiParam {string{合法数据来源}} data_source 数据来源
        @apiParam {string{合法字符编码来源}} data_encoding 字符编码
        @apiParam {string{合法敏感度标识}} sensitivity 敏感度。作为数据使用权限审核的依据
        @apiParam {string{合法数据维护人}} maintainer  数据维护者
        @apiParam {string{小于100字符}} [description] 源数据数据描述

        @apiParam {dict} access_conf_info 接入配置信息

        @apiParam {dict} collection_model 接入方式
        @apiParam {string{"pull":拉,"push":推送}} collection_type 采集方式
        @apiParam {string} time_format 时间格式
        @apiParam {string} increment_field 时间参数，存在多个用','隔开
        @apiParam {int{-1:实时,0:一次性,n:周期性}} period 采集周期,单位s

        @apiParam {dict} [filters] 接入过滤条件
        @apiParam {string{合法分隔符}} [delimiter] 分隔符
        @apiParam {dict{必须为合法的字典,请参考请求参数实例}} [fields] 过滤条件

        @apiParam {dict} resource 接入对象资源
        @apiParam {array{必须为合法的数组,支持多个接入对象。请参考请求参数实例}} scope 接入对象
        @apiParam {string} url 接入url
        @apiParam {string{"post":post请求,"get":get请求}} method 请求方式
        @apiParam {string{json格式,当请求方式为post,body为必填项}} [body] 请求body参数

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .

        @apiParamExample {json} HTTP接入参数样例
        {
            "bk_app_code": "bk_dataweb",
            "bk_username": ",admin",
            "data_scenario": "http",
            "bk_biz_id": 591,
            "description": "xx",
            "access_raw_data": {
                "raw_data_name": "http_new_0x03",
                "maintainer": "xxxx",
                "raw_data_alias": "asdfsaf",
                "data_source": "svr",
                "data_encoding": "UTF-8",
                "sensitivity": "private",
                "description": "xx"
            },
            "access_conf_info": {

                "collection_model": {
                    "collection_type": "pull",
                    "period": 0,
                    "time_format": "yyyy-MM-dd HH:mm:ss",
                    "increment_field":"created_at"
                },
                "filters": {
                    "delimiter": "|",
                    "fields": [{
                        "index": 1,
                        "op": "=",
                        "logic_op": "and",
                        "value": "111"
                    }]
                },
                "resource": {
                    "scope":[{
                        "url":"http://x.x.x.x/v3/access/rawdata/?created_at=<begin>&page_size=1&page=1",
                        "method":"get"
                    }]
                }
            }
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "raw_data_id": 263
            },
            "result": true
        }
        """
        """
        @api {post} v3/access/deploy_plan/ db提交接入部署计划
        @apiName create_deploy_plan_db
        @apiGroup CollectorDeployPlan
        @apiDescription db提交接入部署计划
        @apiParam {string{合法蓝鲸APP标识}} bk_app_code 蓝鲸APP标识
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。用户名需要具有<code>bk_biz_id</code>业务的权限。
        @apiParam {int{CMDB合法的业务ID}} bk_biz_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{合法接入场景}} data_scenario 接入场景
        @apiParam {string} [description] 接入数据备注

        @apiParam {dict} access_raw_data 接入源数据信息
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} raw_data_name 数据英文标识。英文标识在业务下唯一，
        重复创建会报错。这个字段会用来在消息队列中创建对应的channel。
        @apiParam {string{小于15字符,中文}} raw_data_alias 数据别名（中文名）。别名会用在数据展示，请填写可读性强的名字。
        @apiParam {string{合法数据来源}} data_source 数据来源
        @apiParam {string{合法字符编码来源}} data_encoding 字符编码
        @apiParam {string{合法敏感度标识}} sensitivity 敏感度。作为数据使用权限审核的依据
        @apiParam {string{合法数据维护人}} maintainer  数据维护者
        @apiParam {string{小于100字符}} [description] 源数据数据描述

        @apiParam {dict} access_conf_info 接入配置信息
        @apiParam {dict} collection_model 接入方式
        @apiParam {int{"pri":主键,"all":全量,"time":时间范围}} collection_type 接入方式
        @apiParam {string} time_format 时间格式
        @apiParam {string} increment_field 增量字段
        @apiParam {int{-1:实时,0:一次性,n:周期性}} period 采集周期,单位s
        @apiParam {int} start_at 起始位置,采集周期为实时时,0是表示接入存量数据
        @apiParam {int} before_time  数据延迟时间

        @apiParam {dict} [filters] 接入过滤条件
        @apiParam {string{合法分隔符}} [delimiter] 分隔符
        @apiParam {dict{必须为合法的字典,请参考请求参数实例}} [fields] 过滤条件

        @apiParam {dict} resource 接入对象资源
        @apiParam {array{必须为合法的数组,请参考请求参数实例}} scope 接入对象
        @apiParam {string{ip或者域名}} db_host 接入DB host
        @apiParam {int} db_port 接入DB 端口
        @apiParam {string} db_user 接入DB 用户名
        @apiParam {string} db_pass 接入DB 密码
        @apiParam {string} db_name 接入DB 数据库名称
        @apiParam {string} table_name 接入DB 数据库表名称
        @apiParam {int} db_type_id 接入DB 类型id

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .

        @apiParamExample {json} DB接入参数样例
        {
            "bk_app_code": "bk_dataweb",
            "bk_username": "admin",
            "data_scenario": "db",
            "bk_biz_id": 591,
            "description": "xx",
            "access_raw_data": {
                "raw_data_name": "db_new_04",
                "maintainer": "xxxx",
                "raw_data_alias": "asdfsaf",
                "data_source": "svr",
                "data_encoding": "UTF-8",
                "sensitivity": "private",
                "description": "xx"
            },
            "access_conf_info": {
                "collection_model": {
                    "collection_type": "time",
                    "start_at": 0,
                    "period": 0,
                    "time_format":"yyyy-MM-dd HH:mm:ss",
                    "increment_field":"create_at",
                    "before_time":100
                },
                "filters": {
                    "delimiter": "|",
                    "fields": [{
                        "index": 1,
                        "op": "=",
                        "logic_op": "and",
                        "value": "111"
                    }]
                },
                "resource": {
                    "scope":[{
                        "db_host": "x.x.x.x",
                        "db_port": 10000,
                        "db_user": "user",
                        "db_pass": "pwd",
                        "db_name": "bkdata_basic",
                        "table_name": "access_db_info",
                        "db_type_id": 1
                    }]
                }
            }
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "raw_data_id": 263
            },
            "result": true
        }
        """
        """
        @api {post} v3/access/deploy_plan/ 脚本提交接入部署计划
        @apiName create_deploy_plan_script
        @apiGroup CollectorDeployPlan
        @apiDescription 脚本提交接入部署计划
        @apiParam {string{合法蓝鲸APP标识}} bk_app_code 蓝鲸APP标识
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。用户名需要具有<code>bk_biz_id</code>业务的权限。
        @apiParam {int{CMDB合法的业务ID}} bk_biz_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{合法接入场景}} data_scenario 接入场景
        @apiParam {string} [description] 接入数据备注

        @apiParam {dict} access_raw_data 接入源数据信息
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} raw_data_name 数据英文标识。英文标识在业务下唯一，
        重复创建会报错。这个字段会用来在消息队列中创建对应的channel。
        @apiParam {string{小于15字符,中文}} raw_data_alias 数据别名（中文名）。别名会用在数据展示，请填写可读性强的名字。
        @apiParam {string{合法数据来源}} data_source 数据来源
        @apiParam {string{合法字符编码来源}} data_encoding 字符编码
        @apiParam {string{合法敏感度标识}} sensitivity 敏感度。作为数据使用权限审核的依据
        @apiParam {string{合法数据维护人}} maintainer  数据维护者
        @apiParam {string{小于100字符}} [description] 源数据数据描述

        @apiParam {dict} access_conf_info 接入配置信息

        @apiParam {int{-1:实时,0:一次性,n:周期性}} period 采集周期,单位s

        @apiParam {dict} [filters] 接入过滤条件
        @apiParam {string{合法分隔符}} delimiter 分隔符
        @apiParam {dict{必须为合法的字典,请参考请求参数实例}} [fields] 过滤条件

        @apiParam {dict} resource 接入对象资源
        @apiParam {array{必须为合法的数组,支持多个接入对象。请参考请求参数实例}} scope 接入对象

        @apiParam {array} module_scope 模块
        @apiParam {array} host_scope ip
        @apiParam {dict} scope_config 脚本配置

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .

        @apiParamExample {json}  脚本接入参数样例
        {
            "bk_app_code": "bk_dataweb",
            "bk_username": "xxxx",
            "data_scenario": "script",
            "bk_biz_id": 2,
            "description": "xx",

            "access_raw_data": {
                "raw_data_name": "script_07",
                "maintainer": "xxxx",
                "raw_data_alias": "asdfsaf",
                "data_source": "svr",
                "data_encoding": "UTF-8",
                "sensitivity": "private",
                "description": "xx"
            },
            "access_conf_info": {
                "collection_model": {
                    "period": 10
                },
                "filters": {
                    "delimiter": "|",
                    "fields": []
                },
                "resource": {
                    "scope": [
                        {
                        "module_scope": [{
                            "bk_obj_id": "set",
                            "bk_inst_id": 123
                        }],
                        "host_scope": [{
                            "bk_cloud_id": 1,
                            "ip": "x.x.x.x"
                        }],
                        "scope_config": {
                            "content":"xxxx"
                        }
                    }]
                }
            }
        }
       @apiSuccessExample {json} Success-Response:
       HTTP/1.1 200 OK
       {
           "errors": null,
           "message": "ok",
           "code": "1500200",
           "data": {
               "raw_data_id": 263
           },
           "result": true
       }
       """
        """
        @api {post} v3/access/deploy_plan/ 文件上传提交接入部署计划
        @apiName create_deploy_plan_file
        @apiGroup CollectorDeployPlan
        @apiDescription 文件上传提交接入部署计划
        @apiParam {string{合法蓝鲸APP标识}} bk_app_code 蓝鲸APP标识
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。用户名需要具有<code>bk_biz_id</code>业务的权限。
        @apiParam {int{CMDB合法的业务ID}} bk_biz_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{合法接入场景}} data_scenario 接入场景
        @apiParam {string} [description] 接入数据备注

        @apiParam {dict} access_raw_data 接入源数据信息
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} raw_data_name 数据英文标识。英文标识在业务下唯一，
        重复创建会报错。这个字段会用来在消息队列中创建对应的channel。
        @apiParam {string{小于15字符,中文}} raw_data_alias 数据别名（中文名）。别名会用在数据展示，请填写可读性强的名字。
        @apiParam {string{合法数据来源}} data_source 数据来源
        @apiParam {string{合法字符编码来源}} data_encoding 字符编码
        @apiParam {string{合法敏感度标识}} sensitivity 敏感度。作为数据使用权限审核的依据
        @apiParam {string{合法数据维护人}} maintainer  数据维护者
        @apiParam {string{小于100字符}} [description] 源数据数据描述

        @apiParam {dict} access_conf_info 接入配置信息

        @apiParam {dict} collection_model 接入方式
        @apiParam {string{"incr":增量，"all":全量，暂时不支持}} collection_type 接入类型
        @apiParam {int{-1:实时,0:一次性,n:周期性}} period 采集周期,单位s

        @apiParam {dict} [filters] 接入过滤条件
        @apiParam {string{合法分隔符}} [delimiter] 分隔符
        @apiParam {dict{必须为合法的字典,请参考请求参数实例}} [fields] 过滤条件

        @apiParam {dict} resource 接入对象资源
        @apiParam {array{必须为合法的数组,支持多个接入对象。请参考请求参数实例}} scope 接入对象

        @apiParam {string} file_name 文件名称

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .

        @apiParamExample {json}  文件上传接入参数样例
        {
            "bk_app_code": "bk_dataweb",
            "bk_username": "admin",
            "data_scenario": "file",
            "bk_biz_id": 2,
            "description": "xx",

            "access_raw_data": {
                "raw_data_name": "file_223",
                "maintainer": "xxxx",
                "raw_data_alias": "asdfsaf",
                "data_source": "svr",
                "data_encoding": "UTF-8",
                "sensitivity": "private",
                "description": "xx"
            },
            "access_conf_info": {
                "collection_model": {
                    "collection_type": "incr",
                    "period": 0
                },
                "filters": {
                    "delimiter": "|",
                    "fields": []
                },
                "resource": {
                    "scope": [{
                        "file_name":"file_591_test_xls测试2.csv"
                    }]
                }
            }
        }
       @apiSuccessExample {json} Success-Response:
       HTTP/1.1 200 OK
       {
           "errors": null,
           "message": "ok",
           "code": "1500200",
           "data": {
               "raw_data_id": 263
           },
           "result": true
       }
       """
        """
        @api {post} v3/access/deploy_plan/ 离线文件上传提交接入部署计划
        @apiName create_deploy_plan_file
        @apiGroup CollectorDeployPlan
        @apiDescription 离线文件上传提交接入部署计划
        @apiParam {string{合法蓝鲸APP标识}} bk_app_code 蓝鲸APP标识
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。用户名需要具有<code>bk_biz_id</code>业务的权限。
        @apiParam {int{CMDB合法的业务ID}} bk_biz_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{合法接入场景}} data_scenario 接入场景
        @apiParam {string} [description] 接入数据备注

        @apiParam {dict} access_raw_data 接入源数据信息
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} raw_data_name 数据英文标识。英文标识在业务下唯一，
        重复创建会报错。这个字段会用来在消息队列中创建对应的channel。
        @apiParam {string{小于15字符,中文}} raw_data_alias 数据别名（中文名）。别名会用在数据展示，请填写可读性强的名字。
        @apiParam {string{合法数据来源}} data_source 数据来源
        @apiParam {string{合法字符编码来源}} data_encoding 字符编码
        @apiParam {string{合法敏感度标识}} sensitivity 敏感度。作为数据使用权限审核的依据
        @apiParam {string{合法数据维护人}} maintainer  数据维护者
        @apiParam {string{小于100字符}} [description] 源数据数据描述

        @apiParam {dict} access_conf_info 接入配置信息

        @apiParam {dict} collection_model 接入方式
        @apiParam {string{"incr":增量，"all":全量，暂时不支持}} collection_type 接入类型
        @apiParam {int{-1:实时,0:一次性,n:周期性}} period 采集周期,单位s

        @apiParam {dict} [filters] 接入过滤条件
        @apiParam {string{合法分隔符}} [delimiter] 分隔符
        @apiParam {dict{必须为合法的字典,请参考请求参数实例}} [fields] 过滤条件

        @apiParam {dict} resource 接入对象资源
        @apiParam {array{必须为合法的数组,支持多个接入对象。请参考请求参数实例}} scope 接入对象

        @apiParam {string} file_name 文件名称

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .

        @apiParamExample {json}  文件上传接入参数样例
        {
            "bk_app_code": "bk_dataweb",
            "bk_username": "admin",
            "data_scenario": "offlinefile",
            "bk_biz_id": 2,
            "description": "xx",

            "access_raw_data": {
                "raw_data_name": "file_223",
                "maintainer": "xxxx",
                "raw_data_alias": "asdfsaf",
                "data_source": "svr",
                "data_encoding": "UTF-8",
                "sensitivity": "private",
                "description": "xx"
            },
            "access_conf_info": {
                "collection_model": {
                    "collection_type": "incr",
                    "period": 0
                },
                "filters": {
                    "delimiter": "|",
                    "fields": []
                },
                "resource": {
                    "scope": [{
                        "file_name":"file_591_test_xls测试2.csv"
                    }]
                }
            }
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
           "errors": null,
           "message": "ok",
           "code": "1500200",
           "data": {
               "raw_data_id": 263
           },
           "result": true
        }
        """
        logger.info("deploy_plan: basic parameter verification starts")
        params = self.params_valid(serializer=BaseSerializer)
        collector_factory = CollectorFactory.get_collector_factory()
        collector = collector_factory.get_collector_by_data_scenario(params["data_scenario"])(access_param=request.data)

        logger.info("deploy_plan: basic parameter verification starts")
        collector.valid_access_param()
        raw_data_id = collector.update_or_create()
        return Response({"raw_data_id": raw_data_id})

    @perm_check("raw_data.retrieve")
    def retrieve(self, request, raw_data_id):
        """
        @api {get} v3/access/deploy_plan/:raw_data_id/ 查询部署计划
        @apiName retrieve_deploy_plan
        @apiGroup CollectorDeployPlan
        @apiParam {int} raw_data_id 源始数据ID.
        @apiParam {boolean} show_display 是否显示详情
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "bk_biz_id": 2,
                "description": "xx",
                "data_scenario": "log",
                "bk_app_code": "bk_dataweb",
                "access_raw_data": {
                    "bk_biz_id": 2,
                    "data_source": "svr",
                    "maintainer": ",admin",
                    "updated_by": null,
                    "raw_data_name": "log_new_00010",
                    "storage_partitions": 1,
                    "created_at": "2018-11-22T16:38:07",
                    "storage_channel_id": 1000,
                    "data_encoding": "UTF-8",
                    "raw_data_alias": "asdfsaf",
                    "updated_at": null,
                    "bk_app_code": "bk_dataweb",
                    "data_scenario": "log",
                    "created_by": ",admin",
                    "data_category": "",
                    "id": 302,
                    "sensitivity": "private",
                    "description": "xx"
                },
                "access_conf_info": {
                    "collection_model": {
                        "start_at": 1,
                        "increment_field": null,
                        "period": 0,
                        "time_format": null,
                        "collection_type": "incr",
                        "before_time": null
                    },
                    "resource": {
                        "scope": [
                            {
                                "deploy_plan_id": 18,
                                "scope_config": {
                                    "paths": [
                                        "/tmp/*.log",
                                        "/tmp/*.l",
                                        "/tmp/*.aaaz"
                                    ]
                                },
                                "module_scope": [
                                    {
                                        "bk_obj_id": "set",
                                        "bk_inst_id": 123
                                    }
                                ],
                                "host_scope": [
                                    {
                                        "ip": "x.x.x.x",
                                        "bk_cloud_id": 1
                                    }
                                ]
                            }
                        ]
                    },
                    "filters": {
                        "fields": [
                            {
                                "index": 1,
                                "logic_op": "and",
                                "value": "111",
                                "op": "="
                            }
                        ],
                        "delimiter": "|"
                    }
                }
            },
            "result": true
        }
        """
        param = {
            "raw_data_id": int(raw_data_id),
            "bk_username": self.request.query_params.get("bk_username"),
            "show_display": 0
            if not self.request.query_params.get("show_display")
            else self.request.query_params.get("show_display"),
        }
        serializer = RetriveSerializer(data=param)
        if not serializer.is_valid():
            raise ValidationError(message=_(u"参数校验不通过:raw_data_id,show_display,bk_username校验不通过"))

        collector_factory = CollectorFactory.get_collector_factory()
        data = collector_factory.get_collector_by_data_id(raw_data_id)(
            raw_data_id=raw_data_id, show_display=int(param["show_display"])
        ).get()
        return Response(data)

    @perm_check("raw_data.update")
    def update(self, request, raw_data_id):
        """
        @api {put} /v3/access/deploy_plan/:raw_data_id/ 更新接入部署计划
        @apiName update_deploy_plan
        @apiGroup CollectorDeployPlan
        @apiParam {int} raw_data_id 源始数据ID.
        @apiParamExample {json} 日志接入参数样例
        {
            "bk_app_code": "bk_dataweb",
            "bk_username": ",admin",
            "data_scenario": "db",
            "bk_biz_id": 2,
            "description": "xx",
            "access_raw_data": {
                "raw_data_name": "log_new_00007",
                "maintainer": "xxxx",
                "data_scenario": "log",
                "raw_data_alias": "asdfsaf",
                "data_source": "svr",
                "data_encoding": "UTF-8",
                "sensitivity": "private",
                "description": "xx"
            },
            "access_conf_info": {
                "collection_model": {
                    "collection_type": "incr",
                    "start_at": 11111,
                    "period": 0
                },
                "filters": {
                    "delimiter": "|",
                    "fields": [{
                        "index": 1,
                        "op": "=11111",
                        "logic_op": "and",
                        "value": "111"
                    }]
                },
                "resource": {
                    "scope": [
                        {
                        "deploy_plan_id":2,
                        "module_scope": [{
                            "bk_obj_id": "xxxx",
                            "bk_inst_id": 123
                        }],
                        "host_scope": [{
                            "bk_cloud_id": 222,
                            "ip": "x.x.x.x"
                        }],
                        "scope_config": {
                            "paths": [
                                "/tmp/*.log",
                                "/tmp/*.l",
                                "/tmp/*.aaaz"
                            ]
                        }
                    },
                    {
                        "module_scope": [{
                            "bk_obj_id": "xxxx",
                            "bk_inst_id": 123
                        }],
                        "host_scope": [{
                            "bk_cloud_id": 222,
                            "ip": "x.x.x.x"
                        }],
                        "scope_config": {
                            "paths": [
                                "/tmp/*.log",
                                "/tmp/*.l",
                                "/tmp/*.aaaz"
                            ]
                        }
                    }]
                }
            }
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "raw_data_id": 263
            },
            "result": true
        }
        """
        data = request.data
        data["raw_data_id"] = int(raw_data_id)
        params = self.params_valid(serializer=UpdateBaseSerializer)
        access_raw_data = params["access_raw_data"]
        access_raw_data["updated_by"] = params["bk_username"]
        collector_factory = CollectorFactory.get_collector_factory()
        collector = collector_factory.get_collector_by_data_scenario(data["data_scenario"])(
            access_param=params, raw_data_id=raw_data_id
        )
        collector.valid_access_param()
        raw_data_id = collector.update_or_create()
        return Response({"raw_data_id": raw_data_id})

    @perm_check("raw_data.delete")
    def delete(self, request, raw_data_id):
        """
        @api {delete} /v3/access/deploy_plan/:raw_data_id/ 删除部署计划
        @apiName delete_deploy_plan
        @apiGroup CollectorDeployPlan
        @apiParam {int} raw_data_id 源始数据ID.
        @apiParamExample {json} 日志接入参数样例
        {
            "bk_app_code": "bk_dataweb",
            "bk_username": ",admin",
            "bk_biz_id": 2,
            "force": true
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": "ok",
            "result": true
        }
        """
        raw_data_id = int(raw_data_id)
        logger.info("delete collector raw_data %d" % raw_data_id)
        param = self.params_valid(serializer=CollectDeleteSerializer)
        # query data_scenario
        try:
            raw_data = AccessRawData.objects.get(id=raw_data_id)
        except AccessRawData.DoesNotExist:
            return Response("ok")
        data_scenario = raw_data.data_scenario
        collector_factory = CollectorFactory.get_collector_factory()
        collector = collector_factory.get_collector_by_data_scenario(data_scenario)(access_param=request.data)

        # stop collector
        collector.delete(raw_data_id, param["force"])

        # delete raw_data
        rawdata.delete_raw_data(raw_data_id)

        # delete topic
        topic = "%s%d" % (raw_data.raw_data_name, raw_data.bk_biz_id)
        databus_channel = DatabusChannel.objects.get(id=raw_data.storage_channel_id)
        bootstrap_server = "{}:{}".format(
            databus_channel.cluster_domain,
            databus_channel.cluster_port,
        )
        kafka_tool.delete_topic(bootstrap_server, topic)
        return Response("ok")

    @detail_route(methods=["get"])
    def history(self, request, raw_data_id):
        """
        @api {get} v3/access/deploy_plan/:raw_data_id/history/ 接入变更历史
        @apiName deploy_plan_history
        @apiGroup CollectorDeployPlan
        @apiParam {int} raw_data_id 源始数据ID.
        @apiParamExample {json} 参数样例:
        v3/access/deploy_plan/1/history/
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "updated_by": null,
                    "created_at": "2018-10-30T14:58:23",
                    "args": {
                        "log": "xxxx",
                        "scope_config": ""
                    },
                    "updated_at": null,
                    "created_by": "admin",
                    "raw_data_id": 1,
                    "status": "success",
                    "id": 2,
                    "description": "xxxx"
                }
            ],
            "result": true
        }
        """
        # 参数校验
        result = AccessOperationLog.objects.filter(raw_data_id=raw_data_id).order_by("-created_at")
        log_list = result.values()
        for log in log_list:
            # 历史记录某条错误的话, 忽略异常, 不影响整体返回
            try:
                log["args"] = json.loads(log["args"])
            except Exception as e:
                logger.error("raw_data_id {} AccessOperationLog args parse json failed, {}".format(raw_data_id, e))
                continue
            log["args"]["log"] = _(log["args"].get("log", ""))
            if log.get("created_at"):
                log["created_at"] = log.get("created_at", "").strftime(settings.DATA_TIME_FORMAT)
            if log.get("updated_at"):
                log["updated_at"] = log.get("updated_at", "").strftime(settings.DATA_TIME_FORMAT)

        return Response(log_list)

    @detail_route(methods=["get"], url_path="status")
    def status_check(self, request, raw_data_id):
        """
        @api {get} v3/access/deploy_plan/:op_log_id/status/?status="xx" 修改接入状态
        @apiName deploy_plan_status
        @apiGroup CollectorDeployPlan
        @apiParam {int} raw_data_id 源始数据ID.
        @apiParamExample {json} 参数样例:
        v3/access/deploy_plan/1/status/
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": null,
            "result": true
        }
        """
        # 参数校验

        AccessOperationLog.objects.filter(id=raw_data_id).update(status=self.request.query_params["status"])
        return Response()
