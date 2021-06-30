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

from dataflow.flow.controller.node_controller import NodeController
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.node_types import NodeTypes

try:
    from functools import wraps
except ImportError:
    from django.utils.functional import wraps  # Python 2.4 fallback.

from common.auth import perm_check
from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.views import APIViewSet
from django.utils.decorators import available_attrs
from rest_framework.response import Response

from dataflow.flow.app import FlowConfig
from dataflow.flow.handlers.flow import valid_flow_wrap
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.signals import flow_version_wrap
from dataflow.flow.models import FlowNodeRelation
from dataflow.flow.serializer.serializers import CreateNodeSerializer, PartialUpdateNodeSerializer, UpdateNodeSerializer


class NodeViewSet(APIViewSet):
    lookup_field = "nid"
    lookup_value_regex = r"\d+"

    def node_handler_wrap(func):
        """
        返回的节点对象，统一转换为预期数据

        @note 改装饰器必须放置在 flow_version_wrap 之上，这样子才能确保返回节点对象包括版本信息
        """

        @wraps(func, assigned=available_attrs(func))
        def _wrap(*args, **kwargs):
            node = func(*args, **kwargs)
            return Response(node.to_dict(add_has_modify=True))

        return _wrap

    def node_config_wrap(func):
        """
        返回的节点配置，统一转换为预期数据
        """

        @wraps(func, assigned=available_attrs(func))
        def _wrap(*args, **kwargs):
            node = func(*args, **kwargs)
            node_config = node.get_config()
            node_config = NodeUtils.set_input_nodes_config(node.node_type, node_config, node.from_nodes_info)
            # node_config['from_nodes'] = node.from_nodes_info
            return Response(node_config)

        return _wrap

    @valid_flow_wrap("创建节点中")
    @perm_check("flow.update", detail=True, url_key="flow_id")
    @node_handler_wrap
    @flow_version_wrap(FlowConfig.handlers)
    @params_valid(serializer=CreateNodeSerializer)
    def create(self, request, flow_id, params):
        """
        @api {post} /dataflow/flow/flows/:fid/nodes 新增节点
        @apiName add_node
        @apiGroup Flow
        @apiParam {string} node_type 节点类型
        @apiParam {[dict]} from_links 父节点连线
        @apiParam {dict} frontend_info 节点坐标
        @apiParam {dict} config 节点配置项请参照 flow.handlers.forms
        @apiDescription 创建节点:
            实时数据源节点, 离线数据源, 实时节点, HDFS节点, MySQL节点, ES节点, 离线节点等
        @apiParamExample {json} 实时数据源
            {
                "bk_username": "admin",
                "from_links": [],
                "node_type": "stream_source",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "result_table_id": "2_f1_etlsource",
                    "name": "f1_etlsource"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} 离线数据源
            {
                "bk_username": "admin",
                "from_links": [],
                "node_type": "batch_source",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "result_table_id": "2_f1_rtsource",
                    "name": "f1_rtsource"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} 关联数据
            {
                "bk_username": "admin",
                "from_links": [],
                "node_type": "kv_source",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "result_table_id": "2_f1_rtsource",
                    "name": "f1_rtsource"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} 实时节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "realtime",
                "config": {                           # 以滑动窗口为例
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 2,
                    "name": "f1_stream",              # 节点名称
                    "table_name": "van_stream_08",    # 数据输出
                    "output_name": "van_stream_08",   # 输出中文名
                    "window_type": "slide",           # 窗口类型
                    "window_time": 0,                 # 窗口长度
                    "waiting_time": 0,                # 延迟时间
                    "count_freq": 30,                 # 统计频率
                    "sql": "select * from etl_van_test5",
                    "expired_time": 0，                # 过期时间
                    "window_lateness": {
                        allowed_lateness: True,        # 是否计算延迟数据
                        lateness_time: 1,              # 延迟时间
                        lateness_count_freq: 60        # 延迟统计频率
                    }
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} 离线节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "offline",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "window_type": "fixed",
                    "accumulate": false,
                    "name": "etl_p_b_01",
                    "table_name": "etl_p_b_01",
                    "output_name": "etl_p_b_01",
                    "data_start": 0,                   # 累加窗口数据起点
                    "data_end": 23,                    # 累加窗口数据终点
                    "delay": 0,                        # 累加窗口延迟时间
                    "fallback_window": 0,              # 固定窗口长度
                    "fixed_delay": 0,                  # 固定窗口延迟
                    "count_freq": 30,                  # 固定窗口统计频率
                    "schedule_period": "hour",         # 固定窗口统计频率单位
                    "sql": "select * from 591_etl_p_s_04",
                    "dependency_config_type": "unified" | "custom",
                    "unified_config": {
                        "window_size": 1,
                        "window_size_period": 'hour',
                        "dependency_rule": "all_finished"
                    },
                    "custom_config": {
                        "rt1": {
                            "window_delay": 1,
                            "window_size": 1,
                            "window_size_period": 'hour',
                            "dependency_rule": "all_finished"
                        },
                        "rt1": {
                            "window_delay": 1,
                            "window_size": 1,
                            "window_size_period": 'hour',
                            "dependency_rule": "all_finished"
                        }
                    }
                    ...
                    }
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} HDFS节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "hdfs_storage",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",  # 父节点输出rt
                    "name": "2_f1_stream(HDFS)"
                    "expires": 1,
                    "cluster": "xxxx",
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} ES节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "elastic_storage",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "result_table_id": "2_f1_stream",  # 父节点输出rt
                    "name": "2_f1_stream(Elasticsearch)"
                    "expires": 1,
                    "bk_biz_id": 591,
                    "cluster": "es-test"
                    "analyzed_fields": ["field2"],
                    "date_fields": ["field2"],
                    "indexed_fields": ["field2"],
                    "doc_values_fields": ["field2"],
                    "json_fields": ["field2"]
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} tspider节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "tspider_storage",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(Tspider)"
                    "expires": 15,
                    "indexed_fields": ["field2"],
                    "cluster": "tspider-test"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} mysql节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "mysql_storage",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(MySQL)"
                    "expires": 15,
                    "indexed_fields": ["field2"],
                    "cluster": "mysql-test"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} queue节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "queue_storage",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(Queue)"
                    "expires": 15,
                    "cluster": "queue-test"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} druid节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "druid_storage",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(Druid)"
                    "expires": 15,
                    "cluster": "druid-test"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} tredis节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "tredis_storage",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    # list类型
                    "storage_type":"list"
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(Tredis)"
                    "expires": 15,
                    "cluster": "tredis-test",

                    # kv类型
                    "storage_type":"kv"
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(Tredis)"
                    "expires": 15,
                    "cluster": "tredis-test",
                    "keys": [1,2,3,a,b,c]
                    "values": [1,2,3,a,b,c],
                    "separator": ",",
                    "key_prefix": "xx",

                    # 静态关联类型
                    "storage_type":"join"
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(Tredis)"
                    "expires": 15,
                    "cluster": "tredis-test",
                    "keys": [1,2,3,a,b,c],
                    "separator": ":",
                    "key_separator": "_"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} tsdb节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "tsdb_storage",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(Tsdb)"
                    "expires": 15,
                    "cluster": "tsdb-test",
                    "dim_fields": [xxx]     # 可选字段
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} merge节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                },
                {
                    "source": {
                        "node_id": 113,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }
                ],
                "node_type": "merge",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "name": "f1_stream",
                    "table_name": "f1_stream",
                    "output_name": "f1_stream",
                    "description": "xxx"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} split节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "split",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "name": "f1_stream",
                    "table_name": "f1_stream",
                    "output_name": "f1_stream",
                    "config": [
                        {"bk_biz_id": 591, "logic_exp": "xxx"}, {"bk_biz_id": 592, "logic_exp": "xxx"}
                    ],
                    "description": "xxx"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} model节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "model",
                "config": {
                    "from_result_table_ids": ["xxx"], "name": "model_01", "table_name": "model_01",
                    "bk_biz_id": 591, "output_name": "model_01",
                    "model_params": [
                       {"category": "input", "nodeid": "node_input_node_7aaeda", "arg": "serving_fields_mapping",
                        "value": {"AGE": "gseindex"}},      # 类型需要支持强转
                       {"category": "input", "nodeid": "node_input_node_7aaeda", "arg": "input_result_table",
                        "value": input_result_table_id},
                       {"category": "input", "nodeid": "node_input_node_7aaeda", "arg": "training_fields_mapping",
                        "value": {"AGE": "gseindex"}},
                       {"category": "input", "nodeid": "node_input_node_7aaeda", "arg": "group_fields",
                        "value": ["ip", "report_time", "gseindex", "path", "log"]},
                       {"category": "args", "nodeid": "node_code_node_643f7b", "arg": "custom_args", "value": {}},
                       {"category": "output", "nodeid": "node_output_node_1fa807", "arg": "storages_args",
                        "value": {"hdfs": {}}},
                       {"category": "output", "nodeid": "node_output_node_1fa807", "arg": "table_name",
                        "value": "xxx"}],
                    "model_id": "xxx",
                    "model_version_id": 17,
                    "auto_upgrade": True,
                    "serving_scheduler_params": {"serving_mode": "realtime", "first_run_time": None, "period": None,
                                                 "value": None},
                    # 非必须参数
                    "training_scheduler_params": {"value": "6", "first_run_time": "2018-02-08 00:00:00", "period": "h"},
                    # 非必须参数
                    "training_when_serving": True,
                    # 非必须参数
                    "training_from_instance": 1
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} tdw数据源节点
            {
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "tdw_source",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "result_table_id": "xxx",
                    "name": "xxx"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} tdw 存储节点
            {
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "tdw_storage",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "result_table_id": "xxx",  # TDW rt
                    "name": "xxx(TDW)",        # 节点名称
                    "expires": 1,
                    "cluster": "xxx",
                    "bid": "xxx",               # bid
                    "tdbank_address": "xxx",    # TdBank地址
                    "kv_separator": "xxx",      # KV分隔符
                    "record_separator": "xxx",  # 记录分隔符

                    "output_name": "xxx",       # 中文名称
                    "db_name": "xxx"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} tdw离线计算
            {
                # 同平台离线计算表
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "tdw_batch",
                "config": {
                    "from_nodes": [                   # 上游节点信息
                        {
                            'id': 3,
                            'from_result_table_ids': ['1_xxxx']
                        }
                    ],
                    "bk_biz_id": 591,
                    "window_type": "fixed",
                    "accumulate": false,
                    "name": "etl_p_b_01",
                    "output_name": "etl_p_b_01",
                    "data_start": 0,                   # 累加窗口数据起点
                    "data_end": 23,                    # 累加窗口数据终点
                    "delay": 0,                        # 累加窗口延迟时间
                    "fallback_window": 0,              # 固定窗口长度
                    "fixed_delay": 0,                  # 固定窗口延迟
                    "count_freq": 30,                  # 固定窗口统计频率
                    "schedule_period": "hour",         # 固定窗口统计频率单位
                    "sql": "select * from 591_etl_p_s_04",
                    "dependency_config_type": "unified" | "custom",
                    "unified_config": {
                        "window_size": 1,
                        "window_size_period": 'hour',
                        "dependency_rule": "all_finished"
                    },
                    "custom_config": {
                        "rt1": {
                            "window_delay": 1,
                            "window_size": 1,
                            "window_size_period": 'hour',
                            "dependency_rule": "all_finished"
                        },
                        "rt1": {
                            "window_delay": 1,
                            "window_size": 1,
                            "window_size_period": 'hour',
                            "dependency_rule": "all_finished"
                        }
                    },
                     "cluster_id": "xx",
                     "db_name": "xx",
                     "expires': 1
                    ...
                    }
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} tdw-jar离线计算
            {
                # 同平台离线计算表
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "tdw_jar_batch",
                "config": {
                    "from_nodes": [                   # 上游节点信息
                        {
                            'id': 3,
                            'from_result_table_ids': ['1_xxxx']
                        }
                    ],
                    "processing_name": "xxx",
                    "bk_biz_id": 591,
                    "window_type": "fixed",
                    "accumulate": false,
                    "name": "etl_p_b_01",
                    "data_start": 0,                   # 累加窗口数据起点
                    "data_end": 23,                    # 累加窗口数据终点
                    "delay": 0,                        # 累加窗口延迟时间
                    "fallback_window": 0,              # 固定窗口长度
                    "fixed_delay": 0,                  # 固定窗口延迟
                    "count_freq": 30,                  # 固定窗口统计频率
                    "schedule_period": "hour",         # 固定窗口统计频率单位
                    "dependency_config_type": "unified" | "custom",
                    "unified_config": {
                        "window_size": 1,
                        "window_size_period": 'hour',
                        "dependency_rule": "all_finished"
                    },
                    "custom_config": {
                        "rt1": {
                            "window_delay": 1,
                            "window_size": 1,
                            "window_size_period": 'hour',
                            "dependency_rule": "all_finished"
                        },
                        "rt1": {
                            "window_delay": 1,
                            "window_size": 1,
                            "window_size_period": 'hour',
                            "dependency_rule": "all_finished"
                        }
                    },
                    "outputs": [
                        {
                            "bk_biz_id": 591,
                            "table_name": "etl_p_b_01",
                            "output_name": "etl_p_b_01",
                            "cluster_id": "xx",
                            "db_name": "xx",
                            "expires': 1,
                            "expires_unit": "xx",
                            "fields": [
                                {
                                    "field_name": "x",
                                    "field_type": "x",
                                    "description": "x"
                                },
                                {
                                    "field_name": "x",
                                    "field_type": "x",
                                    "description": "x"
                                }
                            ]
                        }
                    ],
                    "tdw_task_config": {
                        "timeout": 1,   # 任务超时时间
                        "program_specific_params": "x", # 程序参数
                        "class_name": "xx",             # 类名
                        "driver_memory": "2g",          # driver 内存
                        "executor_memory": "2g",        # executer 内存
                        "executor_cores": "2",          # 每个 executor 的核数
                        "num_executors": "2",           # 启动的 executor 数量
                        "options": "x",                 # 扩展参数
                        "jar_info": [
                            {
                                "jar_id": 1,            # 待提交的jar包ID
                                "jar_name": "",
                                "jar_created_at": "",
                                "jar_created_by": ""
                            }
                        ]
                    }
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} 流程化建模节点
        {
            "from_links": [{
                "source": {
                    "node_id": 112,
                    "id": "ch_1570",
                    "arrow": "Right"
                },
                "target": {
                    "id": "ch_1536",
                    "arrow": "Left"
                }
            }],
            "node_type": "process_model",
            "config": {
                "from_result_table_ids": ["xxx"],
                "name": "model_01", 		// 节点名称
                "table_name": "model_01",	// 输出表
                "bk_biz_id": 591,
                "output_name": "model_01",	// 中文名
                "input_config": {},
                "output_config": {},
                "schedule_config": {},
                "serving_mode": 'offline',	 // 不允许切换
                "sample_feedback_config": {}, // 是否反馈我的数据及标注
                "model_extra_config": {},
                "upgrade_config": {},		 // 是否自动更新模型
                "model_release_id": 1,		 // 对应于modelflow中的 model_id 和 model_version_id
            },
            "frontend_info": {
                "y": 293,
                "x": 92
            }
        }
        @apiParamExample {json} postgresql节点
            {
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "pgsql_storage",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(Postgresql)"
                    "expires": 15,
                    "expires_unit": "d",
                    "indexed_fields": ["field2"],
                    "cluster": "postgresql-test"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} tpg 节点
            {
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "tpg",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(Tpg)"
                    "indexed_fields": ["field2"],
                    "cluster": "tpg-test"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} tdbank 节点
            {
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "tdbank",
                "config": {
                    "from_result_table_ids": ["xxx"],
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(TDBank)"
                    "bid": "xxxxx"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiParamExample {json} spark_structured_streaming 节点
            {
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "spark_structured_streaming",
                "config": {
                    "from_nodes": [
                        {
                            "id": 1,
                            "from_result_table_ids": ["x1", "x2"]
                        },
                        {
                            "id": 1,
                            "from_result_table_ids": ["x1", "x2"]
                        }
                    ],
                    "bk_biz_id": 591,
                    "name": "xxxx",                    // 节点名称
                    "processing_name": "xxx",         // 数据处理名称
                    "programming_language": "python", // 编程语言
                    "user_main_class": "com.main",    // 程序入口
                    "user_args": "xxxx",              // 程序参数
                    "outputs": [
                        {
                            "bk_biz_id": 591,
                            "table_name": "etl_p_b_01",
                            "output_name": "etl_p_b_01",    // 输出中文名
                            "fields": [
                                {
                                    "field_name": "x",
                                    "field_type": "x",
                                    "field_alias": "x",
                                    "event_time": true,
                                },
                                {
                                    "field_name": "x",
                                    "field_type": "x",
                                    "field_alias": "x",
                                    "event_time": false,
                                }
                            ]
                        }
                    ],
                    "package": [
                        {
                            "id": 1,                   // 待提交的jar包ID
                            "name": "",                // 待提交的jar包名称
                            "created_at": "",
                            "created_by": ""
                        }
                    ],
                   "advanced": {
                       "use_savepoint": true          // 启用 Savepoint
                   }
                }
            }
        @apiParamExample {json} flink_streaming 节点
            {
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "flink_streaming",
                "config": {
                    "from_nodes": [
                        {
                            "id": 1,
                            "from_result_table_ids": ["x1", "x2"]
                        },
                        {
                            "id": 1,
                            "from_result_table_ids": ["x1", "x2"]
                        }
                    ],
                    "bk_biz_id": 591,
                    "name": "xxxx",                   // 节点名称
                    "processing_name": "xxx",         // 数据处理名称
                    "programming_language": "java",   // 编程语言
                    "code": "import xxx",             // 代码信息
                    "user_args": "xxxx",              // 程序参数
                    "outputs": [
                        {
                            "bk_biz_id": 591,
                            "table_name": "etl_p_b_01",
                            "output_name": "etl_p_b_01",    // 输出中文名
                            "fields": [
                                {
                                    "field_name": "x",
                                    "field_type": "x",
                                    "field_alias": "x",
                                    "event_time": true,
                                },
                                {
                                    "field_name": "x",
                                    "field_type": "x",
                                    "field_alias": "x",
                                    "event_time": false,
                                }
                            ]
                        }
                    ],
                    "package": [
                        {
                            "id": 1,                   // 待提交的jar包ID
                            "name": "",                // 待提交的jar包名称
                            "created_at": "",
                            "created_by": ""
                        }
                    ],
                   "advanced": {
                       "use_savepoint": true          // 启用 Savepoint
                   }
                }
            }
        @apiParamExample {json} pulsar 节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": "queue_pulsar",
                "config": {
                    "from_nodes": [
                        {
                            "id": 1,
                            "from_result_table_ids": ["x1", "x2"]
                        },
                        {
                            "id": 1,
                            "from_result_table_ids": ["x1", "x2"]
                        }
                    ],
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(Queue)"
                    "expires": 15,
                    "expires_unit": "d",
                    "cluster": "pulsar-test"
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "status": "no-start",
                    "node_config": {
                        "from_result_table_ids": ["xxx"],
                        "bk_biz_id": 2,
                        "window_type": "fixed",
                        "count_freq": 1,
                        "name": "f1_offline",
                        "parent_tables": null,
                        "fallback_window": 2,
                        "data_start": null,
                        "data_end": null,
                        "fixed_delay": 4,
                        "delay": null,
                        "table_name": "f1_offline",
                        "sql": "select * from 2_f1_stream",
                        "accumulate": false,
                        "expire_day": 7,
                        "output_name": "f1_offline",
                        "schedule_period": "day"
                    },
                    "node_id": 117,
                    "version": 289,
                    "flow_id": 1,
                    "result_table_ids": [
                        "2_input_node_011"
                    ],
                    "has_modify": false,
                    "node_type": "offline",
                    "frontend_info": {
                        "y": 296,
                        "x": 124
                    },
                    "node_name": "f1_offline"
                }
        @apiParamExample {json} ignite节点
            {
                "bk_username": "admin",
                "from_links": [{
                    "source": {
                        "node_id": 112,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "node_type": “ignite”,
                "config": {
                    # 静态关联类型
                    "storage_type":"join"
                    "bk_biz_id": 591,
                    "result_table_id": "2_f1_stream",
                    "name": "2_f1_stream(ignite)"
                    "expires": -1,
                    "expires_unit": "",
                    "cluster": "ignite-test",
                    "indexed_fields": ["field2"],
                    "storage_keys": ["field2"],
                    "max_records": 100000,
                    "from_nodes":[]
                },
                "frontend_info": {
                    "y": 293,
                    "x": 92
                }
            }
        """
        operator = get_request_username()
        node_type = params["node_type"]
        from_links = params["from_links"]
        config_params = params["config"]
        # 冗余 node_type 到 config_params 中
        config_params["node_type"] = node_type
        frontend_info = params["frontend_info"]
        return NodeController.create(node_type, operator, flow_id, from_links, config_params, frontend_info)

    @valid_flow_wrap("更新节点中")
    @perm_check("flow.update", detail=True, url_key="flow_id")
    @node_config_wrap
    @flow_version_wrap(FlowConfig.handlers)
    @params_valid(serializer=PartialUpdateNodeSerializer)
    def partial_update(self, request, flow_id, nid, params):
        """
        @api {patch} /dataflow/flow/flows/:fid/nodes/:nid 传入指定节点参数更新节点
        @apiName partial_update_node
        @apiGroup Flow
        @apiParamExample {json} 请求样例
            {
                "from_nodes": [                   # 上游节点信息
                    {
                        'id': 3,
                        'from_result_table_ids': ['1_xxxx']
                    }
                ],
                "bk_biz_id": 2,
                "name": "f1_stream",              # 节点名称
                "table_name": "van_stream_08",    # 数据输出
                "output_name": "van_stream_08",   # 输出中文名
                "window_type": "slide",           # 窗口类型
                "window_time": 0,                 # 窗口长度
                "waiting_time": 0,                # 延迟时间
                "count_freq": 30,                 # 统计频率
                "sql": "select * from etl_van_test5"

            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "from_nodes": [                   # 上游节点信息
                        {
                            'id': 3,
                            'from_result_table_ids': ['1_xxxx']
                        }
                    ],
                    "bk_biz_id": 2,
                    "name": "f1_stream",              # 节点名称
                    "table_name": "van_stream_08",    # 数据输出
                    "output_name": "van_stream_08",   # 输出中文名
                    "window_type": "slide",           # 窗口类型
                    "window_time": 0,                 # 窗口长度
                    "waiting_time": 0,                # 延迟时间
                    "count_freq": 30,                 # 统计频率
                    "sql": "select * from etl_van_test5",
                }
        """
        request_data = request.data
        # TODO: 新表单改造，后期改造后可去掉
        from_nodes = params.get("from_nodes", [])
        if not from_nodes:
            from_nodes = params.get("inputs", [])
        return NodeController.partial_update(nid, request_data, from_nodes)

    @valid_flow_wrap("更新节点中")
    @perm_check("flow.update", detail=True, url_key="flow_id")
    @node_handler_wrap
    @flow_version_wrap(FlowConfig.handlers)
    @params_valid(serializer=UpdateNodeSerializer)
    def update(self, request, flow_id, nid, params):
        """
        @api {put} /dataflow/flow/flows/:fid/nodes/:nid 更新节点
        @apiName update_node
        @apiGroup Flow
        @apiParam {list} from_links 节点连线关系
        @apiParam {dict} config 节点配置信息
        @apiParam {[dict]} frontend_info 节点坐标
        @apiParam {dict} config 节点配置项请参照 flow.handlers.forms
        @apiParamExample {json} 请求样例
            {
                "operator": "xxx",
                "from_links": [{
                    "source": {
                        "node_id": 4,
                        "id": "ch_1570",
                        "arrow": "Right"
                    },
                    "target": {
                        "id": "ch_1536",
                        "arrow": "Left"
                    }
                }],
                "config": {
                    "sql": "xxx",
                    "xx": "xx"
                },
                "frontend_info": {"y": 296, "x": 124}

            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "node_id": 1,
                    "status": "",
                    "node_config": {},
                    ...
                }
        """
        operator = get_request_username()
        from_links = params["from_links"]
        request_data = params["config"]
        frontend_info = params["frontend_info"]
        return NodeController.update(operator, flow_id, nid, from_links, request_data, frontend_info)

    @valid_flow_wrap("删除节点中")
    @perm_check("flow.update", detail=True, url_key="flow_id")
    @flow_version_wrap(FlowConfig.handlers)
    def destroy(self, request, flow_id, nid):
        """
        @api {delete} /dataflow/flow/flows/:fid/nodes/:nid 删除节点
        @apiName remove_node
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                None
        """
        operator = get_request_username()
        status = NodeController.delete(operator, nid)
        return Response({"status": status})

    @perm_check("flow.retrieve", detail=True, url_key="flow_id")
    def retrieve(self, request, flow_id, nid):
        """
        @api {get} /dataflow/flow/flows/:fid/nodes/:nid 获取节点信息
        @apiName retrieve
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "status": "no-start"
                    "node_config": 2036,
                    "node_id": "xxx",
                    "version": 1,
                    "flow_id": 160,
                    "result_table_ids": [],
                    "has_modify": "xxx",
                    "node_type": "xxx",
                    "frontend_info": {},
                    "node_name": "xxx"
                }
        """
        response_data = NodeController.get_node_info_dict(nid)
        return Response(response_data)

    @detail_route(methods=["get"], url_path="monitor_data")
    @perm_check("flow.retrieve", detail=True, url_key="flow_id")
    def list_monitor_data(self, request, flow_id, nid):
        """
        @api {get} /dataflow/flow/flows/:fid/nodes/:nid/monitor_data 根据nid获取监控打点信息
        @apiName list_node_monitor_data
        @apiGroup Flow
        @apiParam [string] monitor_type 监控指标类型，用逗号分割
            （dmonitor_alerts,data_loss_io_total,rt_status,data_delay_max,data_trend）
        @apiParamExample {json} 获取节点告警信息
            {
                monitor_type: 'dmonitor_alerts,data_loss_io_total,rt_status,data_delay_max,data_trend'
            }
        @apiSuccessExample {json} 成功返回
            {
                "dmonitor_alerts": {
                    "rt_id": [{
                        "msg": xxx
                        ...
                    }]
                },
                "data_loss_io_total": {
                    "rt_id": {
                        "input": [x,x,x],
                        "output": [x,x,x],
                        "time": [x,x,x]
                    }
                }
                "rt_status": {
                    "rt_id": {
                        "interval": 3600,
                        "execute_history": [
                            {
                                "status": "fail",
                                "status_str": "失败"
                                "execute_id": "",
                                "err_msg": "",
                                "err_code": "0",
                                "start_time": "2018-11-11 11:11:11",
                                "end_time": "2018-11-11 12:11:11",
                                "period_start_time": "2018-11-11 11:11:11",
                                "period_end_time": "2018-11-11 12:11:11"
                            }
                        ]
                    }
                }
                "data_delay_max": {
                    "rt_id": {
                        "delay_max": [x,x,x],
                        "time": [x,x,x]
                    }
                }
                "data_trend": {
                    "rt_id": {
                        "input": [x,x,x],
                        "output": [x,x,x]
                        "alert_list": [x,x,x],
                        "time": [x,x,x]
                    }
                }
            }
        """
        monitor_type = request.query_params.get("monitor_type", "")
        response_data = NodeController.list_monitor_data(flow_id, nid, monitor_type)
        return Response(response_data)


class ListNodesViewSet(APIViewSet):
    @list_route(methods=["get"], url_path="calculate_nodes")
    def list_calculate_nodes(self, request):
        """
        @api {get} /dataflow/flow/nodes/calculate_nodes?result_table_id=1&result_table_id=2 通过结果表查询得到计算节点，
            支持批量
        @apiName list_calculate_nodes
        @apiGroup Flow
        @apiParam {string} result_table_id 结果表ID列表，用&连接
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                [
                    {
                        "status": "no-start",
                        "node_config": {
                            "bk_biz_id": 2,
                            "window_type": "none",
                            "counter": null,
                            "count_freq": null,
                            "name": "van_stream_01",
                            "window_time": null,
                            "table_name": "van_stream_01",
                            "sql": "select * from etl_van_test5",
                            "output_name": "van_stream_01",
                            "waiting_time": null
                        },
                        "node_id": 101,
                        "version": null,
                        "flow_id": 83,
                        "result_table_ids": [
                            "591_stream_test_abc"
                        ],
                        "has_modify": false,
                        "node_type": "realtime",
                        "frontend_info": {
                            "y": 308,
                            "x": 313
                        },
                        "node_name": "van_stream_01"
                    }
                ]
        """
        result_table_ids = request.query_params.getlist("result_table_id")
        result_tables = FlowNodeRelation.objects.filter(
            result_table_id__in=result_table_ids,
            node_type__in=NodeTypes.CALC_CATEGORY,
        )
        node_ids = {rt.to_dict()["node_id"] for rt in result_tables}
        calculate_nodes = []
        for node_id in node_ids:
            _handler = NODE_FACTORY.get_node_handler(node_id)
            _calculate_node = _handler.to_dict(add_has_modify=True)
            calculate_nodes.append(_calculate_node)
        return Response(calculate_nodes)

    @list_route(methods=["get"], url_path="storage_nodes")
    def list_storage_nodes(self, request):
        """
        @api {get} /dataflow/flow/nodes/storage_nodes?result_table_id=1&result_table_id=2 通过结果表查询得到存储节点，
            支持批量
        @apiName list_storage_nodes
        @apiGroup Flow
        @apiParam {string} result_table_id 结果表ID列表，用&连接
        @apiSuccessExample {json} 成功返回
            [
                 {
                    "status": "no-start",       # 节点状态
                    "storage_type": "hdfs",     # 存储类型
                    "node_id": 26,
                    "flow_id": 1,
                    "flow_name": "test flow",   # DataFlow 名称
                    "node_config": {
                        "cluster": "default",
                        "expires": 10,
                        "result_table_id": '',
                        "name": '',             # 存储描述
                        "bk_biz_id": 3
                    },
                    "version": "V201708281520124062",
                    "result_table_ids": ["1_realtime_test"],
                    "node_type": "hdfs_storage", # 节点类型
                    "has_modify": False,
                    "frontend_info": {"x":1, "y":1}
                }
            ]
        """
        result_table_ids = request.query_params.getlist("result_table_id")
        result_tables = FlowNodeRelation.objects.filter(
            result_table_id__in=result_table_ids,
            node_type__in=NodeTypes.STORAGE_CATEGORY,
        )
        result_tables = [rt.to_dict() for rt in result_tables]
        node_ids = {rt["node_id"] for rt in result_tables}
        cleaned_nodes = []
        for node_id in node_ids:
            _handler = NODE_FACTORY.get_node_handler(node_id)
            _cleaned_node = _handler.to_dict(add_has_modify=True)
            _cleaned_node["storage_type"] = _handler.storage_type
            _cleaned_node["flow_id"] = _handler.flow_id
            _cleaned_node["flow_name"] = _handler.flow.flow_name

            cleaned_nodes.append(_cleaned_node)

        return Response(cleaned_nodes)

    @list_route(methods=["get"], url_path="processing_nodes")
    def list_processing_nodes(self, request):
        """
        @api {get} /dataflow/flow/nodes/processing_nodes?result_table_id=1&result_table_id=2 通过结果表查询得到
            processing节点，支持批量
        @apiName list_processing_nodes
        @apiGroup Flow
        @apiParam {string} result_table_id 结果表ID列表，用&连接
        @apiSuccessExample {json} 成功返回
            [
                    {
                        "status": "no-start",
                        "node_config": {
                            "bk_biz_id": 2,
                            "window_type": "none",
                            "counter": null,
                            "count_freq": null,
                            "name": "van_stream_01",
                            "window_time": null,
                            "table_name": "van_stream_01",
                            "sql": "select * from etl_van_test5",
                            "output_name": "van_stream_01",
                            "waiting_time": null
                        },
                        "node_id": 101,
                        "version": null,
                        "flow_id": 83,
                        "result_table_ids": [
                            "591_stream_test_abc"
                        ],
                        "has_modify": false,
                        "node_type": "realtime",
                        "frontend_info": {
                            "y": 308,
                            "x": 313
                        },
                        "node_name": "van_stream_01"
                    }
            ]
        """
        result_table_ids = request.query_params.getlist("result_table_id")
        result_tables = FlowNodeRelation.objects.filter(
            result_table_id__in=result_table_ids,
            node_type__in=NodeTypes.PROCESSING_CATEGORY,
        )
        node_ids = {rt.to_dict()["node_id"] for rt in result_tables}
        _nodes = []
        for node_id in node_ids:
            _handler = NODE_FACTORY.get_node_handler(node_id)
            _node = _handler.to_dict(add_has_modify=True, add_related_flow=True)
            _nodes.append(_node)
        return Response(_nodes)
