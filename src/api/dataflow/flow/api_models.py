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

from common.exceptions import ApiResultError
from django.utils.translation import ugettext_lazy as _

from dataflow.flow import exceptions as Errors
from dataflow.flow.exceptions import ResultTableNotFoundError
from dataflow.flow.utils.count_freq import CountFreq
from dataflow.shared.auth.auth_helper import AuthHelper
from dataflow.shared.batch.batch_helper import BatchHelper
from dataflow.shared.databus.databus_helper import DatabusHelper
from dataflow.shared.meta.result_table import result_table_helper as RemoteRTHelper
from dataflow.shared.model.model_helper import ModelHelper
from dataflow.shared.modeling.modeling_helper import ModelingHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.shared.stream.stream_helper import StreamHelper


class APIModel(object):
    def __init__(self, *arg, **kwargs):
        self._data = None

    @property
    def data(self):
        if self._data is None:
            self._data = self._get_data()

        return self._data

    def _get_data(self):
        """
        子类需要重载
        """
        return None

    def is_exist(self):
        return self._get_data is not None


class ResultTable(APIModel):
    """
    DATA_SCEHMA 结构
        {
            "input_args": null,
            "updated_at": "2017-07-12T16:22:32",
            "concurrency": 1,
            "updated_by": "",
            "job_id": null,
            "storages_args": "{\"mysql\": {\"cluster\": \"default\"}}",
            "input_type": null,
            "created_by": "",
            "storages": "mysql",
            "project_id": "1",
            "result_table_id": "3_third_time_custom_data",
            "count_freq": 0,
            "description": "第三次测试自定义数据了",
            "tags": null,
            "kafka_cluster_index": null,
            "job_item_id": null,
            "biz_id": "3",
            "data_id": null,
            "fields": [
                {
                    "default_value": null,
                    "description": "用户",
                    "origins": null,
                    "tags": null,
                    "result_table_id": "3_third_time_custom_data",
                    "created_at": "2017-07-12 16:22:32",
                    "updated_at": "2017-07-12 16:22:32",
                    "is_dimension": 1,
                    "filter": null,
                    "field": "gen_user",
                    "unit": null,
                    "processor_args": null,
                    "id": 2222,
                    "type": "string",
                    "processor": null,
                    "field_index": 1
                },
            ],
            "created_at": "2017-07-12T16:22:32",
            "table_name": "third_time_custom_data",
            "trt_type": 0,
            "input_source": null,
            "template_id": null
        }
    """

    class PROCESSING_TYPES(object):
        STREAM = ["stream"]
        BATCH = ["batch"]
        MODEL_APP = ["model_app"]
        MODEL = ["batch_model", "stream_model"]
        CLEAN_DATA = ["clean"]

    def __init__(self, result_table_id):
        self.result_table_id = result_table_id
        self._data = None

    def has_storage(self, storage):
        """
        查看 RT 是否有指定的存储
        """
        _arr = self.storages_arr
        return storage in _arr

    def has_storage_by_generate_type(self, storage, generate_type):
        """
        查看 RT 是否有指定的存储
        """
        return storage in self.storages and generate_type == self.storages[storage]["generate_type"]

    def get_storage_msg(self, cluster_type):
        if not self.has_storage(cluster_type):
            raise ApiResultError(_("元数据信息未包含%s类型存储.") % cluster_type)
        return self.data["storages"][cluster_type]

    def get_storage_id(self, cluster_type, storage_type):
        """
        获取存储对应的id信息，包括storage_cluster_config_id和channel_cluster_config_id
        @param cluster_type:
        @param storage_type:
        @return: storage_cluster_config_id, channel_cluster_config_id
        """
        storage_msg = self.get_storage_msg(cluster_type)
        if storage_type == "channel":
            if "storage_channel" not in storage_msg:
                raise ApiResultError(_("元数据信息返回未包含storage_channel信息，当前存储类型为%s.") % storage_type)
            return None, storage_msg["storage_channel"]["channel_cluster_config_id"]
        if storage_type == "storage":
            if "storage_cluster" not in storage_msg:
                raise ApiResultError(_("元数据信息返回未包含storage_cluster信息，当前存储类型为%s.") % storage_type)
            return storage_msg["storage_cluster"]["storage_cluster_config_id"], None
        raise ApiResultError(_("元数据信息返回格式错误，不支持当前的存储类型为%s.") % storage_type)

    def has_kafka(self):
        """
        是否具有 kafka 存储，存储配置中显性配置
        """
        return self.has_storage("kafka")

    def has_hdfs(self):
        """
        是否具有 HDFS 存储，存储配置中显性配置以及离线表默认带上 HDFS
        """
        return self.has_storage("hdfs")

    def is_offline(self):
        return self.processing_type in self.PROCESSING_TYPES.BATCH

    def is_clean_data(self):
        return self.processing_type in self.PROCESSING_TYPES.CLEAN_DATA

    def get_kafka_msg(self):
        """
        获取 KAFKA 消息
        @return {dict} 结构化数据
        """
        # 从KAFKA获取数据
        latest_msgs = DatabusHelper.get_result_table_data_tail(self.result_table_id)
        if not latest_msgs:
            return None

        return latest_msgs[0]

    def get_hdfs_msg(self):
        """
        获取 HDFS 消息
        @return {dict} 结构化数据
        """
        # HDFS获取数据
        latest_msgs = BatchHelper.get_result_table_data_tail(self.result_table_id)
        return json.loads(latest_msgs) if latest_msgs else None

    @property
    def table_name(self):
        return self.data["result_table_name"]

    @property
    def result_table_name_alias(self):
        return self.data["result_table_name_alias"]

    def cluster_name(self, cluster_type):
        current_storage_configs = StorekitHelper.get_physical_table(self.result_table_id, cluster_type)
        if not current_storage_configs:
            raise ApiResultError(
                _("错误，获取当前结果表(%(result_table_id)s)存储配置(%(cluster_type)s)为空.")
                % {
                    "result_table_id": self.result_table_id,
                    "cluster_type": cluster_type,
                }
            )
        cluster_name = current_storage_configs["cluster_name"]
        return cluster_name

    @property
    def bk_biz_id(self):
        """
        获取rt的bk_biz_id
        @return:
        """
        return int(self.data["bk_biz_id"])

    @property
    def is_managed(self):
        return bool(self.data["is_managed"])

    def perm_check_tdw_table(self):
        rt_tdw_conf = self.get_extra("tdw")
        cluster_id = rt_tdw_conf["cluster_id"]
        db_name = rt_tdw_conf["db_name"]
        table_name = rt_tdw_conf["table_name"]
        AuthHelper.check_tdw_table_perm(cluster_id, db_name, table_name)

    def get_extra(self, extra_type):
        return self.data["extra"][extra_type]

    @property
    def count_freq(self):
        return self.data["count_freq"]

    @property
    def count_freq_unit(self):
        return self.data["count_freq_unit"]

    def get_count_freq_in_hour(self):
        """
            H-小时任务
            d-天任务
            W-周任务
            M-月任务
            m-分钟任务（最小粒度10分钟）
            O-一次性任务
            R-非周期任务
        @return:
        """
        count_freq = 0
        count_freq_value = self.count_freq
        count_freq_unit = self.count_freq_unit
        if count_freq_unit == "H":
            count_freq_unit = "hour"
            count_freq = count_freq_value
        elif count_freq_unit == "d":
            count_freq_unit = "day"
            count_freq = count_freq_value
        elif count_freq_unit == "W":
            count_freq_unit = "week"
            count_freq = count_freq_value
        elif count_freq_unit == "M":
            # 注意，一个月不等同于30天，只不过当前方法仅仅用于比较统计频率的上下游关系，这里暂取作30
            count_freq_unit = "month"
            count_freq = count_freq_value
        return CountFreq(count_freq, count_freq_unit)

    def get_storage_expires_value(self, cluster_type, expires_unit="d"):
        """
        获取 RT 对应存储的过期时间
        @param cluster_type:
        @param expires_unit:
        @return:
        """
        if not self.has_storage(cluster_type):
            raise Errors.NodeError(
                "Expires query is not allowed for RT(%(result_table_id)s) has not "
                "related storage(%(cluster_type)s)"
                % {
                    "result_table_id": self.result_table_id,
                    "cluster_type": cluster_type,
                }
            )
        expires = self.storages[cluster_type]["expires"]
        if expires == "-1":
            return -1
        else:
            _expires_unit = expires[-1]
            # TODO: 注意，目前过期时间非 d 即 M，M 暂用 30 替换
            _expires = int(expires[:-1]) * (1 if _expires_unit == "d" else 30)
            if expires_unit == "d":
                return _expires
            else:
                return _expires / 30

    @property
    def storages_arr(self):
        return list(self.data["storages"].keys())

    @property
    def storages(self):
        return self.data["storages"]

    @property
    def fields(self):
        return self.data["fields"]

    @property
    def description(self):
        return self.data["description"]

    @property
    def processing_type(self):
        # clean,...
        return self.data["processing_type"]

    @property
    def sensitivity(self):
        return self.data["sensitivity"]

    @property
    def is_public(self):
        return self.sensitivity == "public"

    def list_fields(self):
        # 默认去掉timestamp, offset
        data = [_d for _d in self.fields if _d["field_name"] not in ["timestamp", "offset"]]
        return data

    def _get_data(self):
        """
        获取result_table表基本配置信息
        @return:
        {
            'storages': {
                'kafka': {},
                'mysql': {}
            },
            'result_table_name': 'stream_test_abc',
            'bk_biz_id': 591,
            "result_table_type": {
                "id": 5,
                "result_table_type_code": "stream",
                "result_table_type_name": "清洗表"
            }
        }
        """
        data = RemoteRTHelper.ResultTableHelper.get_result_table(
            self.result_table_id,
            related=["storages", "fields"],
            not_found_raise_exception=False,
            extra=True,
        )
        if not data:
            raise ResultTableNotFoundError(_("结果表 %(result_table_id)s 不存在") % {"result_table_id": self.result_table_id})
        return data

    def is_exist(self):
        try:
            return bool(
                RemoteRTHelper.ResultTableHelper.get_result_table(self.result_table_id, not_found_raise_exception=False)
            )
        except ApiResultError:
            return False

    def list_geog_area_tags(self):
        return [_info["code"] for _info in self.data["tags"]["manage"]["geog_area"]]


class BatchJob(APIModel):
    """
    DataFlow 对应后台的Batch任务实例
    """

    @classmethod
    def create(cls, args, operator):
        """
        创建后台任务实例
        """
        response_data = BatchHelper.create_job(**args)
        job_id = response_data["job_id"]
        return cls(job_id)

    def __init__(self, job_id):
        super(BatchJob, self).__init__()
        self.job_id = job_id

    def update(self, args, operator):
        """
        更新后台任务实例
        """
        args["job_id"] = self.job_id
        BatchHelper.update_job(**args)

    def _get_data(self):
        return None


class ModelAppJob(APIModel):
    """
    DataFlow 对应后台的Batch任务实例
    """

    @classmethod
    def create(cls, args, operator):
        """
        创建后台任务实例
        """
        response_data = ModelingHelper.create_multi_jobs(**args)
        job_id = response_data["job_id"]
        return cls(job_id)

    def __init__(self, job_id):
        super(ModelAppJob, self).__init__()
        self.job_id = job_id

    def update(self, args, operator):
        """
        更新后台任务实例
        """
        args["job_id"] = self.job_id
        ModelingHelper.update_multi_jobs(**args)

    def _get_data(self):
        return None


class StreamJob(APIModel):
    """
    DataFlow 实时任务串起来后对应后台的任务实例
    """

    @classmethod
    def create(cls, args, operator):
        """
        创建后台任务实例
        """
        response_data = StreamHelper.create_job(**args)
        job_id = response_data["job_id"]
        return cls(job_id)

    def __init__(self, job_id):
        super(StreamJob, self).__init__()
        self.job_id = job_id

    def update(self, args, operator):
        """
        更新后台任务实例
        """
        args["job_id"] = self.job_id
        StreamHelper.update_job(**args)

    def _get_data(self):
        return None


class Model(APIModel):
    """
    算法模型
    """

    def __init__(self, model_id, model_version_id):
        super(Model, self).__init__()
        self.model_id = model_id
        self.model_version_id = model_version_id

    def render_config_tempalte(self, tmp, params, default_kwargs):
        """
        根据模板+参数，渲染最终参数

        @param {Dict} tmp 模型模板
        @param {[Dict]} params 模板参数
        @param {[Dict]} default_kwargs 默认参数
        @paramExample default_kwargs 样例
            {
                biz_id: 111,
                project_id: 111,
                table_name: xxxx
            }
        """
        for _p in params:
            _category = _p["category"]
            _node_id = _p["nodeid"]
            _arg_en_name = _p["arg"]
            _value = _p["value"]
            tmp[_category][_node_id]["node_args"][_arg_en_name]["value"] = _value

        self.add_default(tmp, default_kwargs)
        return tmp

    def add_default(self, tmp, default_kwargs):
        project_id = default_kwargs["project_id"]
        bk_biz_id = default_kwargs["bk_biz_id"]
        table_name = default_kwargs["table_name"]

        self.set_value_by_key(tmp, "output&&output_node&&project_id", project_id)
        self.set_value_by_key(tmp, "output&&output_node&&biz_id", bk_biz_id)
        self.set_value_by_key(tmp, "output&&output_node&&table_name", table_name)

    def set_value_by_key(self, tmp, key, value):
        """
        通过 key 设置数值，key 格式为 {category}&&{nodeEnName}&&{argEnName}
        """
        data = self.unpack(key)
        category = data["category"]
        node_en_name = data["node_en_name"]
        arg_en_name = data["arg_en_name"]

        _d_nodes = tmp[category]

        for _node_id, _node in list(_d_nodes.items()):
            if _node["node_en_name"] == node_en_name:
                for _arg_en_name, _arg in list(_node["node_args"].items()):
                    if _arg_en_name == arg_en_name:
                        _arg["value"] = value

        return tmp

    @staticmethod
    def unpack(key):
        arr = key.split("&&")
        return {"category": arr[0], "node_en_name": arr[1], "arg_en_name": arr[2]}

    @staticmethod
    def pack(category, node_en_name, arg_en_name):
        return "{}&&{}&&{}".format(category, node_en_name, arg_en_name)

    @property
    def model_config_template(self):
        return self.data["model_config_template"]

    def _get_data(self):
        api_params = {
            "model_id": self.model_id,
            "model_version_id": self.model_version_id,
        }
        response_data = ModelHelper.get_model_version(**api_params)
        return response_data
