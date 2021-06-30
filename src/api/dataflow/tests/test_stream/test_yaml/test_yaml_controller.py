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
import os

import mock
import pytest
from rest_framework.reverse import reverse
from rest_framework.test import APITestCase

from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util
from dataflow.shared.databus.databus_helper import DatabusHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.stream.api.api_helper import AccessApiHelper, MetaApiHelper
from dataflow.stream.exceptions.comp_execptions import JobExistsError, YamlFormatError, YamlJobLockError
from dataflow.stream.handlers import dataflow_yaml_info
from dataflow.stream.models import DataFlowYamlInfo, ProcessingJobInfo, ProcessingStreamInfo
from tests.utils import UnittestClient


class TestYaml(APITestCase):
    @mock.patch("dataflow.stream.yaml.yaml_controller.get_request_username")
    def test_create_yaml(self, get_request_username):
        yaml_name = "test_yaml"
        project_id = 1
        job_id = "test_job"
        get_request_username.return_value = "admin"
        from dataflow.stream.extend.yaml.yaml_controller import Yaml

        yaml_info = Yaml.create_yaml(yaml_name, project_id, job_id)
        assert yaml_info["yaml_name"] == yaml_name
        assert yaml_info["project_id"] == project_id
        assert yaml_info["job_id"] == job_id
        assert yaml_info["created_by"] == "admin"

        with self.assertRaises(JobExistsError):
            Yaml.create_yaml(yaml_name, project_id, job_id)

    def test_list_all_yaml(self):
        DataFlowYamlInfo.objects.create(
            yaml_id=1,
            yaml_name="yaml_1",
            project_id=1,
            job_id="job1",
            created_by="admin",
        )
        DataFlowYamlInfo.objects.create(
            yaml_id=2,
            yaml_name="yaml_2",
            project_id=2,
            job_id="job2",
            created_by="admin",
        )
        from dataflow.stream.extend.yaml.yaml_controller import Yaml

        yaml_infos = Yaml.list_all_yaml()
        assert len(yaml_infos) == 2
        assert {
            "yaml_id": 1,
            "status": "no-start",
            "yaml_name": "yaml_1",
            "project_id": 1,
        } in yaml_infos
        assert {
            "yaml_id": 2,
            "status": "no-start",
            "yaml_name": "yaml_2",
            "project_id": 2,
        } in yaml_infos

    @mock.patch("dataflow.stream.yaml.yaml_controller.get_request_username")
    def test_check_whether_operated(self, get_request_username):
        DataFlowYamlInfo.objects.create(
            yaml_id=1,
            yaml_name="yaml_1",
            project_id=1,
            job_id="job1",
            created_by="admin",
            is_locked=True,
        )
        get_request_username.return_value = "admin"
        with self.assertRaises(YamlJobLockError):
            from dataflow.stream.extend.yaml.yaml_controller import Yaml

            Yaml(1).check_whether_operated()

    def test_job_id(self):
        DataFlowYamlInfo.objects.create(
            yaml_id=1,
            yaml_name="yaml_1",
            project_id=1,
            job_id="job1",
            created_by="admin",
            is_locked=True,
        )
        from dataflow.stream.extend.yaml.yaml_controller import Yaml

        assert Yaml(1).job_id == "job1"

    def test_lock(self):
        DataFlowYamlInfo.objects.create(yaml_id=1, yaml_name="yaml_1", project_id=1)
        from dataflow.stream.extend.yaml.yaml_controller import Yaml

        yaml = Yaml(1)
        yaml.lock()
        assert yaml.yaml_info.is_locked

    def test_unlock(self):
        DataFlowYamlInfo.objects.create(yaml_id=1, yaml_name="yaml_1", project_id=1)
        from dataflow.stream.extend.yaml.yaml_controller import Yaml

        yaml = Yaml(1)
        yaml.unlock()
        assert not yaml.yaml_info.is_locked

    @mock.patch("dataflow.stream.yaml.yaml_controller.get_request_username")
    def test_destroy(self, get_request_username):
        get_request_username.return_value = "admin"
        from dataflow.stream.extend.yaml.yaml_controller import Yaml

        Yaml(1).destroy()
        DataFlowYamlInfo.objects.create(yaml_id=1, yaml_name="yaml_1", project_id=1, created_by="admin")
        Yaml(1).destroy()
        assert not dataflow_yaml_info.exists(yaml_id=1)

    def test_save_job_info(self):
        # test create
        DataFlowYamlInfo.objects.create(yaml_id=1, yaml_name="yaml_1", project_id=1, job_id="job1")
        from dataflow.stream.extend.yaml.yaml_controller import Yaml

        Yaml(1).save_job_info({"heads": [1, 2], "tails": [3, 4]})
        job_info = ProcessingJobInfo.objects.get(job_id="job1")
        assert job_info.job_id == "job1"
        assert json.loads(job_info.job_config) == {"heads": [1, 2], "tails": [3, 4]}
        assert job_info.code_version == "lol_bill_product"
        assert job_info.cluster_group == "lol"

        # test update
        DataFlowYamlInfo.objects.create(yaml_id=2, yaml_name="yaml_2", project_id=1, job_id="job2")
        ProcessingJobInfo(
            job_id="job2",
            processing_type="stream",
            implement_type="yaml",
            component_type="storm",
            programming_language="java",
            code_version="lol_test",
            cluster_group="lol",
            job_config=json.dumps({"heads": [1, 2], "tails": [3, 4]}),
            created_by="admin",
        ).save()
        Yaml(2).save_job_info({"heads": [1, 2], "tails": [5, 6]})
        job_info = ProcessingJobInfo.objects.get(job_id="job2")
        assert job_info.job_id == "job2"
        assert job_info.code_version == "lol_test"
        assert json.loads(job_info.job_config) == {"heads": [1, 2], "tails": [5, 6]}

    @pytest.mark.skip(reason="构建公共的yaml conf参数，和构建yaml")
    def test_build_conf(self, yaml_id, yaml_file):
        # yaml_conf 参数从文件中获取
        base_path = os.path.abspath(__file__)
        folder = os.path.dirname(base_path)
        test_yaml_conf_file = os.path.join(folder, yaml_file)
        yaml_conf = open(test_yaml_conf_file, "r").read()
        DataFlowYamlInfo.objects.create(yaml_id=yaml_id, yaml_name="yaml_1", project_id=1, job_id="job1")
        return yaml_conf

    def test_save_all_yaml_info(self):
        yaml_id1 = 1
        yaml_file = "test_yaml_conf.yaml"
        yaml_conf = self.test_build_conf(yaml_id1, yaml_file)
        yaml_conf = {"1": yaml_conf}
        # mock  接口
        MetaApiHelper.get_data_processing = mock.Mock(return_value={})
        MetaApiHelper.bulk_delete_data_processing = mock.Mock()
        AccessApiHelper.get_raw_data = mock.Mock(return_value={"storage_channel_id": 1})
        MetaApiHelper.set_data_processing = mock.Mock(side_effect=self.check_save_all_yaml_info_dp)
        DatabusHelper.get_channel_info_by_cluster_name = mock.Mock(return_value={"id": 1, "priority": 0})
        StorekitHelper.create_physical_table = mock.Mock(side_effect=self.check_storage_conf)
        from dataflow.stream.extend.yaml.yaml_controller import Yaml

        job_config, _ = Yaml(1).save_all_yaml_info(yaml_conf)
        assert job_config == {
            "heads": ["132_lol_raw_event_mix1_metric"],
            "tails": ["132_lol_bill_event_mix1_metric"],
        }

        stream_info1 = ProcessingStreamInfo.objects.get(processing_id="132_lol_raw_event_mix1_metric")
        stream_info2 = ProcessingStreamInfo.objects.get(processing_id="132_lol_bill_event_mix1_metric")
        assert json.loads(stream_info2.processor_logic)["storages"] == ["kafka"]
        assert (
            json.loads(stream_info2.processor_logic)["storages_args"]
            == '{"kafka": {"topic": "lol_raw_event_mix1_metric"}}'
        )
        assert json.loads(stream_info1.processor_logic)["storages"] == [
            "hermes",
            "redis",
        ]
        assert "lol_raw_event_mix1_metric" in json.loads(stream_info1.processor_logic)["storages_args"]

    @staticmethod
    def check_save_all_yaml_info_dp(processing_params):
        if processing_params["processing_id"] == "132_lol_raw_event_mix1_metric":
            assert processing_params["inputs"] == [
                {
                    "storage_cluster_config_id": None,
                    "storage_type": "channel",
                    "data_set_id": "3271",
                    "data_set_type": "raw_data",
                    "channel_cluster_config_id": 1,
                }
            ]
            assert processing_params["outputs"] == [
                {
                    "data_set_id": "132_lol_raw_event_mix1_metric",
                    "data_set_type": "result_table",
                    "storage_cluster_config_id": None,
                    "channel_cluster_config_id": None,
                    "storage_type": None,
                }
            ]
        if processing_params["processing_id"] == "132_lol_bill_event_mix1_metric":
            assert processing_params["inputs"] == [
                {
                    "tags": [],
                    "storage_cluster_config_id": None,
                    "storage_type": None,
                    "data_set_id": "132_lol_raw_event_mix1_metric",
                    "data_set_type": "result_table",
                    "channel_cluster_config_id": None,
                }
            ]
            assert processing_params["outputs"] == [
                {
                    "data_set_id": "132_lol_bill_event_mix1_metric",
                    "data_set_type": "result_table",
                    "storage_cluster_config_id": None,
                    "channel_cluster_config_id": 1,
                    "storage_type": "channel",
                }
            ]

    @staticmethod
    def check_storage_conf(**storage_config):
        assert storage_config["cluster_name"] == "lol"
        assert storage_config["expires"] == "14d"

    def test_load_chain(self):
        # save data to processing_stream_info
        yaml1 = 1
        yaml_conf = self.test_build_conf(yaml1, "test_yaml_conf.yaml")
        # mock save_all_yaml_info 依赖接口
        MetaApiHelper.get_data_processing = mock.Mock(return_value={})
        MetaApiHelper.bulk_delete_data_processing = mock.Mock()
        AccessApiHelper.get_raw_data = mock.Mock(return_value={"storage_channel_id": 1})
        MetaApiHelper.set_data_processing = mock.Mock(side_effect=self.check_save_all_yaml_info_dp)
        DatabusHelper.get_channel_info_by_cluster_name = mock.Mock(return_value={"id": 1, "priority": 0})
        StorekitHelper.create_physical_table = mock.Mock(side_effect=self.check_storage_conf)
        from dataflow.stream.extend.yaml.yaml_controller import Yaml

        Yaml(yaml1).save_all_yaml_info({"1": yaml_conf})
        # mock 测试方法依赖的接口
        data_processing_values = {
            "132_lol_raw_event_mix1_metric": {"inputs": [{"data_set_id": "3271"}]},
            "132_lol_bill_event_mix1_metric": {"inputs": [{"data_set_id": "132_lol_raw_event_mix1_metric"}]},
        }
        MetaApiHelper.get_data_processing = mock.Mock(side_effect=lambda p_id: data_processing_values[p_id])
        AccessApiHelper.get_raw_data = mock.Mock(return_value={"bk_biz_id": 132, "topic": "Loyalty_Mixed"})
        result_tables = Yaml(1).load_chain(["132_lol_raw_event_mix1_metric"], ["132_lol_bill_event_mix1_metric"])
        assert "132_data_id_3271" in result_tables
        assert result_tables["132_data_id_3271"]["input_source"] == "Loyalty_Mixed"

        # 测试input为 rt 的场景
        yaml2 = 2
        yaml_conf = self.test_build_conf(yaml2, "test_yaml_conf_input_rt.yaml")
        # mock save_all_yaml_info 依赖接口
        MetaApiHelper.get_data_processing = mock.Mock(return_value={})
        MetaApiHelper.get_result_table = mock.Mock(
            return_value={
                "storages": {
                    "kafka": {
                        "physical_table_name": "lol_raw_event_mix1_metric",
                        "storage_channel": {"channel_cluster_config_id": 6},
                    }
                }
            }
        )
        MetaApiHelper.bulk_delete_data_processing = mock.Mock()
        AccessApiHelper.get_raw_data = mock.Mock(return_value={"storage_channel_id": 1})
        MetaApiHelper.set_data_processing = mock.Mock(side_effect=self.check_save_all_yaml_info_dp)
        DatabusHelper.get_channel_info_by_cluster_name = mock.Mock(return_value={"id": 1, "priority": 0})
        StorekitHelper.create_physical_table = mock.Mock(side_effect=self.check_storage_conf)
        Yaml(yaml2).save_all_yaml_info({"2": yaml_conf})
        # mock 测试方法依赖的接口
        data_processing_values = {
            "132_lol_bill_event_mix7_to_tspider_1": {"inputs": [{"data_set_id": "132_lol_bill_event_mix1_metric"}]},
            "132_lol_bill_event_mix1_metric": {"inputs": [{"data_set_id": "132_lol_raw_event_mix1_metric"}]},
        }
        MetaApiHelper.get_data_processing = mock.Mock(side_effect=lambda p_id: data_processing_values[p_id])
        MetaApiHelper.get_result_table = mock.Mock(
            return_value={"storages": {"kafka": {"physical_table_name": "lol_raw_event_mix1_metric"}}}
        )
        result_tables = Yaml(yaml2).load_chain(
            ["132_lol_bill_event_mix7_to_tspider_1"],
            ["132_lol_bill_event_mix7_to_tspider_1"],
        )
        assert "132_lol_bill_event_mix1_metric_as_upstream" in result_tables
        assert (
            result_tables["132_lol_bill_event_mix1_metric_as_upstream"]["upstream_parent_result_table_id"]
            == "132_lol_bill_event_mix1_metric"
        )

    def test_set_job_conf(self):
        DataFlowYamlInfo.objects.create(yaml_id=1, yaml_name="yaml_1", project_id=1, job_id="job1")
        from dataflow.stream.extend.yaml.yaml_controller import Yaml

        yaml = Yaml(1)
        yaml.save_job_info({"heads": [1, 2], "tails": [3, 4]})

        # test code version concurrency is null
        yaml.set_job_conf(None, None)
        job_info = ProcessingJobInfo.objects.get(job_id=yaml.job_id)
        assert job_info.code_version == "lol_bill_product"
        assert job_info.deploy_config is None

        # test code version
        yaml.set_job_conf("master", "2")
        job_info = ProcessingJobInfo.objects.get(job_id=yaml.job_id)
        assert job_info.code_version == "master"
        assert job_info.deploy_config == '{"concurrency": "2"}'

    def test_check_yaml_conf(self):
        from dataflow.stream.extend.yaml.yaml_controller import Yaml

        yaml1 = """
                count_freq: 0
                id: 132_lol_bill_event_mix7_to_tspider
                parents: 132_lol_bill_event_mix1_metric
                project_id: 3171
                storages: [tspider]
                storages_args:
                  tspider: {cluster: lol, expires: 30, table_name: lol_bill_event_mix7}
                z=====> children: []
                """
        yaml_conf = {"tab1": yaml1}
        with self.assertRaises(YamlFormatError):
            Yaml.check_yaml_conf(yaml_conf)

        yaml2 = """
                count_freq: 0
                fields:
                - {default_value: null, field: timestamp, type: timestamp}
                - {default_value: null, field: iUin, type: string}
                id: 132_lol_bill_event_mix7_to_tspider
                parents: 132_lol_bill_event_mix1_metric
                project_id: 3171
                storages: [tspider
                storages_args:
                  tspider: {cluster: lol, expires: 30, table_name: lol_bill_event_mix7}
                z=====> children: []
                """
        yaml_conf = {"tab1": yaml2}
        with self.assertRaises(YamlFormatError):
            Yaml.check_yaml_conf(yaml_conf)


@pytest.mark.django_db
@pytest.mark.usefixtures("fixture_retrieve")
def test_retrieve(fixture_retrieve):
    yaml_id = fixture_retrieve
    url = reverse("stream_yaml-detail", [yaml_id])
    res = UnittestClient().get(url)
    res_util.check_response(res)
    assert res.data["yaml_name"] == "test_yaml"
    assert res.data["project_id"] == 1
    assert res.data["job_id"] == "test_job"
    assert res.data["created_by"] == "admin"
    assert res.data["status"] == "no-start"
    assert res.data["yaml_conf"] == {}
    assert "created_at" in res.data
