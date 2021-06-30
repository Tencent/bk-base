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
import hashlib
import json
import os
import sys

import yaml
from auth.pizza_settings import AUHT_PROJECT_DIR
from auth.utils.base import compute_file_md5

BK_IAM_CONFIG_PATH = "auth/config/"


def execute_contrast():
    """
    配置比较执行入口
    """
    YamlContrastHandler().execute_contrast()
    JsonContrastHandler().execute_contrast()


class FileType:
    YAML = "yaml"
    JSON = "json"


class BaseContrastHandler:
    """
    BaseContrastHandler 封装了一些配置项的通用方法，针对不同类型的配置文件的详细对比功能交由子类实现
    """

    # 本地环境
    EE = "ee"
    TENCENT = "tencent"

    def __init__(self):
        # 获取当前项目根路径
        self.project_dir = AUHT_PROJECT_DIR

    def execute_contrast(self):
        """
        差异对比方法，交由子类实现
        """
        raise NotImplementedError

    def load_data(self, file_path):
        with open(file_path) as f:
            return f.read()

    def output_different(self, ee_config, tencent_config):
        """
        差异项统一格式输出
        """
        sys.stdout.write(format(self.EE, "*^20s"))
        sys.stdout.write("\n")
        sys.stdout.write(json.dumps(ee_config, indent=2))
        sys.stdout.write("\n")
        sys.stdout.write(format(self.TENCENT, "*^20s"))
        sys.stdout.write("\n")
        sys.stdout.write(json.dumps(tencent_config, indent=2))
        sys.stdout.write("\n\n\n")

    def contrast_file_list(self, ee_file_list, tencent_file_list):
        """
        对比两个环境下配置文件数量的差异
        :param ee_file_list: ee 环境下的文件列表 [action.yaml, roles.yaml, object.yaml]
        :param tencent_file_list: tencent 环境下的文件列表：[action.yaml, roles.yaml, object.yaml]
        :return: 两者共同的配置文件 [action.yaml, roles.yaml, object.yaml]
        """

        added_file_list, reduced_file_list, identical_file_list = self.contrast_list(ee_file_list, tencent_file_list)
        if not added_file_list and not reduced_file_list:
            sys.stdout.write(
                "[BaseContrastHandler contrast_file_list] ee and tencent have the same files, "
                "identical_file_list={}\n".format(identical_file_list)
            )
        else:
            # 输出新增的和减少的文件
            sys.stdout.write(
                "[BaseContrastHandler contrast_file_list]  The ee environment has a few more profiles than "
                "tencent ! added_file_list={}\n".format(added_file_list)
            )
            sys.stdout.write(
                "[BaseContrastHandler contrast_file_list]  The ee environment has a slightly smaller profile than "
                "tencent ! reduced_file_list={}\n".format(reduced_file_list)
            )
        return identical_file_list

    def contrast_list(self, first_list, second_list):
        """
        contrast 接受两个列表，并返回相对于second_list，first_list 新增的、减少的、和相同的部分
        :param first_list: list []
        :param second_list: list []
        :return: added list:[], reduced_list: [], identical_list: []
        """
        # 计算first_list相对于second_list多出来的
        added_list = [obj for obj in first_list if obj not in second_list]
        # 计算first_list相对于second_list少出来的
        reduced_list = [obj for obj in second_list if obj not in first_list]
        # 计算first_list相对于second_list 相同的部分
        identical_list = [obj for obj in first_list if obj in second_list]

        return added_list, reduced_list, identical_list

    def _build_file_path(self, file_name, file_type, env):
        """
        构造文件路径
        :param file_name: 文件名
        :param file_type:文件类型 json or yaml
        :param env:环境 ee or tencent
        :return: 不指定文件名：
                    file_type == "json",env="ee":
                        E:/pizza/auth/config/ee/iam_migrations/
                    file_type == "yaml":
                        E:/pizza/auth/config/ee/core/
                指定文件名：
                        file_type == "json",env="ee":
                            E:/pizza/auth/config/ee/iam_migrations/0013_bk_data_20200819-1446_iam.json
                        file_type == "yaml",env="ee":
                            E:/pizza/auth/config/ee/core/action.yaml
        """
        path = "core"
        if file_type == FileType.JSON:
            path = "iam_migrations"
        if file_name is None:
            return os.path.join(self.project_dir, BK_IAM_CONFIG_PATH + f"{env}/{path}/")
        return os.path.join(self.project_dir, BK_IAM_CONFIG_PATH + f"{env}/{path}/{file_name}")

    def compute_list_md5(self, list_obj):
        """
        计算列表的md5
        :param list_obj: 待计算的列表
        :return: md5 值
        """
        return hashlib.md5(bytes(str(list_obj).encode("utf-8"))).hexdigest()


class YamlContrastHandler(BaseContrastHandler):
    """
    YamlContrastHandler 负责实现yaml配置文件的对比，依次对比顺序为
    文件数量->相同文件MD5对比->MD5不同文件内配置类型数量对比->MD5不同文件内相同配置项配置对比
    """

    def __init__(self):
        super().__init__()
        # 配置配置项-key的映射
        self.yaml_config_type_maps = {
            "actions": "action_id",
            "objects": "object_class",
            "action_relations": "parent",
            "roles": "role_id",
            "policies": "role_id",
        }

    def execute_contrast(self):
        """
        执行yaml配置对比
        """
        sys.stdout.write("[YamlContrastHandler execute_contrast] Start contrasting YAML files..\n")
        # 获取两边配置文件列表，并对比
        ee_yaml_file_list = self.get_yaml_file_list(self.EE)
        tencent_yaml_file_list = self.get_yaml_file_list(self.TENCENT)
        identical_yaml_file_list = self.contrast_file_list(ee_yaml_file_list, tencent_yaml_file_list)
        # 对比相同的文件：
        for yaml_file_name in identical_yaml_file_list:
            # 对比文件MD5
            ee_yaml_file_path = self._build_file_path(yaml_file_name, FileType.YAML, self.EE)
            tencent_yaml_file_path = self._build_file_path(yaml_file_name, FileType.YAML, self.TENCENT)
            # 如果两边MD5 值不一样，代表文件内容有可能不一致, 进入详细内容比较环节：
            if compute_file_md5(ee_yaml_file_path) != compute_file_md5(tencent_yaml_file_path):
                # 加载配置
                ee_yaml_configs = self.load_yaml_data(ee_yaml_file_path)
                tencent_yaml_configs = self.load_yaml_data(tencent_yaml_file_path)
                sys.stdout.write(
                    "[YamlContrastHandler execute_contrast] Start contrasting YAML configs, yaml_file_name={}\n".format(
                        yaml_file_name
                    )
                )
                # 解析配置
                self.parse_yaml_configs(ee_yaml_configs, tencent_yaml_configs)
            else:
                sys.stdout.write(
                    "[YamlContrastHandler execute_contrast] File MD5 is the same, no comparison is "
                    "required, file_name={}\n".format(yaml_file_name)
                )

    def get_yaml_file_list(self, env):
        """
        获取yaml 配置文件的文件列表
        :param env: 环境 EE or Tencent
        :return: list[filename, filename]
        """
        # 获取yaml 配置文件路径
        yaml_config_path = self._build_file_path(None, FileType.YAML, env)
        # 如果路径不存在，打印日志，返回空列表
        if not os.path.exists(yaml_config_path):
            sys.stdout.write(f"[YamlContrastHandler get_yaml_file_list] Warning {yaml_config_path} is not exists !")
            return []
        return os.listdir(yaml_config_path)

    def load_yaml_data(self, yaml_file_path):
        """
        加载yaml配置文件
        :param yaml_file_path: yaml配置文件路径，
        :return: 由yaml解析出来的字典，以object配置为例
        {
            'objects': [
                {
                    'has_object': False,
                    'object_name_en': 'DataSystem',
                    'scope_name_key': '*',
                    'object_name': u'\\u84dd\\u9cb8\\u6570\\u636e\\u5e73\\u53f0',
                    'scope_id_key': '*',
                    'user_mode': False,
                    'to_iam': False,
                    'object_class': 'bkdata'
                }]
        }
        """
        yaml_config_content = yaml.load(self.load_data(yaml_file_path))
        return yaml_config_content

    def parse_yaml_configs(self, ee_yaml_configs, tencent_yaml_configs):
        """
        解析yaml的配置，
        :param ee_yaml_configs, tencent_yaml_configs: yaml配置：
                {
                    'action_relations':[
                        {'parent': 'raw_data.etl',
                        'child': 'raw_data.etl_retrieve'}],
                    'actions': [
                        {
                            'user_mode': False,
                            'can_be_applied': False,
                            'action_name': u'\\u5e73\\u53f0\\u7ba1\\u7406',
                            'action_name_en': 'System Manage',
                            'has_instance': False,
                            'to_iam': False,
                            'object_class':
                            'bkdata',
                            'order': 1,
                            'action_id':
                            'bkdata.admin'}]
                }
                or
                 {
                    'objects': [
                        {
                            'has_object': False,
                            'object_name_en': 'DataSystem',
                            'scope_name_key': '*',
                            'object_name': u'\\u84dd\\u9cb8\\u6570\\u636e\\u5e73\\u53f0',
                            'scope_id_key': '*',
                            'user_mode': False,
                            'to_iam': False,
                            'object_class': 'bkdata'
                        }]
                }
                or
                {
                    'roles': [
                        {
                            'role_name_en': 'SystemAdministrator',
                            'user_mode': True,
                            'description': None,
                            'allow_empty_member': True,
                            'description_en': None,
                            'role_id': 'bkdata.superuser',
                            'role_name': u'\\u5e73\\u53f0\\u8d85\\u7ea7\\u7ba1\\u7406\\u5458',
                            'to_iam': False,
                            'object_class': 'bkdata',
                            'order': 1}],
                    'policies': [
                        {
                            'role_id': 'bkdata.superuser',
                            'object_class': '*',
                            'action_id': '*'}]
                }
        """
        # 如果两个环境内某个yaml所包含的配置类型相同：比如都为["actions","action_relations"]
        if sorted(ee_yaml_configs.keys()) == sorted(tencent_yaml_configs.keys()):
            identical_config_type_list = list(ee_yaml_configs.keys())
        else:
            add_config_type_list, reduced_config_type_list, identical_config_type_list = self.contrast_list(
                ee_yaml_configs, tencent_yaml_configs
            )
            # 输出新增的和减少的文件
            sys.stdout.write(
                "[YamlContrastHandler parse_yaml_configs]  The ee environment has a few more configs than "
                "tencent ! add_config_type_list={}\n".format(add_config_type_list)
            )
            sys.stdout.write(
                "[YamlContrastHandler parse_yaml_configs]  The ee environment has a slightly smaller configs than "
                "tencent ! reduced_file_list={}\n".format(reduced_config_type_list)
            )

        # 遍历两个环境中该yaml中相同的配置项例如：["actions"]
        for yaml_config_type in identical_config_type_list:
            sys.stdout.write(f"[YamlContrastHandler parse_yaml_configs] Start contrasting {yaml_config_type}..\n")
            # 获取到该类型的key值
            key = self.yaml_config_type_maps[yaml_config_type]
            # 进行详细配置的对比
            self.contrast_yaml_configs(key, ee_yaml_configs[yaml_config_type], tencent_yaml_configs[yaml_config_type])

    def contrast_yaml_configs(self, key, ee_yaml_config_content, tencent_yaml_config_content):
        """
        详细比较yaml文件配置
        :param key: 要作为key的字典的某个字段
        :param ee_yaml_config_content: 接受以下五种类型的配置：
                                    action_relations：[
                                        {'parent': 'raw_data.etl', 'child': 'raw_data.etl_retrieve'}]
                                    or
                                    actions：[
                                        {
                                            'user_mode': False,
                                            'can_be_applied': False,
                                            'action_name': u'\\u5e73\\u53f0\\u7ba1\\u7406',
                                            'action_name_en': 'System Manage',
                                            'has_instance': False,
                                            'to_iam': False,
                                            'object_class': 'bkdata',
                                            'order': 1,
                                            'action_id':
                                            'bkdata.admin'
                                        }]
                                    or
                                    'roles': [
                                        {
                                            'role_name_en': 'SystemAdministrator',
                                            'user_mode': True,
                                            'description': None,
                                            'allow_empty_member': True,
                                            'description_en': None,
                                            'role_id': 'bkdata.superuser',
                                            'role_name': u'\\u5e73\\u53f0\\u8d85\\u7ea7\\u7ba1\\u7406\\u5458',
                                            'to_iam': False,
                                            'object_class': 'bkdata',
                                            'order': 1
                                        }]
                                    or
                                     'objects': [
                                        {
                                            'has_object': False,
                                            'object_name_en': 'DataSystem',
                                            'scope_name_key': '*',
                                            'object_name': u'\\u84dd\\u9cb8\\u6570\\u636e\\u5e73\\u53f0',
                                            'scope_id_key': '*',
                                            'user_mode': False,
                                            'to_iam': False,
                                            'object_class': 'bkdata'
                                        }]
                                    or
                                    'policies': [
                                        {
                                            'role_id': 'bkdata.superuser',
                                            'object_class': '*',
                                            'action_id': '*'
                                        }]
         :param ee_yaml_config_content 同 ee_yaml_config_content
        """
        # 生成字典，例如：{role_id：role_config}
        ee_configs_dict = {}
        for ee_config in ee_yaml_config_content:
            ee_configs_dict.setdefault(ee_config[key], []).append(ee_config)

        tencent_configs_dict = {}
        for tencent_config in tencent_yaml_config_content:
            tencent_configs_dict.setdefault(tencent_config[key], []).append(tencent_config)

        # 判断两个环境下某个yaml配置项数量是否有差异，例：actions 中的 action的数量
        if sorted(ee_configs_dict.keys()) == sorted(tencent_configs_dict.keys()):
            sys.stdout.write(f"ee and tencent have the same number of configurations, key={key}\n")
            identical_configs = list(ee_configs_dict.keys())
        else:
            add_configs, reduced_configs, identical_configs = self.contrast_list(
                list(ee_configs_dict.keys()), list(tencent_configs_dict.keys())
            )

            sys.stdout.write(
                "[YamlContrastHandler contrast_yaml_action_configs] The ee environment has a few more "
                "configs than tencent ! add_configs={}\n".format(add_configs)
            )
            sys.stdout.write(
                "[YamlContrastHandler contrast_yaml_action_configs] The ee environment has a slightly "
                "smaller actions than tencent ! "
                "reduced_configs={}\n".format(reduced_configs)
            )

        # 逐个对比每一项的差异, 遍历两个环境共有的配置
        for config in identical_configs:
            ee_config = ee_configs_dict[config]
            tencent_config = tencent_configs_dict[config]
            # 如果两个环境相同配置的md5不同，则证明配置是有差异的
            if self.compute_list_md5(ee_config) != self.compute_list_md5(tencent_config):
                sys.stdout.write("[YamlContrastHandler contrast_yaml_action_configs] Finding differences! \n")
                # 控制台输出配置
                self.output_different(ee_config, tencent_config)


class JsonContrastHandler(BaseContrastHandler):
    def execute_contrast(self):
        """
        执行iam 配置文件对比
        """
        sys.stdout.write("[JsonContrastHandler execute_contrast] Start contrasting Json files..\n")
        ee_json_file_list = self.get_json_file_list(self.EE)
        tencent_json_file_list = self.get_json_file_list(self.TENCENT)
        # 对比文件数量是否有差异，并获取两个环境共同有的文件
        identical_json_file_list = self.contrast_file_list(ee_json_file_list, tencent_json_file_list)
        for json_file_name in identical_json_file_list:
            # 对比文件MD5
            ee_json_file_path = self._build_file_path(json_file_name, FileType.JSON, self.EE)
            tencent_json_file_path = self._build_file_path(json_file_name, FileType.JSON, self.TENCENT)
            # 对比相同文件不同环境下的md5
            if compute_file_md5(ee_json_file_path) != compute_file_md5(tencent_json_file_path):
                sys.stdout.write(f"Start contrasting Json files, file_name={json_file_name}\n")
                ee_json_configs = self.load_json_data(ee_json_file_path)
                tencent_json_configs = self.load_json_data(tencent_json_file_path)
                self.parse_json_configs(ee_json_configs, tencent_json_configs)
            else:
                sys.stdout.write(
                    "[JsonContrastHandler execute_contrast] File MD5 is the same, no comparison is "
                    "required, file_name={}\n".format(json_file_name)
                )

    def parse_json_configs(self, ee_json_configs, tencent_json_configs):
        """
        解析配置文件内容
        :param ee_json_configs, tencent_json_configs:
                            # action 相关
                            {u'operations':
                                [{
                                    u'operation': u'upsert_action',
                                     u'data': {
                                        u'related_actions': [],
                                        u'description': u'\\u666e\\u901a\\u573a\\u666f\\u63a5\\u5165',
                                        u'description_en': u'Business Common Access',
                                        u'related_resource_types': [],
                                        u'name_en': u'Business Common Access',
                                        u'type': u'manage',
                                        u'id': u'biz-common_access',
                                        u'name': u'\\u666e\\u901a\\u573a\\u666f\\u63a5\\u5165'}
                                        }]
                                }]
                                u'system_id': u'bk_data'
                            }
                        # system 相关：
                        {
                            "system_id": "bk_data",
                            "operations": [
                                {
                                    "operation": "upsert_system",
                                    "data": {
                                        "id": "bk_data",
                                        "name": "数据平台",
                                        "name_en": "data",
                                        "description": "简化业务数据的获取，提升数据开发的效率，提供一站式、低门槛的大数据服务",
                                        "description_en": "",
                                        "clients": "bk_dataweb,bk_iam_app,bk_bkdata,data",
                                        "provider_config": {
                                            "host": "http://xxxxx",
                                            "auth": "basic",
                                            "healthz": "/healthz"
                                        }
                                    }
                                }
                            ]
                    }
                    group 相关：
                    {
                        "system_id": "bk_data",
                        "operations": [
                            {
                                "operation": "upsert_action_groups",
                                "data": [
                                    {
                                        "name": "数据集成",
                                        "name_en": "DataHub",
                                        "actions": [
                                            {
                                                "id": "raw_data-create"
                                            },
                                        ]
                                    }]
                            }]
                    }
                    instance_selection 相关：
                    {
                        "system_id": "bk_data",
                        "operations": [
                            {
                                "operation": "upsert_instance_selection",
                                "data": {
                                    "id": "biz",
                                    "name": "业务",
                                    "name_en": "BusinessList",
                                    "resource_type_chain": [
                                        {
                                            "system_id": "bk_data",
                                            "id": "biz"
                                        }
                                    ]
                                }
                            }]
                    }
        """
        ee_operations = ee_json_configs["operations"]
        tencent_operations = tencent_json_configs["operations"]
        self.contrast_json_configs(ee_operations, tencent_operations)

    def contrast_json_configs(self, ee_operations, tencent_operations):
        """
        :param ee_operations: 操作列表：list[operation, operation, operation]
                例：action
                          [
                            {
                                    u'operation': u'upsert_action',
                                     u'data': {
                                        u'related_actions': [],
                                        u'description': u'\\u666e\\u901a\\u573a\\u666f\\u63a5\\u5165',
                                        u'description_en': u'Business Common Access',
                                        u'related_resource_types': [],
                                        u'name_en': u'Business Common Access',
                                        u'type': u'manage',
                                        u'id': u'biz-common_access',
                                        u'name': u'\\u666e\\u901a\\u573a\\u666f\\u63a5\\u5165'}
                                        }]
                                }
                            ]
                    group 操作组：
                         [
                            {
                                "operation": "upsert_action_groups",
                                "data": [
                                    {
                                        "name": "数据集成",
                                        "name_en": "DataHub",
                                        "actions": [
                                            {
                                                "id": "raw_data-create"
                                            },
                                        ]
                                    }]
                            }
                        ]

        """
        ee_operations_dict = {}
        for ee_operation in ee_operations:
            ee_operation_data = ee_operation["data"]
            if isinstance(ee_operation_data, dict):
                ee_operations_dict.setdefault(ee_operation_data["id"], []).append(ee_operation)
            # 处理 操作组 的特殊情况 ee_operation["data"] 为列表
            elif isinstance(ee_operation_data, list):
                for d in ee_operation_data:
                    ee_operations_dict.setdefault(d["name_en"], d)

        tencent_operations_dict = {}
        for tencent_operation in tencent_operations:
            tencent_operation_data = tencent_operation["data"]
            if isinstance(tencent_operation_data, dict):
                # 使用列表存储，防止出现key重复的情况
                tencent_operations_dict.setdefault(tencent_operation_data["id"], []).append(tencent_operation)
            # 处理 操作组 的特殊情况
            elif isinstance(tencent_operation_data, list):
                for d in tencent_operation_data:
                    tencent_operations_dict.setdefault(d["name_en"], d)

        if sorted(ee_operations_dict.keys()) == sorted(tencent_operations_dict.keys()):
            sys.stdout.write(
                "[YamlContrastHandler contrast_yaml_action_configs] EE and Tencent have the same number "
                "of configurations\n"
            )
            identical_configs = list(ee_operations_dict.keys())
        else:
            added_configs, reduced_configs, identical_configs = self.contrast_list(
                list(ee_operations_dict.keys()), list(tencent_operations_dict.keys())
            )
            sys.stdout.write(
                "[JsonContrastHandler contrast_json_configs] The ee environment has a few more "
                "configs than tencent ! added_configs={}\n".format(added_configs)
            )
            sys.stdout.write(
                "[JsonContrastHandler contrast_json_configs] The ee environment has a slightly "
                "smaller actions than tencent ! "
                "reduced_configs={}\n".format(reduced_configs)
            )

        for config in identical_configs:
            ee_config = ee_operations_dict[config]
            tencent_config = tencent_operations_dict[config]
            # 计算单个配置项的md5
            if self.compute_list_md5(ee_config) != self.compute_list_md5(tencent_config):
                sys.stdout.write("[JsonContrastHandler contrast_json_configs] Finding differences! \n")
                self.output_different(ee_config, tencent_config)

    def get_json_file_list(self, env):
        """
        获取json 文件列表
        :param env: 环境： ee or tencent
        :return:
        """
        yaml_config_path = self._build_file_path(None, FileType.JSON, env)
        # 如果路径不存在，打印日志，返回空列表
        if not os.path.exists(yaml_config_path):
            sys.stdout.write(f"[JsonContrastHandler get_json_file_list] Warning {yaml_config_path} is not exists !")
            return []
        return os.listdir(yaml_config_path)

    def load_json_data(self, file_path):
        """
        加载json 相关的配置文件
        :param file_path: 文件路径
        :return: dict
        """
        return json.loads(self.load_data(file_path))
