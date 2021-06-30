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

from dataflow.modeling.model.platform_model import PlatformModel
from dataflow.modeling.settings import MLComponentType
from dataflow.shared.log import modeling_logger as logger


class AlgorithmController(object):
    def __init__(self):
        self.model = PlatformModel()

    def get_algorithm_list(self, framework):
        """
        根据用户指定的framework获取此框架下所有的算法
        @param framework: 框架
        @return: 适应前端展现，结果格式如下
        {
            "spark_mllib": [
                {
                    "alg_info":[
                        {
                           "inputs":["param1(参数1)", "param2(参数2)"],
                           "outputs":["param1(参数1)", "param2(参数2)"],
                           "args":["param1(参数1)", "param2(参数2)"],
                           "description":"描述",
                           "name":"random_forest(随机森林)",
                           "sql": "train model xxx"
                        }
                    ],
                    "alg_group": "特征转换"
                }
            ]
        }
        """
        algorithm_result = self.model.get_algrithm_by_user(framework=framework)
        result = {}
        for algorithm in algorithm_result:
            algorithm_name = algorithm.algorithm_name
            algorithm_type = algorithm.algorithm_type
            algorithm_original_name = algorithm.algorithm_original_name
            framework = algorithm.framework
            # todo 返回内容中暂时屏蔽非spark_mllib的框架内容
            if framework != MLComponentType.SPARK_MLLIB.value:
                continue
            if framework not in result:
                result[framework] = []

            if algorithm_type not in PlatformModel.ALGORITHM_TYPE_MAP:
                algorithm_type = "default"
            algorithm_type = PlatformModel.ALGORITHM_TYPE_MAP[algorithm_type]
            # 找到此framework对应的元素
            algorithm_type_group = {}
            for type_item in result[framework]:
                if type_item["alg_group"] == algorithm_type:
                    algorithm_type_group = type_item

            if not algorithm_type_group:
                algorithm_type_group["alg_group"] = algorithm_type
                algorithm_type_group["alg_info"] = []
                result[framework].append(algorithm_type_group)

            # 填充alg_info内容
            try:
                algorithm_version = self.model.get_algorithm_by_name(algorithm_name, version=1)
                algorithm_item = {
                    "name": "{name}({alias})".format(name=algorithm_original_name, alias=algorithm.algorithm_alias),
                    "description": algorithm.description,
                }
                algorithm_config = json.loads(algorithm_version.config)
                label_columns = []
                predict_output = []
                training_args = []
                feature_columns_mapping = {}
                feature_columns = []
                if "label_columns" in algorithm_config:
                    label_columns = algorithm_config["label_columns"]
                if "predict_output" in algorithm_config:
                    predict_output = algorithm_config["predict_output"]
                if "training_args" in algorithm_config:
                    training_args = algorithm_config["training_args"]
                input_params = []
                input_params.extend(label_columns)
                if "feature_columns_mapping" in algorithm_config:
                    feature_columns_mapping = algorithm_config["feature_columns_mapping"]
                    if feature_columns_mapping:
                        input_params.extend(feature_columns_mapping["spark"]["multi"])
                        input_params.extend(feature_columns_mapping["spark"]["single"])
                if "feature_columns" in algorithm_config:
                    feature_columns = algorithm_config["feature_columns"]
                algorithm_item.update(
                    {
                        "inputs": [
                            "{name}({alias})".format(
                                name=param_item["field_name"],
                                alias=param_item["field_alias"],
                            )
                            for param_item in input_params
                        ],
                        "outputs": [
                            "{name}({alias})".format(
                                name=param_item["field_name"],
                                alias=param_item["field_alias"],
                            )
                            for param_item in predict_output
                        ],
                        "args": [
                            "{name}({alias})".format(
                                name=param_item["field_name"],
                                alias=param_item["field_alias"],
                            )
                            for param_item in training_args
                        ],
                    }
                )
                sql = self.generate_sample_mlsql(
                    algorithm_original_name,
                    algorithm_type,
                    label_columns=label_columns,
                    train_args=training_args,
                    feature_columns=feature_columns,
                    feature_columns_mapping=feature_columns_mapping,
                )
                algorithm_item["sql"] = sql
                algorithm_type_group["alg_info"].append(algorithm_item)
            except Exception as e:
                logger.exception(e)
                logger.error("Get " + algorithm_name + " detail error:%s" % e)
        return result

    def generate_sample_mlsql(
        self,
        algorithm_name,
        algorithm_type,
        label_columns=[],
        train_args=[],
        feature_columns=[],
        feature_columns_mapping={},
    ):
        """
        @param algorithm_name 算法名称
        @param algorithm_type 算法类型
        @param label_columns:标签列
        @param train_args:算法参数
        @param feature_columns 特征列
        @param feature_columns_mapping 特征映射列，用于存储spark_mllib框架下的复杂参数
        @return: 样例MLSQL语句
        """
        input_param_list = []
        if feature_columns_mapping:
            input_param_list.extend(feature_columns_mapping["spark"]["multi"])
            input_param_list.extend(feature_columns_mapping["spark"]["single"])
        if not input_param_list:
            input_param_list.extend(feature_columns)
        # 所有参数中  各取一个，然后拼接为样例SQL
        input_param_str = ""
        label_param_str = ""
        other_param_str = ""
        param_list_str = ""
        if input_param_list:
            input_param = input_param_list[0]
            if "composite_type" in input_param:
                # 为向量或数组
                if input_param["composite_type"] == "Vector":
                    input_param_str = "{name}={value}".format(name=input_param["real_name"], value="<字段1,字段2,字段3>")
                else:
                    input_param_str = "{name}={value}".format(name=input_param["real_name"], value="[字段1,字段2,字段3]")
            else:
                # 单独向量
                input_param_str = "{name}={value}".format(name=input_param["real_name"], value="字段名")
            param_list_str = input_param_str + ", "

        if label_columns:
            label_param = label_columns[0]
            label_param_str = "{name}={value}".format(name=label_param["real_name"], value="字段名")
            param_list_str = param_list_str + label_param_str + ", "

        if train_args:
            train_param = train_args[0]
            if train_param["real_name"] == "weight_col":
                # 权重列作为需要输入特征的列，并没有放入到feature与label中，而是作为普通参数存在的，因此需要特殊处理
                param_value = "字段名"
            else:
                default_value = train_param["default_value"]
                param_type = train_param["field_type"]
                param_value = None
                if default_value:
                    param_value = default_value
                else:
                    # 没有默认值，则根据类型指定
                    if param_type == "string":
                        param_value = "a"
                    elif param_type == "boolean":
                        param_value = "true"
                    elif param_type == "int":
                        param_value = 1
                    elif param_type == "double":
                        param_value = 0.1
                    elif param_type == "long":
                        param_value = 1
                    else:
                        param_value = 1
                if param_type in ["string", "boolean"]:
                    param_value = "'" + param_value + "'"
            other_param_str = "{name}={value}".format(name=train_param["real_name"], value=param_value)
            param_list_str = param_list_str + other_param_str
        param_list_str = param_list_str.strip().strip(",")

        if algorithm_type == "transformer":
            # 生成create语句
            sql = "create table 输出表 \nas run {algorithm_name}(\n{param_list}) as 目标列名 \nfrom 输入表".format(
                algorithm_name=algorithm_name, param_list=param_list_str
            )
        else:
            # 生成train语句
            sql = "train model 模型名称 \n options(algorithm='{algorithm_name}', {param_list})\n from 查询结果集名称".format(
                algorithm_name=algorithm_name, param_list=param_list_str
            )
        return sql
