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

import numpy as np
import tensorflow as tf

INPUT_SHAPE = (28, 28, 1)
NUM_CLASSES = 10


def transform(dataset_dict, model_dict, args):
    # 获取模型
    model = model_dict[args["input_model"]]
    # 获取数据
    train_dataset = dataset_dict[args["input_table"]]
    # 进行操作
    batch_dataset = train_dataset.batch(2 * 2)
    result = model.predict_classes(batch_dataset)
    result_dataset = tf.data.Dataset.from_tensor_slices(result)
    final_dataset = tf.data.Dataset.zip((train_dataset, result_dataset))

    # 返回结果
    dataset_dict[args["output_table"]] = final_dataset
    return {args["output_table"]: final_dataset}, {}


def get_model(args):
    pass


def data_refractor(record):
    """
    data_record:single line of result_set, dict
    """
    # 自定义逻辑
    feature_list = []
    label_list = []
    data_feature_colums = [
        "feature_1",
        "feature_2",
        "feature_3",
        "feature_4",
        "feature_5",
        "feature_6",
        "feature_7",
        "feature_8",
        "feature_9",
        "feature_10",
        "feature_11",
        "feature_12",
        "feature_13",
    ]
    for field_name in record:
        if field_name == "medv":
            label_list.append(record[field_name])
        elif field_name in data_feature_colums:
            feature_list.append(record[field_name])
    # 使用numpy等对数据进行处理
    feature_array = np.array(feature_list)
    label_array = np.array(label_list)
    image = feature_array.astype(np.float32)
    image = np.reshape(image, (13,))
    label = label_array.astype(np.float32)
    label = np.reshape(label, (1,))
    # 返回格式确定
    return image
