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
from bkbase.dataflow.one_model import STRATEGY
from tensorflow.keras import layers

INPUT_SHAPE = (28, 28, 1)
NUM_CLASSES = 10


def transform(dataset_dict, model_dict, args):
    # 获取模型
    model, optimizer, accuracy = get_model(args)
    dataset = dataset_dict[args["input_table"]]
    dataset_iterator = dataset.as_numpy_iterator()

    # 受限于目前实现，需要再次遍历dataset
    def input_fn(_):
        def rdd_generator():
            for element in dataset_iterator:
                yield element

        new_dataset = tf.data.Dataset.from_generator(rdd_generator, (tf.float32), (tf.TensorShape([13])))
        return new_dataset

    coordinator = tf.distribute.experimental.coordinator.ClusterCoordinator(strategy=STRATEGY)

    @tf.function
    def step_fn(iterator):
        def replica_fn(batch_data, labels):
            with tf.GradientTape() as tape:
                pred = model(batch_data, training=True)
                per_example_loss = tf.keras.losses.BinaryCrossentropy(reduction=tf.keras.losses.Reduction.NONE)(
                    labels, pred
                )
                loss = tf.nn.compute_average_loss(per_example_loss)
                gradients = tape.gradient(loss, model.trainable_variables)

            optimizer.apply_gradients(zip(gradients, model.trainable_variables))

            actual_pred = tf.cast(tf.greater(pred, 0.5), tf.int64)
            accuracy.update_state(labels, actual_pred)
            return loss

        batch_data, labels = next(iterator, (None, None))
        losses = STRATEGY.run(replica_fn, args=(batch_data, labels))
        return STRATEGY.reduce(tf.distribute.ReduceOp.SUM, losses, axis=None)

    @tf.function
    def per_worker_dataset_fn():
        return STRATEGY.distribute_datasets_from_function(input_fn)

    per_worker_dataset = coordinator.create_per_worker_dataset(per_worker_dataset_fn)
    per_worker_iterator = iter(per_worker_dataset)

    num_epoches = 4
    steps_per_epoch = 5
    for _ in range(num_epoches):
        accuracy.reset_states()
        for _ in range(steps_per_epoch):
            coordinator.schedule(step_fn, args=(per_worker_iterator,))
        # Wait at epoch boundaries.
        coordinator.join()
    # 返回结果
    model_dict[args["output_model"]] = model
    return {}, {args["output_model"]: model}


def get_model(args):
    """
    如果全新定义模型，此过程必须有，如果是使用已有模型，可以不需要
    """
    # 以下是模型的定义
    with STRATEGY.scope():
        model = tf.keras.Sequential()  # 先建立一个顺序模型
        # 向顺序模型里加入第一个隐藏层，第一层一定要有一个输入数据的大小，需要有input_shape参数
        model.add(layers.Dense(64, activation="relu", input_shape=(13,)))
        model.add(layers.Dense(64, activation="relu"))
        model.add(layers.Dense(1))  # 因为我们是预测房价，不是分类，所以最后一层可以不用激活函数
        model.compile(loss="mse", optimizer="rmsprop")
        optimizer = tf.keras.optimizers.RMSprop(learning_rate=0.1)
        accuracy = tf.keras.metrics.Accuracy()
    return model, optimizer, accuracy


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
    return image, label
