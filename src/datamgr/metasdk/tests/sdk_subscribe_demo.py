# coding=utf-8
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

import json
import sys
import time

from metadata_client import DEFAULT_SETTINGS, MetadataClient


def metadata_client(client_settings):
    return MetadataClient(client_settings)


def show_body(body, message):
    """
    回调函数，函数名自定义，参数签名必须是 function(body, message) or class_method(object, body, message)

    :param body: string json字典格式消息体
    :param message: object 消息对象实例，当消费消息成功时，执行message.ack()发送回执, 订阅进度前进，否则会阻塞消费
    :return: None
    """
    try:
        print(body)
    except Exception as e:
        print("error in show is {}".format(e))
    finally:
        message.ack()


def test_event_subscribe(subscribe_client):

    """
    订阅配置：name/key/refer 组合唯一确定一个条订阅通道, 该通道订阅进度和其他通道互不干扰
            订阅内容仅由 key 决定
    """
    subscribe_config = dict(
        name="your task name",  # 订阅任务名称 (只允许数字、大小写字母以及下划线)
        key="datamonitor",  # 固定 `datamonitor` 数据监控事件
        refer="datamanager",  # 订阅方模块名
    )
    subscriber = subscribe_client.event_subscriber(subscribe_config)
    subscriber.set_callback(callbacks=show_body)
    while True:
        try:
            subscriber.start_to_listening()
        except KeyboardInterrupt:
            print("KeyboardInterrupt")
            sys.exit(0)
        except Exception as e:
            print("scan_event_failed: {}".format(e))
            raise


"""
初始化SDK，必填项(取值需要引用support-files中的同名数值)
    META_API_HOST metaapi地址host
    META_API_PORT metaapi地址端口
    CRYPT_INSTANCE_KEY 加密秘钥
"""
settings = DEFAULT_SETTINGS.copy()
settings.update(dict(META_API_HOST="localhost", META_API_PORT="80", CRYPT_INSTANCE_KEY="test"))

client = metadata_client(settings)
test_event_subscribe(client)
