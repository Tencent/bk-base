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

import pulsar

data = "hello"
serviceUrl = "pulsar://127.0.0.1:6650"
TM = 10000  # Do not wait forever in tests
topic = "testtopic2"


def produce_messages(url, number):
    pulsar_topic = "persistent://public/data/%s" % topic
    client = pulsar.Client(url)
    producer = client.create_producer(pulsar_topic)

    for i in range(number):
        print("send NO.%d" % i)
        producer.send(data)

    # Send one sync message to make sure everything was published
    producer.close()
    client.close()


def print_str(s):
    print("#######################")
    print(list(s))
    print(len(list(s)))

    for b in list(s):
        print(ord(b))


def consume(serviceUrl, topic, number):
    topic = "persistent://public/data/%s" % topic
    client = pulsar.Client(serviceUrl)
    reader1 = client.create_reader(topic, pulsar.MessageId.earliest)

    last_msg_id = pulsar.MessageId.earliest
    s = last_msg_id.serialize()  # str
    print(last_msg_id.serialize())
    print_str(s)

    for i in range(number):
        msg = reader1.read_next(TM)
        last_msg_id = msg.message_id()
        print("Received message '{}' id='{}'".format(msg.data(), last_msg_id))

        s = last_msg_id.serialize()  # str
        print_str(s)

    reader1.close()
    client.close()


if __name__ == "__main__":
    produce_messages(serviceUrl, 1)
    # consume(serviceUrl, 'tt1', 10)
