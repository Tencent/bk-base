/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.bk.base.datahub.hubmgr.service;

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.errors.BadConfException;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.MgrConsts;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConnectorStatHandler extends OpKafkaDataHandler {

    private static final Logger log = LoggerFactory.getLogger(ConnectorStatHandler.class);

    private long msgCount = 0;
    private long msgSize = 0;

    /**
     * 构造函数，给反射机制使用
     */
    public ConnectorStatHandler() {
    }

    /**
     * 获取服务名称
     *
     * @return 服务名称
     */
    public String getServiceName() {
        return MgrConsts.CONNECTOR_STAT_HANDLER;
    }

    /**
     * 获取日志对象
     *
     * @return 日志对象
     */
    public Logger getLog() {
        return log;
    }

    /**
     * 获取需要消费的topic列表
     *
     * @return topic列表
     */
    public String getTopic() {
        String topic = DatabusProps.getInstance().getOrDefault(MgrConsts.DATABUSMGR_CONNECTOR_STAT_TOPIC,
                MgrConsts.DATABUSMGR_CONNECTOR_STAT_TOPIC_DEFAULT);
        if (StringUtils.isBlank(topic)) {
            LogUtils.error(MgrConsts.ERRCODE_BAD_CONFIG, log, "stat config is empty, unable to start service!");
            throw new BadConfException("databusmgr.connector.stat.topic config is empty, unable to start service!");
        }

        return topic;
    }

    /**
     * 处理kafka数据
     *
     * @param records kafka中的消息
     */
    public void processData(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            // 解析kafka的key/value，处理数据
            if (record.value().startsWith("{")) {
                Map<String, Object> map = JsonUtils.toMapWithoutException(record.value());
                if (map.get("metrics") instanceof Map) {
                    Map<String, Object> metrics = (Map<String, Object>) map.get("metrics");
                    if (metrics.containsKey("custom_metrics")) {
                        LogUtils.info(log, "got custom metrics {}", metrics.get("custom_metrics"));
                    }
                }
            } else if (record.value().startsWith("[")) {
                List<Object> list = JsonUtils.toListWithoutException(record.value());
                for (Object obj : list) {
                    if (obj instanceof Map) {
                        Map<String, Object> map = (Map<String, Object>) obj;
                        if (map.get("metrics") instanceof Map) {
                            Map<String, Object> metrics = (Map<String, Object>) map.get("metrics");
                            if (metrics.containsKey("custom_metrics") && metrics.get("custom_metrics") != null) {
                                LogUtils.info(log, "got custom metrics {}", metrics.get("custom_metrics"));
                            }
                        }
                    } else {
                        LogUtils.warn(log, "list entry is not a map object {}", obj);
                    }
                }
            } else {
                LogUtils.warn(log, "not a valid json data: {}", record.value());
            }

            int keyLen = record.key() == null ? 0 : record.key().length();
            int valLen = record.value() == null ? 0 : record.value().length();
            msgSize += keyLen + valLen;
            msgCount++;
            if (msgCount % 1000 == 0) {
                LogUtils.info(log, "processed {} connector stat msg with length {}", msgCount, msgSize);
            }
        }
    }


}
