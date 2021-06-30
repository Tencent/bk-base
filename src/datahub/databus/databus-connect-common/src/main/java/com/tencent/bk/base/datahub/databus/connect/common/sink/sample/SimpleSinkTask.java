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

package com.tencent.bk.base.datahub.databus.connect.common.sink.sample;

import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkTask;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.convert.Converter;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;


public class SimpleSinkTask extends BkSinkTask {

    private static final Logger log = LoggerFactory.getLogger(SimpleSinkTask.class);

    private SimpleSinkConfig config;
    private Converter converter = null;
    private String[] keysInOrder;

    /**
     * 启动数据处理任务，初始化必须的资源
     */
    @Override
    public void startTask() {
        // 自身逻辑
        config = new SimpleSinkConfig(configProperties);

        Map<String, String> columns = ctx.getRtColumns();
        keysInOrder = new String[columns.size()];
        int i = 0;
        for (String key : columns.keySet()) {
            keysInOrder[i] = key;
            i++;
        }
    }

    /**
     * 处理kafka中的消息，并统计处理结果
     *
     * @param records kafka记录
     */
    @Override
    public void processData(Collection<SinkRecord> records) {
        // 把数据从kafka中消费，并打印在日志中
        for (SinkRecord record : records) {
            // 根据所需数据类型获取结果集
            ConvertResult result = converter.getJsonList(record.key().toString(), record.value().toString());
            LogUtils.info(log, "{} - {} - {}", record.kafkaPartition(), record.kafkaOffset(), record.key());
            LogUtils.info(log, "{}", result);
            msgCount++;
            msgSize += result.getMsgSize();
        }
        // 上报埋点数据（成功条数，失败条数，msg大小等信息）
        LogUtils.info(log, "processed {} msgs with {} bytes for {}", msgCount, msgSize, rtId);
        msgCountTotal += msgCount;
        msgSizeTotal += msgSize;
    }
}
