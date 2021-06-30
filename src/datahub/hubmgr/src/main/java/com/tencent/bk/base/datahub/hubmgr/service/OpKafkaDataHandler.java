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
import com.tencent.bk.base.datahub.databus.commons.utils.KafkaUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.DatabusMgr;
import com.tencent.bk.base.datahub.hubmgr.MgrConsts;
import com.tencent.bk.base.datahub.hubmgr.utils.TsdbWriter;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;


public abstract class OpKafkaDataHandler implements Service {

    private Consumer<String, String> consumer = null;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private AtomicBoolean isStop = new AtomicBoolean(false);
    private long timeout = 2000L;

    /**
     * 获取服务名称
     *
     * @return 服务名称
     */
    public abstract String getServiceName();

    /**
     * 获取日志对象
     *
     * @return 日志对象
     */
    public abstract Logger getLog();

    /**
     * 获取需要消费的topic列表
     *
     * @return topic列表
     */
    public abstract String getTopic();

    /**
     * 处理kafka数据
     *
     * @param records kafka中的消息
     */
    public abstract void processData(ConsumerRecords<String, String> records);


    /**
     * 启动服务
     */
    public void start() {
        LogUtils.info(getLog(), "going to start {} ", getServiceName());
        DatabusProps props = DatabusProps.getInstance();
        String addr = props.getOrDefault(MgrConsts.KAFKA_BOOTSTRAP_SERVERS, "");
        if (StringUtils.isBlank(addr)) {
            LogUtils.error(MgrConsts.ERRCODE_BAD_CONFIG, getLog(), "kafka config is empty, unable to start service!");
            throw new BadConfException("kafka.bootstrap.servers config is empty, unable to start service!");
        }
        // 初始化consumer，订阅topic
        consumer = KafkaUtils.initStringConsumer(String.format("%s-%s", MgrConsts.CONNECTOR_MANAGER, getTopic()), addr);
        consumer.subscribe(Arrays.asList(getTopic()));

        // 创建子线程消费并处理数据
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                LogUtils.info(getLog(), "starting poll-handler loop for {}", getTopic());
                while (!isStop.get() && DatabusMgr.isRunning()) {
                    try {
                        ConsumerRecords<String, String> records = consumer.poll(timeout);
                        if (!records.isEmpty()) {
                            processData(records);
                            consumer.commitSync();
                        }
                    } catch (Exception e) {
                        LogUtils.warn(getLog(), "failed to poll-handler data in a loop!", e);
                    }
                }
                LogUtils.info(getLog(), "finish processing msg in {}", getTopic());
            }
        });
    }

    /**
     * 停止服务
     */
    public void stop() {
        LogUtils.info(getLog(), "going to stop {} ", getServiceName());
        // 首先标记状态，然后触发executor的shutdown
        isStop.set(true);
        executorService.shutdown();

        // 等待executor结束执行，并关闭consumer
        try {
            if (executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                if (consumer != null) {
                    consumer.close(5, TimeUnit.SECONDS);
                }
            } else {
                LogUtils.warn(getLog(), "failed to shutdown executor in timeout seconds");
            }
            TsdbWriter.getInstance().flush();
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
