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

package com.tencent.bk.base.datahub.databus.connect.common.api;

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.ConnectorFactory;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is only a temporary extension to Kafka Connect runtime until there is an Embedded API as
 * per KIP-26
 */

public class ConnectEmbedded {

    private static final Logger log = LoggerFactory.getLogger(ConnectEmbedded.class);

    private static final int REQUEST_TIMEOUT_MS = 120000;

    private final Worker worker;

    private final DistributedHerder herder;

    private final CountDownLatch startLatch = new CountDownLatch(1);

    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final CountDownLatch stopLatch = new CountDownLatch(1);

    private final ShutdownHook shutdownHook;

    private final Properties[] connectorConfigs;

    public ConnectEmbedded(Properties workerConfig, Properties... connectorConfigs) throws Exception {
        Time time = new SystemTime();
        DistributedConfig config = new DistributedConfig(Utils.propsToStringMap(workerConfig));

        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
        offsetBackingStore.configure(config);

        //not sure if this is going to work but because we don't have advertised url we can get at
        //least a fairly random
        String workerId = UUID.randomUUID().toString();
        ConnectorFactory connectorFactory = new ConnectorFactory();
        worker = new Worker(workerId, time, connectorFactory, config, offsetBackingStore);

        StatusBackingStore statusBackingStore
                = new KafkaStatusBackingStore(time, worker.getInternalValueConverter());
        statusBackingStore.configure(config);

        ConfigBackingStore configBackingStore
                = new KafkaConfigBackingStore(worker.getInternalValueConverter(), config);

        //advertisedUrl = "" as we don't have the rest server - hopefully this will not break anything
        herder = new DistributedHerder(config, time, worker, statusBackingStore, configBackingStore, "");
        this.connectorConfigs = connectorConfigs;

        shutdownHook = new ShutdownHook();
    }

    /**
     * 启动
     */
    public void start() {
        try {
            LogUtils.info(log, "Kafka ConnectEmbedded starting");
            Runtime.getRuntime().addShutdownHook(shutdownHook);

            worker.start();
            herder.start();

            LogUtils.info(log, "Kafka ConnectEmbedded started");

            for (Properties connectorConfig : connectorConfigs) {
                FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
                String name = connectorConfig.getProperty(ConnectorConfig.NAME_CONFIG);
                herder.putConnectorConfig(name, Utils.propsToStringMap(connectorConfig), true, cb);
                cb.get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }

        } catch (InterruptedException e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONNECTOR_FRAMEWORK_ERR, log, "Starting interrupted ", e);
        } catch (ExecutionException e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONNECTOR_FRAMEWORK_ERR, log,
                    "Submitting connector config failed", e.getCause());
        } catch (TimeoutException e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONNECTOR_FRAMEWORK_ERR, log,
                    "Submitting connector config timed out", e);
        } finally {
            startLatch.countDown();
        }
    }

    /**
     * 停止回调
     */
    public void stop() {
        try {
            boolean wasShuttingDown = shutdown.getAndSet(true);
            if (!wasShuttingDown) {

                LogUtils.info(log, "Kafka ConnectEmbedded stopping");
                herder.stop();
                worker.stop();

                LogUtils.info(log, "Kafka ConnectEmbedded stopped");
            }
        } finally {
            stopLatch.countDown();
        }
    }

    /**
     * 等待停止结束
     */
    public void awaitStop() {
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONNECTOR_FRAMEWORK_ERR, log,
                    "Interrupted waiting for Kafka Connect to shutdown");
        }
    }

    private class ShutdownHook extends Thread {

        @Override
        public void run() {
            try {
                startLatch.await();
                ConnectEmbedded.this.stop();
            } catch (InterruptedException e) {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONNECTOR_FRAMEWORK_ERR, log,
                        "Interrupted in shutdown hook while waiting for Kafka Connect startup to finish");
            }
        }
    }
}
