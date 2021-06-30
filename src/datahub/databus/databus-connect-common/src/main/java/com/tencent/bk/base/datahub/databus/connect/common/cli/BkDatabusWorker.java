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

package com.tencent.bk.base.datahub.databus.connect.common.cli;


import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorFactory;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;


public class BkDatabusWorker {

    private static final Logger log = LoggerFactory.getLogger(BkDatabusWorker.class);

    /**
     * 总线程序入口。启动时需要一个参数(配置文件名称)
     *
     * @param args 参数
     * @throws IOException 异常
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            LogUtils.info(log, "Usage: BkDatabusWorker worker.properties");
            System.exit(1);
        }

        // 获取配置文件中的内容
        String workerPropsFile = args[0];
        Map<String, String> workerProps =
                !workerPropsFile.isEmpty() ? Utils.propsToStringMap(Utils.loadProps(workerPropsFile))
                        : Collections.<String, String>emptyMap();

        // 通过配置文件中的group.id中的信息,获取此cluster对应的源数据所在kafka信息
        // 指定 consumer.bootstrap.servers 等和consumer相关的配置信息
        // 指定config.storage.topic/status.storage.topic/offset.storage.topic等参数
        // 注意 group.id 配置项
        String clusterId = workerProps.get(Consts.CLUSTER_PREFIX + DistributedConfig.GROUP_ID_CONFIG);
        if (StringUtils.isBlank(clusterId)) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR, log,
                    "cluster.group.id is not set in config file {}, unable to start worker", workerPropsFile);
            System.exit(1);
        }

        // 将配置信息传递到BasicProps中,以便其他模块使用
        BasicProps props = BasicProps.getInstance();
        props.addProps(workerProps);

        try {
            props.addProps(HttpUtils.getClusterConfig(clusterId)); // 覆盖文件中的部分配置信息
        } catch (Exception e) {
            LogUtils.warn(log, "failed to get cluster properties from rest api, just use properties file!");
        }
        LogUtils.info(log, "configs: {}", props.toString());

        // 验证配置项中必须要包含集群端口配置项，否则不让启动，程序退出
        if (!props.getClusterProps().containsKey(WorkerConfig.REST_PORT_CONFIG)) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR, log,
                    "cluster.rest.port must set in config file {}, unable to start worker", workerPropsFile);
            System.exit(1);
        }

        Map<String, String> clusterProps = props.getClusterProps();
        clusterProps.putAll(props.getConsumerPropsWithPrefix());
        // 没有配置REST HOST NAME时，使用配置文件中指定的网卡列表，顺序获取IP作为监听的IP地址。
        if (!props.getClusterProps().containsKey(WorkerConfig.REST_HOST_NAME_CONFIG)) {
            clusterProps.put(WorkerConfig.REST_HOST_NAME_CONFIG, com.tencent.bk.base.datahub.databus.commons.utils.Utils
                    .getInnerIp());
        }

        Time time = Time.SYSTEM;
        ConnectorFactory connectorFactory = new ConnectorFactory();
        DistributedConfig config = new DistributedConfig(clusterProps);
        RestServer rest = new RestServer(config);
        URI advertisedUrl = rest.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        // 初始化connector的config、status、offset等信息存储的对象
        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
        offsetBackingStore.configure(config);
        Worker worker = new Worker(workerId, time, connectorFactory, config, offsetBackingStore);
        StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time, worker.getInternalValueConverter());
        statusBackingStore.configure(config);
        ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(worker.getInternalValueConverter(), config);

        // 初始化worker主线程
        DistributedHerder herder = new DistributedHerder(config, time, worker, statusBackingStore, configBackingStore,
                advertisedUrl.toString());
        final Connect connect = new Connect(herder, rest);
        try {
            connect.start();
        } catch (Exception e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONNECTOR_FRAMEWORK_ERR, log, "Failed to start Connect", e);
            connect.stop();
        }

        // Shutdown will be triggered by Ctrl-C or via HTTP shutdown request
        connect.awaitStop();
    }
}
