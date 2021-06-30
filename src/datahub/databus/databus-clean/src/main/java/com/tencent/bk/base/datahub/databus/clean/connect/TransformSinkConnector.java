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

package com.tencent.bk.base.datahub.databus.clean.connect;

import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkConnector;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.TransformUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class TransformSinkConnector extends BkSinkConnector {

    private static final Logger log = LoggerFactory.getLogger(TransformSinkConnector.class);
    private static final String RECOVER_OFFSET_FROM_KEY = "recover.offset.from.key";

    private TransformSinkConfig config;


    /**
     * 启动etl connector
     */
    @Override
    public void startConnector() {
        try {
            config = new TransformSinkConfig(configProperties);
        } catch (ConfigException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR,
                    "bad configuration, unable to start connector!", e);
            throw e;
        }

        // recover offset配置获取
        Map<String, String> clusterProps = BasicProps.getInstance().getClusterProps();
        Map<String, String> consumerProps = BasicProps.getInstance().getConsumerProps();
        if (clusterProps.getOrDefault(RECOVER_OFFSET_FROM_KEY, "false").trim().equalsIgnoreCase("true")) {
            // 获取bootstrap
            String dstBootstrap = config.getString(TransformSinkConfig.PRODUCER_BOOTSTRAP_SERVERS);
            String srcBootstrap = consumerProps.get(Consts.BOOTSTRAP_SERVERS);
            // 当未指定producer的kafka服务地址时，使用默认consumer的kafka服务地址
            if (StringUtils.isBlank(dstBootstrap)) {
                dstBootstrap = srcBootstrap;
            }

            // 获取目标的partition信息
            String rtId = configProperties.get(BkConfig.RT_ID);
            String dstTopic = "table_" + rtId;
            Collection<PartitionInfo> dstPartitions = TransformUtils.getPartitions(dstBootstrap, dstTopic);

            // 获取源的partition信息
            String srcTopic = configProperties.get(Consts.TOPICS);
            Collection<PartitionInfo> srcPartitions = TransformUtils.getPartitions(srcBootstrap, srcTopic);

            // check source and sink partitions
            if (srcPartitions.size() != dstPartitions.size()) {
                String cluster = configProperties.get(BkConfig.GROUP_ID);
                String msg = String
                        .format("Attention partition not equal %d!=%d, cluster=%s, srcTopic=%s, srcBootstrap=%s, "
                                        + "dstTopic=%s, dstBootstrap=%s",
                                srcPartitions.size(), dstPartitions.size(), cluster, srcTopic, srcBootstrap, dstTopic,
                                dstBootstrap);
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR, log, msg);
                throw new ConfigException(msg + ", unable to start the task!!");
            }
            LogUtils.info(log, "partition check done, src topic: {}, partitions: {}, dst topic:{}, partitions: {}",
                    srcTopic, srcPartitions.size(), dstTopic, dstPartitions.size());
        } else {
            LogUtils.info(log, "RECOVER_OFFSET_FROM_KEY set false");
        }

    }

    /**
     * 停止 connector
     *
     * @throws ConnectException 异常
     */
    @Override
    public Class<? extends Task> taskClass() {
        return TransformSinkTask.class;
    }

    /**
     * 获取connector的配置定义
     *
     * @return connector的配置定义
     */
    @Override
    public ConfigDef config() {
        return TransformSinkConfig.config;
    }
}
