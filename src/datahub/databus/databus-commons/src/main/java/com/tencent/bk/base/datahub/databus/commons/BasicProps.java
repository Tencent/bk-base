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

package com.tencent.bk.base.datahub.databus.commons;


import com.tencent.bk.base.datahub.databus.commons.utils.ConnUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class BasicProps {

    private static final Logger log = LoggerFactory.getLogger(BasicProps.class);

    private Map<String, String> config = new HashMap<>();
    private Map<String, String> clusterProps = new HashMap<>();
    private Map<String, String> consumerProps = new HashMap<>();
    private Map<String, String> producerProps = new HashMap<>();
    private Map<String, String> connectorProps = new HashMap<>();
    private Map<String, String> metricProps = new HashMap<>();
    private Map<String, String> ccCacheProps = new HashMap<>();

    // 构建单例模式
    private BasicProps() {
    }

    private static class Holder {

        private static final BasicProps INSTANCE = new BasicProps();
    }

    public static BasicProps getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * 使用属性列表初始化BasicProps对象.
     * 所有配置项按照用途分类,以不同的prefix来定义:
     * deploy.            -> 部署相关配置
     * cluster.           -> kafka connect worker相关配置项
     * consumer.          -> kafka consumer相关配置,用于sink connector
     * monitor.           -> 数据埋点,数据上报,自身检测等相关配置
     * monitor.producer.  -> 埋点数据上报的kafka配置
     *
     * @param props 配置属性
     */
    public void addProps(Map<String, String> props) {
        config.putAll(props);
        clusterProps = originalsWithPrefix(Consts.CLUSTER_PREFIX);
        consumerProps = originalsWithPrefix(Consts.CONSUMER_PREFIX);
        producerProps = originalsWithPrefix(Consts.PRODUCER_PREFIX);
        connectorProps = originalsWithPrefix(Consts.CONNECTOR_PREFIX);
        metricProps = originalsWithPrefix(Consts.MONITOR_PREFIX);
        ccCacheProps = originalsWithPrefix(Consts.CCCACHE_PREFIX);
    }

    /**
     * 获取连接池的默认大小
     *
     * @return 连接池默认大小
     */
    public int getConnectionPoolSize() {
        String key = Consts.CLUSTER_PREFIX + Consts.CONN_POOL_SIZE;
        int value = Consts.CONN_POOL_SIZE_DEFAULT;
        if (config.containsKey(key)) {
            try {
                value = Integer.parseInt(config.get(key));
            } catch (NumberFormatException ignore) {
                LogUtils.warn(log, "parse int failed. conn.pool.size: " + value, ignore);
            }
        }

        return value;
    }

    /**
     * 获取kafka上报埋点信息的topic名称
     *
     * @return 埋点信息的topic名称
     */
    public String getMetricTopic() {
        return config.getOrDefault(Consts.METRIC_TOPIC, Consts.DEFAULT_METRIC_TOPIC);
    }

    /**
     * 获取kafka上清洗失败的消息存储的topic名称
     *
     * @return 清洗失败消息存储的topic名称
     */
    public String getBadMsgTopic() {
        return config.getOrDefault(Consts.BAD_MSG_TOPIC, Consts.DEFAULT_BAD_MSG_TOPIC);
    }

    /**
     * 获取总线任务事件上报的topic名称
     *
     * @return 总线任务上报topic名称
     */
    public String getDatabusEventTopic() {
        return config.getOrDefault(Consts.DATABUS_EVENT_TOPIC, Consts.DEFAULT_DATABUS_EVENT_TOPIC);
    }

    /**
     * 获取总线错误日志上报的topic名称
     *
     * @return 总线错误日志上报的topic名称
     */
    public String getDatabusErrorLogTopic() {
        return config.getOrDefault(Consts.DATABUS_ERROR_LOG_TOPIC, Consts.DEFAULT_DATABUS_ERROR_LOG_TOPIC);
    }

    /**
     * 获取总线集群汇总打点数据的topic名称
     *
     * @return 总线集群打点汇总数据的topic名称
     */
    public String getDatabusClusterStatTopic() {
        return config.getOrDefault(Consts.DATABUS_CLUSTER_STAT_TOPIC, Consts.DEFAULT_DATABUS_CLUSTER_STAT_TOPIC);
    }

    /**
     * 获取是否禁止context刷新
     *
     * @return True/False
     */
    public boolean getDisableContextRefresh() {
        return config.getOrDefault(Consts.CLUSTER_DISABLE_CONTEXT_REFRESH, "false").equalsIgnoreCase("true");
    }

    /**
     * 获取consul服务状态的url地址
     *
     * @return consul服务状态的url地址
     */
    public String getConsulServiceUrl() {
        String dns = config.getOrDefault(Consts.API_DNS, Consts.API_DNS_DEFAULT);
        if (dns.startsWith("test.")) {
            dns = dns.substring(5);
        }
        return "http://ns." + dns + Consts.API_SERVICE_STATUS;
    }

    /**
     * 获取cluster_info的api url地址
     *
     * @return get_cluster_info api url地址
     */
    public String getClusterInfoUrl() {
        return getUrl(Consts.API_CLUSTER_PATH, Consts.API_CLUSTER_PATH_DEFAULT);
    }

    /**
     * 获取rt_info的api url地址
     *
     * @return get_rt_info api url地址
     */
    public String getRtInfoUrl() {
        return getUrl(Consts.API_RT_PATH, Consts.API_RT_PATH_DEFAULT);
    }

    /**
     * 获取固化节点的api url地址
     *
     * @return get_daoanode_info api url地址
     */
    public String getDatanodeInfoUrl() {
        return getUrl(Consts.API_DATANODE_PATH, Consts.API_DATANODE_PATH_DEFAULT);
    }

    /**
     * 获取的dataid到topic映射关系的api url地址
     *
     * @return get_dataids_topics api url地址
     */
    public String getDataidsTopicsUrl() {
        return getUrl(Consts.API_GET_DATAIDS_TOPICS, Consts.API_GET_DATAIDS_TOPICS_DEFAULT);
    }

    /**
     * 获取添加离线任务的url地址
     *
     * @return url地址
     */
    public String getAddOfflineTaskUrl() {
        return getUrl(Consts.API_ADD_OFFLINE_TASK, Consts.API_ADD_OFFLINE_TASK_DEFAULT);
    }

    /**
     * 获取离线任务信息的url地址
     *
     * @return url地址
     */
    public String getOfflineTaskInfoUrl() {
        return getUrl(Consts.API_GET_OFFLINE_TASK, Consts.API_GET_OFFLINE_TASK_DEFAULT);
    }

    /**
     * 获取离线任务列表信息的url地址
     *
     * @return url地址
     */
    public String getOfflineTasksUrl() {
        return getUrl(Consts.API_GET_OFFLINE_TASKS, Consts.API_GET_OFFLINE_TASKS_DEFAULT);
    }

    /**
     * 获取更新离线任务的url地址
     *
     * @return url地址
     */
    public String getUpdateOfflineTaskUrl() {
        return getUrl(Consts.API_UPDATE_OFFLINE_TASK, Consts.API_UPDATE_OFFLINE_TASK_DEFAULT);
    }

    /**
     * 获取标记离线任务结束的url地址
     *
     * @return url地址
     */
    public String getFinishOfflineTaskUrl() {
        return getUrl(Consts.API_FINISH_OFFLINE_TASK, Consts.API_FINISH_OFFLINE_TASK_DEFAULT);
    }

    /**
     * 获取添加总线存储事件消息的url地址
     *
     * @return url地址
     */
    public String getAddDatabusStorageEventUrl() {
        return getUrl(Consts.API_ADD_DATABUS_STORAGE_EVENT, Consts.API_ADD_DATABUS_STORAGE_EVENT_DEFAULT);
    }

    /**
     * 获取hdfs导入任务的最早一批任务列表
     *
     * @return url地址
     */
    public String getEarliestHdfsImportTaskUrl() {
        return getUrl(Consts.API_GET_EARLIEST_HDFS_IMPORT_TASK, Consts.API_GET_EARLIEST_HDFS_IMPORT_TASK_DEFAULT);
    }

    /**
     * 获取更新hdfs导入任务状态的地址
     *
     * @return url地址
     */
    public String getUpdateHdfsImportTaskUrl() {
        return getUrl(Consts.API_UPDATE_HDFS_IMPORT_TASK, Consts.API_UPDATE_HDFS_IMPORT_TASK_DEFAULT);
    }

    /**
     * 获取更新hdfs元数据
     *
     * @return url地址
     */
    public String getUpdateHdfsMaintainDateUrl() {
        return getStorekitUrl(Consts.API_UPDATE_HDFS_MAINTAIN_DATE, Consts.API_UPDATE_HDFS_MAINTAIN_DATE_DEFAULT);
    }

    /**
     * 构造url的地址,作为string返回。
     *
     * @param key 配置文件中定义的url的key
     * @param defaultValue 默认的url值
     * @return 完整的http地址
     */
    public String getUrl(String key, String defaultValue) {
        String dns = getPropsWithDefault(Consts.API_DNS, Consts.API_DNS_DEFAULT);
        String path = getPropsWithDefault(key, defaultValue);
        return "http://" + dns + path;
    }

    /**
     * 构造storekit url的地址,作为string返回。
     *
     * @param key 配置文件中定义的url的key
     * @param defaultValue 默认的url值
     * @return 完整的http地址
     */
    public String getStorekitUrl(String key, String defaultValue) {
        String dns = getPropsWithDefault(Consts.API_STOREKIT_DNS, Consts.API_DNS_DEFAULT);
        String path = getPropsWithDefault(key, defaultValue);
        return "http://" + dns + path;
    }

    /**
     * 获取kafka connect worker集群的配置信息
     *
     * @return 集群的配置信息
     */
    public Map<String, String> getClusterProps() {
        return clusterProps;
    }

    /**
     * 获取cc cache相关配置属性
     *
     * @return cc cache的配置信息
     */
    public Map<String, String> getCcCacheProps() {
        Map<String, String> cache = new HashMap<>();
        cache.putAll(ccCacheProps);
        // 如果配置文件中密码加密过，则将其解密
        String pass = cache.get(Consts.PASSWD);
        if (StringUtils.isNotBlank(pass)) {
            cache.put(Consts.PASSWD, ConnUtils.decodePass(pass));
        }

        return cache;
    }

    /**
     * 获取sink connector的consumer相关配置信息
     *
     * @return consumer相关配置信息
     */
    public Map<String, String> getConsumerProps() {
        return consumerProps;
    }

    /**
     * 获取配置文件中"producer."开头的所有配置,不包含"producer."前缀。
     *
     * @return producer相关的配置信息
     */
    public Map<String, String> getProducerProps() {
        return producerProps;
    }

    /**
     * 获取consumer相关配置信息，包含前缀“consumer.”
     *
     * @return consumer相关配置信息
     */
    public Map<String, String> getConsumerPropsWithPrefix() {
        return getPropsWithPrefix(Consts.CONSUMER_PREFIX);
    }

    /**
     * 获取配置文件中"connector."开头的所有配置,不包含"connector."前缀。
     *
     * @return connector相关的配置信息
     */
    public Map<String, String> getConnectorProps() {
        return connectorProps;
    }

    /**
     * 获取埋点、自检等相关配置信息
     *
     * @return monitor相关配置信息
     */
    public Map<String, String> getMonitorProps() {
        return metricProps;
    }

    /**
     * 获取埋点上报信息的kafka配置项。当producer的bs地址未配置时,使用集群默认的bs地址。
     *
     * @return kafka配置项
     */
    public Map<String, String> getMetricKafkaProps() {
        Map<String, String> result = originalsWithPrefix(Consts.METRIC_PREFIX);
        if (StringUtils.isBlank(result.get(Consts.BOOTSTRAP_SERVERS))) {
            result.put(Consts.BOOTSTRAP_SERVERS, getClusterProps().get(Consts.BOOTSTRAP_SERVERS));
        }
        if (StringUtils.isBlank(result.get(Consts.KEY_SERIALIZER))) {
            result.put(Consts.KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        }
        if (StringUtils.isBlank(result.get(Consts.VALUE_SERIALIZER))) {
            result.put(Consts.VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        }
        return result;
    }

    @Override
    public String toString() {
        return this.config.toString();
    }

    /**
     * 获取配置中以固定prefix开头的所有配置项的kv,并将key中的prefix去掉。
     *
     * @param prefix 配置项prefix
     * @return 符合条件的配置项, 放入map中返回
     */
    public Map<String, String> originalsWithPrefix(String prefix) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                result.put(entry.getKey().substring(prefix.length()), entry.getValue());
            }
        }
        return result;
    }

    /**
     * 获取配置中以固定prefix开头的所有配置项,保留key中的prefix。
     *
     * @param prefix 配置项prefix
     * @return 符合条件的配置项, 放入map中返回
     */
    public Map<String, String> getPropsWithPrefix(String prefix) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    /**
     * 获取某个属性的值,当值为空时,返回默认值
     *
     * @param key 属性的key
     * @param defaultValue 属性默认值
     * @return 属性的值
     */
    public String getPropsWithDefault(String key, String defaultValue) {
        String val = config.get(key);
        val = StringUtils.isBlank(val) ? defaultValue : val;
        return val;
    }
}
