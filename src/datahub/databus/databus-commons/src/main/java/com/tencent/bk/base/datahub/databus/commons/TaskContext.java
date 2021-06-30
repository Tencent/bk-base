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


import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class TaskContext {

    private static final Logger log = LoggerFactory.getLogger(TaskContext.class);

    private Map<String, String> props = new HashMap<>();
    private Map<String, String> columns = new LinkedHashMap<>();
    private Set<String> dimensions = new HashSet<>();
    // rt所在的kafka地址
    private String bsServer = "";

    public TaskContext(Map<String, String> mapProps) {
        props.putAll(mapProps);
        String cols = props.get(Consts.COLUMNS);
        if (StringUtils.isNoneBlank(cols)) {
            for (String entry : cols.split(",")) {
                String[] kv = entry.split("=");
                columns.put(kv[0], kv[1]);
            }
        }
        // 构建维度信息
        String dms = props.get(Consts.DIMENSIONS);
        if (StringUtils.isNotBlank(dms)) {
            for (String dm : dms.split(",")) {
                dimensions.add(dm);
            }
        }
        bsServer = props.getOrDefault(Consts.BOOTSTRAP_SERVERS, "");
    }

    /**
     * 获取数据源的数据格式,可能是etl、avro、parquet等
     *
     * @return 数据源的数据格式
     */
    public String getSourceMsgType() {
        String type = props.get(Consts.MSG_SOURCE_TYPE);
        type = StringUtils.isBlank(type) ? Consts.JSON : type;
        return type;
    }

    /**
     * 设置任务处理的消息类型,可以是etl、avro、json等
     *
     * @param msgType 消息类型
     */
    public void setSourceMsgType(String msgType) {
        props.put(Consts.MSG_SOURCE_TYPE, msgType);
    }

    /**
     * 获取context对应的resultTableId
     *
     * @return resultTableId
     */
    public String getRtId() {
        return props.get(BkConfig.RT_ID);
    }

    /**
     * 获取context对应的rt 类型
     *
     * @return rt 类型
     */
    public String getRtType() {
        return props.get(Consts.RT_TYPE);
    }

    /**
     * 获取存储对应的配置信息
     *
     * @param storageType 存储类型(ES/HDFS/kafka/tspider等)
     * @return 存储对应的配置信息
     */
    public Map<String, Object> getStorageConfig(String storageType) {
        String storageString = props.get(storageType);
        Map<String, Object> storageConfig = new HashMap<>();
        if (StringUtils.isNotBlank(storageString)) {
            Map<String, Object> config = JsonUtils.toMapWithoutException(storageString);
            Object storageConfigString = config.get(Consts.STORAGE_CONFIG);
            if (null != storageConfigString) {
                storageConfig = JsonUtils.toMapWithoutException(storageConfigString.toString());
            }
        }
        return storageConfig;
    }

    /**
     * 获取当前context消费的topic名称
     *
     * @return topic名称
     */
    public String getTopic() {
        String topic = props.get(Consts.TOPICS);
        topic = StringUtils.isBlank(topic) ? props.get(Consts.TOPIC) : topic;
        if (StringUtils.isBlank(topic)) {
            topic = "";
        }
        return topic;
    }

    /**
     * 获取rt对应的kafka bootstrap server地址
     *
     * @return kafka bootstrap server地址
     */
    public String getKafkaBsServer() {
        return bsServer;
    }

    /**
     * 获取当前rt对应的etl conf并返回。
     *
     * @return etl conf
     */
    public String getEtlConf() {
        return props.containsKey(Consts.ETL_CONF) ? props.get(Consts.ETL_CONF) : "{}";
    }

    /**
     * 获取当前rt对应的字段列表(字段名称 -> 字段类型),底层使用Linked Hashmap存储,
     * 保证遍历的时候是有序的。
     *
     * @return resultTable的字段列表
     */
    public Map<String, String> getRtColumns() {
        return Collections.unmodifiableMap(columns);
    }

    /**
     * 获取当前rt中的维度字段集合
     *
     * @return rt的维度字段集合
     */
    public Set<String> getDimensions() {
        return Collections.unmodifiableSet(dimensions);
    }

    /**
     * 获取数据库中此rt对应的字段列表和rt中的字段类型
     *
     * @return 数据库中字段映射
     */
    public Map<String, String> getDbColumnsTypes() {
        Map<String, String> cols = new HashMap<>(columns);

        // 屏蔽屏蔽rt里的offset/iteration_idx字段
        cols.remove(Consts.TIMESTAMP);
        cols.remove(Consts.OFFSET);
        cols.remove(Consts.ITERATION_IDX);

        // 增加数据库中所增加的字段
        cols.put(Consts.THEDATE, EtlConsts.INT);
        cols.put(Consts.DTEVENTTIME, EtlConsts.STRING);
        cols.put(Consts.DTEVENTTIMESTAMP, EtlConsts.LONG);
        cols.put(Consts.LOCALTIME, EtlConsts.STRING);

        return cols;
    }

    /**
     * 获取当前rt对应的数据库中字段列表
     *
     * @return rt对应的数据库表的字段列表
     */
    public Set<String> getDbTableColumns() {
        return new LinkedHashSet<>(getDbColumnsTypes().keySet());
    }

    /**
     * 获取当前rt对应的dataId
     *
     * @return rt对应的dataId(当配置存在问题时, 返回0)
     */
    public int getDataId() {
        String dataId = props.get(Consts.DATA_ID);
        if (StringUtils.isNotBlank(dataId)) {
            try {
                return Integer.parseInt(dataId);
            } catch (Exception e) {
                LogUtils.warn(log, "failed to parse data.id {} from config!", dataId);
            }
        }

        // 默认返回0，代表dataid配置不存在
        return 0;
    }

    /**
     * 获取当前rt对应的hdfs数据类型
     *
     * @return rt对应的hdfs数据类型(不存在hdfs存储时, 返回unknown)
     */
    public String getHdfsDataType() {
        return props.getOrDefault(Consts.HDFS_DATA_TYPE, "unknown");
    }

    /**
     * 比较两个TaskContext,内容相同时返回true,不同时返回false
     *
     * @param obj 对比的对象
     * @return true/false
     */
    @Override
    public boolean equals(Object obj) {
        boolean result = false;
        if (obj instanceof TaskContext) {
            TaskContext o = (TaskContext) obj;
            if (getRtId().equals(o.getRtId())
                    && getTopic().equals(o.getTopic())
                    && getEtlConf().equals(o.getEtlConf())) {
                // 对比字段列表
                Map<String, String> cols = getRtColumns();
                Map<String, String> objCols = o.getRtColumns();
                if (cols.equals(objCols)) {
                    result = true;
                }
            }
        }

        return result;
    }

    /**
     * 计算对象的hash code
     *
     * @return hash code的值
     */
    @Override
    public int hashCode() {
        int code = 0;
        for (String key : props.keySet()) {
            code += key.length();
        }
        return code;
    }

    @Override
    public String toString() {
        return "{TaskContext=" + props.toString() + "}";
    }

    // only for test
    public void setSourceMsgTypeToJson() {
        props.put(Consts.MSG_SOURCE_TYPE, Consts.JSON);
    }
}
