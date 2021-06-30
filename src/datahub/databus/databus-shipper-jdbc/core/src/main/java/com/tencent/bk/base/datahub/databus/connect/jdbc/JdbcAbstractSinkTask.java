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

package com.tencent.bk.base.datahub.databus.connect.jdbc;

import static com.tencent.bk.base.datahub.databus.commons.utils.LogUtils.info;
import static com.tencent.bk.base.datahub.databus.commons.utils.LogUtils.warn;

import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkTask;
import com.tencent.bk.base.datahub.databus.connect.jdbc.util.BatchRecords;
import com.tencent.bk.base.datahub.databus.connect.jdbc.util.BatchRecordsConfig;
import com.tencent.bk.base.datahub.databus.connect.jdbc.util.JdbcUtils;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.ConnUtils;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JdbcAbstractSinkTask extends BkSinkTask {

    private static final Logger log = LoggerFactory.getLogger(JdbcAbstractSinkTask.class);
    private JdbcSinkConfig config;
    private static final String DISABLE_REMOVE_DUPLICATE = "disable.remove.duplicate";
    private static final String UNIQUE_KEY_NAME = "____key";
    private String[] colsInOrder;
    private String insertSql;
    private boolean enableRemoveDup = false;
    private int threshold = 0;
    private int removeDupTimes = 5; // 对于前面几批数据，触发删除重复数据的机制
    private String password = ""; // 用于记录解密后的密码
    private String clusterType = ""; // 可能是tspider、mysql等jdbc类型的存储
    private boolean hasUniqueKey = false;
    private List<String> storageKeys = null;
    private Map<String, Integer> columnMap = new HashMap<>();
    protected BatchRecordsConfig batchRecordsConfig;

    @Override
    protected void startTask() {
        config = new JdbcSinkConfig(configProperties);
        clusterType = StringUtils.split(config.connector, "-")[0];
        Map<String, Object> storageConfig = this.ctx.getStorageConfig(clusterType);
        hasUniqueKey = (boolean) storageConfig.getOrDefault("has_unique_key", false);
        if (hasUniqueKey) {
            storageKeys = (List<String>) storageConfig.get("storage_keys");
        }
        // 初始化
        buildColumnsAndInsertSql();
        password = ConnUtils.decodePass(config.connPass);
        enableRemoveDup = true;
        threshold = config.tableName.startsWith("ai_") ? config.magicNum * 10 : config.magicNum;
        if (configProperties.getOrDefault(DISABLE_REMOVE_DUPLICATE, "").equals("true")) {
            enableRemoveDup = false;
            info(log, "disable remove duplicate for rt {}", config.rtId);
        } else {
            info(log, "enable remove duplicate for rt {}", config.rtId);
        }

        batchRecordsConfig = BatchRecordsConfig.builder()
                .dbName(config.dbName)
                .tableName(config.tableName)
                .colsInOrder(colsInOrder)
                .insertSql(insertSql)
                .connUrl(config.connUrl)
                .connUser(config.connUser)
                .connPass(password)
                .batchSize(config.batchSize)
                .timeoutSeconds(config.batchTimeoutMs / 1000)
                .maxDelayDays(config.maxDelayDays)
                .retryBackoffMs(config.retryBackoffMs)
                .build();
    }

    /**
     * 构建insert语句和columns对象
     */
    private void buildColumnsAndInsertSql() {
        Set<String> columns = ctx.getDbTableColumns();
        if (hasUniqueKey) {
            columns.add(UNIQUE_KEY_NAME);
        }
        colsInOrder = columns.toArray(new String[columns.size()]);
        for (int i = 0; i < colsInOrder.length; i++) {
            columnMap.put(colsInOrder[i], i);
        }

        if (StringUtils.isNotBlank(config.dbName)) {
            insertSql = JdbcUtils.buildInsertSql(config.dbName, config.tableName, colsInOrder, getEscape());
        } else {
            insertSql = JdbcUtils.buildInsertSql(config.tableName, colsInOrder, getEscape());
        }
        info(log, "{} {} {}", rtId, colsInOrder, insertSql);
    }

    /**
     * sql 转义符
     * @return 获取当前存储的sql转义符
     */
    protected abstract String getEscape();

    /**
     * 根据组合字段组成唯一健
     *
     * @param listResult 原始数据
     * @param storageKeys 组合字段
     */
    protected void generateUniqueKey(List<List<Object>> listResult, List<String> storageKeys, SinkRecord record) {
        for (int i = 0; i < listResult.size(); i++) {
            List<Object> rowData = listResult.get(i);
            StringBuilder uniqueKey = new StringBuilder();
            if (storageKeys.isEmpty()) {
                uniqueKey.append(record.topic()).append(record.kafkaPartition()).append(record.kafkaOffset()).append(i);
            } else {
                for (String storageKey : storageKeys) {
                    uniqueKey.append(rowData.get(columnMap.get(storageKey)));
                }
            }
            rowData.set(rowData.size() - 1, uniqueKey.toString());
        }
    }


    @Override
    protected void processData(Collection<SinkRecord> records) {
        BatchRecords batchRecords = new BatchRecords(this.batchRecordsConfig, this.getEscape());
        if (enableRemoveDup && removeDupTimes > 0) {
            info(log, "{} going to enable remove duplication in this round, remaining times {}", rtId, removeDupTimes);
            batchRecords.setRemoveDuplicateThreshold(threshold);
            removeDupTimes--;
        }

        for (SinkRecord record : records) {
            // kafka connect框架在kafka发生抖动的时候，可能拉取到重复的消息，这里需要做处理。
            if (isRecordProcessed(record)) {
                continue;
            }
            final long now = System.currentTimeMillis() / 1000; // 取秒
            final long tagTime = now / 60 * 60; // 取分钟对应的秒数
            String value = record.value().toString();
            String key = (record.key() == null) ? "" : record.key().toString();
            ConvertResult result = converter.getListObjects(key, value, colsInOrder);
            List<List<Object>> listResult = result.getObjListResult();
            if (hasUniqueKey) {
                this.generateUniqueKey(listResult, this.storageKeys, record);
            }
            batchRecords.add(listResult);

            markRecordProcessed(record); // 标记此条消息已被处理过

            msgCountTotal++;
            msgSizeTotal += value.length();

            // 上报打点数据
            try {
                Metric.getInstance().updateStat(rtId, config.connector, record.topic(), clusterType, listResult.size(),
                        value.length(), result.getTag(), tagTime + "");
                Metric.getInstance().updateTopicErrInfo(config.connector, result.getErrors());
                setDelayTime(result.getTagTime(), tagTime);
                Metric.getInstance().updateDelayInfo(config.connector, now, maxDelayTime, minDelayTime);
            } catch (Exception ignore) {
                warn(log, "failed to update stat info for {}, just ignore this. {}", rtId, ignore.getMessage());
            }
        }
        batchRecords.flush(); // 一批数据强制刷到存储中
        // 重置延迟计时
        resetDelayTimeCounter();
    }

}
