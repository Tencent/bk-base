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

package com.tencent.bk.base.datahub.databus.connect.druid;

import com.google.common.collect.ImmutableList;
import com.tencent.bk.base.datahub.databus.connect.druid.transport.TaskWriter;
import com.tencent.bk.base.datahub.databus.connect.druid.transport.config.TaskConfig;
import com.tencent.bk.base.datahub.databus.connect.druid.transport.config.TaskConfig.Builder;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkTask;
import com.tencent.bk.base.datahub.databus.connect.druid.wrapper.DimensionSpecWrapper;
import com.tencent.bk.base.datahub.databus.connect.druid.wrapper.MetricSpecWrapper;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DruidSinkTask extends BkSinkTask {

    public static final String DATA_KEY_TIMESTAMP = "dtEventTimeStamp";
    public static final String ADDING_DATA_KEY_TIMESTAMP_DAY = "__druid_reserved_timestamp_day";
    public static final String ADDING_DATA_KEY_TIMESTAMP_HOUR = "__druid_reserved_timestamp_hour";
    public static final String ADDING_DATA_KEY_TIMESTAMP_MINUTE = "__druid_reserved_timestamp_minute";
    public static final String ADDING_DATA_KEY_TIMESTAMP_SECOND = "__druid_reserved_timestamp_second";
    public static final String ADDING_DATA_KEY_TIMESTAMP_MILLISECOND = "__druid_reserved_timestamp_millisecond";
    public static final String ADDING_DATA_KEY_DAY = "__druid_reserved_day";
    public static final String ADDING_DATA_KEY_HOUR = "__druid_reserved_hour";
    public static final String ADDING_DATA_KEY_MINUTE = "__druid_reserved_minute";
    public static final String ADDING_DATA_KEY_SECOND = "__druid_reserved_second";
    public static final String ADDING_DATA_KEY_MILLISECOND = "__druid_reserved_millisecond";
    public static final String ADDING_DATA_KEY_INGESTION_TIME = "__druid_reserved_ingestion_time";
    public static final String LOCALTIME = "localTime";
    private static final Logger log = LoggerFactory.getLogger(DruidSinkTask.class);
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private Set<String> columns;
    private String[] colsInOrder;
    private DruidSinkConfig config;
    private TaskWriter taskWriter;

    /**
     * 启动DruidSinkTask任务,初始化资源
     */
    @Override
    public void startTask() {
        try {
            LogUtils.info(log, "{}: to create DruidSinkConfig with kafka connect properties: {}", rtId,
                    configProperties);
            Map<String, String> clusterProps = BasicProps.getInstance().getClusterProps();
            configProperties.putAll(clusterProps);
            config = new DruidSinkConfig(configProperties);
            skipProcessEmptyRecords = false;
            columns = ctx.getDbTableColumns();
            colsInOrder = columns.toArray(new String[columns.size()]);
            createTaskWriter();
            LogUtils.info(log, "{}: created taskWriter", rtId);
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, "", rtId + ": startTask() throw ConnectException, start druid task !", e);
            throw new ConnectException(e);
        }

    }

    /**
     * 创建taskWriter
     */
    private void createTaskWriter() {
        String dataSource = config.tableName;
        Builder builder = TaskConfig.builder()
                .dataSourceName(dataSource)
                .timestampColumn(getTimestampColumn())
                .shards(1)
                .bufferSize(config.bufferSize)
                .intermediateHandoffPeriod(config.intermediateHandoffPeriod)
                .maxRowsPerSegment(config.maxRowsPerSegment)
                .maxRowsInMemory(config.maxRowsInMemory)
                .maxTotalRows(config.maxTotalRows)
                .intermediatePersistPeriod(config.intermediatePersistPeriod)
                .zookeeper(config.zookeeperConnect)
                .requiredCapacity(1)
                .maxIdleTime(config.maxIdleTime)
                .shipperCacheSize(config.shipperCacheSize)
                .shipperFlushOvertimeThreshold(config.shipperFlushOvertimeThreshold)
                .shipperFlushSizeThreshold(config.shipperFlushSizeThreshold)
                .druidVersion(config.druidVersion)
                .timestampStrategy(config.timestampStrategy)
                .shipperHttpRequestTimeout(config.shipperHttpRequestTimeout);

        createMetrics().forEach(m -> builder.addMetric(m.type, m.name, m.fieldName));
        createDimensions().forEach(d -> builder.addDimension(d.type, d.name));
        taskWriter = new TaskWriter(builder.build());
        LogUtils.info(log, "{}: created taskWriter ", rtId);

    }

    /**
     * 获取默认的时间戳字段
     *
     * @return 时间戳字段
     */
    private String getTimestampColumn() {
        switch (config.timestampStrategy) {
            case DruidSinkConfig.DATA_TIME:
                return DATA_KEY_TIMESTAMP;
            case DruidSinkConfig.CURRENT_TIME:
                return ADDING_DATA_KEY_INGESTION_TIME;
            default:
                throw new ConfigException(config.timestampStrategy);
        }
    }


    /**
     * 创建维度信息
     *
     * @return 维度信息
     */
    private List<DimensionSpecWrapper> createDimensions() {
        Map<String, String> rtColumns = new HashMap<>(ctx.getRtColumns());
        rtColumns.remove(Consts.TIMESTAMP);
        rtColumns.remove(Consts.OFFSET);
        List<DimensionSpecWrapper> list = rtColumns.entrySet().stream()
                .map(e -> new DimensionSpecWrapper(e.getKey(), mapDataType(e.getValue())))
                .collect(Collectors.toList());
        list.add(DimensionSpecWrapper.INGESTION_TIME);
        list.add(DimensionSpecWrapper.DTEVENTTIME);
        list.add(DimensionSpecWrapper.DTEVENTTIMESTAMP);
        list.add(DimensionSpecWrapper.LOCALTIME);
        list.add(DimensionSpecWrapper.THEDATE);

        return list;
    }


    /**
     * 字段类型转换
     *
     * @param dataBusDataType rt中定义的字段类型
     * @return 转换后的字段类型
     */
    private String mapDataType(String dataBusDataType) {
        switch (dataBusDataType) {
            case "string":
            case "text":
            case "bigint":
            case "bigdecimal":
                return "string";
            case "int":
            case "long":
                return "long";
            case "double":
                return "double";
            default:
                throw new ConfigException("unrecognizable data type from data bus: " + dataBusDataType);
        }
    }

    /**
     * 创建metrics
     *
     * @return metrics对象
     */
    private List<MetricSpecWrapper> createMetrics() {
        return ImmutableList.of(MetricSpecWrapper.COUNT);
    }

    /**
     * 处理kafka中的消息,将数据写入druid中。
     *
     * @param records kafka中的消息记录
     */
    @Override
    public void processData(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            // kafka connect框架在kafka发生抖动的时候，可能拉取到重复的消息，这里需要做处理。
            if (isRecordProcessed(record)) {
                continue;
            }
            consume(record);
        }

        taskWriter.flush();
        context.requestCommit();
    }

    /**
     * 处理单条kafka中的记录
     *
     * @param record kafka中的记录
     */
    private void consume(SinkRecord record) {
        String value = record.value().toString();
        String key = (record.key() == null) ? "" : record.key().toString();
        ConvertResult result = converter.getListObjects(key, value, colsInOrder);
        List<List<Object>> rowsData = result.getObjListResult();
        for (List<Object> rowData : rowsData) {
            Map<String, Object> immutable = Collections.unmodifiableMap(transform(rowData, result.getTheDate()));
            taskWriter.write(immutable);
        }

        markRecordProcessed(record); // 标记已经flush 到druid的kafka offset
        msgCountTotal++;
        msgSizeTotal += value.length();

        try {
            final long now = System.currentTimeMillis() / 1000; // 取秒
            final long tagTime = now / 60 * 60; // 取分钟对应的秒数
            Metric.getInstance().updateStat(rtId, config.connectorName, record.topic(), "druid",
                    result.getJsonResult().size(), value.length(), result.getTag(), tagTime + "");
            setDelayTime(result.getTagTime(), tagTime);
            Metric.getInstance().updateDelayInfo(config.connectorName, now, maxDelayTime, minDelayTime);
        } catch (Exception e) {
            LogUtils.warn(log, "{}: Failed to update stat info, just ignore this. {}", rtId, e.getMessage());
        } finally {
            resetDelayTimeCounter();
        }
    }

    /**
     * 转换时间字段
     *
     * @param rowData 原始数据
     * @return 转换后的map
     */
    private Map<String, Object> transform(List<Object> rowData, int date) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < colsInOrder.length; i++) {
            if (colsInOrder[i].equals(LOCALTIME)) {
                map.put(DimensionSpecWrapper.LOCALTIME.name, rowData.get(i));
            } else {
                map.put(colsInOrder[i], rowData.get(i));
            }
        }

        map.put(ADDING_DATA_KEY_INGESTION_TIME, System.currentTimeMillis());

        Instant instant = Instant.ofEpochMilli((long) map.get(DATA_KEY_TIMESTAMP));
        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());

        ZonedDateTime day = zonedDateTime.truncatedTo(ChronoUnit.DAYS);
        map.put(ADDING_DATA_KEY_DAY, day.toLocalDateTime().format(formatter));
        map.put(ADDING_DATA_KEY_TIMESTAMP_DAY, day.toInstant().toEpochMilli());

        ZonedDateTime hour = zonedDateTime.truncatedTo(ChronoUnit.HOURS);
        map.put(ADDING_DATA_KEY_HOUR, hour.toLocalDateTime().format(formatter));
        map.put(ADDING_DATA_KEY_TIMESTAMP_HOUR, hour.toInstant().toEpochMilli());

        ZonedDateTime minute = zonedDateTime.truncatedTo(ChronoUnit.MINUTES);
        map.put(ADDING_DATA_KEY_MINUTE, minute.toLocalDateTime().format(formatter));
        map.put(ADDING_DATA_KEY_TIMESTAMP_MINUTE, minute.toInstant().toEpochMilli());

        ZonedDateTime second = zonedDateTime.truncatedTo(ChronoUnit.SECONDS);
        map.put(ADDING_DATA_KEY_SECOND, second.toLocalDateTime().format(formatter));
        map.put(ADDING_DATA_KEY_TIMESTAMP_SECOND, second.toInstant().toEpochMilli());

        ZonedDateTime millisecond = zonedDateTime.truncatedTo(ChronoUnit.MILLIS);
        map.put(ADDING_DATA_KEY_MILLISECOND, millisecond.toLocalDateTime().format(formatter));
        map.put(ADDING_DATA_KEY_TIMESTAMP_MILLISECOND, millisecond.toInstant().toEpochMilli());

        map.put(DimensionSpecWrapper.THEDATE.name, date);
        return map;
    }

    /**
     * 停止运行，将一些使用到的资源清理掉。
     */
    @Override
    public void stopTask() {
        if (taskWriter != null) {
            taskWriter.close();
        }
    }
}
