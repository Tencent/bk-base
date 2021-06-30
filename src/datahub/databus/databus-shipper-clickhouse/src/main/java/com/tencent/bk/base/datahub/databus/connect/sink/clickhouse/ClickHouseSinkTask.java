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

package com.tencent.bk.base.datahub.databus.connect.sink.clickhouse;


import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkTask;
import com.tencent.bk.base.datahub.databus.commons.BulkProcessor;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;


public class ClickHouseSinkTask extends BkSinkTask {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseSinkTask.class);
    private static final ConcurrentMap<String, ClickHouseBean> ckBeans = new ConcurrentHashMap<>();
    private ClickHouseSinkConfig config;
    private String[] sourceColsOrder;
    private Map<String, String> sourceColTypes = new HashMap<>();
    private SecureRandom random = new SecureRandom();

    static {
        Runnable task = () -> ckBeans.forEach((rtId, bean) -> {
            try {
                if (!bean.updateWeights(rtId)) {
                    LogUtils.warn(log, rtId + ": update weights failed");
                }
            } catch (Exception e) {
                LogUtils.error(ClickHouseConsts.CLICKHOUSE_UPDATE_WEIGHTS_ERROR, log,
                        rtId + ": update weights failed", e);
            }
        });

        Executors.newSingleThreadScheduledExecutor()
                .scheduleWithFixedDelay(task, 1, ClickHouseConsts.DEFAULT_UPDATE_WEIGHTS_INTERVAL, TimeUnit.HOURS);
    }


    /**
     * 启动任务,初始化资源
     */
    @Override
    public void startTask() {
        try {
            config = new ClickHouseSinkConfig(configProperties);
            sourceColsOrder = ctx.getDbColumnsTypes().keySet().toArray(new String[0]);
            sourceColTypes = ctx.getDbColumnsTypes();
            initial();
            LogUtils.info(log, "{}: task created", config.connector);
        } catch (Exception e) {
            LogUtils.error(ClickHouseConsts.CLICKHOUSE_WRITE_ERROR, log, config.connector + ": task create failed", e);
            throw new ConnectException(e);
        }
    }


    /**
     * 启动任务,初始化资源
     */
    public void initial() {
        ConcurrentMap<String, Integer> weightMap = ClickHouseBean.getWeightMap(config.weightsUrl);
        PrizeWheel wheel = new PrizeWheel(weightMap);
        List<BulkProcessor<Map<String, Object>>> bulkProcessors = new ArrayList<>();
        Consumer<List<Map<String, Object>>> flush = rows -> {
            try {
                writeWithRetry(rows);
            } catch (Exception e) {
                LogUtils.error(ClickHouseConsts.CLICKHOUSE_WRITE_ERROR, log,
                        config.connector + ": flush data failed", e);
                throw e;
            }
        };

        for (int i = 0; i < config.processorsSize; i++) {
            bulkProcessors.add(new BulkProcessor<>(config.flushSize, config.flushInterval.longValue(), flush));
        }

        start = System.currentTimeMillis();
        ClickHouseBean bean = new ClickHouseBean()
                .setWeightsUrl(config.weightsUrl)
                .setWeightMap(weightMap)
                .setWheel(wheel)
                .setProcessorsProps(config.processorProps)
                .setBulkProcessors(bulkProcessors);
        ckBeans.putIfAbsent(rtId, bean);
        ckBeans.get(rtId).maintainProcessors(config.processorProps, flush);
        LogUtils.info(log, "{}: task resource initialized, info: {}",
                config.connector, ckBeans.get(rtId).toString());
    }

    /**
     * 写一批数据到clickhouse, 失败会重试
     *
     * @param rows 待写入clickhouse的数据
     * @return 已经读取数据的行数
     */
    private void writeWithRetry(List<Map<String, Object>> rows) {
        if (rows.isEmpty()) {
            return;
        }
        int count = 0;
        while (writeData(rows) < 0) {
            count++;
            if (count > ClickHouseConsts.MAX_RETRY) {
                throw new ConnectException(rtId + ": retry writing failed");
            }
        }
    }


    /**
     * 写一批数据到clickhouse
     * 写失败的可能原因：1）网络中断；2）节点宕机；3）正在merge的partition过多，写阻塞
     *
     * @param rows 待写入clickhouse的数据
     * @return 已经读取数据的行数
     */
    private long writeData(List<Map<String, Object>> rows) {
        String url = ckBeans.get(rtId).getWheel().rotate();
        try (ClickHouseConnection conn = new ClickHouseDataSource(url).getConnection()) {
            conn.createStatement().write()
                    .send(config.insertPrefix, stream -> streamWrite(stream, rows), ClickHouseFormat.RowBinary);

            return rows.size();
        } catch (Exception e) {
            LogUtils.error(ClickHouseConsts.CLICKHOUSE_WRITE_ERROR, log, "{}: write error", config.connector, e);
            if (e instanceof SQLException) {
                Metric.getInstance().reportEvent(config.connector, ClickHouseConsts.CLICKHOUSE_WRITE_ERROR,
                        String.valueOf(((SQLException) e).getErrorCode()), e.getMessage());
                ckBeans.get(rtId).removeWeight(url);
            }
        }

        return -1;
    }


    /**
     * 流式写一批数据到clickhouse
     *
     * @param stream 流客户端
     * @param rows 待写入clickhouse的数据
     * @return 已经读取数据的行数
     * @throws IOException 异常
     */
    private void streamWrite(ClickHouseRowBinaryStream stream, List<Map<String, Object>> rows) throws IOException {
        for (Map<String, Object> row : rows) {
            for (int i = 0; i < config.colsInOrder.length; i++) {
                String colName = config.colsInOrder[i];
                Object value = row.get(colName.toLowerCase());
                String type = config.colTypes.get(colName);
                switch (type) {
                    case ClickHouseConsts.INT64:
                        long valueInt64 = (value == null) ? 0 : (long) value;
                        stream.writeInt64(valueInt64);
                        break;
                    case ClickHouseConsts.STRING:
                        String valueString = (value == null) ? "" : (String) value;
                        stream.writeString(valueString);
                        break;
                    case ClickHouseConsts.INT32:
                        int valueInt32 = (value == null) ? 0 : (int) value;
                        stream.writeInt32(valueInt32);
                        break;
                    case ClickHouseConsts.FLOAT32:
                        float valueFloat32 = (value == null) ? 0 : (float) value;
                        stream.writeFloat32(valueFloat32);
                        break;
                    case ClickHouseConsts.FLOAT64:
                        double valueFloat64 = (value == null) ? 0 : (double) value;
                        stream.writeFloat64(valueFloat64);
                        break;
                    case ClickHouseConsts.DATE_TIME:
                        Date valueDate = (value == null) ? new Date(0L) : (Date) value;
                        stream.writeDateTime(valueDate);
                        break;
                    default:
                        String msg = String.format("%s: write error, unsupported type: %s", config.connector, type);
                        LogUtils.error(ClickHouseConsts.CLICKHOUSE_UNSUPPORTED_DATA_TYPE, log, msg);
                        throw new ConnectException(msg);
                }
            }
        }
    }


    /**
     * 处理kafka msg
     *
     * @param records kafka msg记录
     */
    @Override
    public void processData(Collection<SinkRecord> records) {
        try {
            List<Map<String, Object>> data = records.stream()
                    .flatMap(e -> transformRecord(e).stream())
                    .collect(Collectors.toList());
            List<BulkProcessor<Map<String, Object>>> processors = ckBeans.get(rtId).getBulkProcessors();
            int bound = Math.min(config.processorsSize, processors.size());
            processors.get(random.nextInt(bound)).addAll(data);
            context.requestCommit();
        } catch (Exception e) {
            throw new ConnectException(e);
        }
    }


    /**
     * 转换单条kafka中的记录
     *
     * @param record kafka中的记录
     */
    private List<Map<String, Object>> transformRecord(SinkRecord record) {
        String value = record.value().toString();
        String key = (record.key() == null) ? "" : record.key().toString();
        ConvertResult result = converter.getListObjects(key, value, sourceColsOrder);
        List<Map<String, Object>> rowsData = result.getObjListResult()
                .stream()
                .map(e -> transform(e))
                .collect(Collectors.toList());

        markRecordProcessed(record); // 标记已经flush 到clickhouse的kafka offset
        msgCountTotal++;
        msgSizeTotal += value.length();
        try {
            final long now = System.currentTimeMillis() / 1000; // 取秒
            final long tagTime = now / 60 * 60; // 取分钟对应的秒数
            Metric.getInstance().updateStat(rtId, config.connector, record.topic(), "clickhouse",
                    rowsData.size(), value.length(), result.getTag(), tagTime + "");
            setDelayTime(result.getTagTime(), tagTime);
            Metric.getInstance().updateDelayInfo(config.connector, now, maxDelayTime, minDelayTime);
        } catch (Exception e) {
            LogUtils.warn(log, "{}: report state failed, exception: {}", config.connector, e);
        } finally {
            resetDelayTimeCounter();
        }

        return rowsData;
    }


    /**
     * 转换时间字段
     *
     * @param rowData 原始数据
     * @return 转换后的map
     */
    private Map<String, Object> transform(List<Object> rowData) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < sourceColsOrder.length; i++) {
            String col = sourceColsOrder[i].toLowerCase();
            map.put(col, rowData.get(i));
            if (ClickHouseConsts.DT_EVENT_TIMESTAMP.equals(col)) {
                map.put(ClickHouseConsts.TIME_COLUMN, new Date((long) rowData.get(i)));
            }
        }
        return map;
    }


    /**
     * 停止运行,将一些使用到的资源释放掉。
     */
    @Override
    public void stopTask() {
        LogUtils.info(log, "{}: task stopping", config.connector);
    }


    /**
     * 当发生topic的分区rebalance时,关闭资源
     *
     * @param partitions 分配的topic分的区列表
     */
    @Override
    public void close(Collection<TopicPartition> partitions) {
        LogUtils.info(log, "{}: close partitions: {}", config.connector, partitions);
    }

}



