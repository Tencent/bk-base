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


package com.tencent.bk.base.datahub.databus.connect.sink.iceberg;

import static com.tencent.bk.base.datahub.databus.commons.Consts.ICEBERG_ACQUIRE_LOCK_TIMEOUT;
import static com.tencent.bk.base.datahub.databus.commons.Consts.STORAGE_HDFS;
import static com.tencent.bk.base.datahub.databus.commons.Consts.TDW_FINISH_DIR;
import static com.tencent.bk.base.datahub.databus.connect.source.iceberg.HdfsSourceTask.API_REPORT_DATA;
import static com.tencent.bk.base.datahub.databus.connect.source.iceberg.HdfsSourceTask.API_REPORT_DATA_DEFAULT;
import static com.tencent.bk.base.datahub.iceberg.C.DATABUSEVENT;
import static com.tencent.bk.base.datahub.iceberg.C.DT_ADD;
import static com.tencent.bk.base.datahub.iceberg.C.DT_RESET;
import static com.tencent.bk.base.datahub.iceberg.C.DT_SDK_MSG;

import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkTask;
import com.tencent.bk.base.datahub.iceberg.BkTable;
import com.tencent.bk.base.datahub.iceberg.CommitMsg;
import com.tencent.bk.base.datahub.iceberg.DataBuffer;
import com.tencent.bk.base.datahub.iceberg.SnapshotEventCollector;
import com.tencent.bk.base.datahub.iceberg.Utils;
import com.tencent.bk.base.datahub.iceberg.parser.InnerMsgParser;
import com.tencent.bk.base.datahub.iceberg.parser.ParseResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkTask extends BkSinkTask {

    private static final Logger log = LoggerFactory.getLogger(IcebergSinkTask.class);
    private final int intervalFactor = 5000;
    private IcebergSinkConfig config;
    private String[] colsInOrder;
    private DataBuffer dataBuffer;
    private InnerMsgParser parser;
    private BkTable table;

    /**
     * 启动任务,初始化资源
     */
    @Override
    public void startTask() {
        // 创建任务配置对象
        config = new IcebergSinkConfig(configProperties);
        buildSchemas();

        LogUtils.info(log, "{}: starting init table, customProperties is {}", rtId, config.customProperties);
        initTable();
        parser = new InnerMsgParser(config.rtId, colsInOrder, table.schema());

        BasicProps props = BasicProps.getInstance();
        String dns = props.getPropsWithDefault(Consts.API_DNS, Consts.API_DNS_DEFAULT);
        String reportPath = props.getPropsWithDefault(API_REPORT_DATA, API_REPORT_DATA_DEFAULT);
        // 启用iceberg事件收集
        SnapshotEventCollector.getInstance().setReportUrl(dns, reportPath);

        // kafka-connect 为poll方式，设置拉取为空执行put方法
        skipProcessEmptyRecords = false;
        // 记录任务开始时间
        start = System.currentTimeMillis();
    }


    /**
     * 处理kafka msg
     *
     * @param records kafka msg记录
     */
    @Override
    public void processData(Collection<SinkRecord> records) {
        if (dataBuffer == null) {
            // 初始化dataBuffer
            LogUtils.info(log, "{}: going to init dataBuffer", rtId);
            dataBuffer = new DataBuffer(table, config.interval, config.flushSize,
                    partitionId * intervalFactor);
        }
        final long now = System.currentTimeMillis() / 1000;
        final long tagTime = now / 60 * 60;
        try {
            // 检查异步刷盘状态
            dataBuffer.checkState();

            for (SinkRecord record : records) {
                if (isRecordProcessed(record)) {
                    continue;
                }

                String value = record.value().toString();
                String key = (record.key() == null) ? "" : record.key().toString();
                String tp = getOffsetMsgKey(record.topic(), record.kafkaPartition());
                Map<String, String> offsetMsg = ImmutableMap.of(tp, String.valueOf(record.kafkaOffset()));

                if (isDatabusEvent(key, value)) {
                    // 处理总线事件消息，将其上报到storekit中进行后续处理
                    LogUtils.info(log, "{} got databus event msg {}", rtId, key);
                    Map<String, String> eventMsg = com.tencent.bk.base.datahub.databus.commons.utils.Utils
                            .parseDatabusEvent(key);
                    String finishDir = eventMsg.get(TDW_FINISH_DIR);
                    boolean isSucc = HttpUtils.addDatabusStorageEvent(rtId, STORAGE_HDFS, TDW_FINISH_DIR, finishDir);
                    if (isSucc) {
                        // 记录此总线事件消息已经被处理，在iceberg刷盘时将消息的offset信息写入commit msg里。
                        dataBuffer.add(new ArrayList<>(), offsetMsg);
                    } else {
                        LogUtils.warn(log, "{} failed to send databus hdfs storage event {}", rtId, eventMsg);
                    }
                } else {
                    Optional<ParseResult> result = parser.parse(key, value);
                    if (result.isPresent()) {
                        List<Record> recs = result.get().getRecords();
                        // 获取所需字段列表的值组成的记录
                        dataBuffer.add(recs, offsetMsg);
                        markRecordProcessed(record); // 标记此条消息已被处理过
                        msgCountTotal++;
                        recordCountTotal += recs.size();
                        try {
                            Metric.getInstance().updateStat(rtId, config.connector, record.topic(), config.clusterType,
                                    recs.size(), value.length(), result.get().getMetricTag(), tagTime + "");
                            setDelayTime(result.get().getTagTime(), tagTime);
                            Metric.getInstance().updateDelayInfo(config.connector, now, maxDelayTime, minDelayTime);
                        } catch (Exception e) {
                            LogUtils.warn(log, "failed to update stat info for {}, just ignore this. {}", rtId,
                                    e.getMessage());
                        }
                    }
                }
            }
            // 重置延迟计时
            resetDelayTimeCounter();
        } catch (Exception e) {
            String stackTrace = ExceptionUtils.getStackTrace(e);
            LogUtils.warn(log, "{}: failed to process data, exception: {}", rtId, stackTrace);
            if (e instanceof CommitFailedException && e.getMessage().contains("waiting for lock on")) {
                Metric.getInstance().reportEvent(name, ICEBERG_ACQUIRE_LOCK_TIMEOUT, stackTrace,
                        e.getMessage());
            }
            throw new ConnectException("failed to process data.", e);
        }
    }

    /**
     * 判断此条kafka msg是否为事件消息
     *
     * @param key kafka的msg的key值
     * @param value kafka的msg的value值
     * @return True/False
     */
    private boolean isDatabusEvent(String key, String value) {
        return value.length() == 0 && key.startsWith(DATABUSEVENT);
    }

    /**
     * 根据数据的字段信息获取其schema信息
     */
    private void buildSchemas() {
        Map<String, String> columns = ctx.getDbColumnsTypes();
        colsInOrder = columns.keySet().toArray(new String[0]);
        LogUtils.info(log, "{} cols: {}, {}", rtId, colsInOrder, columns);
    }

    /**
     * 初始化iceberg table相关资源
     */
    private void initTable() {
        try {
            Map<String, String> props = new HashMap<>();
            props.put("fs.automatic.close", "false");

            // 设置hiveMeta及hdfs相关资源以及高可用配置
            config.customProperties.forEach((k, v) -> props.put(k, v.toString()));
            table = new BkTable(config.dbName, config.tableName, props);
            table.loadTable();
        } catch (Exception e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.READ_HDFS_JSON_FAIL, log,
                    "failed to init iceberg table by hdfs resource", e);
            throw new ConnectException(e);
        }
    }

    /**
     * 停止运行,将一些使用到的资源释放掉。
     */
    @Override
    public void stopTask() {
        LogUtils.info(log, "{} iceberg sink task stopping", rtId);
        closeResource();
    }

    /**
     * 当发生topic的分区rebalance时,关闭资源
     *
     * @param partitions 分配的topic分的区列表
     */

    @Override
    public void close(Collection<TopicPartition> partitions) {
        LogUtils.info(log, "{} close partitions: {}", rtId, partitions);
        closeResource();
    }

    /**
     * 重置offset
     *
     * @throws ConnectException 异常
     */
    protected void resetOffsets(TopicPartition tp) throws ConnectException {
        Map<String, Long> offsetInfo = readOffset();
        String offsetMsgKey = getOffsetMsgKey(tp.topic(), tp.partition());
        long offset = offsetInfo.getOrDefault(offsetMsgKey, -1L);

        if (offset >= 0) {
            LogUtils.info(log, "{}: reset offset to {} in {}-{}", rtId, offset, tp.topic(), tp.partition());
            context.offset(tp, offset + 1);
        }
    }

    /**
     * 读取offset值
     *
     * @throws ConnectException 异常
     */
    public Map<String, Long> readOffset() throws ConnectException {
        // commitMsg 返回本身是顺序的,新commit在前
        List<CommitMsg> commitMsgList = new ArrayList<>();
        table.filterSnapshots(s -> {
            if (s.summary().containsKey(DT_SDK_MSG)) {
                Optional<CommitMsg> msg = Utils.parseCommitMsg(s.summary().get(DT_SDK_MSG));
                if (msg.isPresent()) {
                    String op = msg.get().getOperation();
                    if (DT_RESET.equals(op) || DT_ADD.equals(op)) {
                        commitMsgList.add(msg.get());
                    }
                }
            }

            return false;  // 无需返回结果
        });

        Map<String, Long> addOffsetInfo = new HashMap<>();
        Map<String, Long> resetOffsetInfo = new HashMap<>();
        // 读取offset信息，如果存在reset类型offset msg，忽略reset之前的add类型offset，取reset 之后add,reset类型最大offset
        commitMsgList.forEach(commitMsg -> {
            Map<String, String> offsetMsg = commitMsg.getData();
            // 当地读取到最新的reset类型msg时，只记录最近的一条的offset信息
            String operation = commitMsg.getOperation();
            if (DT_RESET.equals(operation)) {
                offsetMsg.forEach((k, v) -> resetOffsetInfo.putIfAbsent(k, Long.parseLong(v)));
            } else if (DT_ADD.equals(operation)) {
                offsetMsg.forEach((k, v) -> {
                    // 判断之前offset信息中是否已经记录tp的reset 类型offset，如果存在，则可以丢弃
                    if (!resetOffsetInfo.containsKey(k)) {
                        try {
                            long offset = Long.parseLong(v);
                            long currentOffset = addOffsetInfo.getOrDefault(k, 0L);
                            addOffsetInfo.put(k, Math.max(offset, currentOffset));
                        } catch (NumberFormatException ignore) {
                            // 对于数据导入，其operation也时DT_ADD，这种情况下value并非为数值，解析为offset会失败
                        }
                    }
                });
            }
        });

        Map<String, Long> toResetOffsetInfo = new HashMap<>(resetOffsetInfo);
        addOffsetInfo.forEach((k, v) -> {
            // 如果不存在reset，则以add为准
            long offset = resetOffsetInfo.getOrDefault(k, v);
            toResetOffsetInfo.put(k, Math.max(v, offset));
        });
        LogUtils.info(log, "{}: addOffsetInfo is {}, resetOffsetInfo is {}, toResetOffsetInfo is {},",
                rtId, addOffsetInfo, resetOffsetInfo, toResetOffsetInfo);

        return toResetOffsetInfo;
    }

    /**
     * 获取commit offsetMsg 的key
     */
    private String getOffsetMsgKey(String topic, int partition) {
        return String.format("%s-%s", topic, partition);
    }

    /**
     * 关闭资源
     */
    public void closeResource() throws ConnectException {
        try {
            if (dataBuffer != null) {
                dataBuffer.close();
            }
        } catch (Exception failed) {
            LogUtils.warn(log, "{}: data buffer closed exception: {}", rtId, failed);
            throw new ConnectException("failed to close data buffer");
        } finally {
            if (dataBuffer != null) {
                dataBuffer = null;
            }
        }
    }
}
