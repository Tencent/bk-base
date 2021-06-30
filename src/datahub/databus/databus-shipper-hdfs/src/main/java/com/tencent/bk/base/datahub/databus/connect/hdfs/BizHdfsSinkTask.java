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


package com.tencent.bk.base.datahub.databus.connect.hdfs;

import com.tencent.bk.base.datahub.databus.connect.hdfs.storage.HdfsStorage;
import com.tencent.bk.base.datahub.databus.connect.hdfs.storage.Storage;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkTask;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

public class BizHdfsSinkTask extends BkSinkTask {

    private static final Logger log = LoggerFactory.getLogger(BizHdfsSinkTask.class);

    private BizHdfsSinkConfig config;

    private Map<TopicPartition, BizTopicPartitionWriter> topicPartitionWriters = new ConcurrentHashMap<>();
    private Storage storage;
    private Configuration conf;
    private BizDataPartitioner partitioner;
    // 构建一个日期（yyyyMMddHH0000）到对应时间戳的映射，最多包含12条记录（即12个小时的映射）
    private LRUCache<Long, Long> dateToTsCache = new LRUCache<>(12);
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * 启动hdfs sink任务,初始化资源
     */
    @Override
    public void startTask() {
        // 如果命令行参数传入了时区信息，则使用，否则使用机器默认的时区信息
        String tz = System.getProperty(EtlConsts.DISPLAY_TIMEZONE);
        if (tz != null) {
            dateFormat.setTimeZone(TimeZone.getTimeZone(tz));
        }

        config = new BizHdfsSinkConfig(configProperties);
        // 数据可能非常稀疏，即使没有消费到数据，也要触发processData的调用，使数据落盘
        skipProcessEmptyRecords = false;
        try {
            // 初始化hdfs相关资源
            initHdfs();
        } catch (Exception e) {
            cleanResourcesQuietly();
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR,
                    rtId + " Couldn't start hdfs sink task due to exception.", e);
            throw new ConnectException(rtId + " Couldn't start hdfs sink task due to exception.", e);
        }
    }

    /**
     * 停止运行,将一些使用到的资源释放掉。
     */
    @Override
    public void stopTask() {
        LogUtils.info(log, "{} HDFS sink task stopping", rtId);
        cleanResourcesQuietly();
        if (this.storage != null) {
            try {
                this.storage.close();
            } catch (IOException e) {
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_CLOSE_PARTITION_WRITER_ERR,
                        rtId + " Error closing storage.", e);
            }
        }

    }

    /**
     * 处理kafka中的消息,将数据写入hdfs中
     *
     * @param records kafka中的消息记录
     */
    @Override
    public void processData(Collection<SinkRecord> records) {
        // 消费kafka中的数据,通过数据转换,将转换后的数据放入buffer中等待写入hdfs
        final long now = System.currentTimeMillis() / 1000;  // 取秒
        final long tagTime = now / 60 * 60; // 取分钟对应的秒数
        String badMsg = "";
        int badMsgPar = 0;
        long badMsgOffset = 0L;

        long loopStart = System.currentTimeMillis();
        for (SinkRecord record : records) {
            // kafka connect框架在kafka发生抖动的时候，可能拉取到重复的消息，这里需要做处理。
            if (isRecordProcessed(record)) {
                continue;
            }
            String topic = record.topic();
            int partition = record.kafkaPartition();
            TopicPartition tp = new TopicPartition(topic, partition);

            BizTopicPartitionWriter writer = topicPartitionWriters.get(tp);
            if (writer != null) {
                ParsedMsg msg = new ParsedMsg(record, converter);
                msgCountTotal++;
                msgSizeTotal += msg.getMsgSize();

                writer.buffer(msg);

                markRecordProcessed(record); // 标记此条消息已被处理过
                // 上报打点数据
                try {
                    Metric.getInstance().updateStat(rtId, config.connector, msg.getTopic(), "hdfs", msg.getCount(),
                            msg.getMsgSize(), msg.getTag(), getMetricTag(msg.getDateTime()));
                    if (msg.getErrors().size() > 0) {
                        Metric.getInstance().updateTopicErrInfo(config.connector, msg.getErrors());
                        badMsg = msg.getFailedResult();
                        badMsgPar = record.kafkaPartition();
                        badMsgOffset = record.kafkaOffset();
                    }
                    setDelayTime(msg.getTagTime(), tagTime);
                    Metric.getInstance().updateDelayInfo(config.connector, now, maxDelayTime, minDelayTime);
                } catch (Exception ignore) {
                    LogUtils.warn(log, "failed to update stat info for {}, just ignore this. {}", rtId,
                            ignore.getMessage());
                }
            } else {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.HDFS_NO_PARTITION_WRITER, log,
                        "{} Can not find tp writer while putting record to buffer, partition: {}", rtId, tp);
                // 抛出异常,因为无法处理对应partiton的数据,无法写入到hdfs中 TODO
                cleanResourcesQuietly();
                throw new ConnectException(rtId + " Null partition writer for " + tp);
            }
        }

        // 每批数据仅打印一条数据处理异常的消息
        if (StringUtils.isNoneBlank(badMsg)) {
            LogUtils.warn(log, "{} Bad Message: {}-{}-{}, {}", rtId, tagTime, badMsgPar, badMsgOffset, badMsg);
        }

        // 重置延迟计时
        resetDelayTimeCounter();

        try {
            // 调用数据写入hdfs逻辑，触发数据定期刷新到WAL中
            Set<TopicPartition> assignment = new HashSet<>(context.assignment());
            for (TopicPartition tp : assignment) {
                BizTopicPartitionWriter writer = topicPartitionWriters.get(tp);
                writer.write(false); // 将数据写入到hdfs中(hdfs temp file)
            }
        } catch (ConnectException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_ERR,
                    rtId + " got a exception while writing HDFS temp file", e);
            // TODO 可能需要优化异常处理逻辑
            cleanResourcesQuietly();
            throw e;
        } finally {
            long end = System.currentTimeMillis();
            if (end - loopStart > 30000) {
                LogUtils.warn(log, "{} TIMEOUT: it takes {}ms to handle {} message(s)", rtId, end - loopStart,
                        records.size());
            }
        }
    }

    /**
     * consumer监听事件,当有partitions分配时被触发。
     * 根据分配的partitions来创建对应的写入对象并恢复到上次的offset。
     *
     * @param partitions 分配的topic的分区partitions
     */
    @Override
    public void open(Collection<TopicPartition> partitions) {
        LogUtils.info(log, "{} open partitions: {}", rtId, partitions);
        for (TopicPartition tp : partitions) {
            BizTopicPartitionWriter topicPartitionWriter = new BizTopicPartitionWriter(tp, storage, partitioner, config,
                    context);
            topicPartitionWriters.put(tp, topicPartitionWriter);
            LogUtils.info(log, "{} open tp {} and start recover", config.rtId, tp);
            // We need to immediately start recovery to ensure we pause consumption of messages for the
            // assigned topics while we try to recover offsets and rewind.
            topicPartitionWriters.get(tp).recoverWithRetry();
        }
    }

    /**
     * 当发生topic的分区rebalance时,关闭对应的所有的writer
     *
     * @param partitions 分配的topic分的区列表
     */
    @Override
    public void close(Collection<TopicPartition> partitions) {
        LogUtils.info(log, "{} close partitions: {}", rtId, partitions);
        closeWriters();
    }

    /**
     * 关闭task使用的一些资源
     */
    private void cleanResourcesQuietly() {
        // 这里的调用可能是重复调用，为确保writer全部被关闭
        closeWriters();
    }

    /**
     * 关闭所有的recordwriter对象，清理资源
     */
    private void closeWriters() {
        // Close any writers we have. We may get assigned the same partitions and end up duplicating
        // some effort since we'll have to reprocess those messages. It may be possible to hold on to
        // the TopicPartitionWriter and continue to use the temp file, but this can get significantly
        // more complex due to potential failures and network partitions. For example, we may get
        // this close, then miss a few generations of group membership, during which
        // data may have continued to be processed and we'd have to restart from the recovery stage,
        // make sure we apply the Wal, and only reuse the temp file if the starting offset is still
        // valid. For now, we prefer the simpler solution that may result in a bit of wasted effort.
        LogUtils.info(log, "{} going to close tp writers for {}", rtId, context.assignment());
        for (TopicPartition tp : context.assignment()) {
            try {
                BizTopicPartitionWriter writer = topicPartitionWriters.remove(tp);
                if (writer != null) {
                    writer.close();
                } else {
                    LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.HDFS_NO_PARTITION_WRITER, log,
                            "{} Can not find tp writer while trying to close data writer, partition: {} assignment: {}",
                            rtId, tp, context.assignment());
                }
            } catch (ConnectException e) {
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_CLOSE_PARTITION_WRITER_ERR,
                        rtId + " Error closing writer for " + tp.toString(), e);
            }
        }
        topicPartitionWriters.forEach((p, w) -> {
            if (w != null) {
                w.close();
            } else {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.HDFS_NO_PARTITION_WRITER, log,
                        "{} Can not find tp writer while trying to close data writer, partition: {} assignment: {}",
                        rtId, p, context.assignment());
            }
        });
        topicPartitionWriters.clear();
    }

    /**
     * 初始化hdfs相关资源
     */
    private void initHdfs() {
        try {
            LogUtils.info(log, "{} Hadoop configuration directory: {} hdfsUrl: {}", rtId, config.hdfsConfDir,
                    config.hdfsUrl);
            conf = new Configuration();

            // 当hdfs配置目录为空时，不使用hdfs-site.xml和hdfs-core.xml配置文件链接hdfs集群
            if (StringUtils.isNotBlank(config.hdfsConfDir)) { // 添加hdfs相关配置文件
                conf.addResource(new Path(config.hdfsConfDir + "/core-site.xml"));
                conf.addResource(new Path(config.hdfsConfDir + "/hdfs-site.xml"));
            }
            conf.setBoolean("fs.automatic.close", false);

            // 使用hdfs任务的custom properties来设置链接配置项
            for (Map.Entry<String, Object> entry : config.customProperties.entrySet()) {
                // 按照属性值的类型设置hdfs conf对象
                conf.set(entry.getKey(), entry.getValue().toString());
            }

            LogUtils.info(log, "{} create hdfs storage and partitioner instance, and create directories on hdfs", rtId);
            storage = createStorage();
            partitioner = createPartitioner();

            createDir(config.topicsDir);
            createDir(config.topicsDir + HdfsConsts.TEMPFILE_DIRECTORY);
            createDir(config.logsDir);

            for (TopicPartition tp : context.assignment()) {
                BizTopicPartitionWriter topicPartitionWriter = new BizTopicPartitionWriter(tp, storage, partitioner,
                        config, context);
                topicPartitionWriters.put(tp, topicPartitionWriter);
            }
            LogUtils.info(log, "{} BizDataWriter initiation is done", rtId);
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_ERR,
                    rtId + " create directories on hdfs failed!", e);
            throw new ConnectException(e);
        }
    }

    /**
     * 在hdfs上创建目录
     *
     * @param dir 目录名称
     * @throws IOException 异常
     */
    private void createDir(String dir) throws IOException {
        String path = config.hdfsUrl + "/" + dir;
        if (!storage.exists(path)) {
            storage.mkdirs(path);
        }
    }

    /**
     * 根据config中的配置,创建hdfs的存储对象并返回。
     *
     * @return hdfs存储对象实例
     */
    private Storage createStorage() {
        try {
            return new HdfsStorage(conf, config.hdfsUrl);
        } catch (IOException e) {
            throw new ConnectException("Failed to connect HDFS " + config.hdfsUrl, e);
        }
    }

    /**
     * 根据HdfsSinkConnector配置文件中设置的分区class来生成对应的分区实例。
     *
     * @return 分区实例。
     */
    private BizDataPartitioner createPartitioner() {
        return new BizDataPartitioner();
    }

    /**
     * 根据数据的时间（yyyyMMddHHmmss）来构建metric tag
     *
     * @param datetime 日期
     * @return 用于打点数据核对的metric tag
     */
    private String getMetricTag(long datetime) {
        // 将分钟和秒去掉，变成整数小时
        long key = datetime / 10000 * 10000;
        Long ts = dateToTsCache.get(key);
        if (ts == null) {
            Date date = dateFormat.parse(Long.toString(key), new ParsePosition(0));
            ts = date.getTime() / 1000; // 将毫秒转换为秒
            dateToTsCache.put(key, ts);
        }

        return rtId + "|" + ts;
    }

}
