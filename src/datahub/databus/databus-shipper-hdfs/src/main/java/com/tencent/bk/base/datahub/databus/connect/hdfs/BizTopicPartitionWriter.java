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

import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.hdfs.storage.Storage;
import com.tencent.bk.base.datahub.databus.connect.hdfs.writer.RecordWriterJson;
import com.tencent.bk.base.datahub.databus.connect.hdfs.writer.RecordWriterParquet;
import com.tencent.bk.base.datahub.databus.connect.hdfs.wal.Wal;
import com.tencent.bk.base.datahub.databus.connect.hdfs.writer.RecordWriter;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BizTopicPartitionWriter {

    private static final Logger log = LoggerFactory.getLogger(BizTopicPartitionWriter.class);
    private static final Map<String, String> DATA_PATH_TO_DATA_FORMAT = new ConcurrentHashMap<>();
    private static final Map<String, Long> DATA_PATH_TO_TOUCH_TIME = new ConcurrentHashMap<>();
    // 定期清理数据
    private static final ScheduledThreadPoolExecutor EXEC = new ScheduledThreadPoolExecutor(1);
    // hdfs日期元数据缓存
    private LRUCache<Long, Boolean> hdfsMaintainDateCache = new LRUCache<>(365);
    private long lastHdfsMaintainFailedDate = 0;

    static {
        EXEC.scheduleAtFixedRate(() -> {
            try {
                Set<String> dataPaths = new HashSet<>(DATA_PATH_TO_TOUCH_TIME.keySet());
                long now = System.currentTimeMillis();
                for (String dataPath : dataPaths) {
                    long touchTime = DATA_PATH_TO_TOUCH_TIME.get(dataPath);
                    if (now - touchTime > 3600000) { // 超过一个小时未被访问
                        DATA_PATH_TO_TOUCH_TIME.remove(dataPath);
                        DATA_PATH_TO_DATA_FORMAT.remove(dataPath);
                    }
                }
                LogUtils.info(log, "active dataPaths are {}", DATA_PATH_TO_TOUCH_TIME.toString());
            } catch (Exception e) {
                LogUtils.warn(log, "failed to clean outdated dataPath entries! {}", e.getMessage());
            }
        }, 0, 1, TimeUnit.HOURS);
    }

    private Wal wal;
    private Map<String, String> tempFiles;
    private Map<String, RecordWriter> writers;
    private TopicPartition tp;
    private BizDataPartitioner partitioner;
    private String url;
    private String topicsDir;
    private State state;
    private Queue<ParsedMsg> buffer;
    private boolean recovered;
    private Storage storage;
    private SinkTaskContext context;
    private int recordCounter;
    private int flushSize;
    private long multiPathsStartTime;
    private long rotateIntervalMs;
    private long multiPathsRotateIntervalMs;
    private long lastRotate;
    private Configuration conf;
    private Set<String> appended;
    private long offset;
    private long firstOffset;
    private boolean sawInvalidOffset;
    private Map<String, Long> startOffsets;
    private Map<String, Long> offsets;
    private long timeoutMs;
    private long failureTime;
    private final String zeroPadOffsetFormat;
    private final String connector;
    private final String clusterName;
    private Schema lastSchema;

    private String tableName;
    private String bizId;
    private String rtId;

    // 连续3个错误作为阈值,每次recover过程中错误直接等待20s后再重试
    private int retryTimes = 0;
    private int retryLimit = 3;
    private long sleepIntervalMs = 20000L;

    // hdfs metadata
    private Map<String, String> lastDataTime; // 这里记录的是encodedPartition的时间
    private Map<String, List<String>> lastCommittedDataTime; // 这里记录的时间同上，只不过是使用的commitedfile作为key

    private HdfsMeta hdfsMeta;

    private ParsedMsg lastMsg = null;


    public BizTopicPartitionWriter(TopicPartition tp, Storage storage, BizDataPartitioner partitioner,
            BizHdfsSinkConfig config, SinkTaskContext context) {
        this.tp = tp;
        this.context = context;
        this.storage = storage;
        this.partitioner = partitioner;
        this.url = storage.url();
        this.conf = storage.conf();

        // 从配置文件中获取connector的名称，便于打点上报数据
        clusterName = config.cluster;
        connector = config.connector;
        topicsDir = config.topicsDir;
        recordCounter = 0;
        flushSize = config.flushSize;
        rotateIntervalMs = config.rotateIntervalMs;
        multiPathsRotateIntervalMs = config.multiPathsRotateIntervalMs;
        lastRotate = System.currentTimeMillis();
        timeoutMs = config.retryBackoff;
        tableName = config.tableName;
        bizId = config.bizId + "";
        rtId = config.rtId;
        wal = storage.wal(config.logsDir, tp);

        this.hdfsMeta = new HdfsMeta();

        buffer = new LinkedList<>();
        writers = new HashMap<>();
        tempFiles = new HashMap<>();
        appended = new HashSet<>();
        startOffsets = new HashMap<>();
        offsets = new HashMap<>();
        lastDataTime = new HashMap<>();
        lastCommittedDataTime = new HashMap<>();
        state = State.RECOVERY_STARTED;
        failureTime = -1L;
        offset = -1L;
        firstOffset = -1L;
        sawInvalidOffset = false;
        zeroPadOffsetFormat = "%0" + config.zeroPadWidth + "d";
        multiPathsStartTime = 0L;
    }

    // the state enum for partition writer
    private enum State {
        RECOVERY_STARTED,
        RECOVERY_PARTITION_PAUSED,
        WAL_APPLIED,
        WAL_TRUNCATED,
        OFFSET_RESET,
        WRITE_STARTED,
        WRITE_PARTITION_PAUSED,
        SHOULD_ROTATE,
        TEMP_FILE_CLOSED,
        WAL_APPENDED,
        FILE_COMMITTED;

        private static State[] vals = values();

        public State next() {
            return vals[(this.ordinal() + 1) % vals.length];
        }
    }

    /**
     * 尝试从wal中恢复，会多次重试，连续失败后，抛出异常，使整个任务失败
     */
    public void recoverWithRetry() {
        // hdfs租约Lease的soft limit是60s，这里最多重试3次，在每次重试过程中，timeout为1分钟。重试间隔30s，如果仍然失败，则抛出异常
        for (int i = 0; i < retryLimit; i++) {
            try {
                recover();
                return;
            } catch (Exception e) {
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_WAL_RECOVERY_FAIL,
                        rtId + " Recovery failed at state " + state, e);
                try {
                    Thread.sleep(sleepIntervalMs);
                } catch (InterruptedException ie) {
                    throw new ConnectException(ie);
                }
            }
        }
        // 删除WAL文件，此时hdfs上的WAL文件可能已损坏，无法恢复，导致无法通过重试机制继续数据处理流程
        deleteWal();
        String msg = rtId + " failed to recover for many times! " + tp;
        Metric.getInstance().reportEvent(Thread.currentThread().getName(), Consts.TASK_START_FAIL, "", msg);
        // 多次recover均失败,在此处抛出异常中断任务,等待外部重启任务执行
        throw new ConnectException(msg);
    }

    /**
     * 按照当前状态顺序进行恢复操作。
     */
    @SuppressWarnings("fallthrough")
    public void recover() {
        switch (state) {
            case RECOVERY_STARTED:
                LogUtils.info(log, "{} Started recovery for topic partition {}", rtId, tp);
                pause();
                nextState();
            case RECOVERY_PARTITION_PAUSED:
                applyWal();
                nextState();
            case WAL_APPLIED:
                truncateWal();
                nextState();
            case WAL_TRUNCATED:
                resetOffsets();
                nextState();
            case OFFSET_RESET:
                resume();
                nextState();
                LogUtils.info(log, "{} Finished recovery for topic partition {}", rtId, tp);
                break;
            default:
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.HDFS_INVALID_WAL_STATE, log,
                        "{} not a valid state to write record for topic partition. state: {} partition: {}", rtId,
                        state, tp);
        }
    }

    /**
     * 写入数据
     */
    @SuppressWarnings("fallthrough")
    public void write(boolean forceRotate) {
        long now = System.currentTimeMillis();
        if (failureTime > 0) {
            if (failureTime + timeoutMs > now) {
                LogUtils.info(log, "{} going to sleep {}ms as last commit is failure", rtId,
                        failureTime + timeoutMs - now);
                // 保证写失败后，过了timeout时间后，才再次写操作
                try {
                    Thread.sleep(failureTime + timeoutMs - now);
                } catch (InterruptedException ignore) {
                    // ignore
                }
            }
            // 重置failureTime
            failureTime = -1L;
        }

        if (state.compareTo(State.WRITE_STARTED) < 0) {
            // 处于恢复状态，先恢复，再执行写的逻辑
            LogUtils.warn(log, "{} state {} is before WRITE_STARTED, need to recover first!", rtId, state);
            // recover 失败时，会抛出异常中断整个任务
            recover();
        }

        // 关闭partitionWriter时，设置强制刷新标识
        if (forceRotate) {
            if (buffer.isEmpty() && state == State.WRITE_STARTED) {
                LogUtils.info(log, "{} force rotate! buffer size {}, temp files {}", rtId, buffer.size(),
                        tempFiles.toString());
                pause();
                setState(State.SHOULD_ROTATE);
            } else {
                LogUtils.warn(log, "{} buffer size {}, state {}, unable to force rotate! temp files {}", rtId,
                        buffer.size(), state, tempFiles.toString());
                return;
            }
        } else {
            if (buffer.isEmpty() && state == State.WRITE_STARTED) {
                if (shouldRotate(now)) {
                    forceRotate = true;
                    setState(State.SHOULD_ROTATE);
                }
            }
        }

        while (!buffer.isEmpty() || forceRotate) {
            try {
                switch (state) {
                    case WRITE_STARTED:
                        pause();
                        nextState();
                    case WRITE_PARTITION_PAUSED:
                        ParsedMsg msg = buffer.peek();
                        if (msg != null) {
                            // 比较schema，如果schema有变化，则强制rotate，将数据写入文件中(可能存在异常数据，其avroValues为null)
                            Schema currentSchema =
                                    msg.getAvroValues() == null ? lastSchema : msg.getAvroValues().getSchema();
                            if (lastSchema == null) {
                                lastSchema = currentSchema;
                            }
                            if (lastSchema == null || lastSchema.equals(currentSchema) || currentSchema == null) {
                                writeRecord(msg);
                                // 将已写入成功的数据从队列上移除
                                buffer.poll();
                                lastMsg = msg;

                                // 当收到一条总线事件消息时，或者达到commit文件的阈值时，进入下一状态，commit文件
                                if (lastMsg.getIsDatabusEvent() || shouldRotate(now)) {
                                    LogUtils.info(log,
                                            "{} Starting commit and rotation for topic partition {} with start "
                                                    + "offsets {} and end offsets {}. DatabusEvent: {}",
                                            rtId, tp, startOffsets, offsets, lastMsg.getIsDatabusEvent());
                                    nextState(); // Fall through and try to rotate immediately
                                } else {
                                    break;
                                }
                            } else {
                                // 数据的schema发生变化，强制写文件
                                LogUtils.info(log, "{} schema changed from {} to {}", rtId, lastSchema, currentSchema);
                                lastSchema = null;
                                nextState();
                            }
                        }
                    case SHOULD_ROTATE:
                        if (forceRotate) {
                            LogUtils.info(log, "{} Force to rotate for {}", rtId, tp);
                            if (!tempFiles.isEmpty()) {
                                LogUtils.info(log,
                                        "{} Starting commit and rotation for topic partition {} with start offsets {}"
                                                + " and end offsets {}",
                                        rtId, tp, startOffsets, offsets);
                            } else {
                                //没有数据任务空跑，这里需要维护打点信息
                                long reportOffset = offset == -1 ? -1 : offset - 1;
                                LogUtils.info(log, "{} Reporting offset {} of {}", rtId, reportOffset, tp);
                            }
                            forceRotate = false;
                        }
                        lastRotate = System.currentTimeMillis();
                        closeTempFiles();
                        nextState();
                    case TEMP_FILE_CLOSED:
                        appendToWal();
                        nextState();
                    case WAL_APPENDED:
                        commitFiles();
                        nextState();
                    case FILE_COMMITTED:
                        setState(State.WRITE_PARTITION_PAUSED);
                        break;
                    default:
                        LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.HDFS_INVALID_WAL_STATE, log,
                                "{} not a valid state to write record for topic partition. state: {} partition: {}",
                                rtId, state, tp);
                }

                retryTimes = 0;
            } catch (IOException e) {
                // TODO 如果是IOException,则认为和hdfs之间的连接发生异常,需要重新打开文件建立连接 (e.g. java.nio.channels.ClosedChannelException)
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_HANDLE_MSG_ERR,
                        String.format("%s Exception on %s, state: %s buffer size: %s", rtId, tp, state, buffer.size()),
                        e);
                failureTime = System.currentTimeMillis();
                setRetryTimeout(timeoutMs);
                if (retryTimes++ >= retryLimit) {
                    LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_REACH_TASK_RESTART_LIMIT,
                            rtId + " write: too many retries, threshold is " + retryLimit, e);
                    throw new ConnectException(e);
                }
                // 进入下一轮循环
                break;
            }
        }
        if (buffer.isEmpty()) {
            resume();
            state = State.WRITE_STARTED;
        }
    }

    /**
     * 关闭数据写入,清理资源
     *
     * @throws ConnectException 异常
     */
    public void close() throws ConnectException {
        // 强制rotate，将临时文件的数据刷新到正式目录中
        try {
            write(true);
        } catch (ConnectException ignore) {
            LogUtils.warn(log, rtId + " failed to force rotate hdfs files!", ignore);
        }
        LogUtils.info(log, "{} Closing TopicPartitionWriter {}", rtId, tp);
        try {
            for (Map.Entry<String, String> entry : tempFiles.entrySet()) {
                if (writers.containsKey(entry.getKey())) {
                    LogUtils.info(log, "{} Discarding in progress tempfile {} for {} {}", rtId,
                            entry.getValue(), tp, entry.getKey());
                    try {
                        closeTempFile(entry.getKey());
                    } catch (IOException | NullPointerException ioe) {
                        LogUtils.warn(log, String.format("%s failed to process temp file %s", rtId,
                                entry.getValue()), ioe);
                    } catch (IllegalArgumentException e) {
                        if (!HdfsConsts.CLOSE_EXCEPTION.equalsIgnoreCase(e.getMessage())) {
                            throw e;
                        }
                    }
                    // 避免close临时文件时抛出异常，导致删除临时文件未被调用
                    deleteTempFile(entry.getKey());
                }
            }
        } finally {
            writers.clear();
            try {
                wal.close();
            } catch (ConnectException e) {
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_CLOSE_WAL_ERR,
                        "Error closing " + wal.getLogFile(), e);
                throw new ConnectException(rtId + " Error closing writer: " + e.getMessage());
            } finally {
                startOffsets.clear();
                offsets.clear();
                lastDataTime.clear();
                lastCommittedDataTime.clear();
            }
        }
    }

    /**
     * 将数据放入buffer中
     *
     * @param parsedMsg 处理后的kafka记录
     */
    public void buffer(ParsedMsg parsedMsg) {
        buffer.add(parsedMsg);
    }

    /**
     * 获取目录名称
     *
     * @param encodedPartition 编码的分区名称
     * @return 目录名称
     */
    private String getDirectory(String encodedPartition) {
        return encodedPartition;
    }

    /**
     * 进入下一个状态
     */
    private void nextState() {
        state = state.next();
    }

    /**
     * 设置状态
     *
     * @param state 状态
     */
    private void setState(State state) {
        this.state = state;
    }

    /**
     * 是否需要rotate
     *
     * @param now 当前时间戳
     * @return 是否需要轮转
     */
    private boolean shouldRotate(long now) {
        if (recordCounter >= flushSize) {
            return true;
        } else if (rotateIntervalMs <= 0) {
            return false;
        } else {
            if (tempFiles.keySet().size() > 1) {
                if (multiPathsStartTime == 0L) {
                    multiPathsStartTime = System.currentTimeMillis(); // 数据跨时间分区了，首次发现需要写入多个目录中，记录当前时间
                    return false;
                } else {
                    // 数据跨小时时间分区了，按照配置时间定期commit一次，避免数据乱序生成多个碎文件
                    boolean rotate = (now - multiPathsStartTime >= multiPathsRotateIntervalMs);
                    if (rotate) {
                        multiPathsStartTime = 0L; // 重置发现多个目录需要写入的时间点
                    }
                    return rotate;
                }
            } else {
                return now - lastRotate >= rotateIntervalMs;
            }
        }
    }

    /**
     * 暂停处理
     */
    private void pause() {
        context.pause(tp);
    }

    /**
     * 恢复处理
     */
    private void resume() {
        context.resume(tp);
    }


    /**
     * 读取offset值
     *
     * @throws ConnectException 异常
     */
    private void readOffset() throws ConnectException {
        // 存在两个版本的offset目录，如果新版的目录存在，则使用新版的，否则使用旧版的
        String offsetDir = FileUtils.getOffsetRecordDir(url, topicsDir, clusterName, connector, tp);
        try {
            if (storage.exists(offsetDir)) {
                offset = FileUtils.getMaxOffsetFromRecordDir(storage, offsetDir);
            } else {
                // 尝试从旧版的offset目录中恢复
                String oldOffsetDir = FileUtils.getOldOffsetRecordDir(url, topicsDir, clusterName, connector, tp);
                LogUtils.info(log, "{} offset dir {} doesn't exist, going to use try old offset dir {}", rtId,
                        offsetDir, oldOffsetDir);
                if (storage.exists(oldOffsetDir)) {
                    offset = FileUtils.getMaxOffsetFromRecordDir(storage, oldOffsetDir);
                }
            }
            LogUtils.info(log, "{} readOffset(): {} for {}-{} from {}", rtId, offset, tp.topic(), tp.partition(),
                    offsetDir);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    /**
     * 重置offset
     *
     * @throws ConnectException 异常
     */
    private void resetOffsets() throws ConnectException {
        if (!recovered) {
            readOffset();
            if (offset > 0) {
                LogUtils.info(log, "{} Resetting offset for {} to {}", rtId, tp, offset);
                context.offset(tp, offset);
            }
            recovered = true;
        }
    }

    /**
     * 获取writer
     *
     * @param encodedPartition 编码后的分区
     * @param containsJson 目标hdfs目录中是否包含json格式的文件
     * @return writer对象
     * @throws ConnectException 异常
     */
    private RecordWriter getWriter(String encodedPartition, boolean containsJson) throws ConnectException {
        if (writers.containsKey(encodedPartition)) {
            return writers.get(encodedPartition);
        } else {
            String tempFile = getTempFile(encodedPartition);
            LogUtils.info(log, "{} new temp file: {}", rtId, tempFile);
            // todo fix this
            RecordWriter writer = containsJson ? new RecordWriterJson(url, conf, tempFile)
                    : new RecordWriterParquet(url, conf, tempFile);
            writers.put(encodedPartition, writer);
            return writer;
        }
    }

    /**
     * 获取临时文件
     *
     * @param encodedPartition 编码后的分区
     * @return 临时文件
     */
    private String getTempFile(String encodedPartition) {
        String tempFile;
        if (tempFiles.containsKey(encodedPartition)) {
            tempFile = tempFiles.get(encodedPartition);
        } else {
            String directory = HdfsConsts.TEMPFILE_DIRECTORY + getDirectory(encodedPartition);
            tempFile = FileUtils.tempFileName(url, topicsDir, directory, getDataFileExtension(encodedPartition));
            tempFiles.put(encodedPartition, tempFile);
        }
        return tempFile;
    }

    /**
     * 应用WAL
     *
     * @throws ConnectException 异常
     */
    private void applyWal() throws ConnectException {
        if (!recovered) {
            wal.apply();
        }
    }

    /**
     * 截断WAL
     *
     * @throws ConnectException 异常
     */
    private void truncateWal() throws ConnectException {
        if (!recovered) {
            wal.truncate();
        }
    }

    /**
     * 删除wal文件
     *
     * @throws ConnectException 异常
     */
    private void deleteWal() throws ConnectException {
        if (!recovered) {
            wal.deleteBadLogFile();
        }
    }

    /**
     * 写kafka记录到文件中
     *
     * @param msg 处理后的kafka记录
     * @throws IOException 异常
     */
    private void writeRecord(ParsedMsg msg) throws IOException {
        // 当任务长时间停止后，重新启动，offset目录中记录的offset在kafka中可能已过期，此时拿到的
        // 第一条kafka消息中的offset会大于从offset目录中读取到的offset，这种情况需要重置offset的值
        if (firstOffset == -1) {
            firstOffset = msg.getOffset();
            if (firstOffset > offset) {
                LogUtils.info(log,
                        "{} partition {} first msg offset is {}, greater than the offset {} recovered from offset "
                                + "directory",
                        tp.partition(), rtId, firstOffset, offset);
                offset = firstOffset;
            }
        }

        long expectedOffset = offset + recordCounter;
        if (offset == -1) {
            // 没有从offset目录中找到上次消费的offset值
            offset = msg.getOffset();
        } else if (msg.getOffset() < expectedOffset) {
            // 当消费者来不及处理数据时，kafka topic可能被打爆，导致offset重置，这时候消息会不连续，但是消息的offset会大于预期的offset。
            // 只有当消息的offset小于预期的offset时，将此情况作为异常情况处理
            // Currently it's possible to see stale data with the wrong offset after a rebalance when you
            // rewind, which we do since we manage our own offsets. See KAFKA-2894.
            if (!sawInvalidOffset) {
                LogUtils.warn(log,
                        "{} Ignoring stale out-of-order record in {}-{}. Has offset {} instead of expected offset {}",
                        rtId, msg.getTopic(), msg.getPartition(), msg.getOffset(), expectedOffset);
            }
            sawInvalidOffset = true;
            return;
        }

        if (sawInvalidOffset) {
            LogUtils.info(log, "{} Recovered from stale out-of-order records in {}-{} with offset {}", rtId,
                    msg.getTopic(), msg.getPartition(), expectedOffset);
            sawInvalidOffset = false;
        }

        String encodedPartition = BizDataPartitioner.encodePartition(tableName, bizId, msg.getDateTime());
        boolean containsJson = containsJson(
                encodedPartition); // 即将写入数据的hdfs目录中是否包含json文件，如果包含，则用json格式写入，如果没有，用parquet格式写入

        GenericArray values = msg.getAvroValues();
        // 存在脏数据，其内部包含的values为null值
        if (values != null && values.size() > 0) {
            GenericRecord record = (GenericRecord) values.get(0);
            RecordWriter writer = getWriter(encodedPartition, containsJson);
            if (containsJson) {
                ((RecordWriterJson) writer).write(StringUtils.join(msg.getJsonResult(), "\n"));
            } else {
                ((RecordWriterParquet) writer).write(msg.getAvroValues());
            }
            String timeStr = record.get("dtEventTime").toString();
            lastDataTime.put(encodedPartition, timeStr);
        }

        // 记录当前消息的offset信息
        if (!startOffsets.containsKey(encodedPartition)) {
            startOffsets.put(encodedPartition, msg.getOffset());
        }
        offsets.put(encodedPartition, msg.getOffset());

        recordCounter++;
    }

    /**
     * 关闭临时文件
     *
     * @param encodedPartition 编码的分区
     */
    private void closeTempFile(String encodedPartition) throws IOException {
        if (writers.containsKey(encodedPartition)) {
            RecordWriter writer = writers.get(encodedPartition);
            writer.close();
            writers.remove(encodedPartition);
        } else {
            LogUtils.warn(log, "{} no writer found for {}, unable to close temp file!", rtId, encodedPartition);
        }
    }

    /**
     * 关闭临时文件
     */
    private void closeTempFiles() {
        for (Map.Entry<String, String> entry : tempFiles.entrySet()) {
            try {
                closeTempFile(entry.getKey());
            } catch (IOException | NullPointerException ioe) {
                LogUtils.warn(log,
                        String.format("%s failed to process temp file %s", rtId, entry.getValue()), ioe);
            } catch (IllegalArgumentException e) {
                if (!HdfsConsts.CLOSE_EXCEPTION.equalsIgnoreCase(e.getMessage())) {
                    throw e;
                }
            }
        }
    }

    /**
     * 追加到WAL
     *
     * @param encodedPartition 编码的分区
     * @throws IOException 异常
     */
    private void appendToWal(String encodedPartition) throws IOException {
        String tempFile = tempFiles.get(encodedPartition);
        if (appended.contains(tempFile)) {
            return;
        }
        if (!startOffsets.containsKey(encodedPartition)) {
            return;
        }
        long startOffset = startOffsets.get(encodedPartition);
        long endOffset = offsets.get(encodedPartition);
        String directory = getDirectory(encodedPartition);
        String committedFile = FileUtils.committedFileName(url, topicsDir, directory, tp,
                startOffset, endOffset,
                getDataFileExtension(encodedPartition),
                zeroPadOffsetFormat);
        String offsetRecordPath = getOffsetRecordPath(endOffset);

        wal.append(tempFile, committedFile);
        wal.append(Wal.offsetMaker, offsetRecordPath);
        wal.append(Wal.hdfsMetaMarker, committedFile + "##" + lastDataTime.get(encodedPartition) + "##" + rtId);
        File file = new File(committedFile);
        if (file.getParent() != null && storage.exists(file.getParent().replaceFirst("hdfs:/", "hdfs://") + "/_READ")) {
            lastCommittedDataTime.put(committedFile, Arrays.asList(lastDataTime.get(encodedPartition), "1"));
        } else {
            lastCommittedDataTime.put(committedFile, Arrays.asList(lastDataTime.get(encodedPartition), "0"));
        }

        LogUtils.info(log, "{} Wal append {}, {}", rtId, tempFile, committedFile);
        appended.add(tempFile);
    }

    /**
     * 追加到WAL
     *
     * @throws IOException 异常
     */
    private void appendToWal() throws IOException {
        if (!tempFiles.isEmpty()) {
            beginAppend();
            for (String encodedPartition : tempFiles.keySet()) {
                appendToWal(encodedPartition);
            }
            endAppend();
            recordOffset();
        }
    }

    /**
     * 开始追加
     */
    private void beginAppend() {
        if (!appended.contains(Wal.beginMarker)) {
            wal.append(Wal.beginMarker, "");
        }
    }

    /**
     * 结束追加
     */
    private void endAppend() {
        if (!appended.contains(Wal.endMarker)) {
            wal.append(Wal.endMarker, "");
        }
    }

    /**
     * 提交文件
     *
     * @throws IOException 异常
     */
    private void commitFiles() throws IOException {
        appended.clear();
        for (String encodedPartition : tempFiles.keySet()) {
            commitFile(encodedPartition);
        }
        tempFiles.clear();

        try {
            LogUtils.info(log, "{} submit hdfs metadata: {}", rtId, lastCommittedDataTime);
            hdfsMeta.submitMetadata(rtId, tp.partition(), lastCommittedDataTime);
        } catch (Exception e) {
            LogUtils.warn(log, "{} something wrong when submit hdfs metadata. {}", rtId, e.getMessage());
        }
        lastDataTime.clear();
        lastCommittedDataTime.clear();

        if (lastMsg != null && lastMsg.getIsDatabusEvent()) {
            // 调用总线的api接口，通知其收到的总线事件消息
            LogUtils.info(log, "{} got databus event msg {}", rtId, lastMsg.getKey());
            Map<String, String> eventMsg = Utils.parseDatabusEvent(lastMsg.getKey());
            boolean isSucc = HttpUtils.addDatabusStorageEvent(rtId, Consts.STORAGE_HDFS, Consts.TDW_FINISH_DIR,
                    eventMsg.get(Consts.TDW_FINISH_DIR));
            if (!isSucc) {
                LogUtils.warn(log, "{} failed to send databus hdfs storage event {}", rtId, eventMsg);
            }
            lastMsg = null; // 重置lastMsg，避免后续无新的msg时，频繁触发此接口调用
        }
    }

    /**
     * 提交文件
     *
     * @param encodedPartition 编码后的分区
     * @throws IOException 异常
     */
    private void commitFile(String encodedPartition) throws IOException {
        if (!startOffsets.containsKey(encodedPartition)) {
            return;
        }
        long startOffset = startOffsets.get(encodedPartition);
        long endOffset = offsets.get(encodedPartition);
        String tempFile = tempFiles.get(encodedPartition);
        String directory = getDirectory(encodedPartition);
        String committedFile = FileUtils.committedFileName(url, topicsDir, directory, tp,
                startOffset, endOffset,
                getDataFileExtension(encodedPartition),
                zeroPadOffsetFormat);

        String directoryName = FileUtils.directoryName(url, topicsDir, directory);
        if (!storage.exists(directoryName)) {
            // 确保HDFS元数据信息
            long date = BizDataPartitioner.parseDateFromPartition(encodedPartition);
            updateHDFSMaintainDate(date);
            storage.mkdirs(directoryName);
        }
        storage.commit(tempFile, committedFile);
        startOffsets.remove(encodedPartition);
        offsets.remove(encodedPartition);
        offset = offset + recordCounter;
        recordCounter = 0;
        LogUtils.info(log, "{} Committed {} for {}", rtId, committedFile, tp);
    }

    /**
     * 记录offset信息
     */
    private void recordOffset() {
        for (String encodedPartition : tempFiles.keySet()) {
            if (!encodedPartition.equals(BizDataPartitioner.badRecordPartition)) {
                long endOffset = offsets.get(encodedPartition);
                recordOffset(endOffset);
            }
        }
    }

    /**
     * 记录offset信息
     *
     * @param offset offset信息
     */
    private void recordOffset(long offset) {
        String offsetDir = FileUtils.getOffsetRecordDir(url, topicsDir, clusterName, connector, tp);
        String recordDirPath = getOffsetRecordPath(offset);

        LogUtils.info(log, "{} record offset {}", rtId, recordDirPath);
        try {
            storage.mkdirs(recordDirPath);
            FileUtils.cleanOffsetRecordFiles(storage, offset, offsetDir);
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_ERR,
                    rtId + " failed to record offset: " + recordDirPath, e);
        }
    }

    /**
     * 获取offset的路径
     *
     * @param offset offset信息
     * @return offset的路径
     */
    private String getOffsetRecordPath(long offset) {
        String offsetDir = FileUtils.getOffsetRecordDir(url, topicsDir, clusterName, connector, tp);
        String recordDirPath = offsetDir + "/" + offset;
        return recordDirPath;
    }

    /**
     * 删除临时文件
     *
     * @param encodedPartition 编码的分区
     */
    private void deleteTempFile(String encodedPartition) {
        storage.delete(tempFiles.get(encodedPartition));
    }

    /**
     * 设置重试超时时间
     *
     * @param timeoutMs 超时时间
     */
    private void setRetryTimeout(long timeoutMs) {
        context.timeout(timeoutMs);
    }

    /**
     * 判定文件夹中是否有json文件
     */
    private Boolean containsJson(String encodedPartition) throws IOException {
        DATA_PATH_TO_TOUCH_TIME.put(encodedPartition, System.currentTimeMillis()); // 更新dataPath使用的时间戳
        if (DATA_PATH_TO_DATA_FORMAT.containsKey(encodedPartition)) {
            return DATA_PATH_TO_DATA_FORMAT.get(encodedPartition).equals(HdfsConsts.JSON);
        } else {
            String path = FileUtils.directoryName(url, topicsDir, getDirectory(encodedPartition));
            if (storage.exists(path)) {
                LogUtils.info(log, "{} calling listStatus for {}", rtId, path);
                for (FileStatus fileStatus : storage.listStatus(path)) {
                    if (fileStatus.getPath().getName().contains(HdfsConsts.JSON_SUFFIX)) {
                        DATA_PATH_TO_DATA_FORMAT.put(encodedPartition, HdfsConsts.JSON);
                        return true;
                    }
                }
                // 目录中没有json格式的文件，应该写入parquet格式的文件
                DATA_PATH_TO_DATA_FORMAT.put(encodedPartition, HdfsConsts.PARQUET);
            }

            return false;
        }
    }

    /**
     * 获取数据后缀名称
     *
     * @param encodedPartition 编码的分区
     * @return 数据后缀名称
     */
    private String getDataFileExtension(String encodedPartition) {
        try {
            return containsJson(encodedPartition) ? HdfsConsts.JSON_SUFFIX : HdfsConsts.PARQUET_SUFFIX;
        } catch (Exception e) {
            return HdfsConsts.JSON_SUFFIX;
        }
    }

    /**
     * 更新HDFS元数据
     *
     * @param datetime 日期yyyyMMdd
     */
    private void updateHDFSMaintainDate(long datetime) {
        if (datetime <= 0) {
            return;
        }

        // 已存在则跳过
        if (hdfsMaintainDateCache.get(datetime) != null) {
            return;
        }

        // 最近1天默认会创建元数据, 记录下
        Date date;
        try {
            date = new SimpleDateFormat("yyyyMMdd").parse(String.valueOf(datetime));
        } catch (Exception e) {
            LogUtils.warn(log, "parse date failed, datetime={}", datetime);
            return;
        }

        // delta:相差天数
        long delta = TimeUnit.DAYS.convert(date.getTime() - new Date().getTime(), TimeUnit.MILLISECONDS);
        if (Math.abs(delta) > 1) {
            // 注册元数据
            if (!HttpUtils.updateHdfsMaintainDate(rtId, delta)) {
                if (lastHdfsMaintainFailedDate != datetime) {
                    LogUtils.warn(log, "{} update hdfs maintain date {} error!", rtId, datetime);
                    lastHdfsMaintainFailedDate = datetime;
                    return;
                }
                // 如果连续失败了2次了, 就不再调用了, 避免频繁大量错误请求
                LogUtils.warn(log, "{} update hdfs maintain date {} failed twice, not update again!", rtId, date);
            }
        }
        LogUtils.info(log, "{} update hdfs maintain date {}", rtId, datetime);
        hdfsMaintainDateCache.put(datetime, true);
    }

}
