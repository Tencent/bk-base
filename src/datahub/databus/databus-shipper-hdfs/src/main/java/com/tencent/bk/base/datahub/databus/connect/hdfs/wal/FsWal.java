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

package com.tencent.bk.base.datahub.databus.connect.hdfs.wal;

import com.tencent.bk.base.datahub.databus.connect.hdfs.FileUtils;
import com.tencent.bk.base.datahub.databus.connect.hdfs.HdfsMeta;
import com.tencent.bk.base.datahub.databus.connect.hdfs.storage.Storage;
import com.tencent.bk.base.datahub.databus.connect.hdfs.wal.WalFile.Reader;
import com.tencent.bk.base.datahub.databus.connect.hdfs.wal.WalFile.Writer;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FsWal implements Wal {

    private static final Logger log = LoggerFactory.getLogger(FsWal.class);
    private static final long MAX_SLEEP_INTERVAL_MS = 16000L;
    private static final long[] SLEEP_INTERVAL_ARR = {4000, 8000, 16000, 32000};

    private WalFile.Writer writer = null;
    private WalFile.Reader reader = null;
    private String logFile = null;
    private Configuration conf = null;
    private Storage storage = null;
    private TopicPartition tp = null;

    public FsWal(String logsDir, TopicPartition topicPart, Storage storage)
            throws ConnectException {
        this.storage = storage;
        this.conf = storage.conf();
        this.tp = topicPart;
        String url = storage.url();
        logFile = FileUtils.logFileName(url, logsDir, topicPart);
    }

    /**
     * 追加文件
     *
     * @param tempFile 临时文件
     * @param committedFile 提交的文件
     * @throws DataException 异常
     */
    @Override
    public void append(String tempFile, String committedFile) throws ConnectException {
        try {
            acquireLease();
            WalEntry key = new WalEntry(tempFile);
            WalEntry value = new WalEntry(committedFile);
            writer.append(key, value);
            writer.hsync();
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.APPEND_WAL_ERR,
                    "Error appending WAL file: " + logFile, e);
            close();
            throw new DataException(e);
        }
    }

    /**
     * 获取租借锁
     *
     * @throws ConnectException 异常
     */
    public void acquireLease() throws ConnectException {
        // 当发生异常时,可以重试多次,每次时间间隔增大,其他异常直接抛出ConnectException
        IOException exception = null;
        if (writer == null) {
            for (int i = 0; i <= SLEEP_INTERVAL_ARR.length; i++) {
                try {
                    writer = WalFile.createWriter(conf, Writer.file(new Path(logFile)), Writer.appendIfExists(true));
                    LogUtils.info(log, "{} Successfully acquired lease for {}", tp, logFile);

                    // 成功获取文件lease,调用返回
                    return;
                } catch (IOException e) {
                    LogUtils.warn(log, String.format("%s Cannot acquire lease on Wal %s", tp, logFile), e);
                    if (i < SLEEP_INTERVAL_ARR.length) {
                        try {
                            Thread.sleep(SLEEP_INTERVAL_ARR[i]);
                        } catch (InterruptedException ie) {
                            throw new ConnectException(ie);
                        }
                    }
                    exception = e; // 记录当时发生的异常
                }
            }
        }

        if (exception != null) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_ACQUIRE_LEASE_TIMEOUT,
                    tp + " Cannot acquire lease after timeout, will retry.", exception);
            throw new ConnectException(exception);
        }
    }

    /**
     * 应用wal文件
     *
     * @throws ConnectException 异常
     */
    @Override
    public void apply() throws ConnectException {
        try {
            if (!storage.exists(logFile)) {
                return;
            }

            // 从wal文件中恢复时,必须目录中只有一个log文件,并且log文件内容非空
            FileStatus[] statuses = storage.listStatus(logFile);
            if (statuses.length != 1) {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.HDFS_WAL_RECOVERY_FAIL, log,
                        "{} Expected one log file at path: {}. Found multiple files {}.", tp, logFile, statuses.length);
                throw new AssertionError(
                        String.format("%s Expected one log file at path: %s. Found multiple files %d.", tp, logFile,
                                statuses.length));
            }

            // 执行到这里时，肯定只有一个log文件
            if (statuses[0].getLen() == 0) {
                LogUtils.warn(log, String.format("%s Found empty log file at path: %s", tp, logFile));
                return;
            }

            acquireLease();
            if (reader == null) {
                reader = new WalFile.Reader(conf, Reader.file(new Path(logFile)));
            }
            Map<WalEntry, WalEntry> entries = new HashMap<>();
            WalEntry key = new WalEntry();
            WalEntry value = new WalEntry();

            Map<String, Long> offsetMap = new HashMap<String, Long>();

            LogUtils.info(log, "{} Starting apply Wal {}", tp, logFile);
            Map<String, List<String>> lastDataTime = new HashMap<>();
            String rtId = null;

            while (reader.next(key, value)) {
                String keyName = key.getName();
                if (keyName.equals(beginMarker)) {
                    entries.clear();
                } else if (keyName.equals(endMarker)) {
                    for (Map.Entry<WalEntry, WalEntry> entry : entries.entrySet()) {
                        String tempFile = entry.getKey().getName();
                        String committedFile = entry.getValue().getName();

                        if (tempFile.equals(offsetMaker)) {
                            putIfBiggerOffset(offsetMap, committedFile);
                        } else if (tempFile.equals(hdfsMetaMarker)) {
                            String[] parts = committedFile.split("##");
                            String path = parts[0];
                            String dataTime = parts[1];
                            rtId = parts[2];
                            if (!storage.exists(path)) {
                                File file = new File(path);
                                if (file.getParent() != null && storage
                                        .exists(file.getParent().replaceFirst("hdfs:/", "hdfs://")
                                                + "/_READ")) {
                                    lastDataTime.put(path, Arrays.asList(dataTime, "1"));
                                } else {
                                    lastDataTime.put(path, Arrays.asList(dataTime, "0"));
                                }
                            }
                        } else {
                            if (!storage.exists(committedFile)) {
                                storage.commit(tempFile, committedFile);
                            }
                        }
                    }
                } else {
                    WalEntry mapKey = new WalEntry(key.getName());
                    WalEntry mapValue = new WalEntry(value.getName());
                    entries.put(mapKey, mapValue);
                }
            }

            if (lastDataTime.size() > 0) {
                HdfsMeta hdfsMeta = new HdfsMeta();
                try {
                    LogUtils.info(log, "{} submit hdfs metadata while recovering: {}", rtId, lastDataTime);
                    hdfsMeta.submitMetadata(rtId, tp.partition(), lastDataTime);
                } catch (Exception e) {
                    LogUtils.warn(log, "{} something wrong when submit hdfs metadata {}", rtId, e.getMessage());
                }
            }

            for (Map.Entry<String, Long> entry : offsetMap.entrySet()) {
                String offsetFilePath = entry.getKey() + "/" + entry.getValue();
                if (!storage.exists(offsetFilePath)) {
                    storage.mkdirs(offsetFilePath);
                    LogUtils.info(log, "{} Wal commit offset file {}", rtId, offsetFilePath);
                }
            }

            LogUtils.info(log, "{} Applied Wal {}", rtId, logFile);
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_APPLY_WAL_ERR,
                    String.format("%s Error applying WAL file: %s", tp.toString(), logFile), e);
            close();
            throw new DataException(e);
        }
    }

    /**
     * 如果offset记录文件比之前记录的hash中的更大，更新
     */
    private void putIfBiggerOffset(Map<String, Long> offsetMap, String offsetFilePath) {
        Path path = new Path(offsetFilePath);
        if (path.getParent() == null) {
            return;
        }

        String partitionDir = path.getParent().toString();
        Long offset = Long.parseLong((new Path(offsetFilePath)).getName());

        if (offsetMap.containsKey(partitionDir)) {
            if (offsetMap.get(partitionDir) < offset) {
                offsetMap.put(partitionDir, offset);
            }
        } else {
            offsetMap.put(partitionDir, offset);
        }
    }

    /**
     * 将当前wal文件改名,后续可以新建wal文件并写入处理进度
     *
     * @throws ConnectException 异常
     */
    @Override
    public void truncate() throws ConnectException {
        try {
            String oldLogFile = logFile + ".1";
            storage.delete(oldLogFile);
            storage.commit(logFile, oldLogFile);
        } finally {
            close();
        }
    }

    /**
     * 当wal文件已损坏时，调用此方法将其移除，避免多次重试均无法恢复的问题。
     *
     * @throws ConnectException 异常
     */
    @Override
    public void deleteBadLogFile() throws ConnectException {
        try {
            // 将wal文件和之前的wal日志文件均删除
            LogUtils.info(log, "{} going to delete wal file {} {}.1", tp.toString(), logFile, logFile);
            storage.delete(logFile);
            storage.delete(logFile + ".1");
        } finally {
            close();
        }
    }

    /**
     * 关闭
     *
     * @throws ConnectException 异常
     */
    @Override
    public void close() throws ConnectException {
        IOException ioe = null;
        try {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    ioe = e;
                }
            }
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    ioe = e;
                }
            }
            LogUtils.info(log, "{} reader and writer closed for {}", tp.toString(), logFile);
            if (ioe != null) {
                throw ioe;
            }
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_CLOSE_WAL_ERR,
                    "error closing wal " + logFile, e);
            throw new DataException("Error closing " + logFile, e);
        } finally {
            writer = null;
            reader = null;
        }
    }

    /**
     * 获取日志文件
     *
     * @return 日志文件
     */
    @Override
    public String getLogFile() {
        return logFile;
    }
}
