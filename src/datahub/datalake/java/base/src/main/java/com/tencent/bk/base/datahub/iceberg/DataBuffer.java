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

package com.tencent.bk.base.datahub.iceberg;

import static com.tencent.bk.base.datahub.iceberg.C.DFT_MAX_COMMIT_DATAFILES;
import static com.tencent.bk.base.datahub.iceberg.C.MAX_COMMIT_DATAFILES;

import com.tencent.bk.base.datahub.iceberg.errors.CommitFileFailed;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataBuffer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DataBuffer.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final Table table;
    private final PartitionTool tool;
    private final Map<String, PartitionDataFile> datafiles = new ConcurrentHashMap<>();
    private final Timer timer = new Timer("timer-" + Thread.currentThread().getId());
    private final int interval;
    private final int limit;
    private final int maxFiles; // 达到此数据文件数量时，就需要刷盘，避免占用过多内存。
    private final AtomicBoolean appendMode = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Map<String, String> msgData = new ConcurrentHashMap<>();
    private int successCommit = 0;
    private int currentCnt = 0; // 当前未commit的数据量
    private long lastCommit = System.currentTimeMillis(); // 上次commit的时间戳
    private String filename = String.format("%s.%s", UUID.randomUUID().toString(), C.FILE_FORMAT); // 当前的数据文件名称
    private CommitFileFailed exception;
    private Set<DataFile> toDelete;

    /**
     * 构造函数
     *
     * @param table 表对象
     */
    public DataBuffer(BkTable table) {
        // 默认5分钟 或 100w数据 触发commit
        this(table, 300_000, 1_000_000);
    }

    /**
     * 构造函数
     *
     * @param table 表对象
     * @param interval commit的最大时间间隔，毫秒数
     * @param limit commit的最大数据量，数据条数
     */
    public DataBuffer(BkTable table, int interval, int limit) {
        this(table, interval, limit, 0);
    }

    /**
     * 构造函数
     *
     * @param table 表对象
     * @param interval commit的最大时间间隔，毫秒数
     * @param limit commit的最大数据量，数据条数
     * @param firstCommitDelayMs 首次flush数据时，在interval间隔上增加一个延迟时间
     */
    public DataBuffer(BkTable table, int interval, int limit, int firstCommitDelayMs) {
        this(table.table(), interval, limit,
                Utils.getOrDefaultInt(table.table().properties().get(MAX_COMMIT_DATAFILES), DFT_MAX_COMMIT_DATAFILES),
                firstCommitDelayMs, true);
    }

    /**
     * 构造函数，用于更新数据或者删除数据。此场景下通过close方法刷盘数据，定时刷盘机制关闭。默认刷盘时间和刷盘数据量设置为Integer的最大值。
     *
     * @param table 表对象
     * @param rewriteFiles 需重写的数据文件
     */
    public DataBuffer(BkTable table, Set<DataFile> rewriteFiles) {
        this(table.table(), rewriteFiles);
    }

    /**
     * 构造函数，用于更新数据或者删除数据。此场景下通过close方法刷盘数据，定时刷盘机制关闭。默认刷盘时间和刷盘数据量设置为Integer的最大值。
     *
     * @param table iceberg表对象
     * @param rewriteFiles 需重写的数据文件
     */
    public DataBuffer(Table table, Set<DataFile> rewriteFiles) {
        this(table, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, 0, false);
        this.toDelete = rewriteFiles;
    }

    /**
     * 构造函数
     *
     * @param table 表对象
     * @param interval commit的最大时间间隔，毫秒数
     * @param limit commit的最大数据量，数据条数
     * @param maxFiles 触发commit的最大数据文件数
     * @param firstCommitDelayMs 首次commit的时间间隔增量
     * @param isAppend 是否为追加模式
     */
    private DataBuffer(Table table, int interval, int limit, int maxFiles, int firstCommitDelayMs, boolean isAppend) {
        this.table = table;
        this.interval = interval;
        this.limit = limit;
        this.maxFiles = maxFiles;
        this.tool = new PartitionTool(table.schema(), table.spec());

        if (isAppend) {
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    if (needFlush()) {
                        try {
                            flush();
                        } catch (CommitFileFailed e) {
                            timer.cancel();
                            log.warn(String.format("%s periodic flushing failed.", table), e);
                            exception = e;
                        }
                    }
                }
            }, 200, 1_000); // 每隔一段时间尝试flush一次数据
            lastCommit += firstCommitDelayMs; // 增加首次flush数据所需的时间间隔
        } else {
            this.appendMode.set(false);
        }
    }

    /**
     * 添加数据到表中。此处实现使用了锁，是为了保证add过程中不会flush，flush过程中也不会触发add。
     *
     * @param records 待添加的数据集
     * @param msg 待添加到表snapshot中的commit msg，包含在Snapshot#summary()的数据里。
     * @throws CommitFileFailed 写数据文件异常
     */
    public void add(List<Record> records, Map<String, String> msg) {
        checkState();

        lock.lock();
        try {
            for (Record r : records) {
                Partition partition = tool.partition(r);
                String location = table.locationProvider().newDataLocation(table.spec(), partition, filename);
                if (!datafiles.containsKey(location)) {
                    FileAppender<Record> appender = Parquet
                            .write(table.io().newOutputFile(location))
                            .forTable(table)
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .build();
                    datafiles.put(location, new PartitionDataFile(location, partition, appender));
                    log.info("{} adding data file {}", table, location);
                }
                datafiles.get(location).append(r);
                currentCnt++;
            }

            // 将msg内容加入到commitMsg对象
            msgData.putAll(msg);
        } catch (Exception e) {
            timer.cancel();
            reset(); // 发生异常时，重置计数器
            throw new CommitFileFailed(e, "%s failed to append data files %s", table, datafiles.keySet());
        } finally {
            lock.unlock();
        }

        if (needFlush()) { // 达到条件，刷盘
            flush();
        }
    }

    /**
     * 关闭表，将未刷盘的数据commit到表中。
     *
     * @throws CommitFileFailed 写数据文件异常
     */
    @Override
    public void close() {
        timer.cancel(); // 停掉timer中schedule的任务
        closed.set(true);
        flush();
    }

    /**
     * 检查DataBuffer对象状态，如果存在异常，抛出异常。
     *
     * @throws CommitFileFailed commit文件异常
     */
    public void checkState() {
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * 将缓存的数据commit到表中。此方法保证同时只能有一个线程进入数据文件刷盘的过程和刷盘后的状态重置。
     *
     * @throws CommitFileFailed 写数据文件异常
     */
    private void flush() {
        lock.lock();
        try {
            if (currentCnt > 0) {
                log.info("{} committing {} records with data files {}", table, currentCnt, datafiles.keySet());
                Set<DataFile> toAdd = new HashSet<>();
                for (PartitionDataFile dataFile : datafiles.values()) {
                    dataFile.close();
                    DataFile file = DataFiles.builder(table.spec())
                            .withInputFile(table.io().newInputFile(dataFile.location))
                            .withPartition(dataFile.partition)
                            .withMetrics(dataFile.fileAppender.metrics())
                            .build();
                    toAdd.add(file);
                }

                if (appendMode.get()) {
                    TableUtils.appendTableDataFiles(table, toAdd, msgData);
                } else {
                    TableUtils.rewriteTableDataFiles(table, toAdd, toDelete, msgData);
                }
                successCommit++;
            } else if (closed.get() && !appendMode.get() && toDelete.size() > 0 && datafiles.size() == 0) {
                // 此情况为删除数据，且符合条件的数据文件中所有数据均被删除，没有新增的数据文件
                TableUtils.deleteTableDataFiles(table, toDelete, msgData);
                successCommit++;
            }
        } catch (Exception e) {
            timer.cancel();
            throw new CommitFileFailed(e, "%s flushing %s records failed: %s", table, currentCnt, datafiles.keySet());
        } finally {
            try {
                reset(); // 重置计数器，重置文件名。必须保证在锁释放前执行。
            } catch (Exception ignore) {
                // ignore
            }

            lock.unlock();
        }
    }

    /**
     * 是否达到commit数据到表中的条件。
     *
     * @return True/False
     */
    private boolean needFlush() {
        return currentCnt >= limit || System.currentTimeMillis() - interval >= lastCommit
                || datafiles.size() >= maxFiles;
    }

    /**
     * 重置buffer中的计数器
     */
    private void reset() {
        this.currentCnt = 0;
        this.lastCommit = System.currentTimeMillis();
        this.filename = String.format("%s.%s", UUID.randomUUID().toString(), C.FILE_FORMAT);
        datafiles.clear();
        msgData.clear();
    }

    /**
     * 获取当前buffer中实际commit成功的次数。供测试使用。
     *
     * @return buffer中实际commit成功的次数。
     */
    protected int getSuccessCommit() {
        return successCommit;
    }

    static class PartitionTool {

        private final Transform[] transforms;
        private final Types.NestedField[] sourceFields;

        /**
         * 构造函数
         *
         * @param schema 表的schema
         * @param spec 表的分区信息
         */
        PartitionTool(Schema schema, PartitionSpec spec) {
            List<PartitionField> fields = spec.fields();
            int size = fields.size();
            transforms = new Transform[size];
            sourceFields = new Types.NestedField[size];

            for (int i = 0; i < size; i += 1) {
                PartitionField field = fields.get(i);
                transforms[i] = field.transform();
                sourceFields[i] = schema.findField(field.sourceId());
            }
        }

        /**
         * 计算一条数据的分区值
         *
         * @param r 一条数据
         * @return 数据的分区值
         */
        public Partition partition(Record r) {
            int size = transforms.length;
            Object[] partitionTuple = new Object[size];
            for (int i = 0; i < size; i += 1) {
                Object val = r.getField(sourceFields[i].name());
                if (val != null) {
                    Type.TypeID t = sourceFields[i].type().typeId();
                    if (t.equals(Type.TypeID.DATE)) {
                        val = (int) ChronoUnit.DAYS.between(Utils.EPOCH_DAY, (LocalDate) val);
                    } else if (t.equals(Type.TypeID.TIME)) {
                        val = ((LocalTime) val).toNanoOfDay() / 1000;
                    } else if (t.equals(Type.TypeID.TIMESTAMP)) {
                        val = ChronoUnit.MICROS.between(Utils.EPOCH, (OffsetDateTime) val);
                    }
                    partitionTuple[i] = transforms[i].apply(val);
                } else {
                    partitionTuple[i] = null;
                }
            }
            return new Partition(partitionTuple);
        }
    }

    static class PartitionDataFile {

        private final String location;
        private final StructLike partition;
        private final FileAppender<Record> fileAppender;

        /**
         * 构造函数
         *
         * @param l 文件地址
         * @param p 文件分区值
         * @param appender 数据文件写入对象
         */
        PartitionDataFile(String l, StructLike p, FileAppender<Record> appender) {
            this.location = l;
            this.partition = p;
            this.fileAppender = appender;
        }

        /**
         * 在数据文件中追加一条数据
         *
         * @param r 一条数据
         */
        public void append(Record r) {
            fileAppender.add(r);
        }

        /**
         * 关闭此数据文件
         *
         * @throws IOException 数据文件IO异常
         */
        public void close() throws IOException {
            fileAppender.close();
        }

        String getLocation() {
            return location;
        }

        StructLike getPartition() {
            return partition;
        }

        FileAppender<Record> getFileAppender() {
            return fileAppender;
        }
    }

}