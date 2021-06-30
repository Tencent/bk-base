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


package com.tencent.bk.base.datahub.databus.connect.jdbc.util;

import com.tencent.bk.base.datahub.databus.connect.jdbc.error.ConnectionBrokenException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.DataTooLongException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.DuplicateEntryException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.IncorrectStringException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.TspiderInstanceDownException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.TspiderNoPartitionException;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;
import com.tencent.bk.base.datahub.databus.commons.utils.ConnPool;
import com.tencent.bk.base.datahub.databus.commons.utils.ConnUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class BatchRecords {

    private static final Logger log = LoggerFactory.getLogger(BatchRecords.class);
    private static final int MAX_COLUMNS = 15;

    private final BatchRecordsConfig config;

    private int threshold = 0;
    private int processedCount = 0;
    private List<List<Object>> buffered;
    private String escape;


    public BatchRecords(BatchRecordsConfig config, String escape) {
        this.config = config;
        this.escape = escape;
        this.buffered = new ArrayList<>();
    }

    public void setRemoveDuplicateThreshold(int threshold) {
        this.threshold = threshold;
    }

    /**
     * 将待写入数据库的记录放到buffer中
     *
     * @param recordList 数据列表
     */
    public void add(List<List<Object>> recordList) {
        buffered.addAll(recordList);
        processedCount += recordList.size();
        // 当记录数量超出batch时，批量提交
        if (buffered.size() >= config.getBatchSize()) {
            flush();
        }
    }

    /**
     * 将缓存的数据刷入到数据库中存储
     */
    public void flush() {
        if (buffered.size() > 0) {
            Connection connection = ConnPool
                    .getConnection(config.getConnUrl(), config.getConnUser(), config.getConnPass());
            if (connection == null) {
                connection = ConnUtils.initConnection(config.getConnUrl(), config.getConnUser(), config.getConnPass(),
                        config.getRetryBackoffMs());
                if (connection == null) {
                    LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, "unable to connect db {} {}",
                            config.getConnUrl(), config.getConnPass());
                    throw new ConnectException(
                            "unable to connect database " + config.getConnUrl() + " " + config.getConnPass());
                }
            }
            // 对于小规模的数据,先删除,然后再添加,从而避免重复数据
            if (processedCount <= threshold && config.getColsInOrder().length <= MAX_COLUMNS) {
                cleanDataFromDb(connection, buffered);
            }

            try {
                JdbcUtils.batchInsert(connection, config.getInsertSql(), buffered, config.getTimeoutSeconds());
                ConnPool.returnToPool(config.getConnUrl(), connection);
            } catch (TspiderNoPartitionException e) {
                LogUtils.warn(log, "no partition in tspider! {} {} {}", config.getDbName(), config.getTableName(),
                        e.getMessage());
                handleTspiderNoPartition(connection, buffered);
            } catch (DuplicateEntryException e) {
                handleDuplicateEntry(connection, buffered);
            } catch (IncorrectStringException e) {
                handleIncorrectString(connection, buffered);
            } catch (DataTooLongException e) {
                handleDataTooLong(connection, buffered);
            } catch (ConnectionBrokenException e) {
                // 连接发生异常时，尝试关闭connection对象
                ConnUtils.closeQuietly(connection);
                handleConnectionBroken(buffered);
            } catch (TspiderInstanceDownException e) {
                handleTspiderInstanceDown(connection, buffered);
            }
            buffered.clear();
            this.threshold = 0;
            this.processedCount = 0;
        }
    }


    /**
     * 处理数据写入时提示主键冲突的异常
     *
     * @param connection 数据库连接
     * @param records 待写入的数据列表
     */
    private void handleDuplicateEntry(Connection connection, List<List<Object>> records) {
        // 逐条写入，当再次碰到duplicate entry的异常时，跳过此条数据
        for (List<Object> record : records) {
            List<List<Object>> tmp = new ArrayList<>();
            tmp.add(record);
            try {
                JdbcUtils.batchInsert(connection, config.getInsertSql(), tmp, config.getTimeoutSeconds());
            } catch (DuplicateEntryException e) {
                LogUtils.warn(log, "discard data {}. {} -- {}", StringUtils.join(record, ", "), config.getInsertSql(),
                        e.getMessage());
            } catch (Exception e) {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log,
                        "got exception during handling DuplicateEntry exception", e);
                LogUtils.warn(log, "discard data {}. {} -- {}", StringUtils.join(record, ", "), config.getInsertSql(),
                        e.getMessage());
            }
        }
        ConnPool.returnToPool(config.getConnUrl(), connection);
    }

    /**
     * 处理数据写入时提示不正确的字符串的异常
     *
     * @param connection 数据库连接
     * @param records 待写入的数据列表
     */
    private void handleIncorrectString(Connection connection, List<List<Object>> records) {
        // 逐条写入，当再次碰到不正确的字符串时，跳过此条数据
        for (List<Object> record : records) {
            List<List<Object>> tmp = new ArrayList<>();
            tmp.add(record);
            try {
                JdbcUtils.batchInsert(connection, config.getInsertSql(), tmp, config.getTimeoutSeconds());
            } catch (IncorrectStringException e) {
                LogUtils.warn(log, "discard data {}. {} -- {}", StringUtils.join(record, ", "), config.getInsertSql(),
                        e.getMessage());
            } catch (Exception e) {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log,
                        "got exception during handling IncorrectString exception", e);
                LogUtils.warn(log, "discard data {}. {} -- {}", StringUtils.join(record, ", "), config.getInsertSql(),
                        e.getMessage());
            }
        }
        ConnPool.returnToPool(config.getConnUrl(), connection);
    }

    /**
     * 清理数据库中的记录
     *
     * @param connection 数据库连接对象
     * @param records 待清理的数据库记录
     */
    private void cleanDataFromDb(Connection connection, List<List<Object>> records) {
        try {
            JdbcUtils.deleteRecords(connection, config.getDbName(), config.getTableName(), config.getColsInOrder(),
                    records, this.escape);
        } catch (Exception ignore) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR,
                    String.format("failed to clean %s records from %s %s!", records.size(), config.getDbName(),
                            config.getTableName()), ignore);
        }
    }

    /**
     * 当tspider集群中有实例异常时，此批数据部分写入成功，部分写入失败，需要回滚写入成功 的内容，并使任务异常，等待tspider实例重启后恢复正常。
     *
     * @param connection 数据库连接对象
     * @param records 待清理的数据库记录
     */
    private void handleTspiderInstanceDown(Connection connection, List<List<Object>> records) {
        // 部分数据写入成功，部分失败，需要清理数据
        cleanDataFromDb(connection, records);
        // 等待一段时间，然后抛出致命异常，使任务状态变为失败，等待下一轮任务状态检查和拉起
        try {
            Thread.sleep(15000);
        } catch (InterruptedException ignore) {
            // ignore
        }
        throw new ConnectException(
                "failed to write tspider as some instance is down! " + config.getDbName() + " " + config
                        .getTableName());
    }

    /**
     * tspider中不存在数据里指定的thedate分区时,需要将这些不符合条件的数据清理掉, 并将正常的数据尝试再次写入tspider里。
     *
     * @param connection 数据库连接
     * @param records 待写入的数据列表
     */
    private void handleTspiderNoPartition(Connection connection, List<List<Object>> records) {
        // 数据中可能存在未来时间的thedate，或者历史时间的thedate，无法写入，需要对数据进行清理
        LogUtils.warn(log, "failed to write tspider {} {}, clean some old or future date records. record size: {}",
                config.getDbName(), config.getTableName(), records.size());
        int index = 0;
        for (String col : config.getColsInOrder()) {
            if (Consts.THEDATE.equals(col)) { // tspider中分区字段为thedate
                break;
            }
            index++;
        }

        // 获取当前的日期,转换为int
        int today = getBeforeDateAsInt(0);
        int before = getBeforeDateAsInt(config.getMaxDelayDays());
        List<List<Object>> bad = new ArrayList<>();
        List<List<Object>> good = new ArrayList<>();
        for (List<Object> record : records) {
            Object obj = record.get(index);
            if (obj instanceof Integer) {
                int value = (Integer) obj;
                if (value > today || value < before) {
                    bad.add(record);
                } else {
                    good.add(record);
                }
            } else {
                bad.add(record);
            }
        }
        if (bad.size() == 0) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.TSPIDER_PARTITION_ERR, log,
                    "all records has valid value for thedate, but got 'Table has no partition' error from tspider!!! "
                            + " {}",
                    JdbcUtils.getRecordsInString(config.getInsertSql(), good));
            throw new ConnectException(
                    "unable to continue, 'Table has no partition' exception but all thedate values are valid in ("
                            + before + " " + today + ")!");
        } else {
            LogUtils.warn(log, "{} bad records for tspider...  {}", bad.size(),
                    JdbcUtils.getRecordsInString(config.getInsertSql(), bad));
            // 重试一次写入，如果还是失败，则丢弃
            try {
                JdbcUtils.batchInsert(connection, config.getInsertSql(), good, config.getTimeoutSeconds());
            } catch (Exception ignore) {
                LogUtils.warn(log, "write tspider failed after filter and retry, discard {} records... {}", good.size(),
                        JdbcUtils.getRecordsInString(config.getInsertSql(), good));
            }
        }
        ConnPool.returnToPool(config.getConnUrl(), connection);
    }

    /**
     * 处理数据写入时,提示字段太长的异常
     *
     * @param connection 数据库连接
     * @param records 待写入的数据列表
     */
    private void handleDataTooLong(Connection connection, List<List<Object>> records) {
        // 逐条写入,当再次碰到字段过长的异常时,跳过
        for (List<Object> record : records) {
            List<List<Object>> tmp = new ArrayList<>();
            tmp.add(record);
            try {
                JdbcUtils.batchInsert(connection, config.getInsertSql(), tmp, config.getTimeoutSeconds());
            } catch (DataTooLongException e) {
                LogUtils.warn(log, JdbcUtils.getRecordsInString(config.getInsertSql(), tmp) + " --- " + e.getMessage());
            } catch (Exception e) {
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR,
                        "got exception during handling DataTooLong exception. " + JdbcUtils
                                .getRecordsInString(config.getInsertSql(), tmp), e);
            }
        }
        ConnPool.returnToPool(config.getConnUrl(), connection);
    }

    /**
     * 处理数据库连接异常时的数据写入
     *
     * @param records 待写入的数据列表
     */
    private void handleConnectionBroken(List<List<Object>> records) {
        // 重建数据库连接
        Connection connection = ConnUtils
                .initConnection(config.getConnUrl(), config.getConnUser(), config.getConnPass(),
                        config.getRetryBackoffMs());
        if (connection == null) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, "unable to connect db {} {}",
                    config.getConnUrl(), config.getConnPass());
            throw new ConnectException(
                    "unable to connect database " + config.getConnUrl() + " " + config.getConnPass());
        }
        try {
            JdbcUtils.batchInsert(connection, config.getInsertSql(), records, config.getTimeoutSeconds());
            ConnPool.returnToPool(config.getConnUrl(), connection);
        } catch (Exception ignore) {
            // 关闭链接对象
            ConnUtils.closeQuietly(connection);
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR,
                    "failed to handle connection broken exception. " + JdbcUtils
                            .getRecordsInString(config.getInsertSql(), records), ignore);
            throw new ConnectException("failed to handle connection broken exception.", ignore);
        }
    }


    /**
     * 获取delayDays天数前的日期值(年月日),数值型
     *
     * @param delayDays 天数
     * @return 日期值(年月日), 数值
     */
    private int getBeforeDateAsInt(int delayDays) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String today = df.format(new Date(System.currentTimeMillis() - delayDays * 24 * 3600 * 1000));
        return Integer.parseInt(today);
    }

}
