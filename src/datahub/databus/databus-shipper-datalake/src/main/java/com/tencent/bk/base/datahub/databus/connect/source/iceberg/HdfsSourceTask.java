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

package com.tencent.bk.base.datahub.databus.connect.source.iceberg;

import static com.tencent.bk.base.datahub.iceberg.C.DTETS;
import static com.tencent.bk.base.datahub.iceberg.C.ET;
import static com.tencent.bk.base.datahub.iceberg.C.THEDATE;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.datahub.databus.connect.common.source.BkSourceTask;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.iceberg.BkTable;
import com.tencent.bk.base.datahub.iceberg.C;
import com.tencent.bk.base.datahub.iceberg.CommitMsg;
import com.tencent.bk.base.datahub.iceberg.DataBuffer;
import com.tencent.bk.base.datahub.iceberg.SnapshotEventCollector;
import com.tencent.bk.base.datahub.iceberg.Utils;
import com.tencent.bk.base.datahub.iceberg.errors.CommitFileFailed;
import com.tencent.bk.base.datahub.iceberg.errors.TableNotExists;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsSourceTask extends BkSourceTask {

    public static final String API_REPORT_DATA = "api.report.data";
    public static final String API_REPORT_DATA_DEFAULT = "/v3/hubmanager/data/report/";
    public static final String API_GET_TRANSFORM_TASKS = "api.get.transform.tasks";
    public static final String API_GET_TRANSFORM_TASKS_DEFAULT = "/v3/storekit/load_into_datalake/get_earliest/";
    public static final String API_FINISH_TRANSFORM_TASK = "api.finish.transform.task";
    public static final String API_FINISH_TRANSFORM_TASK_DEFAULT = "/v3/storekit/load_into_datalake/finish_task/";

    private static final Logger log = LoggerFactory.getLogger(HdfsSourceTask.class);
    private static final ObjectMapper OM = new ObjectMapper();

    private static final String ID = "id";
    private static final String RESULT_TABLE_ID = "result_table_id";
    private static final String DB_NAME = "db_name";
    private static final String TABLE_NAME = "table_name";
    private static final String DATA_DIR = "data_dir";
    private static final String TIMEZONE_ID = "timezone_id";
    private static final String PROPERTIES = "properties";
    private static final String FIELD_NAMES = "field_names";
    private static final String FILE_TRANSFORM_STATUS = "file_transform_status";
    private static final String SEPARATOR = ",";
    private static final String DATA_FILES = "data_files";
    private static final String FINISHED = "finished";
    private static final String GEOG_AREA = "geog_area";
    private static final String DEFAULT_AREA = "inland";
    private static final String HDFS_CLUSTER_NAME = "hdfs_cluster_name";
    private static final String HTTP = "http";
    private static final String COLON = ":";
    private static final String FLUSH_SIZE = "flush.size";
    private static final String INTERVAL = "interval";
    private static final int BUFFER_SIZE = 100;
    private static final int MIN_FLUSH_SIZE = 5_000_000;
    private static final int MIN_FLUSH_INTERVAL = 300_000;  // 5min

    // 测试使用
    private static final String TASK_FILE = "/tmp/tasks.json";
    private static final String USE_TASK_FILE = "use.task.file";
    private final Set<Long> processedTasksInFile = new HashSet<>();
    private HdfsSourceConfig config;
    private String geogArea;
    private FileSystem fs;  //同一个worker只会处理同一个HDFS集群的任务
    private DataTransTask lastProcessedTask;  // 上一个处理的任务
    private BkTable table;
    private boolean useFile;  // 是否使用文件作为任务列表
    private boolean addEtField = false;
    private String dns;

    /**
     * 解析任务配置，初始化任务所需资源
     */
    @Override
    public void startTask() {
        config = new HdfsSourceConfig(configProperties);
        BasicProps props = BasicProps.getInstance();
        geogArea = props.getClusterProps().getOrDefault(GEOG_AREA, DEFAULT_AREA);
        useFile = Boolean.parseBoolean(props.getClusterProps().getOrDefault(USE_TASK_FILE, ""));
        dns = props.getPropsWithDefault(Consts.API_DNS, Consts.API_DNS_DEFAULT);
        String reportPath = props.getPropsWithDefault(API_REPORT_DATA, API_REPORT_DATA_DEFAULT);
        // 启用iceberg事件收集
        SnapshotEventCollector.getInstance().setReportUrl(dns, reportPath);
        setThreadName();
    }

    /**
     * 周期性拉取任务列表并处理
     *
     * @return 处理进展。当前返回null，因为处理进展存储在iceberg表的commitMsg里。
     */
    @Override
    public List<SourceRecord> pollData() {
        if (System.currentTimeMillis() - start < 90000) {
            // 等待一小段时间，避免马上开始数据处理后又发生任务重新分配
            sleepQuietly(3);
            return null;
        }

        List<Map<String, Object>> transTasks = getDataTransTasks();
        // 周期性的通过接口调用获取数据转换任务列表，将归属本workerId的任务按照顺序逐个处理
        int processedTasks = 0;
        for (Map<String, Object> t : transTasks) {
            DataTransTask task = new DataTransTask(t);
            if (processedTasksInFile.contains(task.taskId)) {
                continue;
            }
            if (task.taskId % config.totalWorkers == config.workerId) {
                if (table == null || lastProcessedTask == null || !lastProcessedTask.rtId.equals(task.rtId)) {
                    // 当table未初始化，或者当前任务rt和上一个处理rt不同时，重新初始化table对象
                    table = new BkTable(task.dbName, task.tableName, task.props);
                    try {
                        table.loadTable();
                        addEtField = table.schema().findField(ET) != null;
                    } catch (TableNotExists e) {
                        table = null;
                        LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR,
                                String.format("skip this task as table not exists. %s", task), e);
                        continue;
                    }
                }

                try {
                    initFileSystem(table);
                    appendTable(table, task);
                    lastProcessedTask = task;
                    processedTasks++;
                } catch (IOException e) {
                    // 当发生异常时，是hdfs可能异常，这里记录下日志，进入下一个任务的处理，避免一直卡住所有后续任务
                    LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_ERR,
                            String.format("failed to process task %s!", task), e);
                    lastProcessedTask = null;
                    closeFileSystem();
                } catch (CommitFileFailed | CommitFailedException e) {
                    LogUtils.warn(log, String.format("skip %s as commit retry timeout!", task), e);
                    lastProcessedTask = null;
                }
            } else {
                LogUtils.debug(log, "task is not owned by this worker: {}", t);
            }
        }

        // 重置table对象
        table = null;
        lastProcessedTask = null;

        if (processedTasks == 0) {
            // 在一个周期内，本worker没有需要处理的任务，等待一个周期后再继续。等待时间根据拉取到的任务数量而定
            int sleepSeconds = transTasks.size() >= 30 ? 30 : 90;
            sleepQuietly(sleepSeconds);
            LogUtils.info(log, " no task processed in last loop, sleeping {} seconds", sleepSeconds);
        } else {
            sleepQuietly(5);  // 避免太频繁请求接口获取任务列表
        }

        return null;
    }

    /**
     * 关闭HDFS的资源
     */
    protected void stopTask() {
        closeFileSystem();
    }

    /**
     * 将数据文件转换后，追加写入数据湖表中。
     *
     * @param table 数据湖表
     * @param task 数据转换任务
     * @throws IOException IO异常
     */
    private void appendTable(BkTable table, DataTransTask task) throws IOException {
        int interval = Utils.getOrDefaultInt(task.props.getOrDefault(INTERVAL, ""), MIN_FLUSH_INTERVAL);
        interval = Math.max(interval, MIN_FLUSH_INTERVAL);
        int flushSize = Utils.getOrDefaultInt(task.props.getOrDefault(FLUSH_SIZE, ""), MIN_FLUSH_SIZE);
        flushSize = Math.max(flushSize, MIN_FLUSH_SIZE);

        try (DataBuffer buffer = new DataBuffer(table, interval, flushSize)) {
            Map<String, FileStatus> dataFiles = getDataFilesInPath(fs, new Path(task.dataDir));
            if (dataFiles.size() == 0) {
                LogUtils.warn(log, "skip empty data directory {}", task);
            } else {
                DirTransStatus status = recoverFromLastCommit(table, task.dataDir, dataFiles.keySet());

                while (!(isStop.get() || status.finished.get())) {
                    FileStatus df = dataFiles.get(status.currentFile);
                    processFile(task, status, df, buffer, table.schema());
                }

                log.info("end processing {}, finish status is {}. task isStop is {}",
                        task.dataDir, status.finished.get(), isStop.get());
            }
        }

        if (!isStop.get()) {
            finishDataTransTask(task);
        }
    }

    /**
     * 初始化FileSystem对象
     *
     * @param table iceberg表对象
     * @throws IOException IO异常
     */
    private void initFileSystem(BkTable table) throws IOException {
        if (fs == null) {
            fs = FileSystem.newInstance(table.hadoopConf());  // 初始化HDFS
        }
    }

    /**
     * 关闭HDFS的FileSystem，以便重新创建FileSystem对象。
     */
    private void closeFileSystem() {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException ioe) {
                // ignore
            }
        }
        fs = null;
    }

    /**
     * 处理数据文件
     *
     * @param task 数据转换任务
     * @param status 目录数据转换进度
     * @param f 当前处理数据文件
     * @param buffer DataBuffer对象，往iceberg表写数据用
     * @param schema iceberg表schema
     * @throws IOException IO异常
     */
    private void processFile(DataTransTask task, DirTransStatus status, FileStatus f, DataBuffer buffer, Schema schema)
            throws IOException {
        LogUtils.info(log, "process data file {} at {}", status.currentFile, status.currentLineNum);
        if (f == null) {
            // 数据文件不存在，属于异常场景，跳过此文件。进入下一个文件后，从行号0开始。
            LogUtils.warn(log, "could not find data file {}, skip it!", status.currentFile);
            status.goNextFile(status.currentLineNum);
            return;
        }

        try (ParquetReader<GenericRecord> reader = ParquetReader.<GenericRecord>builder(
                new AvroReadSupport<>(), f.getPath()).withConf(fs.getConf()).build()) {
            // 跳过部分行数据，恢复到上一次处理的位置（文件+行号）
            long readLines = 0;
            for (; readLines < status.currentLineNum; readLines++) {
                if (reader.read() == null) {
                    LogUtils.warn(log, "");
                    break;
                }
            }

            List<Record> records = new ArrayList<>(BUFFER_SIZE);
            GenericRecord r;
            while ((r = reader.read()) != null) {
                if (isStop.get()) {
                    break;  // 当任务停止的时候，立即终止数据文件处理流程
                }
                readLines++;
                status.currentLineNum = readLines;

                records.add(convertToIcebergRecord(r, schema, task));
                if (records.size() == BUFFER_SIZE) {
                    buffer.add(records, status.toCommitData());
                    records.clear();
                }
            }

            status.goNextFile(readLines);
            buffer.add(records, status.toCommitData());  // 记录此文件已处理完毕
        } catch (ParquetRuntimeException e) {
            // 当前处理的parquet文件有问题，跳过此文件
            LogUtils.warn(log, String.format("bad data file %s, skip it!", status.currentFile), e);
            status.goNextFile(status.currentLineNum);
            buffer.add(new ArrayList<>(), status.toCommitData());  // 记录此文件已处理完毕
        }
    }

    /**
     * 设置线程名称
     */
    private void setThreadName() {
        Thread.currentThread().setName(String.format("%s(worker-%s/%d)", name, config.workerId, config.totalWorkers));
    }

    /**
     * 通过API接口获取数据转换任务列表
     *
     * @return 数据转换任务列表
     */
    private List<Map<String, Object>> getDataTransTasks() {
        if (useFile) {
            try {
                File file = new File(TASK_FILE);
                String jsonTasks = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
                ObjectMapper om = new ObjectMapper();
                TypeReference<List<Map<String, Object>>> tr = new TypeReference<List<Map<String, Object>>>() {
                };

                return om.readValue(jsonTasks, tr);
            } catch (IOException ioe) {
                LogUtils.warn(log, "failed to read and parse task json file " + TASK_FILE, ioe);
            }
        } else {
            String query = String.format("%s=%s&%s=%s", HDFS_CLUSTER_NAME, config.hdfsCluster, GEOG_AREA, geogArea);
            try {
                URL url = getUrl(API_GET_TRANSFORM_TASKS, API_GET_TRANSFORM_TASKS_DEFAULT, query);
                ApiResult result = OM.readValue(url, ApiResult.class);
                if (result.isResult()) {
                    return (List<Map<String, Object>>) result.getData();
                } else {
                    LogUtils.warn(log, "query data transform tasks ({}), got bad api result: {}", query, result);
                }

            } catch (IOException e) {
                LogUtils.warn(log, "get data transform tasks by api failed. " + query, e);
            }
        }

        return new ArrayList<>();
    }

    /**
     * 通过API接口更新任务状态为完成
     *
     * @param task 数据转换任务
     */
    private void finishDataTransTask(DataTransTask task) {
        LogUtils.info(log, "{} finished transform data files in {}", task.rtId, task.dataDir);
        if (useFile) {
            processedTasksInFile.add(task.taskId);
            return;
        }
        String query = ID + "=" + task.taskId;
        try {
            URL url = getUrl(API_FINISH_TRANSFORM_TASK, API_FINISH_TRANSFORM_TASK_DEFAULT, query);
            ApiResult result = OM.readValue(url, ApiResult.class);
            if (!result.isResult()) {
                LogUtils.warn(log, "bad api result for finish data transform task {}: {}", task, result);
            }

        } catch (IOException e) {
            LogUtils.warn(log, "finish data transform task by api failed. " + task, e);
        }
    }


    /**
     * 从iceberg表commit记录中寻找当前数据目录的数据转换进度并恢复。
     *
     * @param table iceberg表对象
     * @param dataDir 数据目录
     * @param fileSet 目录下的数据文件名称集合
     * @return 目录数据转换进度对象
     */
    private DirTransStatus recoverFromLastCommit(BkTable table, String dataDir, Set<String> fileSet) {
        // 对一个数据目录下的数据文件，是顺序处理的。这里获取到的第一条记录即为此目录的最新处理进度。
        List<Map<String, String>> lt = new ArrayList<>();
        table.filterSnapshots(s -> {
            if (lt.isEmpty() && s.summary().containsKey(C.DT_SDK_MSG)) {
                Optional<CommitMsg> msg = Utils.parseCommitMsg(s.summary().get(C.DT_SDK_MSG));
                if (msg.isPresent()) {
                    CommitMsg m = msg.get();
                    if (m.getOperation().equals(C.DT_ADD) && dataDir.equals(m.getData().get(DATA_DIR))) {
                        lt.add(m.getData());
                        return true;
                    }
                }
            }

            return false;
        });

        // 未找到此数据目录被处理的记录，默认从第一个文件开始处理
        DirTransStatus status = lt.isEmpty() ? new DirTransStatus(dataDir, fileSet) : new DirTransStatus(lt.get(0));
        LogUtils.info(log, "recover from status: {}", status);

        return status;
    }

    /**
     * 将parquet文件中读取的一条记录转换为iceberg的record对象
     *
     * @param gr parquet中的一条记录
     * @param schema iceberg表的schema
     * @param t 数据转换任务
     * @return iceberg的record对象
     */
    private Record convertToIcebergRecord(GenericRecord gr, Schema schema, DataTransTask t) {
        Record record = org.apache.iceberg.data.GenericRecord.create(schema);
        for (int i = 0; i < t.fieldNames.length; i++) {
            Object obj = gr.get(t.fieldNames[i]);
            if (obj == null) {
                continue;  // 跳过null值的处理
            }

            if (DTETS.equals(t.fieldNames[i])) {
                // 特殊处理thedate/dtEventTime字段
                Instant instant = Instant.ofEpochMilli((Long) obj);
                if (addEtField) {
                    // 用dtEventTimeStamp的时间戳构建OffsetDateTime字段作为分区字段的值存储
                    record.setField(ET, instant.atOffset(ZoneOffset.UTC));
                }

                if (t.hasThedateField && gr.get(THEDATE) == null) {
                    // 对于thedate，计算本地的日期，然后组成年月日的数值
                    LocalDateTime time = LocalDateTime.ofInstant(instant, t.tzId);
                    int date = time.getYear() * 10000 + time.getMonthValue() * 100 + time.getDayOfMonth();
                    record.setField(THEDATE, date);
                }
            } else if (obj instanceof Utf8) {
                obj = obj.toString();  // avro中的string为org.apache.avro.util.Utf8类，并非java的string类，需要进行转换
            }

            record.setField(t.lowerNames[i], obj);
        }

        return record;
    }

    /**
     * 获取path下的数据文件列表，按照文件名称先后顺序排列
     *
     * @param path 数据文件的目录
     * @throws IOException 异常
     */
    private Map<String, FileStatus> getDataFilesInPath(FileSystem fs, Path path) throws IOException {
        try {
            return Arrays.stream(fs.listStatus(path))
                    .filter(FileStatus::isFile)
                    .filter(f -> f.getPath().getName().endsWith(".parquet"))
                    .collect(Collectors.toMap(f -> f.getPath().getName(), Function.identity()));
        } catch (FileNotFoundException e) {
            LogUtils.warn(log, "data dir does not exist: {}", path);
            return new HashMap<>();
        }
    }

    /**
     * 休眠一段时间
     *
     * @param seconds 秒数
     */
    private void sleepQuietly(int seconds) {
        for (int i = 0; i < seconds; i++) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignore) {
                // just ignore
            }
            if (isStop.get()) {
                break;
            }
        }
    }

    /**
     * 获取指定路径的API地址
     *
     * @param pathKey 路径的key
     * @param defaultPath 默认路径值
     * @param query 查询字符串
     * @return API地址的URL
     * @throws MalformedURLException 异常的API URL
     */
    private URL getUrl(String pathKey, String defaultPath, String query) throws MalformedURLException {
        BasicProps props = BasicProps.getInstance();
        String path = props.getPropsWithDefault(pathKey, defaultPath);
        String file = StringUtils.isBlank(query) ? path : path + "?" + query;

        if (dns.contains(COLON)) {
            String[] hostPort = StringUtils.split(dns, COLON);
            return new URL(HTTP, hostPort[0], Integer.parseInt(hostPort[1]), file);
        } else {
            return new URL(HTTP, dns, file);
        }
    }


    private static class DataTransTask {

        private final long taskId;
        private final String rtId;
        private final String dbName;
        private final String tableName;
        private final String dataDir;
        private final ZoneId tzId;
        private final Map<String, String> props;
        private final String[] fieldNames;
        // 以下属性通过计算得出
        private final String[] lowerNames;
        private final boolean hasThedateField;

        /**
         * 构造函数
         *
         * @param task 接口返回的任务信息
         */
        DataTransTask(Map<String, Object> task) {
            taskId = Long.parseLong(task.get(ID).toString());
            rtId = task.get(RESULT_TABLE_ID).toString();
            dbName = task.get(DB_NAME).toString();
            tableName = task.get(TABLE_NAME).toString();
            dataDir = task.get(DATA_DIR).toString();
            tzId = ZoneId.of(task.get(TIMEZONE_ID).toString());
            props = new HashMap<>();
            ((Map<?, ?>) task.get(PROPERTIES)).forEach((k, v) -> props.put(k.toString(), v.toString()));
            fieldNames = StringUtils.split(task.get(FIELD_NAMES).toString(), ",");
            lowerNames = Utils.lowerCaseArray(fieldNames);
            hasThedateField = Utils.getIndex(fieldNames, THEDATE) >= 0;
        }

        @Override
        public String toString() {
            return String.format("[id=%s,rt_id=%s,data_dir=%s]", taskId, rtId, dataDir);
        }

    }

    private static class DirTransStatus {

        private final String dir;
        private final String[] files;
        private final AtomicBoolean finished;
        private String currentFile;
        private int currentFileIdx;
        private long currentLineNum;

        /**
         * 构造函数
         *
         * @param map CommitMsg的data部分
         */
        DirTransStatus(Map<String, String> map) {
            dir = map.get(DATA_DIR);
            files = StringUtils.split(map.get(DATA_FILES), SEPARATOR);
            finished = new AtomicBoolean(Boolean.parseBoolean(map.get(FINISHED)));
            String[] arr = StringUtils.split(map.get(FILE_TRANSFORM_STATUS), SEPARATOR);
            currentFile = arr[0];
            currentFileIdx = IntStream.range(0, files.length).filter(i -> files[i].equals(currentFile)).findFirst()
                    .orElse(-1);
            currentLineNum = Long.parseLong(arr[1]);
            if (currentFileIdx == -1) {
                LogUtils.warn(log, "bad dir transform status, current file not in files. {}", map);
                throw new IllegalArgumentException(String.format("%s and %s mismatch: %s",
                        FILE_TRANSFORM_STATUS, DATA_FILES, map));
            }
        }

        /**
         * 构造函数
         *
         * @param dataDir 数据目录
         * @param fileSet 目录下数据文件集合
         */
        DirTransStatus(String dataDir, Set<String> fileSet) {
            dir = dataDir;
            files = fileSet.stream().sorted().toArray(String[]::new);
            finished = new AtomicBoolean(false);
            currentFileIdx = 0;
            currentFile = files[currentFileIdx];
            currentLineNum = 0;
        }

        /**
         * 进入下一个数据文件处理
         *
         * @param currentFileLineNum 当前处理的数据文件的处理记录数
         */
        void goNextFile(long currentFileLineNum) {
            if (currentFileIdx == files.length - 1) {  // 没有下一个数据文件需要处理
                finished.set(true);
                currentLineNum = currentFileLineNum;
            } else {
                currentFileIdx++;
                currentFile = files[currentFileIdx];
                currentLineNum = 0;  // 从第一行开始读取
            }
        }

        /**
         * 转换为CommitMsg的data部分
         *
         * @return CommitMsg的data部分
         */
        Map<String, String> toCommitData() {
            Map<String, String> data = new LinkedHashMap<>();
            data.put(DATA_DIR, dir);
            data.put(DATA_FILES, StringUtils.join(files, SEPARATOR));
            data.put(FINISHED, finished.toString());
            data.put(FILE_TRANSFORM_STATUS, currentFile + SEPARATOR + currentLineNum);

            return data;
        }

        @Override
        public String toString() {
            return toCommitData().toString();
        }
    }
}
