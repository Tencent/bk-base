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

package com.tencent.bk.base.datahub.databus.connect.source.hdfs;

import com.tencent.bk.base.datahub.databus.connect.source.hdfs.bean.BkhdfsPullerTask;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.common.source.BkSourceTask;
import com.tencent.bk.base.datahub.databus.connect.source.hdfs.errors.FileTooBigError;
import com.tencent.bk.base.datahub.databus.connect.source.hdfs.reader.HdfsReaderFactory;
import com.tencent.bk.base.datahub.databus.connect.source.hdfs.reader.RecordsReader;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.callback.ProducerCallback;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.AvroUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkHdfsSourceTask extends BkSourceTask {

    private static final Logger log = LoggerFactory.getLogger(BkHdfsSourceTask.class);

    private static final String FILE_TOO_BIG = "FILE_TOO_BIG";
    private static final String EMPTY_DATA_DIR = "EMPTY_DATA_DIR";
    private static final String BAD_DATA_FILE = "BAD_DATA_FILE";

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    private BkHdfsSourceConfig config;
    private Map<String, FileSystem> hdfsFileSystems = new HashMap<>(4);
    private Map<String, Configuration> hdfsConfiguration = new HashMap<>(4);
    private Map<String, KafkaProducer<String, String>> producers = new HashMap<>(4);

    // 集群中干活的worker总数，也就是集群的并发度
    private int workerNum = 1;
    private long parquetReadMaxRecords = 20000000L;
    private long jsonReadMaxRecords = 10000000L;
    private String geogArea = "";
    private long maxMessageBytePerTime = -1L;

    /**
     * 启动task,初始化资源和属性
     */
    @Override
    public void startTask() {
        config = new BkHdfsSourceConfig(configProperties);
        // 初始化各类资源
        String num = configProperties.getOrDefault("worker.num", "1");
        try {
            workerNum = Integer.parseInt(num);
            if (workerNum <= 0) {
                workerNum = 1;
            }
            LogUtils.info(log, "worker {}: going to start hdfs import with {} workers", config.workerId, workerNum);
        } catch (NumberFormatException ignore) {
            LogUtils.warn(log, "bad config for worker.num with value {}", num);
        }

        jsonReadMaxRecords = getMaxRecords("read.max.records", jsonReadMaxRecords);
        parquetReadMaxRecords = getMaxRecords("read.parquet.max.records", parquetReadMaxRecords);

        geogArea = BasicProps.getInstance().getClusterProps().getOrDefault("geog.area", "");
        LogUtils.info(log, "I am in {}", geogArea);
    }

    private long getMaxRecords(String key, long defaultVal) {
        String readMaxRecordsConf = BasicProps.getInstance().getClusterProps().get(key);
        if (StringUtils.isNoneBlank(readMaxRecordsConf)) {
            try {
                long tmp = Long.parseLong(readMaxRecordsConf);
                if (tmp >= 10000) {
                    // 此配置的最小值设置为10000，避免太小引起多数离线分发任务执行失败
                    return tmp;
                }
            } catch (NumberFormatException ignore) {
                LogUtils.warn(log, "bad config for read.max.records with value {}", readMaxRecordsConf);
            }
        }
        return defaultVal;
    }

    /**
     * 获取当前任务对应的最大能读取的记录数
     *
     * @param task 当前需要导入数据的任务
     * @return 当前任务的数据类型对应的最大能读取的记录数
     */
    private long getReadMaxRecords(BkhdfsPullerTask task) {
        return HdfsReaderFactory.PARQUET_TYPE.equals(task.getContext().getHdfsDataType()) ? parquetReadMaxRecords
                : jsonReadMaxRecords;
    }

    /**
     * 执行导入数据的task
     *
     * @param task 需要执行的任务
     */
    protected void executeTask(BkhdfsPullerTask task) {
        if (null == task.getContext()) {
            sleepQuietly(5);
            return;
        }
        long taskId = task.getTaskId();
        LogUtils.info(log, "taskId:{}, NumInstances:{}, current InstanceId:{}", taskId, this.workerNum,
                config.workerId);
        // 将目录下文件排序，按照顺序处理
        Path workDir = new Path(task.getDataDir());
        List<List<Object>> buffer = new ArrayList<>(config.msgBatchRecords);
        long readMaxRecords = this.getReadMaxRecords(task);
        Producer<String, String> producer = null;
        int recordsInDir = 0;
        String lastReadFileName = null;
        long lastReadLineNumber = 0;
        try {
            FileSystem fs = getHdfsFileSystem(task.getHdfsConfDir(), task.getHdfsCustomProperty());
            producer = getKafkaProducer(task.getChannelServer());
            // 主体处理逻辑，首先判断目录是否存在，然后恢复到上次处理的位置，顺序读取文件，写入pulsar中
            if (fs.exists(workDir)) {
                // 恢复机制，回到上次处理的文件和行号
                List<FileStatus> files = getDataFilesInPath(fs, workDir);
                int lastWorkIdx = 0;
                if (StringUtils.isNotBlank(task.getStatus())) {
                    lastWorkIdx = findFileIndex(files, task.getLastReadFileName());
                    if (lastWorkIdx == -1) {
                        // 在目录中未找到文件，需要重置index避免IndexOutOfBound
                        LogUtils.warn(log,
                                "worker {}: file in status {} not found in dir {}, going to reading from the first "
                                        + "file",
                                config.workerId, task.getStatus(), workDir);
                        lastWorkIdx = 0;
                    }
                }
                final long start = System.currentTimeMillis();
                LogUtils.info(log,
                        "worker {}: {} going to process {} files in {}. last work status is {}, file index is {}",
                        config.workerId, task.getResultTableId(), files.size(), workDir, task.getStatus(), lastWorkIdx);
                List<String> processedFiles = new ArrayList<>();
                // 遍历未处理的文件
                for (int i = lastWorkIdx; i < files.size(); i++) {
                    FileStatus file = files.get(i);
                    if (file.getPath().getName().startsWith("_")) {
                        // 跳过一些非数据文件
                        continue;
                    }
                    lastReadFileName = file.getPath().getName();
                    processedFiles.add(file.getPath().getName());
                    lastReadLineNumber = this.readFile(task, file, fs, buffer, producer);
                    recordsInDir += lastReadLineNumber;
                    LogUtils.debug(log, "worker {}: {} processed {} records of file {} in {}", config.workerId,
                            task.getResultTableId(), lastReadLineNumber, file.getPath().getName(), task.getDataDir());
                    // 检查此目录里所有处理过的记录数量
                    if (recordsInDir > readMaxRecords) {
                        LogUtils.warn(log,
                                "worker {}: {} processed {} records in dir {}, greater than read.max.records {}",
                                config.workerId, task.getResultTableId(), recordsInDir, task.getDataDir(),
                                readMaxRecords);
                        throw new FileTooBigError(
                                String.format("%s data directory %s is too bigger to process!", task.getResultTableId(),
                                        task.getDataDir()));
                    }
                }
                // 将本目录中剩余的记录批量发送到pulsar中
                if (buffer.size() > 0) {
                    composeRecordAndSend(buffer, task, producer);
                }

                long end = System.currentTimeMillis();
                LogUtils.info(log, "worker {}: {} takes {} to process {} records in {} files {} in {}", config.workerId,
                        task.getResultTableId(), end - start, recordsInDir, processedFiles.size(),
                        StringUtils.join(processedFiles, ", "), task.getDataDir());
            } else {
                // 目录不存在，无法处理，将任务标记为完成
                LogUtils.warn(log,
                        "worker {}: {} data dir {} not exists on hdfs, going to mark import hdfs task finish. hdfs "
                                + "conf dir is {}",
                        config.workerId, task.getResultTableId(), task.getDataDir(), task.getHdfsConfDir());
            }

            // 更新数据库中任务状态，在这里时，本目录里的所有文件已处理完毕
            if (lastReadFileName == null) {
                // 上报事件
                Metric.getInstance().reportEvent(task.getResultTableId(), Consts.BATCH_IMPORT_EMPTY_DIR, "",
                        workDir + " is empty!");
                // 一个文件也没有处理过，这里只能认为目录内容为空，并做标记
                HttpUtils.updateHdfsImportTaskStatus(taskId, task.getResultTableId(), task.getDataDir(),
                        EMPTY_DATA_DIR + "|-1", true);
            } else {
                HttpUtils.updateHdfsImportTaskStatus(taskId, task.getResultTableId(), task.getDataDir(),
                        lastReadFileName + "|-1", true);
            }
        } catch (Exception e) {
            if (lastReadFileName != null) {
                LogUtils.debug(log, "worker {}: {} processed {} records of file {} in {}, then got {}", config.workerId,
                        task.getResultTableId(), lastReadLineNumber, lastReadFileName, task.getDataDir(), e.getClass());
                if (e instanceof IOException) {
                    // 对于IO类型的异常，可以通过重试机制解决
                    HttpUtils.updateHdfsImportTaskStatus(taskId, task.getResultTableId(), task.getDataDir(),
                            lastReadFileName + "|" + lastReadLineNumber, false);
                } else if (e instanceof FileTooBigError) {
                    // 上报事件
                    Metric.getInstance().reportEvent(task.getResultTableId(), Consts.BATCH_IMPORT_TOO_MANY_RECORDS, "",
                            String.format("%s contains more than %s records", task.getDataDir(), readMaxRecords));
                    // 对于文件或者目录中数据量过大的任务，终止处理，标记任务状态
                    HttpUtils.updateHdfsImportTaskStatus(taskId, task.getResultTableId(), task.getDataDir(),
                            lastReadFileName + "|" + lastReadLineNumber + "|" + FILE_TOO_BIG, true);
                } else {
                    // 上报事件
                    Metric.getInstance().reportEvent(task.getResultTableId(), Consts.BATCH_IMPORT_BAD_DATA_FILE,
                            ExceptionUtils.getStackTrace(e),
                            String.format("process data file %s in %s failed!", lastReadFileName, task.getDataDir()));
                    // 对于其他类型的异常，暂且认为此任务无法处理，做标记后跳过
                    HttpUtils.updateHdfsImportTaskStatus(taskId, task.getResultTableId(), task.getDataDir(),
                            lastReadFileName + "|" + lastReadLineNumber + "|" + BAD_DATA_FILE, true);
                }
            }
            LogUtils.warn(log, String.format("%s exception during processing data dir %s ", task.getResultTableId(),
                    task.getDataDir()), e);
        }
    }

    /**
     * 读取hdfs 文件写入pulsar
     *
     * @param task 从hdfs 导入数据到pulsar的任务
     * @param file 当前需要读取的文件
     * @param fs hdfs 文件系统
     * @param buffer 数据缓冲
     * @param producer pulsar的producer 客户端
     * @return 已经读取数据的行数
     * @throws IOException 异常
     */
    protected long readFile(BkhdfsPullerTask task, FileStatus file, FileSystem fs, List<List<Object>> buffer,
            Producer<String, String> producer) throws IOException {
        RecordsReader reader = HdfsReaderFactory
                .createRecordsReader(fs, getHdfsConfiguration(task.getHdfsConfDir(), task.getHdfsCustomProperty()),
                        file.getPath(), task.getColsInOrder());
        if (reader == null) {
            LogUtils.info(log, "worker {}: {} file {}, unkown dataType: {}, continue", config.workerId,
                    task.getResultTableId(), file.getPath().getName(), task.getContext().getHdfsDataType());
            return 0;
        }
        try {
            // 当处于任务恢复状态时，比对当前文件是否就是上次处理了一部分的文件
            if (file.getPath().getName().equals(task.getLastReadFileName())) {
                // 当前文件为上次处理到一半的文件，判断文件行号
                if (task.getLastReadLineNumber() == -1) { // 当前文件已处理完毕
                    return 0;
                } else {
                    reader.seekTo(task.getLastReadLineNumber());
                }
            }
            while (!reader.reachEnd()) {
                if (isStop.get()) {
                    // 任务已被停止，终止读取
                    LogUtils.debug(log, "begin to stop worker {}: {} processed {} records of file {} in {}",
                            config.workerId, task.getResultTableId(), reader.processedCnt(), reader.getFilename(),
                            task.getDataDir());
                    reader.closeResource();
                    // 将本目录中剩余的记录批量发送到pulsar中
                    if (buffer.size() > 0) {
                        composeRecordAndSend(buffer, task, producer);
                    }
                    HttpUtils.updateHdfsImportTaskStatus(task.getTaskId(), task.getResultTableId(), task.getDataDir(),
                            reader.getFilename() + "|" + reader.processedCnt(), false);
                    return reader.processedCnt();
                }

                // 循环读取文件中内容，直到全部读取完毕
                buffer.addAll(reader.readRecords(config.msgBatchRecords));
                if (buffer.size() >= config.msgBatchRecords) {
                    composeRecordAndSend(buffer, task, producer);
                }
                // 检查此文件已处理的记录数量
                if (reader.processedCnt() > this.getReadMaxRecords(task)) {
                    LogUtils.warn(log,
                            "worker {}: {} processed {} records in file {} {}, greater than read.max.records {}",
                            config.workerId, task.getResultTableId(), reader.processedCnt(), task.getDataDir(),
                            reader.getFilename(), this.getReadMaxRecords(task));
                    throw new FileTooBigError(
                            String.format("%s data file %s %s is too bigger to process!", task.getResultTableId(),
                                    task.getDataDir(), reader.getFilename()));
                }
            }
            return reader.processedCnt();
        } finally {
            reader.closeResource();
        }
    }


    /**
     * 拉取hdfs中的数据，发送到目的地的kafka中。
     *
     * @return 处理的结果
     */
    @Override
    public List<SourceRecord> pollData() {
        if (config.workerId >= workerNum) {
            LogUtils.warn(log, "worker {}: total workers are {}, this worker is out of range, just do nothing",
                    config.workerId, workerNum);
            sleepQuietly(90);
            return null;
        }

        // 获取要处理的任务列表，根据本task的id来过滤需要处理的任务
        List<Map<String, Object>> hdfsTasks;
        try {
            hdfsTasks = HttpUtils.getHdfsImportTasks(geogArea);
            LogUtils.debug(log, "worker {}: hdfs puller worker got {} tasks", config.workerId, hdfsTasks.size());
        } catch (ConnectException ignore) {
            // 当发生异常时，是api服务器不稳定，或者负载太高，这里记录下日志，并重新开始poll的逻辑
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.DATABUS_API_ERR,
                    String.format("worker %s: failed to get hdfs puller tasks from API, got exception!",
                            config.workerId), ignore);
            sleepQuietly(5);
            return null;
        }

        // 本循环周期内处理的任务数量，用于决定是否在进入下一次循环周期前等待一段时间
        int processedTasks = 0;
        for (Map<String, Object> task : hdfsTasks) {
            long taskId = Long.parseLong(task.get("id").toString());
            // 只处理属于本任务的hdfs import task
            if (taskId % workerNum == config.workerId) {
                // 获取任务的基础信息
                this.executeTask(new BkhdfsPullerTask(task, config.workerId));
                processedTasks++;
            } else {
                LogUtils.trace(log, "worker {}: task {} is not owned by this worker, just ignore it", config.workerId,
                        task);
            }
        }

        if (processedTasks == 0) {
            // 在一个周期内，本worker没有需要处理的任务，等待一个周期后再继续。等待时间根据拉取到的任务数量而定
            int sleepSeconds = hdfsTasks.size() >= 30 ? 30 : 90;
            sleepQuietly(sleepSeconds);
        }

        return null;
    }

    /**
     * 获取hdfs fs对象
     *
     * @param hdfsConfDir hdfs配置目录
     * @return hdfs fs对象
     */
    private FileSystem getHdfsFileSystem(String hdfsConfDir, String hdfsCustomProperty) {
        return hdfsFileSystems.computeIfAbsent(hdfsConfDir,
                conf -> initHdfsFsByConf(getHdfsConfiguration(hdfsConfDir, hdfsCustomProperty)));
    }


    /**
     * 获取hdfs configure
     *
     * @param hdfsConfDir hdfs配置目录
     * @return hdfs Configuration
     */
    private Configuration getHdfsConfiguration(String hdfsConfDir, String hdfsCustomProperty) {
        if (!hdfsFileSystems.containsKey(hdfsConfDir)) {
            // 这里存在两种情况:
            // 1) hdfsConfDir是hdfs配置文件所在目录，hdfsCustomProperty为空
            // 2) hdfsConfDir是hdfs集群名称，hdfsCustomProperty中包含连接hdfs集群的配置信息
            Configuration conf = new Configuration();
            if (StringUtils.isBlank(hdfsCustomProperty)) {
                conf.addResource(new Path(hdfsConfDir + "/core-site.xml"));
                conf.addResource(new Path(hdfsConfDir + "/hdfs-site.xml"));
            } else {
                try {
                    for (Map.Entry<String, Object> entry : JsonUtils.readMap(hdfsCustomProperty).entrySet()) {
                        // 按照属性值设置hdfs的配置
                        conf.set(entry.getKey(), entry.getValue().toString());
                    }
                } catch (IOException ioe) {
                    LogUtils.warn(log, "failed to parse json config {} for HDFS", hdfsCustomProperty);
                }
            }
            hdfsConfiguration.put(hdfsConfDir, conf);
        }

        return hdfsConfiguration.get(hdfsConfDir);
    }

    /**
     * 获取kafka producer对象
     *
     * @param kafkaBs kafka的bs地址
     * @return kafka producer对象
     */
    private Producer<String, String> getKafkaProducer(String kafkaBs) {
        if (!producers.containsKey(kafkaBs)) {
            initProducer(kafkaBs);
        }

        return producers.get(kafkaBs);
    }

    /**
     * 初始化hdfs相关的资源
     *
     * @param conf hdfs 配置
     * @return hdfs的FileSystem对象
     */
    private FileSystem initHdfsFsByConf(Configuration conf) {
        try {
            Configuration hdfsConf = new Configuration();
            hdfsConf.addResource(conf);
            hdfsConf.setBoolean("fs.automatic.close", false);

            // 创建FileSystem对象
            FileSystem fs = FileSystem.get(hdfsConf);
            LogUtils.debug(log, "hdfs fs object created! Conf  {}", conf);
            return fs;
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_ERR,
                    "failed to create hdfs file system object", e);
            throw new ConnectException(e);
        }
    }

    /**
     * 初始化kafka producer
     *
     * @param bootstrapServer kafka的bs地址
     */
    private void initProducer(String bootstrapServer) {
        try {
            Map<String, Object> producerProps = new HashMap<>(BasicProps.getInstance().getProducerProps());
            // producer的配置从配置文件中获取
            producerProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);

            // 序列化配置,发送数据的value为字节数组
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            LogUtils.debug(log, "_initProducer_: kafka_producer: {}", producerProps);

            KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
            producers.put(bootstrapServer, producer);
            maxMessageBytePerTime = Long.parseLong(producerProps.getOrDefault(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                    producerProps.get(ProducerConfig.BATCH_SIZE_CONFIG)).toString());
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.KAFKA_CONNECT_FAIL,
                    "_initProducer_: failed to initialize kafka producer", e);
            throw new ConnectException(e);
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
     * 获取path下的数据文件列表，按照文件名称先后顺序排列
     *
     * @param path 数据文件的目录
     * @throws IOException 异常
     */
    private List<FileStatus> getDataFilesInPath(FileSystem fs, Path path) throws IOException {
        FileStatus[] datas = fs.listStatus(path);
        List<FileStatus> dataFiles = new ArrayList<>(datas.length);

        for (FileStatus data : datas) {
            if (data.isFile()) {
                dataFiles.add(data);
            }
        }

        return sortFilesByName(dataFiles);
    }

    /**
     * 按照文件/文件夹的名称进行排序
     *
     * @param files 待排序的文件/文件夹列表
     * @return 按照名称顺序排列的列表
     */
    private List<FileStatus> sortFilesByName(List<FileStatus> files) {
        // 根据名称对目录排序
        files.sort((file1, file2) -> {
            return file1.getPath().getName().compareTo(file2.getPath().getName());
        });

        return files;
    }

    /**
     * 在文件列表中找出指定文件名所在的index
     *
     * @param files 文件列表
     * @param fileNameToFind 需要寻找的文件名
     * @return 文件所在的index
     */
    private int findFileIndex(List<FileStatus> files, String fileNameToFind) {
        for (int i = 0; i < files.size(); i++) {
            if (fileNameToFind.equals(files.get(i).getPath().getName())) {
                return i;
            }
        }

        return -1;
    }

    /**
     * 组装发送到kafka中的消息的内容，并发送到目的地topic中
     *
     * @param buffer 待组装的数据
     * @param task 数据的字段数组，按顺序排列
     * @param producer kafka producer
     */
    protected void composeRecordAndSend(List<List<Object>> buffer, BkhdfsPullerTask task,
            Producer<String, String> producer) {
        String rtId = task.getResultTableId();
        // 将时间戳转换为10s的倍数
        long tagTime = System.currentTimeMillis() / 10000 * 10;
        // 获取数据中的dtEventTimestamp，作为input tag
        long inputTagTime = tagTime;
        for (int i = 0; i < task.getColsInOrder().length; i++) {
            if (Consts.DTEVENTTIMESTAMP.equals(task.getColsInOrder()[i])) {
                if (!buffer.isEmpty()) {
                    inputTagTime = (Long) buffer.get(0).get(i) / 1000;
                }
                break;
            }
        }

        final long finalInputTagTime = inputTagTime;
        String inputTag = getMetricTag(rtId, inputTagTime);
        String outputTag = getMetricTag(rtId, tagTime);

        Consumer<List<List<Object>>> sender = (tempBuffer) -> {
            if (tempBuffer.isEmpty()) {
                return;
            }
            // 组装数据
            GenericArray<GenericRecord> arr = AvroUtils
                    .composeAvroRecordArray(tempBuffer, task.getColsInOrder(), task.getRecordSchema());
            GenericRecord msgRecord = AvroUtils.composeInnerAvroRecord(task.getMsgSchema(), arr, tagTime, outputTag);
            // 转换为发送到kafka中的key和value
            String msgValue = AvroUtils.getAvroBinaryString(msgRecord, task.getDataFileWriter());
            String msgKey =
                    dateFormat.format(new Date(finalInputTagTime * 1000)) + "&time=" + System.currentTimeMillis();
            sendRecordsToKafka(producer, task.getTopic(), msgKey, msgValue);
            // 打点信息
            Metric.getInstance()
                    .updateStat(rtId, "puller-" + task.getTopic(), task.getTopic(), "kafka", tempBuffer.size(),
                            msgValue.length(), inputTag, outputTag);
        };

        long singleRecordSize = calculateRecordSize(buffer.get(0), task);
        long effectiveRecordSize = this.maxMessageBytePerTime - task.getSchemaByteSize();
        if (singleRecordSize > effectiveRecordSize) {
            throw new FileTooBigError(
                    String.format("%s data file %s singer record is too bigger to process!", task.getResultTableId(),
                            task.getDataDir()));
        }
        if (buffer.size() * singleRecordSize <= effectiveRecordSize) {
            sender.accept(buffer);
            buffer.clear();
            return;
        }
        //如果buffer的总大小超出了kafka限制则将buffer 拆开打包发送
        int startIndex = 0;
        long totalSize = singleRecordSize;
        for (int i = 1; i < buffer.size(); i++) {
            if (totalSize + singleRecordSize >= effectiveRecordSize) {
                sender.accept(buffer.subList(startIndex, i));
                startIndex = i;
                totalSize = 0;
            }
            totalSize += singleRecordSize;
        }
        if (startIndex < buffer.size() - 1) {
            sender.accept(buffer.subList(startIndex, buffer.size() - 1));
        }
        // 清空缓存数据
        buffer.clear();
    }

    /**
     * 计算一条数据的大小
     *
     * @param record 从hdfs 读取出来的数据
     * @param task 当前导入任务
     * @return 当前这条数据的字节数
     */
    private long calculateRecordSize(List<Object> record, BkhdfsPullerTask task) {
        int totalSize = 0;
        for (int i = 0; i < task.getColsInOrder().length; i++) {
            String filedName = task.getColsInOrder()[i];
            Object fieldValue = record.get(i);
            totalSize += getFieldTypeSize(task.getColumns().get(filedName), fieldValue);
        }
        return totalSize;
    }

    /**
     * 计算数据类型所占字节大小
     *
     * @param type schema字段类型
     * @param fieldValue 对应的值
     * @return 数据所占字节大小
     */
    public int getFieldTypeSize(String type, Object fieldValue) {
        int byteSize;
        switch (type) {
            case EtlConsts.STRING:
                byteSize = fieldValue != null ? ((String) fieldValue).getBytes(StandardCharsets.UTF_8).length : 0;
                break;
            case EtlConsts.BIGINT:
            case EtlConsts.LONG:
            case EtlConsts.DOUBLE:
                byteSize = 8; // 8 byte
                break;
            default:
                byteSize = 4; // 4 byte
        }
        return byteSize;

    }


    /**
     * 发送消息到kafka中
     *
     * @param producer kafka的producer
     * @param topic 发送数据的目的地topic
     * @param msgKey 发送数据的msg key
     * @param msgValue 发送数据的msg value
     */
    protected void sendRecordsToKafka(Producer<String, String> producer, String topic, String msgKey, String msgValue) {
        if (StringUtils.isNotBlank(msgValue)) {
            // 发送消息到kafka中，使用转换结束时的时间（秒）作为msg key（避免日志中记录avro格式的消息内容）
            producer.send(new ProducerRecord<>(topic, msgKey, msgValue), new ProducerCallback(topic, msgKey, ""));
        } else {
            LogUtils.warn(log, String.format("worker %d: the avro msg is empty, something is wrong!", config.workerId));
        }
    }


    /**
     * 获取数据的metric tag，用于打点上报和avro中给metric tag字段赋值
     *
     * @param rtId 数据的result table id
     * @param tagTime 当前分钟的时间戳
     * @return metric tag的字符串
     */
    private String getMetricTag(String rtId, long tagTime) {
        return rtId + "|" + tagTime;
    }
}
