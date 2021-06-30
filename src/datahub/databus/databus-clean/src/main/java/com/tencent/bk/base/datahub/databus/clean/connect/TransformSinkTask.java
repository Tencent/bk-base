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

package com.tencent.bk.base.datahub.databus.clean.connect;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkTask;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.callback.ProducerCallback;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.convert.ConverterFactory;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.TransformUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.ZkUtils;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformSinkTask extends BkSinkTask {

    private static final Logger log = LoggerFactory.getLogger(TransformSinkTask.class);
    private static final String RECOVER_OFFSET_FROM_KEY = "recover.offset.from.key";
    private static final String AVRO_MSG_BATCH_COUNT = "avro.msg.batch.count";

    private SimpleDateFormat dateFormat;
    private SimpleDateFormat dteventtimeDateFormat;
    private SimpleDateFormat dataTimeFormat;

    private TransformSinkConfig config;
    private String[] colsInOrder;
    private Map<String, String> columns;
    private Schema recordSchema;
    private Schema recordArrSchema;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private GenericRecord msgRecord;

    private Producer<String, String> producer;
    private String destTopic;
    // 积累一定量的数据往先kafka发送
    private int flushThreshhold = 100;
    private long maxMessageBytePerTime = -1L;
    private long schemaByteSize;
    private boolean recoverOffsetFromKey = false;
    private final AtomicBoolean needThrowException = new AtomicBoolean(false);

    /**
     * 通过解析配置，构建数据转换处理的对象，生成写入kafka的producer
     */
    @Override
    protected void startTask() {
        // 初始化时间对象
        dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        dteventtimeDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dataTimeFormat = new SimpleDateFormat("yyyyMMddHH");
        // 如果命令行参数传入了时区信息，则使用，否则使用机器默认的时区信息
        String tz = System.getProperty(EtlConsts.DISPLAY_TIMEZONE);
        if (tz != null) {
            dateFormat.setTimeZone(TimeZone.getTimeZone(tz));
            dteventtimeDateFormat.setTimeZone(TimeZone.getTimeZone(tz));
        }

        config = new TransformSinkConfig(configProperties);

        // 创建kafka producer，获取相应的topic配置信息
        initProducer();
        // recover offset配置获取
        Map<String, String> clusterProps = BasicProps.getInstance().getClusterProps();
        if (clusterProps.getOrDefault(RECOVER_OFFSET_FROM_KEY, "false").trim().equalsIgnoreCase("true")) {
            recoverOffsetFromKey = true;
        }

        try {
            int i = Integer.parseInt(clusterProps.getOrDefault(AVRO_MSG_BATCH_COUNT, "100"));
            flushThreshhold = i > 0 ? i : flushThreshhold;
        } catch (NumberFormatException e) {
            // ignore
        }

        // 注册回调函数
        ZkUtils.registerCtxChangeCallback(rtId, this);
    }

    /**
     * 打开kafka topic对应的partitions，做一些初始化工作，找到上次消费的offset。
     *
     * @param partitions kafka分区
     */
    @Override
    public void open(Collection<TopicPartition> partitions) {
        if (recoverOffsetFromKey) {
            String bootstrap = config.getString(TransformSinkConfig.PRODUCER_BOOTSTRAP_SERVERS);
            // 当未指定producer的kafka服务地址时，使用默认consumer的kafka服务地址
            if (StringUtils.isBlank(bootstrap)) {
                Map<String, String> clusterInfo = BasicProps.getInstance().getClusterProps();
                bootstrap = clusterInfo.get(Consts.BOOTSTRAP_SERVERS);
            }

            for (TopicPartition tp : partitions) {
                Long offset = TransformUtils.getLastCheckpoint(bootstrap, destTopic, tp.partition());
                if (offset != null) {
                    LogUtils.info(log, "{} {} recovering from target kafka with offset: {}", rtId, tp, offset);
                    context.offset(tp, offset);
                } else {
                    LogUtils.info(log, "{} {} failed to recovering offset from dest kafka", rtId, tp);
                }
            }
        } else {
            super.open(partitions);
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        LogUtils.info(log, "close partitions: {}", partitions);
        super.close(partitions);
    }

    /**
     * 从kafka topic上获取到消息，通过数据转换，提取用户所需的数据记录， 将数据记录打包，然后转换为Avro格式的数据，写到kafka sink topic中
     */
    @Override
    protected void processData(Collection<SinkRecord> records) {
        if (needThrowException.get()) {
            throw new ConnectException(rtId + " got something wrong when sending msg to kafka, please check the log!");
        }

        // 按照partition将record进行分组，每组里的数据批量处理
        Map<Integer, List<SinkRecord>> parToRecords = new HashMap<>();
        for (SinkRecord record : records) {
            if (!parToRecords.containsKey(record.kafkaPartition())) {
                parToRecords.put(record.kafkaPartition(), new ArrayList<>());
            }
            parToRecords.get(record.kafkaPartition()).add(record);
        }

        if (log.isDebugEnabled()) {
            LogUtils.debug(log, "{} got msg of {} partitions in {} records. start transforming them", config.connector,
                    parToRecords.size(), records.size());
        }

        final long now = System.currentTimeMillis() / 1000;  // 取秒
        final long tagTime = now / 60 * 60; // 取分钟对应的秒数
        int bufferCount = 0;
        Map<SinkRecord, List<List<Object>>> buffer = new LinkedHashMap<>();

        for (Map.Entry<Integer, List<SinkRecord>> entry : parToRecords.entrySet()) {
            int cnt = 0;

            List<SinkRecord> sinkRecords = entry.getValue();
            for (SinkRecord record : sinkRecords) {
                cnt++;
                // kafka connect框架在kafka发生抖动的时候，可能拉取到重复的消息，这里需要做处理。
                if (isRecordProcessed(record)) {
                    continue;
                }
                byte[] val = (byte[]) record.value();
                String keyStr = this.getRecordKey(record);
                ConvertResult result = converter.getListObjects(keyStr, val, colsInOrder);
                List<List<Object>> listResult = result.getObjListResult();
                buffer.put(record, listResult);
                bufferCount += listResult.size();

                // 当buffer中数据条数达到阈值,或者当前处理的是最后一条msg时,将消息发送到kafka中
                if (result.getIsDatabusEvent() || (bufferCount >= flushThreshhold) || cnt == sinkRecords.size()) {
                    try {
                        sendCompactKafkaMsg(tagTime, buffer, entry.getKey());
                    } catch (Exception e) {
                        LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_AVRO_DATA,
                                rtId + " exception during compose and send result to kafka: " + buffer, e);
                        throw new ConnectException(e);
                    }
                    buffer.clear(); // 清空buffer中的数据
                    bufferCount = 0;

                    if (result.getIsDatabusEvent()) {
                        // 将总线事件消息向后续的kafka中传递
                        sendDatabusEventMsg(destTopic, entry.getKey(), keyStr);
                    }
                }

                markRecordProcessed(record); // 标记此条消息已被处理过

                msgCountTotal++;
                msgSizeTotal += val.length;

                // 增加数据打点
                try {
                    Metric.getInstance()
                            .updateStat(rtId, config.connector, record.topic(), "kafka", listResult.size(), val.length,
                                    result.getTag(), getMetricTag(tagTime));
                    setDelayTime(result.getTagTime(), now);
                    Metric.getInstance().updateDelayInfo(config.connector, now, maxDelayTime, minDelayTime);
                    if (result.getErrors().size() > 0) {
                        Metric.getInstance().updateTopicErrInfo(config.connector, result.getErrors());
                        // 上报清洗失败的异常数据
                        Metric.getInstance()
                                .setBadEtlMsg(rtId, config.dataId, keyStr, new String(val, StandardCharsets.UTF_8),
                                        entry.getKey(), record.kafkaOffset(), result.getFailedResult(), now);
                    }
                } catch (Exception ignore) {
                    LogUtils.warn(log, "failed to update stat info for {}, just ignore this. {}", rtId,
                            ignore.getMessage());
                }
            }
        }

        resetDelayTimeCounter(); //延迟计数
    }


    /**
     * 计算一条数据的大小
     *
     * @param record 从hdfs 读取出来的数据
     * @return 当前这条数据的字节数
     */
    private long calculateRecordSize(List<Object> record) {
        int totalSize = 0;

        for (int i = 0; i < colsInOrder.length; i++) {
            String filedName = colsInOrder[i];
            Object fieldValue = record.get(i);
            totalSize += getFieldTypeSize(columns.get(filedName), fieldValue);
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
            case EtlConsts.TEXT:
                byteSize = fieldValue != null ? fieldValue.toString().getBytes(StandardCharsets.UTF_8).length : 0;
                break;
            case EtlConsts.BIGINT:
            case EtlConsts.LONG:
            case EtlConsts.DOUBLE:
            case EtlConsts.BIGDECIMAL:
                byteSize = 8; // 8 byte
                break;
            default:
                byteSize = 4; // 4 byte
        }

        return byteSize;

    }


    private String getRecordKey(SinkRecord record) {
        // 获取key
        String keyStr = "";
        if (record.key() != null) {
            if (record.key() instanceof String) {
                keyStr = (String) record.key();
            } else if (record.key() instanceof byte[]) {
                keyStr = new String((byte[]) record.key(), StandardCharsets.UTF_8);
            }
        }
        return keyStr;
    }

    /**
     * 停止运行，将一些使用到的资源清理掉。
     */
    @Override
    protected void stopTask() {
        ZkUtils.removeCtxChangeCallback(rtId, this);
        LogUtils.info(log, "transform task going to stop!");
        if (producer != null) {
            producer.close(10, TimeUnit.SECONDS);
        }
    }

    /**
     * 创建转换器，初始化一些信息
     */
    @Override
    protected void createConverter() {
        LogUtils.info(log, "create converter and columns schema! {}", rtId);
        initParser();
    }

    /**
     * 初始化清洗的converter,以及需要使用的字段
     */
    private void initParser() {
        ctx.setSourceMsgType(Consts.ETL);
        converter = ConverterFactory.getInstance().createConverter(ctx);
        columns = new HashMap<>(ctx.getRtColumns());
        // 屏蔽用户的dtEventTime、dtEventTimeStamp、localTime字段,屏蔽rt里的offset字段
        columns.remove(Consts.DTEVENTTIME);
        columns.remove(Consts.DTEVENTTIMESTAMP);
        columns.remove(Consts.LOCALTIME);
        columns.remove(Consts.OFFSET);
        columns.remove(Consts.ITERATION_IDX);

        colsInOrder = columns.keySet().toArray(new String[columns.size()]);
        // 生成avro schema和一些处理的对象。创建record schema时,在columns的基础上增加了localTime、dtEventTime、dtEventTimestamp等字段
        String schemaStr = TransformUtils.getAvroSchema(columns);
        recordSchema = new Schema.Parser().parse(schemaStr);
        recordArrSchema = Schema.createArray(recordSchema);
        Schema msgSchema = SchemaBuilder.record("kafkamsg_" + config.dataId)
                .fields()
                .name(Consts._TAGTIME_).type().longType().longDefault(0)
                .name(Consts._METRICTAG_).type().stringType().noDefault()
                .name(Consts._VALUE_).type().array().items(recordSchema).noDefault()
                .endRecord();

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(msgSchema);
        dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(CodecFactory.snappyCodec());
        msgRecord = new GenericData.Record(msgSchema);
        schemaByteSize = recordSchema.toString().getBytes(StandardCharsets.UTF_8).length + msgSchema.toString()
                .getBytes(StandardCharsets.UTF_8).length;
        LogUtils.debug(log, "msgSchema {}", msgSchema.toString(true));
    }

    /**
     * 初始化kafka producer,用于发送清洗后的结果数据
     */
    private void initProducer() {
        try {
            Map<String, Object> producerProps = new HashMap<>(config.originalsWithPrefix(Consts.PRODUCER_PREFIX));
            // 兼容外部版逻辑，当producer的kafka未设置时，清洗完的数据写入原kakfa中
            String bsServers = (String) producerProps.get(Consts.BOOTSTRAP_SERVERS);
            if (StringUtils.isBlank(bsServers)) {
                Map<String, String> clusterInfo = BasicProps.getInstance().getClusterProps();
                producerProps.put(Consts.BOOTSTRAP_SERVERS, clusterInfo.get(Consts.BOOTSTRAP_SERVERS));
            }
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");

            // 从集群配置文件中获取producer相关配置项
            producerProps.putAll(BasicProps.getInstance().getProducerProps());
            producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, rtId + "-" + Math.round(Math.random() * 10000));
            maxMessageBytePerTime = Long.parseLong(producerProps.getOrDefault(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                    producerProps.get(ProducerConfig.BATCH_SIZE_CONFIG)).toString());
            producer = new KafkaProducer<>(producerProps);
            destTopic = "table_" + rtId;
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.KAFKA_CONNECT_FAIL,
                    rtId + " failed to initialize kafka producer", e);
            throw new ConnectException(e);
        }
    }

    /**
     * 将清洗出的结果集发送到kafka inner中
     *
     * @param tagTime tagTime时间戳
     * @param records 结果记录 SinkRecord --> 清洗后的结果
     * @param partition kafka分区编号
     */
    private void sendCompactKafkaMsg(long tagTime, Map<SinkRecord, List<List<Object>>> records, int partition) {
        if (null == records || records.isEmpty()) {
            return;
        }

        if (log.isDebugEnabled()) {
            LogUtils.debug(log, "sending {} records to downstreaming kafka(topic: {}, partition: {})", records.size(),
                    destTopic, partition);
        }

        List<List<Object>> firstItem = records.values().iterator().next();
        long singleRecordSize = 0;
        if (!firstItem.isEmpty()) {
            singleRecordSize = calculateRecordSize(firstItem.get(0));
        }

        long effectiveRecordSize = this.maxMessageBytePerTime - this.schemaByteSize;
        if (singleRecordSize > effectiveRecordSize) {
            throw new RecordTooLargeException(
                    String.format("The single message is %s bytes when serialized which is larger than %s.",
                            singleRecordSize, effectiveRecordSize));
        }

        long tsMs = tagTime * 1000;
        class RecordGroup {

            long dataTime;
            long lastOffset;
            List<GenericRecord> records = new ArrayList<>();
        }

        Map<String, RecordGroup> recordGroup = new HashMap<>();
        for (Map.Entry<SinkRecord, List<List<Object>>> entry : records.entrySet()) {
            for (List<Object> value : entry.getValue()) {
                GenericRecord record = writeObjectToRecord(value, tagTime);
                if (null != record.get(Consts.DTEVENTTIMESTAMP)) {
                    tsMs = (long) record.get(Consts.DTEVENTTIMESTAMP);
                }
                RecordGroup group = recordGroup
                        .computeIfAbsent(dataTimeFormat.format(new Date(tsMs)), (time) -> new RecordGroup());
                group.lastOffset = entry.getKey().kafkaOffset();
                group.dataTime = tsMs;
                group.records.add(record);
            }
        }

        for (RecordGroup group : recordGroup.values()) {
            if (singleRecordSize * group.records.size() <= effectiveRecordSize) {
                this.sendCompactMsgToKafka(group.records, group.dataTime, group.lastOffset, tagTime, partition);
            } else {
                //如果buffer的总大小超出了kafka限制则将buffer 拆开打包发送
                int startIndex = 0;
                long totalSize = singleRecordSize;
                for (int i = 1; i < group.records.size(); i++) {
                    if (totalSize + singleRecordSize >= effectiveRecordSize) {
                        this.sendCompactMsgToKafka(group.records.subList(startIndex, i), group.dataTime,
                                group.lastOffset, tagTime, partition);
                        startIndex = i;
                        totalSize = 0;
                    }
                    totalSize += singleRecordSize;
                }

                if (startIndex < group.records.size() - 1) {
                    this.sendCompactMsgToKafka(group.records.subList(startIndex, group.records.size() - 1),
                            group.dataTime, group.lastOffset, tagTime, partition);
                }
            }
        }
    }

    /**
     * 将一批GenericRecord 打包成avro包发送到kafka
     *
     * @param genericRecords GenericRecord 列表
     * @param tsMs 数据时间（小时级别相同的数据）
     * @param offset 这一批数据最大的offset
     * @param tagTime tag时间
     * @param partition 分区号
     */
    private void sendCompactMsgToKafka(List<GenericRecord> genericRecords, long tsMs, long offset, long tagTime,
            int partition) {
        GenericArray<GenericRecord> array = new GenericData.Array<>(genericRecords.size(), recordArrSchema);
        array.addAll(genericRecords);
        msgRecord.put(Consts._VALUE_, array);
        msgRecord.put(Consts._TAGTIME_, tagTime);
        msgRecord.put(Consts._METRICTAG_, getMetricTag(tagTime));

        String msgValue = TransformUtils.getAvroBinaryString(msgRecord, dataFileWriter);
        if (StringUtils.isNotBlank(msgValue)) {
            // MARK dateStr 和 tagTime为啥设成一样的?
            String dateStr = dateFormat.format(new Date(tsMs));
            String keyStr = dateStr + "&tagTime=" + dateStr + "&offset=" + offset;
            // 发送消息到kafka中，使用转换结束时的时间（秒）作为msg key
            producer.send(new ProducerRecord<>(destTopic, partition, keyStr, msgValue),
                    new ProducerCallback(destTopic, partition, keyStr, "", needThrowException));
        } else {
            LogUtils.warn(log, "the avro msg is empty, something is wrong!");
        }
    }

    /**
     * 将一条Record 对应的值转成GenericRecord
     *
     * @param value Record 对应的值
     * @param tagTime tag时间
     * @return 对应的值转成GenericRecord
     */
    private GenericRecord writeObjectToRecord(List<Object> value, long tagTime) {
        GenericRecord avroRecord = new GenericData.Record(recordSchema);
        for (int i = 0; i < colsInOrder.length; i++) {
            String col = colsInOrder[i];
            try {
                // 构造dtEventTimeStamp、dtEventTime、localTime等字段
                if (col.equals(Consts.TIMESTAMP)) {
                    long tsMs = (long) value.get(i);
                    avroRecord.put(Consts.DTEVENTTIME, dteventtimeDateFormat.format(new Date(tsMs)));
                    avroRecord.put(Consts.DTEVENTTIMESTAMP, tsMs);
                    avroRecord.put(Consts.LOCALTIME, dteventtimeDateFormat.format(new Date(tagTime * 1000)));
                    // TIMESTAMP单位为秒, avro 类型为int
                    avroRecord.put(Consts.TIMESTAMP, (int) (tsMs / 1000));
                    continue;
                }
                // 根据数据的类型将其转换为不同的类型
                Object colVal = value.get(i);
                if (colVal instanceof BigDecimal) {
                    BigDecimal bigDecimal = (BigDecimal) colVal;
                    bigDecimal = bigDecimal.setScale(TransformUtils.DEFAULT_DECIMAL_SCALE, BigDecimal.ROUND_HALF_UP);
                    avroRecord.put(col, ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray()));
                } else if (colVal instanceof BigInteger) {
                    BigInteger bigInteger = (BigInteger) colVal;
                    avroRecord.put(col, ByteBuffer.wrap(bigInteger.toByteArray()));
                } else if (colVal instanceof Map) {
                    // 这种情况colVal类型应该为json字符串，使用HashMap来表示，这里需要做一次转换
                    avroRecord.put(col, JsonUtils.toJsonWithoutException(colVal));
                } else {
                    avroRecord.put(col, colVal);
                }

            } catch (AvroRuntimeException ignore) {
                // 字段和schema不匹配，或者设置值的时候发生异常，记录一下，不影响整体流程
                LogUtils.warn(log, "failed to set value for avro record! name: {} value: {} schema: {} errMsg: {}", col,
                        value.get(i), avroRecord.getSchema().toString(false), ignore.getMessage());
            }
        }

        return avroRecord;
    }

    /**
     * 发送总线事件消息到下游的kafka中
     *
     * @param topic 目标topic
     * @param partition 目标partition
     * @param msgKey kafka消息的key
     */
    private void sendDatabusEventMsg(String topic, int partition, String msgKey) {
        LogUtils.info(log, "{} sending databus event {} to {}-{}", rtId, msgKey, topic, partition);
        producer.send(new ProducerRecord<>(topic, partition, msgKey, ""),
                new ProducerCallback(topic, partition, msgKey, ""));
    }

    /**
     * 获取数据的metric tag，用于打点上报和avro中给metric tag字段赋值
     *
     * @param tagTime 当前分钟的时间戳
     * @return metric tag的字符串
     */
    private String getMetricTag(long tagTime) {
        return config.connector + "|" + tagTime;
    }


    @VisibleForTesting
    protected long getLastCheckTime() {
        return lastCheckTime;
    }

    @VisibleForTesting
    protected void setLastCheckTime(long newLastCheckTime) {
        this.lastCheckTime = newLastCheckTime;
    }

    @VisibleForTesting
    protected void setMsgCountTotal(long msgCountTotal) {
        this.msgCountTotal = msgCountTotal;
    }
}
