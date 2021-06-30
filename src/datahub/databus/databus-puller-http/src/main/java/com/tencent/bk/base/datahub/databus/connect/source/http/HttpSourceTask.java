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

package com.tencent.bk.base.datahub.databus.connect.source.http;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.common.source.BkSourceTask;
import com.tencent.bk.base.datahub.databus.connect.source.http.HttpSourceConnectorConfig.Method;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.connector.source.BkSourceRecord;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSourceTask extends BkSourceTask {

    private static final Logger log = LoggerFactory.getLogger(HttpSourceTask.class);
    private static final String POSITION = "position";
    private static final String HTTP_DATA_ID = "http_data_id";
    private static final String http_PULLER_LOG_TOPIC = "databus_http_puller_log";
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private HttpSourceConnectorConfig config;
    private Producer<String, String> producer;
    private ScheduledExecutorService scheduledExecutorService;
    private BlockingQueue<BkSourceRecord<String>> queue = new LinkedBlockingQueue<>(1);

    /**
     * 时间格式化
     */
    private TimeFormat timeFieldFormat;
    /**
     * 最后一次拉取的时间
     */
    private long lastTime;
    /**
     * 是否为带时间查询
     */
    private boolean isTimeQuery;

    @Override
    protected void startTask() {
        this.config = new HttpSourceConnectorConfig(configProperties);
        if (StringUtils.isBlank(config.httpUrl)) {
            throw new ConfigException("Invalid http url : " + config.httpUrl);
        }

        if (config.periodSecond <= 0) {
            throw new ConfigException("Invalid collect period : " + config.periodSecond);
        }

        this.isTimeQuery = StringUtils.isNotBlank(config.timeFormat);
        if (isTimeQuery) {
            if (StringUtils.isBlank(config.startTimeField) && StringUtils.isBlank(config.endTimeField)) {
                throw new ConfigException("Invalid startTimeField: " + config.startTimeField + " or endTimeField: "
                        + config.endTimeField);
            }
            String offset = null;
            Map<String, Object> offsetMap = context.offsetStorageReader()
                    .offset(Collections.singletonMap(HTTP_DATA_ID, this.dataId));
            if (null != offsetMap) {
                offset = offsetMap.get(POSITION).toString();
            }
            this.timeFieldFormat = new TimeFormat(config.timeFormat);
            lastTime = StringUtils.isBlank(offset) ? System.currentTimeMillis() : Long.valueOf(offset);
            LogUtils.info(log, "{} fetched lastTime {} from BkDatabusContext.", config.dataId, lastTime);
        }

        this.producer = initProducer();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("Http-puller-#" + config.dataId).build());

        scheduledExecutorService
                .scheduleWithFixedDelay(() -> executeCollect(), 1, config.periodSecond, TimeUnit.SECONDS);
    }

    @Override
    protected List<SourceRecord> pollData() {
        ArrayList<SourceRecord> records = new ArrayList<>();
        try {

            // 从队列中获取需要处理的文件，逐行读取，发送到目的地的kafka中
            BkSourceRecord<String> record = this.queue.take();
            if (record != null) {
                CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                producer.send(new ProducerRecord<>(config.destTopic, record.getKey(), record.getValue()),
                        (recordMetadata, e) -> {
                            if (null != e) {
                                completableFuture.completeExceptionally(e);
                            } else {
                                completableFuture.complete(null);
                            }
                        });
                completableFuture.get();
                record.ack();
                if (record.getRecordSequence() > 0) {
                    this.lastTime = record.getRecordSequence();
                    // 构建SourceRecord，其中记录当前处理的文件和行号信息
                    Map<String, String> sourceOffset = Collections
                            .singletonMap(POSITION, String.valueOf(record.getRecordSequence()));
                    Map<String, String> sourcePartition = Collections.singletonMap(HTTP_DATA_ID, this.dataId);
                    records.add(new SourceRecord(sourcePartition, sourceOffset, http_PULLER_LOG_TOPIC, null,
                            String.format("%s %s pull http %s to line %s (-1 is EOF)", dataId,
                                    dateFormat.format(new Date(System.currentTimeMillis())), config.httpUrl,
                                    record.getRecordSequence())));
                    LogUtils.info(log, "{} send position info to offset kafka. {}", dataId, record.getRecordSequence());
                }
            }
        } catch (Exception ignore) {
            LogUtils.warn(log, dataId + " exception during poll from queue...", ignore);
        }

        return records;
    }


    /**
     * 执行采集
     */
    protected void executeCollect() {
        try {
            do {
                if (config.httpMethod == Method.get) {
                    doGetCollect();
                } else if (config.httpMethod == Method.post) {
                    doPostCollect();
                } else {
                    LogUtils.warn(log, "{} schedule  collect http data failed! Invalid HttpMethod:{}", config.dataId,
                            config.httpMethod);
                }
            } while (isTimeQuery && this.lastTime < System.currentTimeMillis() - TimeUnit.SECONDS
                    .toMillis(config.periodSecond));
        } catch (Throwable e) {
            LogUtils.warn(log, "{} do {} collect http data failed! error:{}", config.dataId, config.httpMethod,
                    e.getMessage());
        }
    }

    /**
     * post方式拉取
     */
    protected void doPostCollect() throws InterruptedException {
        String url = config.httpUrl;
        Map<String, String> params = new HashMap<>();
        long endTime = getEndTime(lastTime, config.periodSecond);
        if (isTimeQuery) {
            if (StringUtils.isNotBlank(config.startTimeField)) {
                params.put(config.startTimeField, timeFieldFormat.format(lastTime));
            }
            if (StringUtils.isNotBlank(config.endTimeField)) {
                params.put(config.endTimeField, timeFieldFormat.format(endTime));
            }
        }
        LogUtils.info(log, "Starting http source for dataId {}; url:{},params:{}", config.dataId, url, params);
        String response = null;
        int retryCount = 0;
        do {
            retryCount++;
            try {
                response = HttpUtils.post(url, params);
            } catch (Exception e) {
                LogUtils.warn(log, "{} execute post for url {} failed! error:{}", config.dataId, url, e.getMessage());
            }
        } while (null == response && retryCount <= config.retryTimes);
        this.putResponse(response, endTime);
    }

    /**
     * 获取上一次拉取后这个周期的时间戳与当前时间减去延迟时间的最小值
     *
     * @return 最后拉取的时间
     */
    public long getEndTime(long lastTime, int periodSecond) {
        return Math.min(lastTime + TimeUnit.SECONDS.toMillis(periodSecond), System.currentTimeMillis());
    }


    /**
     * get 方式拉取
     *
     * @throws IOException 网络IO异常
     */
    protected void doGetCollect() throws IOException, InterruptedException {
        String url = config.httpUrl;
        long endTime = getEndTime(lastTime, config.periodSecond);
        if (isTimeQuery) {
            // 有时间字段的，给url增加时间参数
            url = UrlUtils.buildUrl(url, config.startTimeField, config.endTimeField, timeFieldFormat.format(lastTime),
                    timeFieldFormat.format(endTime));
        }
        LogUtils.info(log, "Starting http source for dataId {}; url:{}", config.dataId, url);
        String response = null;
        int retryCount = 0;
        do {
            retryCount++;
            try {
                response = HttpUtils.get(url);
            } catch (Exception e) {
                LogUtils.warn(log, "{} execute url {} failed! error:{}", config.dataId, url, e.getMessage());
            }
        } while (null == response && retryCount <= config.retryTimes);
        this.putResponse(response, endTime);
    }

    /**
     * 将拉取的结果发送到pulsar
     *
     * @param response 拉取的结果
     * @param endTime 最后次拉取的时间
     */
    private void putResponse(String response, long endTime) throws InterruptedException {
        if (null != response) {
            // 监控打点, 将时间戳转换为10s的倍数
            long tagTime = System.currentTimeMillis() / 10000 * 10;
            String outputTag = getMetricTag(config.dataId, tagTime);
            String msgKey = String.format("ds=%d&tag=%s", tagTime, outputTag);
            BkSourceRecord<String> sourceRecord = new BkSourceRecord(msgKey, generateEvent(response), response.length(),
                    -1, endTime);
            this.queue.put(sourceRecord);
        }
    }

    /**
     * 生成结果
     *
     * @param data 拉取结果
     * @return 最后发送到pulsar中的消息
     */
    private String generateEvent(String data) {
        JsonObject event = new JsonObject();
        event.addProperty("dataid", config.dataId);
        if (isTimeQuery) {
            JsonParser jsonParser = new JsonParser();
            event.add("data", jsonParser.parse(data).getAsJsonObject().get("data"));
        } else {
            event.addProperty("data", data);
        }
        event.addProperty("type", "httpbeat");
        event.addProperty("timezone", TimeZone.getDefault().getRawOffset() / TimeUnit.HOURS.toMillis(1));
        event.addProperty("datetime", dateFormat.format(new Date()));
        event.addProperty("utctime", dateFormat.format(getUtcTime()));
        return event.toString();
    }

    /**
     * 获取UTC时间
     *
     * @return utc时间date
     */
    private Date getUtcTime() {
        Calendar cal = Calendar.getInstance();
        int offset = cal.get(Calendar.ZONE_OFFSET);
        int dstoff = cal.get(Calendar.DST_OFFSET);
        cal.add(Calendar.MILLISECOND, -(offset + dstoff));
        return cal.getTime();
    }

    /**
     * 获取数据的metric tag，用于打点上报和avro中给metric tag字段赋值
     *
     * @param dataId dataid
     * @param tagTime 当前分钟的时间戳
     * @return metric tag的字符串
     */
    private String getMetricTag(int dataId, long tagTime) {
        // tag=dataId|timestamp|ip
        return String.format("%s|%d|%s", dataId, tagTime, Metric.getInstance().getWorkerIp());
    }

    /**
     * 停止任务
     */
    @Override
    public void stopTask() {
        if (null != this.scheduledExecutorService) {
            this.scheduledExecutorService.shutdown();
        }

        try {
            producer.close(5, TimeUnit.SECONDS);
        } catch (Exception ignore) {
            LogUtils.warn(log, dataId + " failed to close kafka producer!", ignore);
        }
    }

    /**
     * 初始化kafka producer,用于发送结果数据
     */
    private KafkaProducer<String, String> initProducer() {
        try {
            Map<String, Object> producerProps = new HashMap<>(BasicProps.getInstance().getProducerProps());
            // producer的配置从配置文件中获取
            producerProps.put(Consts.BOOTSTRAP_SERVERS, config.destKafkaBs);

            // 序列化配置,发送数据的value为字节数组
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            LogUtils.debug(log, "_initProducer_: kafka_producer: {}", producerProps);

            return new KafkaProducer<>(producerProps);
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.KAFKA_CONNECT_FAIL,
                    "_initProducer_: failed to initialize kafka producer", e);
            throw new ConnectException(e);
        }
    }

}
