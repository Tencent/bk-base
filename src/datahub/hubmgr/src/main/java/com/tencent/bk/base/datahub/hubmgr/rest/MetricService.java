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

package com.tencent.bk.base.datahub.hubmgr.rest;


import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DATABUSMGR_ERROR_LOG_TOPIC;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DATABUSMGR_ERROR_LOG_TOPIC_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DATABUSMGR_EVENT_TOPIC;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DATABUSMGR_EVENT_TOPIC_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.KAFKA_BOOTSTRAP_SERVERS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.REST_SERVICE;

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.errors.BadConfException;
import com.tencent.bk.base.datahub.databus.commons.utils.KafkaUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.rest.bean.Msg;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/metric")
public class MetricService {

    private static final Logger log = LoggerFactory.getLogger(MetricService.class);
    private static final Random RANDOM = new Random();
    private static final DatabusProps PROPS = DatabusProps.getInstance();

    /**
     * 检索错误信息并返回
     *
     * @param keyword 检索的key
     * @param minute 查询的时间段，默认最近30分钟内
     * @param limit 返回的数据条数
     * @return 匹配到的错误信息，列表返回
     */
    @GET
    @Path("/error/search")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult searchErrors(@DefaultValue("") @QueryParam("keyword") String keyword,
            @DefaultValue("30") @QueryParam("minute") int minute,
            @DefaultValue("100") @QueryParam("limit") int limit) {
        minute = minute > 0 ? minute : 30;
        limit = limit > 0 ? limit : 100;
        String topic = PROPS.getOrDefault(DATABUSMGR_ERROR_LOG_TOPIC, DATABUSMGR_ERROR_LOG_TOPIC_DEFAULT);

        return new ApiResult(searchKafka(keyword, topic, minute, limit));
    }

    /**
     * 查看最近的和key相关的错误消息
     *
     * @param keyword 检索的key
     * @param limit 返回的数据条数
     * @return 匹配到的错误信息，列表返回
     */
    @GET
    @Path("/error/tail")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult tailErrors(@DefaultValue("") @QueryParam("keyword") String keyword,
            @DefaultValue("100") @QueryParam("limit") int limit) {
        limit = limit > 0 ? limit : 100;
        String topic = PROPS.getOrDefault(DATABUSMGR_ERROR_LOG_TOPIC, DATABUSMGR_ERROR_LOG_TOPIC_DEFAULT);

        return new ApiResult(tailKafka(keyword, topic, limit));
    }

    /**
     * 检索事件信息并返回
     *
     * @param keyword 检索的key
     * @param minute 查询的时间段，默认最近30分钟内
     * @param limit 返回的数据条数
     * @return 匹配到的事件信息，列表返回
     */
    @GET
    @Path("/event/search")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult searchEvent(@DefaultValue("") @QueryParam("keyword") String keyword,
            @DefaultValue("30") @QueryParam("minute") int minute,
            @DefaultValue("100") @QueryParam("limit") int limit) {
        minute = minute > 0 ? minute : 30;
        limit = limit > 0 ? limit : 100;
        String topic = PROPS.getOrDefault(DATABUSMGR_EVENT_TOPIC, DATABUSMGR_EVENT_TOPIC_DEFAULT);

        return new ApiResult(searchKafka(keyword, topic, minute, limit));
    }

    /**
     * 查看最近的和key相关的事件消息
     *
     * @param keyword 检索的key
     * @param limit 返回的数据条数
     * @return 匹配到的事件信息，列表返回
     */
    @GET
    @Path("/event/tail")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult tailEvent(@DefaultValue("") @QueryParam("keyword") String keyword,
            @DefaultValue("100") @QueryParam("limit") int limit) {
        limit = limit > 0 ? limit : 100;
        String topic = PROPS.getOrDefault(DATABUSMGR_EVENT_TOPIC, DATABUSMGR_EVENT_TOPIC_DEFAULT);

        return new ApiResult(tailKafka(keyword, topic, limit));
    }

    /**
     * 检索指定的topic中包含keyword的消息，将符合条件的消息返回。
     *
     * @param keyword 检索关键字
     * @param topic topic名称
     * @param minute 消息生成时间在指定的时间范围内
     * @param limit 消息条数
     * @return 符合条件的消息记录
     */
    private Object[] searchKafka(String keyword, String topic, int minute, int limit) {
        Consumer<String, String> consumer = initConsumer();
        resetOffsetForSearch(consumer, topic, 0, minute);
        Object[] result = filter(consumer, keyword, true, limit, false);
        closeConsumer(consumer);

        return result;
    }

    /**
     * 查看指定topic尾部的消息，按照keyword对消息的key进行过滤，将符合条件的消息返回。
     *
     * @param keyword 检索关键字
     * @param topic topic名称
     * @param limit 消息条数
     * @return 符合条件的消息记录
     */
    private Object[] tailKafka(String keyword, String topic, int limit) {
        Consumer<String, String> consumer = initConsumer();
        resetOffsetForTail(consumer, topic, 0, limit * 10);
        Object[] result = filter(consumer, keyword, false, limit, true);
        closeConsumer(consumer);

        return result;
    }

    /**
     * 初始化kafka consumer
     *
     * @return kafka consumer
     */
    private Consumer<String, String> initConsumer() {
        String kafkaAddr = PROPS.getOrDefault(KAFKA_BOOTSTRAP_SERVERS, "");
        if (StringUtils.isBlank(kafkaAddr)) {
            LogUtils.warn(log, "kafka.bootstrap.servers config is empty!");
            throw new BadConfException("kafka.bootstrap.servers config is empty!");
        }

        // 初始化consumer
        String groupId = String.format("%s-%s-%s", REST_SERVICE, RANDOM.nextInt(100), RANDOM.nextInt(1000));
        return KafkaUtils.initStringConsumer(groupId, kafkaAddr);
    }

    /**
     * 从队列尾部向前寻找指定的时间段，重置offset，用于检索数据
     *
     * @param consumer kafka consumer
     * @param topic kafka topic名称
     * @param partition kafka topic分区
     * @param minutes 分钟数
     */
    private void resetOffsetForSearch(Consumer<String, String> consumer, String topic, int partition, int minutes) {
        TopicPartition tp = new TopicPartition(topic, partition);
        List<TopicPartition> tps = Arrays.asList(tp);
        consumer.assign(tps);
        consumer.seekToBeginning(tps);

        // 寻找合适的offset去消费
        long ts = System.currentTimeMillis() - minutes * 60000L;
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(Collections.singletonMap(tp, ts));
        if (offsets.get(tp) != null) {
            long seekOffset = offsets.get(tp).offset();
            LogUtils.info(log, "reset offset to {} for topic partition {}-{} by time", seekOffset, topic, partition);
            consumer.seek(tp, seekOffset);
        }
    }

    /**
     * 从队列尾部向前寻找指定的消息条数，重置offset，用于检索数据
     *
     * @param consumer kafka consumer
     * @param topic kafka topic名称
     * @param partition kafka topic分区
     * @param backCount 向前寻找的记录数量
     */
    private void resetOffsetForTail(Consumer<String, String> consumer, String topic, int partition, int backCount) {
        TopicPartition tp = new TopicPartition(topic, partition);
        List<TopicPartition> tps = Arrays.asList(tp);
        consumer.assign(tps);
        consumer.seekToEnd(tps);

        // 寻找合适的offset去消费
        long endOffset = consumer.position(tp);
        long seekOffset = endOffset > backCount ? endOffset - backCount : 0;
        LogUtils.info(log, "reset offset to {} for topic partition {}-{} by msg count", seekOffset, topic, partition);
        consumer.seek(tp, seekOffset);
    }

    /**
     * 根据关键字检索kafka中的消息，将符合条件的结果返回
     *
     * @param consumer kafka consumer
     * @param keyword 检索的关键字
     * @param searchValue 是否检索kafka消息的value
     * @param limit 最多返回的kafka消息数量
     * @return 符合条件的kafka消息
     */
    private Object[] filter(Consumer<String, String> consumer, String keyword, boolean searchValue,
            int limit, boolean readToEnd) {
        LogUtils.info(log, "find msg match {} and search value is {}. result limit is {} with read to end set to {}",
                keyword, searchValue, limit, readToEnd);
        // 读取数据
        ArrayBlockingQueue<ConsumerRecord<String, String>> buffer = new ArrayBlockingQueue<>(limit);
        long start = System.currentTimeMillis();

        // 最多执行10s，避免接口耗时太长
        while (System.currentTimeMillis() - start < 10000) {
            ConsumerRecords<String, String> records = consumer.poll(2000);
            LogUtils.info(log, "got {} records to search", records.count());
            if (records.isEmpty()) {
                // 当kafka中最近1s没有新数据时，认为所有数据消费完毕
                LogUtils.info(log, "reached end of topic when searching keyword {}", keyword);
                break;
            } else {
                for (ConsumerRecord<String, String> record : records) {
                    // 按照检索条件过滤
                    if (record.key().contains(keyword) || (searchValue && record.value().contains(keyword))) {
                        if (!buffer.offer(record)) {
                            if (readToEnd) {
                                // 需要读到最后一条kafka消息，这里需在buffer中删掉一条记录，保证有空间可以添加
                                buffer.poll();
                                if (!buffer.offer(record)) {
                                    LogUtils.warn(log, "failed to add record {} to result", record);
                                }
                            } else {
                                // buffer已满，直接返回结果
                                return reverseResult(buffer);
                            }
                        }
                    }
                }
            }
        }

        return reverseResult(buffer);
    }

    /**
     * 将队列中的元素倒排序，放入列表中返回。
     *
     * @param buffer 队列对象
     * @return 调换顺序后的列表
     */
    private Object[] reverseResult(ArrayBlockingQueue<ConsumerRecord<String, String>> buffer) {
        // 将结果集按照倒序排列返回，最新的在最前面
        Object[] result = new Object[buffer.size()];
        AtomicInteger idx = new AtomicInteger(buffer.size());
        buffer.forEach(r -> result[idx.decrementAndGet()] = new Msg(r.key(), r.value()));

        return result;
    }

    /**
     * 关闭kafka consumer
     *
     * @param consumer kafka consumer对象
     */
    private void closeConsumer(Consumer<String, String> consumer) {
        try {
            consumer.close(500, TimeUnit.MICROSECONDS);
        } catch (Exception e) {
            LogUtils.warn(log, "failed to close consumer! ", e);
        }
    }

}
