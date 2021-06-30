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

import static com.tencent.bk.base.datahub.databus.commons.Consts.RT_ID;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ICEBERG_EVENT_TOPIC;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ICEBERG_EVENT_TOPIC_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ICEBERG_TABLE_STAT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.KAFKA_BOOTSTRAP_SERVERS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ZERO;
import static com.tencent.bk.base.datahub.iceberg.C.ICEBERG_SNAPSHOT;
import static com.tencent.bk.base.datahub.iceberg.C.OPERATION;
import static com.tencent.bk.base.datahub.iceberg.C.SUMMARY;
import static com.tencent.bk.base.datahub.iceberg.C.TABLE_NAME;
import static com.tencent.bk.base.datahub.iceberg.C.TIMESTAMP;
import static org.apache.iceberg.SnapshotSummary.ADDED_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.ADDED_RECORDS_PROP;
import static org.apache.iceberg.SnapshotSummary.CHANGED_PARTITION_COUNT_PROP;
import static org.apache.iceberg.SnapshotSummary.DELETED_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.DELETED_RECORDS_PROP;

import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.rest.dto.ReportDataParam;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.TsdbWriter;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/data")
public class DataService {

    private static final Logger log = LoggerFactory.getLogger(DataService.class);

    private static final ConcurrentHashMap<String, KafkaProducer<String, String>> PRODUCERS = new ConcurrentHashMap<>();

    private static final String ICEBERG_CREATE_TABLE = "iceberg_create_table";
    private static final String ICEBERG_DROP_TABLE = "iceberg_drop_table";
    private static final String ICEBERG_RENAME_TABLE = "iceberg_rename_table";
    private static final String SEPARATOR = "|";

    /**
     * 接收上报的数据，落地到存储中
     *
     * @param param 参数列表
     * @return 返回上报的数据
     */
    @POST
    @Path("/report")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult report(final ReportDataParam param) {
        try {
            switch (param.getType()) {
                case ICEBERG_SNAPSHOT:
                    reportIcebergSnapshot(param.getData());
                    break;

                case ICEBERG_CREATE_TABLE:
                case ICEBERG_DROP_TABLE:
                case ICEBERG_RENAME_TABLE:
                    // 未来新增此类事件和数据处理逻辑
                    log.info("got iceberg table event {}", param);
                    break;

                default:
                    log.info("got unknown type data {}", param);
            }

            return new ApiResult(param);
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, "process reported data failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false, null, "");
        }
    }

    /**
     * 上报iceberg快照数据到kafka和influxdb中
     *
     * @param data iceberg快照的数据
     */
    private void reportIcebergSnapshot(Map<String, Object> data) {
        long now = System.currentTimeMillis() / 1000;  // 当前的时间戳，单位秒
        if (data.containsKey(TIMESTAMP)) {
            now = (Long) data.get(TIMESTAMP) / 1000;
        }

        String tableName = data.get(TABLE_NAME).toString();
        String operation = data.get(OPERATION).toString();
        String rtId = CommUtils.parseResultTableId(tableName);
        data.put(RT_ID, rtId);

        Map<String, String> summary = (Map<String, String>) data.get(SUMMARY);
        String addedFiles = summary.getOrDefault(ADDED_FILES_PROP, ZERO);
        String deletedFiles = summary.getOrDefault(DELETED_FILES_PROP, ZERO);
        String addRecords = summary.getOrDefault(ADDED_RECORDS_PROP, ZERO);
        String deletedRecords = summary.getOrDefault(DELETED_RECORDS_PROP, ZERO);
        String changedPartitions = summary.getOrDefault(CHANGED_PARTITION_COUNT_PROP, ZERO);

        // 将数据落地到influxdb中
        String tagStr = String.format("table=%s,operation=%s,rt_id=%s", tableName, operation, rtId);
        String fs = "added_files=%si,deleted_files=%si,added_records=%si,deleted_records=%si,changed_partitions=%si";
        String fieldsStr = String.format(fs, addedFiles, deletedFiles, addRecords, deletedRecords, changedPartitions);
        TsdbWriter.getInstance().reportData(ICEBERG_TABLE_STAT, tagStr, fieldsStr, now);

        // 落地一份到kafka中备查
        String addr = DatabusProps.getInstance().getProperty(KAFKA_BOOTSTRAP_SERVERS);
        String topic = DatabusProps.getInstance().getOrDefault(ICEBERG_EVENT_TOPIC, ICEBERG_EVENT_TOPIC_DEFAULT);
        if (StringUtils.isNotBlank(addr)) {
            KafkaProducer<String, String> producer = PRODUCERS.computeIfAbsent(addr, this::initProducer);
            String key = tableName + SEPARATOR + operation;
            String value = JsonUtils.toJsonWithoutException(data);
            producer.send(new ProducerRecord<>(topic, key, value), (metadata, e) -> {
                if (e != null) {
                    log.error(String.format("send kafka msg failed. [topic=%s, key=%s, value=%s]", topic, key, value),
                            e);
                }
            });
        }
    }

    /**
     * 初始化kafka producer
     *
     * @param kafkaAddr kafka的地址
     * @return kafka的producer
     */
    private KafkaProducer<String, String> initProducer(String kafkaAddr) {
        Properties props = new Properties();
        props.putAll(DatabusProps.getInstance().originalsWithPrefix("producer."));
        props.put(Consts.KEY_DESER, StringDeserializer.class.getName());
        props.put(Consts.VALUE_DESER, StringDeserializer.class.getName());
        props.put(Consts.KEY_SERIALIZER, StringSerializer.class.getName());
        props.put(Consts.VALUE_SERIALIZER, StringSerializer.class.getName());
        props.put(Consts.BOOTSTRAP_SERVERS, kafkaAddr);

        return new KafkaProducer<>(props);
    }
}
