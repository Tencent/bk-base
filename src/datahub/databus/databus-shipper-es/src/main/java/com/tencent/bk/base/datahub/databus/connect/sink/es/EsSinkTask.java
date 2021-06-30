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


package com.tencent.bk.base.datahub.databus.connect.sink.es;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.tencent.bk.base.datahub.databus.commons.transfer.Transfer;
import com.tencent.bk.base.datahub.databus.commons.transfer.TransferFactory;
import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkTask;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.ZkUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class EsSinkTask extends BkSinkTask {

    private static final Logger log = LoggerFactory.getLogger(EsSinkTask.class);

    private EsSinkConfig config;
    private String index;
    private Transfer transfer;
    private boolean noType = false;
    private boolean needParseJsonField = false;
    private List<String> jsonFields = null;
    private boolean hasUniqueKey = false;
    private List<String> storageKeys = null;

    /**
     * 启动EsSinkTask任务,初始化资源
     */
    @Override
    public void startTask() {
        // 自身逻辑,创建所需的对象
        config = new EsSinkConfig(configProperties);
        Map<String, Object> esStorageConfig = this.ctx.getStorageConfig("es");
        jsonFields = (List<String>) esStorageConfig.get("json_fields");
        needParseJsonField =
                !Consts.CLEAN.equalsIgnoreCase(this.ctx.getRtType()) && (null != jsonFields && !jsonFields.isEmpty());
        hasUniqueKey = (boolean) esStorageConfig.getOrDefault("has_unique_key", false);
        if (hasUniqueKey) {
            storageKeys = (List<String>) esStorageConfig.get("storage_keys");
        }
        // 获取可用的索引列表(connector所在集群负责index创建，task所在集群无需负责index创建)
        EsClientUtils.getInstance()
                .addRtToEsCluster(this.config.esClusterName, this.config.esHost, this.config.esHttpPort, "",
                        this.config.enableAuth, this.config.authUser, this.config.authPassword);
        initEsIndex();
        // 注册回调函数，当rt配置发生变化时，会将isTaskContextChanged设置为true
        ZkUtils.registerCtxChangeCallback(rtId, this);
        String dataType = BasicProps.getInstance().getClusterProps().getOrDefault("value.converter", "");
        transfer = TransferFactory.genConvert(dataType);
        if (null != config.version && !"".equals(config.version.trim()) && config.version.indexOf(".") > 0) {
            try {
                noType = Integer.parseInt(config.version.substring(0, config.version.indexOf(".")))
                        >= EsSinkConfig.ES_7_BIG_VERSION;
            } catch (NumberFormatException e) {
                LogUtils.warn(log, "{} failed to parse es cluster version {}, just ignore. {}", rtId, config.version,
                        e.getMessage());
            } catch (Throwable e) {
                LogUtils.warn(log, "{} failed to parse es cluster version {}, Unknown error ! {}", rtId, config.version,
                        e.getMessage());
            }
        }

    }

    /**
     * 处理kafka中的消息,将数据写入es中。
     *
     * @param records kafka中的消息记录
     */
    @Override
    public void processData(Collection<SinkRecord> records) {
        checkAndPause();

        // 处理kafka中的数据，调用存储逻辑
        final long now = System.currentTimeMillis() / 1000;  // 取秒
        final long tagTime = now / 60 * 60; // 取分钟对应的秒数

        for (SinkRecord record : records) {
            // kafka connect框架在kafka发生抖动的时候，可能拉取到重复的消息，这里需要做处理。
            if (isRecordProcessed(record)) {
                continue;
            }
            String key = (record.key() == null) ? "" : record.key().toString();
            ConvertResult result = transfer.getConvertResult(converter, key, record.value());
            // 此处去掉es里的doc id，使用es自动生成的doc id。如果此处设置了doc id，es在写入数据前会进行检索，性能上存在损耗。
            //String id = record.topic() + "+" + record.kafkaPartition() + "+" + record.kafkaOffset() + "+";
            List<String> jsonResult = result.getJsonResult();
            for (int i = 0; i < jsonResult.size(); i++) {
                String json = jsonResult.get(i);
                json = needParseJsonField ? parseJsonField(json, this.jsonFields) : json;
                IndexRequest doc;
                if (noType) {
                    doc = new IndexRequest(index).source(json, XContentType.JSON);
                } else {
                    doc = new IndexRequest(index, config.typeName).source(json, XContentType.JSON);
                }
                if (hasUniqueKey) {
                    doc.id(generateUniqueKey(json, storageKeys, record, i));
                }
                EsClientUtils.getInstance().addDocToEsBulkProcessor(config.esClusterName, doc);
            }

            markRecordProcessed(record); // 标记此条消息已被处理过
            msgCountTotal++;
            msgSizeTotal += transfer.getMsgLength();

            // 上报打点数据
            try {
                Metric.getInstance().updateStat(config.rtId, config.connector, record.topic(), "es", jsonResult.size(),
                        transfer.getMsgLength(), result.getTag(), tagTime + "");
                if (result.getErrors().size() > 0) {
                    Metric.getInstance().updateTopicErrInfo(config.connector, result.getErrors());
                    // 上报清洗失败的异常数据
                    Metric.getInstance()
                            .setBadEtlMsg(rtId, ctx.getDataId(), key, transfer.getMsgStr(), record.kafkaPartition(),
                                    record.kafkaOffset(), result.getFailedResult(), now);
                }
                setDelayTime(result.getTagTime(), tagTime);
                Metric.getInstance().updateDelayInfo(config.connector, now, maxDelayTime, minDelayTime);
            } catch (Exception ignore) {
                LogUtils.warn(log, "failed to update stat info for {}, just ignore this. {}", rtId,
                        ignore.getMessage());
            }
        }

        // 重置延迟计时
        resetDelayTimeCounter();
    }

    /**
     * 将json 某个字段string类型的json串转成json对象
     *
     * @param orgJson 原始json串
     * @return 转换之后的json串
     */
    protected String parseJsonField(String orgJson, List<String> jsonFields) {
        if (null == jsonFields || jsonFields.isEmpty()) {
            return orgJson;
        }
        ObjectNode json;
        try {
            json = (ObjectNode) JsonUtils.MAPPER.readTree(orgJson);
            for (String jsonField : jsonFields) {
                JsonNode fieldJson = json.get(jsonField);
                if (null != fieldJson && fieldJson.getNodeType() == JsonNodeType.STRING && StringUtils
                        .isNotBlank(fieldJson.textValue())) {
                    json.put(jsonField, JsonUtils.MAPPER.readTree(fieldJson.textValue()));
                }
            }
        } catch (IOException e) {
            Metric.getInstance().reportEvent(config.connector, Consts.TASK_RUN_FAIL, ExceptionUtils.getStackTrace(e),
                    e.getMessage());
            LogUtils.warn(log, "failed to parse json :{} field for {}, just ignore this. {}", orgJson, config.rtId,
                    e.getMessage());
            json = null;
        }
        return null != json ? json.toString() : orgJson;
    }

    /**
     * 根据组合字段组成唯一健
     *
     * @param orgJson 原始数据
     * @param storageKeys 组合字段
     * @return 唯一健
     */
    protected String generateUniqueKey(String orgJson, List<String> storageKeys, SinkRecord record, int index) {
        try {
            ObjectNode json = (ObjectNode) JsonUtils.MAPPER.readTree(orgJson);
            StringBuilder uniqueKey = new StringBuilder("");
            if (storageKeys.isEmpty()) {
                uniqueKey.append(record.topic()).append(record.kafkaPartition()).append(record.kafkaOffset())
                        .append(index);
            } else {
                for (String storageKey : storageKeys) {
                    uniqueKey.append(json.get(storageKey).textValue());
                }
            }
            return uniqueKey.toString();
        } catch (IOException e) {
            LogUtils.warn(log, "failed to parse json :{} field for {}, just ignore this. {}", orgJson, config.rtId,
                    e.getMessage());
        }
        return null;
    }

    /**
     * 初始化写入的es的index,
     */
    private void initEsIndex() {
        // 默认使用固定的alias来写入数据，alias名称和rt的名称相同
        index = rtId.toLowerCase();
        LogUtils.info(log, "send to alias {} for rt {}", index, rtId);
    }

    /**
     * 检查集群的内存状态，内存过低时，暂停消费kafka中的数据
     */
    private void checkAndPause() {
        if (EsClientUtils.getInstance().isInLowMemory()) {
            LogUtils.warn(log, "{} stop consuming in es {} as memory is very low!!!", rtId, config.esClusterName);
            for (TopicPartition tp : context.assignment()) {
                context.pause(tp);
            }

            while (!isStop.get()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignore) {
                    // ignore
                }
                if (!EsClientUtils.getInstance().isInLowMemory()) {
                    LogUtils.info(log, "{} resume consuming in es {} as memory is in good state!", rtId,
                            config.esClusterName);
                    for (TopicPartition tp : context.assignment()) {
                        context.resume(tp);
                    }
                    // 返回主循环，以便继续处理kafka中的数据
                    return;
                }
            }
        }
    }
}
