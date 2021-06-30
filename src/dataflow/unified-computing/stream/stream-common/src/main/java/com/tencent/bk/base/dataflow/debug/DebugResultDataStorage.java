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

package com.tencent.bk.base.dataflow.debug;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.common.HttpUtils;
import com.tencent.bk.base.dataflow.core.common.Tools;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.topology.StreamTopology;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugResultDataStorage extends RichMapFunction<Row, Row> implements ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DebugResultDataStorage.class);
    // 10 sec
    private static final long DEFAULT_UDF_DEBUG_TIME_INTERVAL_MS = 10 * 1000L;
    private Node node;
    private StreamTopology topology;
    private LimitQueue<String> queue;
    private Map<String, Object> resultData;
    private int filedIndex;
    private ObjectMapper objectMapper = new ObjectMapper();
    private Map<String, String> resultDataParams;
    private String timeZone;
    private SimpleDateFormat dayDateFormat;
    private SimpleDateFormat dateFormat;
    private SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
        {
            setTimeZone(TimeZone.getTimeZone("UTC"));
        }
    };
    private transient ProcessingTimeService processingTimeService;

    public DebugResultDataStorage(Node node, StreamTopology topology) {
        this.node = node;
        this.topology = topology;
        queue = new LimitQueue<>(20);
        this.timeZone = topology.getTimeZone();
        this.dayDateFormat = new SimpleDateFormat("yyyyMMdd") {
            {
                setTimeZone(TimeZone.getTimeZone(timeZone));
            }
        };
        this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
            {
                setTimeZone(TimeZone.getTimeZone(timeZone));
            }
        };
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("Register debug result data timer.");
        processingTimeService =
                ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
        processingTimeService
                .registerTimer(processingTimeService.getCurrentProcessingTime() + DEFAULT_UDF_DEBUG_TIME_INTERVAL_MS,
                        this);
    }

    @Override
    public Row map(Row row) throws Exception {
        filedIndex = 0;
        resultData = new HashMap<>();
        for (NodeField field : node.getFields()) {
            String fieldName = field.getField();
            switch (fieldName) {
                case ConstantVar.EVENT_TIME:
                    Long dtEventTimeStamp = utcFormat.parse(row.getField(filedIndex).toString()).getTime();
                    String dtEventTime = dateFormat.format(new Date(dtEventTimeStamp));
                    resultData.put(field.getField(), dtEventTime);
                    break;
                case ConstantVar.BKDATA_PARTITION_OFFSET:
                    break;
                default:
                    resultData.put(fieldName, row.getField(filedIndex));
            }

            filedIndex++;
        }
        queue.offer(Tools.writeValueAsString(resultData));

        return null;
    }

    private void reportResultData() {
        if (this.queue.size() > 0) {
            reportResultDataByRestApi(topology.getDebugConfig().getDebugResultDataApi(), this.queue);
            this.queue.clear();
        }
    }

    /**
     * use rest api
     *
     * @param restApiUrl rest api ur
     * @param queue result data
     */
    private void reportResultDataByRestApi(String restApiUrl, LimitQueue<String> queue) {
        try {
            String resultData = URLEncoder.encode(queue.getQueue().toString(), "utf-8");
            // url编码时 空格会转化为+
            resultData = resultData.replaceAll("\\+", "%20");
            resultDataParams = new HashMap<>();
            resultDataParams.put("job_id", topology.getJobId());
            resultDataParams.put("result_table_id", node.getNodeId());
            resultDataParams.put("result_data", resultData);
            resultDataParams.put("debug_date", String.valueOf(new Date().getTime()));
            resultDataParams.put("thedate", dayDateFormat.format(new Date()));
            LOG.info("Begin to save result data {} and params is {} and url is {}", node.getNodeId(),
                    resultDataParams.toString(),
                    MessageFormat.format(restApiUrl, topology.getDebugConfig().getDebugId()));
            String result = HttpUtils
                    .post(MessageFormat.format(restApiUrl, topology.getDebugConfig().getDebugId()), resultDataParams);

            JsonNode tree = objectMapper.readTree(result);
            if (!tree.get("result").asBoolean()) {
                throw new RuntimeException(
                        "Set result data by api return value is false, and parameters is " + resultDataParams.toString()
                                + " ;result is " + tree.toString());
            }
        } catch (Exception e) {
            LOG.error("Failed to save result data by rest api.", e);
        } finally {
            LOG.info("End of save result data {}", node.getNodeId());
        }
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        LOG.info("Debug result data on processing time.");
        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
        reportResultData();
        processingTimeService.registerTimer(currentProcessingTime + DEFAULT_UDF_DEBUG_TIME_INTERVAL_MS, this);
    }
}
