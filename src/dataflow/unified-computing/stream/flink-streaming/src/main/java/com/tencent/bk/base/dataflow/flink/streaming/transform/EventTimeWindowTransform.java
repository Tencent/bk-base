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

package com.tencent.bk.base.dataflow.flink.streaming.transform;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.transform.AbstractTransform;
import com.tencent.bk.base.dataflow.debug.DebugResultDataStorage;
import com.tencent.bk.base.dataflow.flink.schema.SchemaFactory;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.flink.streaming.metric.LatenessCounterBkDataSideOutputProcess;
import com.tencent.bk.base.dataflow.flink.streaming.runtime.FlinkStreamingRuntime;
import com.tencent.bk.base.dataflow.flink.streaming.table.RegisterTable;
import com.tencent.bk.base.dataflow.flink.streaming.transform.window.AllowedLatenessEventTimeTrigger;
import com.tencent.bk.base.dataflow.flink.streaming.transform.window.AllowedLatenessSessionEventTimeTrigger;
import com.tencent.bk.base.dataflow.flink.streaming.transform.window.ProcessingTimeAndEventTimeTrigger;
import com.tencent.bk.base.dataflow.metric.MetricMapper;
import java.util.Map;
import java.util.TimeZone;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.BkDataGroupWindowAggregateStreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventTimeWindowTransform extends AbstractTransform {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventTimeWindowTransform.class);

    private TransformNode node;
    private String processArgs;

    private StreamTableEnvironment tableEnv;
    private TypeInformation<?>[] fieldsTypes;
    private RowTypeInfo typeInfo;
    private FlinkStreamingTopology topology;
    private Map<String, DataStream<Row>> dataStreams;

    public EventTimeWindowTransform(TransformNode node, Map<String, DataStream<Row>> dataStreams,
            FlinkStreamingRuntime runtime) {
        this.tableEnv = runtime.getTableEnv();
        this.node = node;
        this.processArgs = node.getProcessorArgs();
        this.fieldsTypes = new SchemaFactory().getFieldsTypes(node);
        this.typeInfo = new RowTypeInfo(fieldsTypes);
        this.topology = runtime.getTopology();
        this.dataStreams = dataStreams;
    }

    /**
     * 构建 event time window transform节点 包含run sql和注册子rt用到的sql表
     */
    @Override
    public void createNode() {
        // 1、设置侧输出延迟数据输出标签
        BkDataGroupWindowAggregateStreamQueryConfig queryConfig = new BkDataGroupWindowAggregateStreamQueryConfig();
        String tag = "OutputTag-" + this.node.getNodeId();
        queryConfig.sideOutputLateData(tag);
        // 2、设置侧输出延迟数据的处理器
        queryConfig.setSideOutputProcess(new LatenessCounterBkDataSideOutputProcess(this.node, this.topology));

        // 3、根据窗口类型定制触发器
        ConstantVar.WindowType windowType = this.node.getWindowConfig().getWindowType();

        // 累加窗口设置
        if (windowType == ConstantVar.WindowType.accumulate) {
            // 设置计算为累加计算
            queryConfig.setAccumulate(true);
            // 设置时区偏移（按天的时候需要设置时区偏移，flink底层是使用UTC时区计算的）
            queryConfig.setBkSqlWindowOffset(TimeZone.getTimeZone(this.topology.getTimeZone()).getRawOffset());
        }

        if (this.node.getWindowConfig().isAllowedLateness()) {
            configureAllowedLateness(queryConfig, windowType);
        } else {
            // 不允许延迟数据进入窗口计算
            switch (windowType) {
                case session:
                    // 会话窗口增加超时机制，
                    // 超时时间 = sessionGap + expiredTime。
                    int sessionGap = this.node.getWindowConfig().getSessionGap();
                    int expiredTime = this.node.getWindowConfig().getExpiredTime();
                    if (sessionGap > 0 && expiredTime > 0) {
                        LOGGER.info("[{}] session window setting sessionGap timeout.", this.node.getNodeId());
                        int timeOut = sessionGap + expiredTime;
                        LOGGER.info("[{}] session window set timeout = {}", this.node.getNodeId(), timeOut);
                        ProcessingTimeAndEventTimeTrigger trigger = ProcessingTimeAndEventTimeTrigger
                                .of(Time.seconds(timeOut));
                        queryConfig.trigger(trigger);
                    }
                    break;
                case sliding:
                case tumbling:
                case accumulate:
                    break;
                default:
            }
        }
        // run sql
        Table windowTable = tableEnv.sqlQuery(processArgs);
        DataStream<Row> appendDataStream = tableEnv.toAppendStream(windowTable, Row.class, queryConfig);
        
        DataStream<Row> outDataStream = appendDataStream.map(new MapOutputFunction(node)).name(node.getNodeId())
                .returns(typeInfo);
        // metric
        outDataStream.map(new MetricMapper(node, topology)).name("Metric");

        // debug
        if (topology.isDebug()) {
            outDataStream.map(new DebugResultDataStorage(node, topology));
        }

        // 注册表
        RegisterTable.registerFlinkTable(node, outDataStream, tableEnv);

        this.dataStreams.put(node.getNodeId(), outDataStream);
    }

    private void configureAllowedLateness(
            BkDataGroupWindowAggregateStreamQueryConfig queryConfig,
            ConstantVar.WindowType windowType) {
        // 允许延迟数据进入窗口计算
        queryConfig.allowedLateness(Time.hours(this.node.getWindowConfig().getLatenessTime()).toMilliseconds());
        // tumbling sliding accumulate 使用同一个trigger
        switch (windowType) {
            case tumbling:
            case sliding:
            case accumulate:
                queryConfig.trigger(AllowedLatenessEventTimeTrigger.of(
                        this.node.getWindowConfig().getLatenessCountFreq(),
                        this.node.getWindowConfig().getLatenessTime()));
                break;
            case session:
                // 会话窗口增加超时机制，
                // 超时时间 = sessionGap + expiredTime。
                int sessionGap = this.node.getWindowConfig().getSessionGap();
                int expiredTime = this.node.getWindowConfig().getExpiredTime();
                if (sessionGap > 0 && expiredTime > 0) {
                    // 当有设置过期时间
                    LOGGER.info("[{}] session window setting sessionGap timeout.", this.node.getNodeId());
                    int timeOut = sessionGap + expiredTime;
                    LOGGER.info("[{}] session window set timeout = {}", this.node.getNodeId(), timeOut);
                    ProcessingTimeAndEventTimeTrigger trigger = ProcessingTimeAndEventTimeTrigger
                            .of(Time.seconds(timeOut));
                    queryConfig.trigger(trigger);
                } else {
                    // 当没有设置过期时间
                    queryConfig.trigger(AllowedLatenessSessionEventTimeTrigger.create());
                }
                break;
            default:
        }
    }
}
