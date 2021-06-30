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

package com.tencent.bk.base.dataflow.flink.streaming.transform.associate.ignite;

import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.IGNITE_DEFAULT_ASYNC_IO_CONCURRENCY;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.IGNITE_DEFAULT_ASYNC_IO_TIMEOUT_SEC;

import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.flink.schema.SchemaFactory;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.types.Row;

public class IgniteAsyncStaticJoin extends IgniteStaticJoin {

    //异步IO配置,默认10个并发
    private int asyncConcurrency;

    public IgniteAsyncStaticJoin(TransformNode node, Map<String, DataStream> dataStreams,
            FlinkStreamingTopology topology) {
        super(node, dataStreams, topology);
    }

    @Override
    public void initStorageParameter() {
        super.initStorageParameter();
        this.asyncConcurrency = (int) joinArgs
                .getOrDefault("async_io_concurrency", IGNITE_DEFAULT_ASYNC_IO_CONCURRENCY);
    }

    @Override
    public DataStream<List<Row>> doStaticJoin() {
        //accumulate data as a batch and send it to the next operator
        DataStream<List<Row>> batchStream = dataStreams
                .get(node.getSingleParentNode().getNodeId())
                .transform(getBulkOperatorName(), getBulkTypeInfo(), getBulkOperator());
        //use flink AsyncIO to process batch data and query ignite in parallel
        DataStream asyncStream = AsyncDataStream
                .unorderedWait(batchStream, getIgniteAsyncOperator(), IGNITE_DEFAULT_ASYNC_IO_TIMEOUT_SEC,
                        TimeUnit.SECONDS, asyncConcurrency)
                .name(getIgniteAsyncOperatorName())
                .returns(getIgniteAsyncTypeInfo());
        //filter null data
        DataStream<List<Row>> filterStream = asyncStream.filter(new FilterFunction<List<Row>>() {
            @Override
            public boolean filter(List<Row> value) throws Exception {
                return value != null && value.size() != 0;
            }
        });
        return filterStream;
    }

    public String getBulkOperatorName() {
        return "bulk_operator";
    }

    public ListTypeInfo getBulkTypeInfo() {
        RowTypeInfo bulkTypeInfo = new RowTypeInfo(new SchemaFactory().getFieldsTypes(node.getSingleParentNode()));
        ListTypeInfo bulkListTypeInfo = new ListTypeInfo<Row>(bulkTypeInfo);
        return bulkListTypeInfo;
    }

    public OneInputStreamOperator getBulkOperator() {
        return new BulkAddOperator(node, topology);
    }

    public String getIgniteAsyncOperatorName() {
        return "ignite_async_static_join";
    }

    public ListTypeInfo getIgniteAsyncTypeInfo() {
        RowTypeInfo igniteOpTypeInfo = new RowTypeInfo(new SchemaFactory().getFieldsTypes(node));
        ListTypeInfo igniteOpListTypeInfo = new ListTypeInfo<Row>(igniteOpTypeInfo);
        return igniteOpListTypeInfo;
    }

    public AsyncFunction getIgniteAsyncOperator() {
        IgniteClusterInfo igniteClusterInfo = new IgniteClusterInfo(cacheName, igniteHost, ignitePort, ignitePassword,
                igniteUser, igniteCluster);
        return new IgniteAsyncStaticJoinOperator(
                node,
                topology,
                joinType,
                streamKeyIndexs,
                keySeparator,
                asyncConcurrency,
                igniteClusterInfo);
    }
}
