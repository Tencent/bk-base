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

import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.flink.schema.SchemaFactory;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.flink.streaming.transform.associate.AbstractStaticJoin;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.types.Row;

public class IgniteStaticJoin extends AbstractStaticJoin {

    //ignite连接信息
    protected String igniteHost;
    protected int ignitePort;
    protected String ignitePassword;
    protected String igniteUser;
    protected String igniteCluster;
    protected String cacheName;
    protected String keySeparator;
    protected List<String> keyOrder;

    public IgniteStaticJoin(TransformNode node, Map<String, DataStream> dataStreams, FlinkStreamingTopology topology) {
        super(node, dataStreams, topology);
    }

    @Override
    public void initStorageParameter() {
        Map<String, Object> storageInfo = (HashMap<String, Object>) joinArgs.get("storage_info");
        this.igniteHost = String.valueOf(storageInfo.get("host"));
        this.ignitePort = Integer.parseInt(String.valueOf(storageInfo.get("port")));
        this.ignitePassword = String.valueOf(storageInfo.get("password"));
        this.igniteUser = String.valueOf(storageInfo.get("ignite_user"));
        this.igniteCluster = String.valueOf(storageInfo.get("ignite_cluster"));
        this.cacheName = String.valueOf(storageInfo.get("cache_name"));
        this.keySeparator = String.valueOf(storageInfo.get("key_separator"));
        this.keyOrder = (ArrayList<String>) storageInfo.get("key_order");
    }

    @Override
    public void initStreamKeyIndexs() {
        List<Map<String, String>> joinKeys = (List<Map<String, String>>) joinArgs.get("join_keys");
        List<String> streamKeys = new ArrayList<>();
        // 静态关联 ignite key的拼接，并非简单字典顺序,要使用storekit指定的顺序来拼接
        LinkedHashMap<String, String> keyOrderMap = keyOrder.stream()
                .collect(Collectors.toMap(key -> key, String::new, (e1, e2) -> e2, LinkedHashMap::new));
        //利用LinkedHashMap对stream按static预设的顺序排序
        joinKeys.stream().forEach(joinKey -> {
            String staticKey = joinKey.get("static");
            String streamKey = joinKey.get("stream");
            keyOrderMap.put(staticKey, streamKey);
        });
        //value为排序后的stream字段
        streamKeys.addAll(keyOrderMap.values());
        for (String key : streamKeys) {
            this.streamKeyIndexs.add(node.getSingleParentNode().getFieldIndex(key));
        }
    }

    @Override
    public DataStream<List<Row>> doStaticJoin() {
        RowTypeInfo typeInfo = new RowTypeInfo(new SchemaFactory().getFieldsTypes(node));
        ListTypeInfo listTypeInfo = new ListTypeInfo<Row>(typeInfo);
        DataStream<List<Row>> midStream = dataStreams.get(node.getSingleParentNode().getNodeId())
                .transform(getOperatorName(), listTypeInfo, getOperator())
                .filter(new FilterFunction<List<Row>>() {
                    @Override
                    public boolean filter(List<Row> value) throws Exception {
                        return value != null && value.size() != 0;
                    }
                });
        return midStream;
    }

    public String getOperatorName() {
        return "ignite_static_join";
    }

    public OneInputStreamOperator getOperator() {
        IgniteClusterInfo igniteClusterInfo = new IgniteClusterInfo(cacheName, igniteHost, ignitePort, ignitePassword,
                igniteUser, igniteCluster);
        return new IgniteStaticJoinOperator(
                node,
                topology,
                joinType,
                streamKeyIndexs,
                keySeparator,
                igniteClusterInfo);
    }
}
