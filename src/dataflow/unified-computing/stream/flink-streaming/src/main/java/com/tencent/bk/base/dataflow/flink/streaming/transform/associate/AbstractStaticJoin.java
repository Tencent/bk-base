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

package com.tencent.bk.base.dataflow.flink.streaming.transform.associate;

import com.tencent.bk.base.dataflow.core.common.Tools;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.flink.schema.SchemaFactory;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public abstract class AbstractStaticJoin implements Serializable {

    private static final long serialVersionUID = 1L;
    // 静态关联公共配置
    protected TransformNode node;
    protected FlinkStreamingTopology topology;
    protected Map<String, DataStream> dataStreams;
    protected TypeInformation<?>[] fieldsTypes;
    protected Map<String, Object> joinArgs;
    // 静态关联初始化参数
    protected String joinType;
    protected String staticTableName;
    protected String bizId;
    protected List<Integer> streamKeyIndexs = new ArrayList<>();

    public AbstractStaticJoin(TransformNode node,
            Map<String, DataStream> dataStreams,
            FlinkStreamingTopology topology) {
        this.node = node;
        this.topology = topology;
        this.dataStreams = dataStreams;
        this.fieldsTypes = new SchemaFactory().getFieldsTypes(node);
        this.joinArgs = Tools.readMap(node.getProcessorArgs());
        initCommonParameter();
        initStorageParameter();
        initStreamKeyIndexs();
    }

    /**
     * 解析参数
     */
    private void initCommonParameter() {
        this.joinType = String.valueOf(joinArgs.get("type"));
        this.staticTableName = String.valueOf(joinArgs.get("table_name"));
        this.bizId = String.valueOf(joinArgs.get("biz_id"));
    }

    public abstract void initStorageParameter();

    public abstract void initStreamKeyIndexs();

    public abstract DataStream<List<Row>> doStaticJoin();

    public void setJoinArgs(Map<String, Object> joinArgs) {
        this.joinArgs = joinArgs;
    }

    public List<Integer> getStreamKeyIndexs() {
        return streamKeyIndexs;
    }
}
