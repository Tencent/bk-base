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

import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.transform.AbstractTransform;
import com.tencent.bk.base.dataflow.flink.streaming.runtime.FlinkStreamingRuntime;
import com.tencent.bk.base.dataflow.flink.streaming.transform.join.JoinTransform;
import com.tencent.bk.base.dataflow.flink.streaming.transform.associate.StaticJoinTransform;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformStage {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformStage.class);

     // 支持的transform类型
     // 1. 普通的transform计算 单个父表 没有窗口
     // 2. 数据时间的窗口计算
     // 3. 实时流关联
    private static final Map<String, Class> TRANSFORMERS = new HashMap<String, Class>() {
        {
            put("common_transform", CommonTransformTransform.class);
            put("event_time_window", EventTimeWindowTransform.class);
            put("join_transform", JoinTransform.class);
            put("static_join_transform", StaticJoinTransform.class);
        }
    };

    // transform node, 即你需要的transform节点
    private AbstractTransform transform;

    public TransformStage(TransformNode node, Map<String, DataStream<Row>> dataStreams, FlinkStreamingRuntime runtime) {
        this.transform = createTransformer(node, dataStreams, runtime);
    }

    /**
     * transform 实例初始化
     *
     * @param node
     * @param dataStreams
     * @param runtime
     * @return
     */
    private AbstractTransform createTransformer(TransformNode node, Map<String, DataStream<Row>> dataStreams,
            FlinkStreamingRuntime runtime) {
        Class transformerClass = getTransformerClass(node);
        AbstractTransform transform = null;

        try {
            transform = (AbstractTransform) transformerClass
                    .getConstructor(TransformNode.class, Map.class, FlinkStreamingRuntime.class)
                    .newInstance(node, dataStreams, runtime);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }

        return transform;
    }

    /**
     * 根据rt的processor type获取transform class
     *
     * @param node rt
     * @return 返回transform class
     */
    private static Class getTransformerClass(TransformNode node) {
        Class transformerClass = TRANSFORMERS.get(node.getProcessorType());
        return transformerClass;
    }

    /**
     * 执行transform实例的距离逻辑
     */
    public void transform() {
        transform.createNode();
    }
}
