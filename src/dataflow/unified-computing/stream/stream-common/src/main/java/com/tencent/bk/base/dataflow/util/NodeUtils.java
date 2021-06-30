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

package com.tencent.bk.base.dataflow.util;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.topo.WindowConfig;
import java.util.Arrays;
import java.util.List;

public final class NodeUtils {


    /**
     * 获取节点类型
     *
     * @param node 节点
     * @return
     */
    public static String getNodeType(Node node) {
        String nodeType = "other";
        if (node instanceof SourceNode) {
            nodeType = "source";
        } else if (node instanceof TransformNode) {
            nodeType = "transform";
        } else if (node instanceof SinkNode) {
            nodeType = "sink";
        }
        return nodeType;
    }

    /**
     * 解析origin
     *
     * @param origin 传入的参数
     * @return list 第一个值为表名，第二个值为字段名
     */
    public static List<String> parseOrigin(String origin) {
        String[] str = origin.split(":");
        return Arrays.asList(str);
    }

    /**
     * 获取sink的topic名称
     *
     * @param nodeId node id
     * @return sink topic
     */
    public static String generateKafkaTopic(String nodeId) {
        return "table_" + nodeId;
    }

    /**
     * 判断node是否含有offset字段
     *
     * @param fields fields
     * @return have offset field?
     */
    public static boolean haveOffsetField(List<NodeField> fields) {
        for (NodeField field : fields) {
            if (ConstantVar.BKDATA_PARTITION_OFFSET.equals(field.getField())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取node的统计频率
     *
     * @param node node
     * @return count freq
     */
    public static int getCountFreq(Node node) {
        if (node instanceof TransformNode) {
            WindowConfig windowConfig = ((TransformNode) node).getWindowConfig();
            if (null != windowConfig) {
                return windowConfig.getCountFreq();
            }
        }
        return 0;
    }

    /**
     * 获取node的等待时间，即水位
     *
     * @param node node
     * @return waiting time
     */
    public static int getWaitingTime(Node node) {
        if (node instanceof TransformNode) {
            WindowConfig windowConfig = ((TransformNode) node).getWindowConfig();
            if (null != windowConfig) {
                return windowConfig.getWaitingTime();
            }
        }
        return 0;
    }

    /**
     * 创建flink表
     *
     * @param parent 父表
     * @param child 子表
     * @return 表名
     */
    public static String createSqlTableName(Node parent, Node child) {
        return String.format("%s___%s", child.getNodeName(), parent.getNodeName());
    }

    public static String getEventTimeFields(Node node) {
        return node.getCommonFields() + ", rowtime.rowtime";
    }

    /**
     * 根据node获取biz id
     * 默认“_”分割取第一位
     *
     * @param node node
     * @return biz id
     */
    public static String getBizId(Node node) {
        String bizId;
        try {
            bizId = node.getNodeId().split("_")[0];
            return bizId;
        } catch (Exception e) {
            return "0";
        }
    }
}
