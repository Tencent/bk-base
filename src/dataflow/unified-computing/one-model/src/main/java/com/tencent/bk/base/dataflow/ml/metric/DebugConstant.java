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

package com.tencent.bk.base.dataflow.ml.metric;

import com.tencent.bk.base.dataflow.ml.topology.ModelTopology;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag;

public class DebugConstant {

    private static ModelTopology topology;
    private static Broadcast<ModelTopology> broadcastTopology;
    private static Broadcast<String> activeNodeId;
    private static String runMode;
    private static Broadcast<String> broadcastRunMode;

    private static SparkContext sc;

    public static void initRunMode(SparkSession sparkSession, ModelTopology topology) {
        sc = sparkSession.sparkContext();

        DebugConstant.topology = topology;
        ClassTag<ModelTopology> classTag =
                scala.reflect.ClassTag$.MODULE$.apply(topology.getClass());
        DebugConstant.broadcastTopology = sc.broadcast(topology, classTag);

        DebugConstant.runMode = topology.getRunMode();
        ClassTag<String> stringClassTag =
                scala.reflect.ClassTag$.MODULE$.apply(runMode.getClass());
        DebugConstant.broadcastRunMode = sc.broadcast(runMode, stringClassTag);
    }

    public static Broadcast<ModelTopology> getBroadcastTopology() {
        return broadcastTopology;
    }

    public static String getRunMode() {
        return runMode;
    }

    public static Broadcast<String> getActiveNodeId() {
        return activeNodeId;
    }

    public static void setActiveNodeId(String nodeId) {
        ClassTag<String> classTag =
                scala.reflect.ClassTag$.MODULE$.apply(nodeId.getClass());
        activeNodeId = sc.broadcast(nodeId, classTag);
    }

    public static Broadcast<String> getBroadcastRunMode() {
        return broadcastRunMode;
    }

    public static ModelTopology getTopology() {
        return topology;
    }
}
