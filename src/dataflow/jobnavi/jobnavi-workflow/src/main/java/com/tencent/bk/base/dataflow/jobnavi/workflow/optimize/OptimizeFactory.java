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

package com.tencent.bk.base.dataflow.jobnavi.workflow.optimize;

import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.AbstractLogicalNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.StateLogicalNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.SubtaskLogicalNode;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;


public class OptimizeFactory {

    private static final Logger logger = Logger.getLogger(OptimizeFactory.class);
    private static final Map<String, String> optimizeMap = new HashMap<>();

    static {
        registerOptimizeMap(SubtaskLogicalNode.class.getName(), OptimizeSubtaskExecutor.class.getName());
        registerOptimizeMap(StateLogicalNode.class.getName(), OptimizeStateExecutor.class.getName());
    }


    public static AbstractOptimizeExecutor getOptimizeExecutor(AbstractLogicalNode node) throws Exception {
        if (optimizeMap.get(node.getClass().getName()) != null) {
            AbstractOptimizeExecutor optimizeExecutor;
            try {
                optimizeExecutor = (AbstractOptimizeExecutor) Class.forName(optimizeMap.get(node.getClass().getName()))
                        .getDeclaredConstructor(AbstractLogicalNode.class).newInstance(node);
                return optimizeExecutor;
            } catch (Exception e) {
                logger.error("fail to construct an optimize executor", e);
                throw e;
            }
        }
        String message = "can not find an optimize executor";
        logger.error(message);
        throw new Exception(message);

    }

    public static void registerOptimizeMap(String className, String executorName) {
        optimizeMap.put(className, executorName);
    }
}
