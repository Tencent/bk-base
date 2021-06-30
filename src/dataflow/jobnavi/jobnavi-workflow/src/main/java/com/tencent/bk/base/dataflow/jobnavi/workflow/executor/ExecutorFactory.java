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

package com.tencent.bk.base.dataflow.jobnavi.workflow.executor;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.AbstractExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.ExpStateExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.SubtaskExecutionNode;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class ExecutorFactory {

    private static final Logger logger = Logger.getLogger(ExecutorFactory.class);
    private static final Map<String, String> executionMap = new HashMap<>();

    static {
        registerOptimizeMap(SubtaskExecutionNode.class.getName(), SubtaskExecutor.class.getName());
        registerOptimizeMap(ExpStateExecutionNode.class.getName(), ExpStateExecutor.class.getName());
    }


    /**
     * get node executor
     *
     * @param node
     * @return node executor
     * @throws Exception
     */
    public static AbstractNodeExecutor getNodeExecutor(AbstractExecutionNode node) throws Exception {

        logger.info("start find executor");
        if (executionMap.get(node.getClass().getName()) != null) {
            try {

                logger.info("can find ");
                AbstractNodeExecutor nodeExecutor = (AbstractNodeExecutor) Class
                        .forName(executionMap.get(node.getClass().getName()))
                        .getDeclaredConstructor(AbstractExecutionNode.class).newInstance(node);
                logger.info("find the executor");
                return nodeExecutor;
            } catch (Throwable e) {
                logger.error("fail to construct a node executor", e);
                throw e;
            }
        }
        String message = "can not find a node executor";
        logger.error(message);
        throw new NaviException(message);

    }

    public static void registerOptimizeMap(String className, String executorName) {
        executionMap.put(className, executorName);
    }
}
