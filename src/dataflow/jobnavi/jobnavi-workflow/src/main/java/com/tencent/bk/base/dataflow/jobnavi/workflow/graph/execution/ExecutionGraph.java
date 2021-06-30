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

package com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution;

import com.tencent.bk.base.dataflow.jobnavi.workflow.Configuration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutionGraph {

    private Map<String, AbstractExecutionNode> nodeMap = new HashMap<>();
    private List<AbstractExecutionNode> head = new ArrayList<>();
    private List<AbstractExecutionNode> savepoint = new ArrayList<>();
    private Configuration configuration;
    private Map<String, Object> parameter = new HashMap<>();

    public Map<String, AbstractExecutionNode> getNodeMap() {
        return nodeMap;
    }

    public void setNodeMap(Map<String, AbstractExecutionNode> nodeMap) {
        this.nodeMap = nodeMap;
    }

    public List<AbstractExecutionNode> getHead() {
        return head;
    }

    public void setHead(List<AbstractExecutionNode> head) {
        this.head = head;
    }

    public List<AbstractExecutionNode> getSavepoint() {
        return savepoint;
    }

    public void setSavepoint(List<AbstractExecutionNode> savepoint) {
        this.savepoint = savepoint;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public void setConfiguration(Map<String, Object> configuration) {
        this.configuration = new Configuration(configuration);
    }

    public Map<String, Object> getParameter() {
        return parameter;
    }

    public void setParameter(Map<String, Object> parameter) {
        this.parameter = parameter;
    }

    /**
     * dump execution graph to HTTP json
     *
     * @return
     */
    public Map<String, Object> toHttpJson() {
        Map<String, Object> httpJson = new HashMap<>();
        Map<String, Object> nodeMap = new HashMap<>();
        for (Map.Entry<String, AbstractExecutionNode> entry : this.nodeMap.entrySet()) {
            nodeMap.put(entry.getKey(), entry.getValue().toHttpJson());
        }
        httpJson.put("nodeMap", nodeMap);
        List<String> head = new ArrayList<>();
        for (AbstractExecutionNode node : this.head) {
            head.add(node.getId());
        }
        httpJson.put("head", head);
        if (configuration != null) {
            httpJson.put("configuration", configuration.toHttpJson());
        }
        if (!parameter.isEmpty()) {
            httpJson.put("parameter", parameter);
        }
        return httpJson;
    }
}
