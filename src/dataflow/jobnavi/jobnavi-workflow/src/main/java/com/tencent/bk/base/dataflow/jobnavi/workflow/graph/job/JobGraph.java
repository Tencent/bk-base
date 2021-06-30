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

package com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job;

import com.tencent.bk.base.dataflow.jobnavi.workflow.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;


public class JobGraph {

    private static final Logger logger = Logger.getLogger(JobGraph.class);

    private Map<String, Object> parameter = new HashMap<>();
    private Configuration conf;
    private Map<String, AbstractLogicalNode> nodeMap = new HashMap<>();
    private Map<String, List<String>> parentMap = new HashMap<>();
    private List<AbstractLogicalNode> head;

    public JobGraph() {

    }

    /**
     * construct job graph
     * @param head
     */
    public JobGraph(List<AbstractLogicalNode> head) {
        this.head = head;
        for (AbstractLogicalNode node : head) {
            generateGraph(node);
        }
    }

    private void generateGraph(AbstractLogicalNode node) {
        nodeMap.put(node.getId(), node);
        if (node.getChildren() == null) {
            return;
        }
        for (AbstractLogicalNode child : node.getChildren()) {
            List<String> parents;
            if (parentMap.containsKey(child.getId())) {
                parents = parentMap.get(child.getId());
            } else {
                parents = new ArrayList<>();
            }
            parents.add(node.getId());
            parentMap.put(child.getId(), parents);
            generateGraph(child);
        }
    }

    public Map<String, Object> getParameter() {
        return parameter;
    }

    public void setParameter(Map<String, Object> parameter) {
        this.parameter = parameter;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Map<String, AbstractLogicalNode> getNodeMap() {
        return nodeMap;
    }

    public void setNodeMap(Map<String, AbstractLogicalNode> nodeMap) {
        this.nodeMap = nodeMap;
    }

    public Map<String, List<String>> getParentMap() {
        return parentMap;
    }

    public void setParentMap(Map<String, List<String>> parentMap) {
        this.parentMap = parentMap;
    }

    public List<AbstractLogicalNode> getHead() {
        return head;
    }

    public void setHead(List<AbstractLogicalNode> head) {
        this.head = head;
    }

    /**
     * parse job graph json string
     *
     * @param json
     * @throws NaviException
     */
    public void parseJson(String json) throws NaviException {
        Map<String, Object> config = JsonUtils.readMap(json);
        if (config.get("param") != null) {
            this.parameter = (Map<String, Object>) config.get("param");
        }
        if (config.get("config") != null) {
            setConf(new Configuration((Map<String, Object>) config.get("config")));
        }
        if (config.get("node") == null) {
            logger.error("node can't be null");
            throw new NaviException("node can't be null");
        }
        Map<String, Object> nodes = (Map<String, Object>) config.get("node");
        Map<String, List<String>> childMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : nodes.entrySet()) {
            Map<String, Object> node = (Map<String, Object>) entry.getValue();
            if (node.get("node_type") == null) {
                logger.error("node type can't be null");
                throw new NaviException("node type can't be null");
            } else if ((node.get("node_type").equals("subtask"))) {
                SubtaskLogicalNode subtaskNode = new SubtaskLogicalNode();
                subtaskNode.parseJson(entry.getKey(), node);
                nodeMap.put(subtaskNode.getId(), subtaskNode);
            } else if (node.get("node_type").equals("state")) {
                StateLogicalNode stateNode = new StateLogicalNode();
                stateNode.parseJson(entry.getKey(), node);
                nodeMap.put(stateNode.getId(), stateNode);
            }
            List<String> childList;
            if (node.get("children") != null) {
                childList = (List<String>) node.get("children");
                childMap.put(entry.getKey(), childList);
            }
        }
        for (Map.Entry<String, List<String>> child : childMap.entrySet()) {
            updateGraph(nodeMap.get(child.getKey()), child.getValue());
        }
        this.head = searchHead();
    }


    private boolean check() {
        return true;
    }

    private List<AbstractLogicalNode> searchHead() {
        List<AbstractLogicalNode> head = new ArrayList<>();
        for (Map.Entry<String, AbstractLogicalNode> entry : nodeMap.entrySet()) {
            if (!parentMap.containsKey(entry.getKey())) {
                head.add(entry.getValue());
            }
        }
        return head;
    }

    private void updateGraph(AbstractLogicalNode node, List<String> children) {
        List<AbstractLogicalNode> childrenList = new ArrayList<>();
        for (String child : children) {
            childrenList.add(nodeMap.get(child));
            List<String> parents;
            if (parentMap.containsKey(child)) {
                parents = parentMap.get(child);
            } else {
                parents = new ArrayList<>();
            }
            parents.add(node.getId());
            parentMap.put(child, parents);
        }
        node.setChildren(childrenList);

    }


}


