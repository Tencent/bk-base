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

package com.tencent.bk.base.dataflow.core.topo;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar.Component;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Node implements Serializable {

    private static final long serialVersionUID = 1L;


    protected String nodeId;

    protected String nodeName;
    protected String description;
    protected String type;

    /**
     * parent node and child node
     */
    protected List<Node> parents = new ArrayList<>();
    protected List<Node> children = new ArrayList<>();

    /**
     * node's field
     */
    protected List<NodeField> fields = new ArrayList<>();

    protected String role;

    public Node() {

    }

    public Node(AbstractBuilder builder) {
        this.nodeId = builder.nodeId;
        this.nodeName = builder.nodeName;
        this.fields = builder.fields;
        this.description = builder.description;
        this.type = builder.type;
    }

    public List<NodeField> getFields() {
        return fields;
    }

    public void setFields(List<NodeField> fields) {
        this.fields = fields;
    }

    public String[] getFieldNames() {
        List<String> fieldsList = new ArrayList<>();
        for (NodeField field : fields) {
            fieldsList.add(field.getField());
        }
        return fieldsList.toArray(new String[fieldsList.size()]);
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<Node> getParents() {
        return parents;
    }

    public void setParents(List<Node> parents) {
        this.parents = parents;
    }

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(List<Node> children) {
        this.children = children;
    }


    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * 获取父表id的列表
     *
     * @return 父表id的列表
     */
    public Map<String, Node> getParentNodes() {
        Map<String, Node> parentNodeIds = new HashMap<>();
        for (Node node : parents) {
            parentNodeIds.put(node.getNodeId(), node);
        }
        return parentNodeIds;
    }


    /**
     * 获取node的单个父节点，用于普通transform和sink等
     *
     * @return 单父node
     */
    public Node getSingleParentNode() {
        if (parents.size() > 1) {
            throw new RuntimeException(String.format("The node [%s] has more than one parent node.", nodeId));
        }
        return parents.get(0);
    }

    /**
     * 获取当前node的字段数量
     *
     * @return 字段数
     */
    public int getFieldsSize() {
        return fields.size();
    }

    /**
     * 获取当前node的所有字段名称
     *
     * @return node的fields
     */
    public String getCommonFields() {
        List<String> fieldsList = new ArrayList<>();
        for (NodeField field : fields) {
            fieldsList.add(field.getField());
        }
        String tmpFields = fieldsList.toString();
        tmpFields = tmpFields.substring(1, tmpFields.length() - 1);
        return tmpFields;
    }

    /**
     * 根据字段名称获取字段的index
     *
     * @param field 字段名称
     * @return 字段index
     */
    public int getFieldIndex(String field) {
        int index = -1;
        for (NodeField tmpField : fields) {
            index++;
            if (tmpField.getField().equals(field)) {
                return index;
            }
        }
        throw new RuntimeException(String.format("The node [%s] not have the field [%s].", getNodeId(), field));
    }

    /**
     * 获取时间字段索引，若不存在，返回 -1
     *
     * @return
     */
    public int getEventTimeFieldIndex() {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).isEventTime()) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        return nodeId;
    }

    @Deprecated
    public void map(Map<String, Object> info, Component jobType) {
        setNodeId(info.get("id").toString());
        setNodeName(info.get("name").toString());
        if (info.get("type") != null) {
            setType(info.get("type").toString());
        }
        if (info.get("description") != null) {
            setDescription(info.get("description").toString());
        }
        if (info.get("role") != null) {
            setRole(info.get("role").toString());
        }
        if (info.get("fields") != null) {
            fields = MapNodeUtil.mapFields((List<Map<String, Object>>) info.get("fields"));
        }
    }

    public abstract static class AbstractBuilder {

        protected String nodeId;
        protected String nodeName;
        protected List<NodeField> fields;
        protected String description;
        protected String type;

        public AbstractBuilder(Map<String, Object> info) {
            this.nodeId = info.get("id").toString();
            this.nodeName = info.get("name").toString();
            if (null != info.get("type")) {
                this.type = info.get("type").toString();
            }
            if (null != info.get("description")) {
                this.description = info.get("description").toString();
            }
            if (info.get("fields") != null) {
                this.fields = MapNodeUtil.mapFields((List<Map<String, Object>>) info.get("fields"));
            }
        }

        public abstract Node build();
    }
}
