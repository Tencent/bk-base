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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractExecutionNode {

    private String id;
    private List<AbstractExecutionNode> parents = new ArrayList<>();
    private List<AbstractExecutionNode> children = new ArrayList<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<AbstractExecutionNode> getChildren() {
        return children;
    }

    public void setChildren(List<AbstractExecutionNode> children) {
        this.children = children;
    }

    public List<AbstractExecutionNode> getParents() {
        return parents;
    }

    public void setParents(List<AbstractExecutionNode> parents) {
        this.parents = parents;
    }

    /**
     * dump execution node to HTTP json
     *
     * @return
     */
    public Map<String, Object> toHttpJson() {
        Map<String, Object> httpJson = new HashMap<>();
        httpJson.put("id", id);
        List<String> parents = new ArrayList<>();
        List<String> children = new ArrayList<>();
        if (!this.parents.isEmpty()) {
            for (AbstractExecutionNode node : this.parents) {
                parents.add(node.getId());
            }
            httpJson.put("parents", parents);
        }
        if (!this.children.isEmpty()) {
            for (AbstractExecutionNode node : this.children) {
                children.add(node.getId());
            }
            httpJson.put("children", children);
        }

        return httpJson;
    }

}
