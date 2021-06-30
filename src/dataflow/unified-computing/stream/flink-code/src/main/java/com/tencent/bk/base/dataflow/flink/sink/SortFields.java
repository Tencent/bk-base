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

package com.tencent.bk.base.dataflow.flink.sink;

import com.tencent.bk.base.dataflow.core.topo.NodeField;
import java.util.ArrayList;
import java.util.List;

public class SortFields {

    List<NodeField> unSortedFields;
    String[] orderFieldNames;

    public SortFields(List<NodeField> unSortedFields, String[] orderFieldNames) {
        this.unSortedFields = unSortedFields;
        this.orderFieldNames = orderFieldNames;
    }

    /**
     * 根据传入字段名称列表返回排序后的 NodeField 列表
     *
     * @return
     */
    public List<NodeField> getSortedNodeFieldsByName() {

        if (this.unSortedFields != null && !this.unSortedFields.isEmpty()
                && this.orderFieldNames != null && this.orderFieldNames.length > 0) {
            List sortedList = new ArrayList<NodeField>();
            for (String filedName : this.orderFieldNames) {
                NodeField found = getNodeField(this.unSortedFields, filedName);
                if (found != null) {
                    sortedList.add(found);
                }
                this.unSortedFields.remove(found);
            }
            sortedList.addAll(this.unSortedFields);
            return sortedList;
        } else {
            return this.unSortedFields;
        }

    }

    private NodeField getNodeField(List<NodeField> list, String filedName) {
        for (NodeField person : list) {
            if (person.getField().equals(filedName)) {
                return person;
            }
        }
        return null;
    }
}
