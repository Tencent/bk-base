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

package com.tencent.bk.base.dataflow.flink.streaming.transform.join;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.types.Row;

public class JoinedKeySelector implements KeySelector<Row, List> {

    private List<Integer> keyIndexs;
    private String nodeId;

    public JoinedKeySelector(List<Integer> keyIndexs, String nodeId) {
        this.keyIndexs = keyIndexs;
        this.nodeId = nodeId;
    }

    @Override
    public List getKey(Row input) throws Exception {
        List<String> joinedKeys = new ArrayList<>();
        for (int keyIndex : keyIndexs) {
            if (null == input.getField(keyIndex)) {
                // null 值应关联不上
                // getKey 对于同一条数据会被 flink 调用两次，应保证幂等，故不可用随机数
                // TODO: 这里只保证了尽可能的随机
                joinedKeys.add(input.toString().concat(this.nodeId));
            } else {
                joinedKeys.add(input.getField(keyIndex).toString());
            }
        }
        return joinedKeys;
    }
}
