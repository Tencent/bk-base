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

package com.tencent.bk.base.datahub.iceberg;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CommitMsg {

    private String operation;
    private Map<String, String> data;

    /**
     * 空构造函数。用于ObjectMapper解析字符串为CommitMsg对象时使用。
     */
    public CommitMsg() {
    }

    /**
     * 构造函数
     *
     * @param operation 操作类型
     * @param data 数据
     */
    public CommitMsg(String operation, Map<String, String> data) {
        this.operation = operation;
        this.data = data;
    }

    // getter & setter
    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }

    /**
     * equals方法，对比对象是否相同。
     *
     * @param o 对比的对象
     * @return True/False
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommitMsg commitMsg = (CommitMsg) o;
        return Objects.equals(operation, commitMsg.operation)
                && Objects.equals(data, commitMsg.data);
    }

    /**
     * 计算哈希值。
     *
     * @return 哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hash(operation, data);
    }

    /**
     * 转换为json字符串
     *
     * @return json字符串
     */
    @Override
    public String toString() {
        String value = data.entrySet()
                .stream()
                .map(e -> String.format("\"%s\": \"%s\"", e.getKey(), e.getValue()))
                .collect(Collectors.joining(", "));

        return String.format("{\"%s\": \"%s\", \"%s\": {%s}}",
                C.OPERATION, operation, C.DATA, value);
    }
}