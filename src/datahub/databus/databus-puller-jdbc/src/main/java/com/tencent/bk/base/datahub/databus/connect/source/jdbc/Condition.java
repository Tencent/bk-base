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

package com.tencent.bk.base.datahub.databus.connect.source.jdbc;


import java.util.Map;

/**
 * 过滤条件
 * {
 * "key":"id",
 * "logic_op":"and",
 * "op":"in",
 * "value":"3"
 * }
 */
public class Condition {

    public static String LOGIC_OP_AND = "and";
    public static String LOGIC_OP_OR = "or";

    public static String OP_EQUAL = "=";
    public static String OP_NOT_EQUAL = "!=";
    public static String OP_IN = "in";

    private String key;
    private String value;

    private boolean isEqual;
    private boolean isNotEqual;
    private boolean isIn;
    private boolean isOr;
    private boolean isAnd;

    public Condition() {
    }

    /**
     * 检测数据是否匹配规则
     *
     * @param data 待检测的数据
     * @return 是否匹配
     */
    public boolean match(Map<String, Object> data) {
        return isEqual && data.getOrDefault(key, "").toString().equals(value)
                || isIn && data.getOrDefault(key, "").toString().contains(value)
                || isNotEqual && !data.getOrDefault(key, "").toString().equals(value);
    }

    public boolean isOr() {
        return isOr;
    }

    public boolean isAnd() {
        return isAnd;
    }

    /**
     * 设置op, 初始化部分变量
     *
     * @param logicOp 逻辑操作, 当前仅支持 and, or
     */
    public void setLogicOp(String logicOp) {
        isOr = logicOp.equals(Condition.LOGIC_OP_OR);
        isAnd = logicOp.equals(Condition.LOGIC_OP_AND);
    }

    /**
     * 设置op, 初始化部分变量
     *
     * @param op 字段比较, 当前仅支持 =, !=, in
     */
    public void setOp(String op) {
        isEqual = op.equals(Condition.OP_EQUAL);
        isNotEqual = op.equals(Condition.OP_NOT_EQUAL);
        isIn = op.equals(Condition.OP_IN);
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
