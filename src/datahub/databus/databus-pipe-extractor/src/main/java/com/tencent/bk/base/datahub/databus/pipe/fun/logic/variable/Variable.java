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

package com.tencent.bk.base.datahub.databus.pipe.fun.logic.variable;

public class Variable {

    protected Object val = null;

    public void initVal(Object obj) {
        val = null;
    }

    public Object getVal() {
        return val;
    }


    /**
     * 都按照string比较，值相等才返回true
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Variable) {
            Variable variable = (Variable) obj;
            return variable != null && this.val != null && this.val.toString().equals(variable.val.toString());
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (null != this.val) {
            return this.val.hashCode();
        }
        return super.hashCode();
    }

    /**
     * 都按照string比较，值不相等才返回true
     */
    public boolean notEquals(Variable variable) {
        return !this.val.toString().equals(variable.val.toString());
    }

    /**
     * 都是number类型，且值大于参数才返回true
     */
    public boolean greater(Variable variable) {
        return (this.val instanceof Number && variable.val instanceof Number)
                && ((Number) this.val).doubleValue() > ((Number) variable.val).doubleValue();
    }

    /**
     * 都是number类型，且值不大于参数才返回true
     */
    public boolean notGreater(Variable variable) {
        return (this.val instanceof Number && variable.val instanceof Number)
                && ((Number) this.val).doubleValue() <= ((Number) variable.val).doubleValue();
    }

    /**
     * 都是number类型，且值小于参数才返回true
     */
    public boolean less(Variable variable) {
        return (this.val instanceof Number && variable.val instanceof Number)
                && ((Number) this.val).doubleValue() < ((Number) variable.val).doubleValue();
    }

    /**
     * 都是number类型，且值不小于参数才返回true
     */
    public boolean notLess(Variable variable) {
        return (this.val instanceof Number && variable.val instanceof Number)
                && ((Number) this.val).doubleValue() >= ((Number) variable.val).doubleValue();
    }

    /**
     * 都是string类型，且以参数为开头才返回true
     */
    public boolean startsWith(Variable variable) {
        return (this.val instanceof String && variable.val instanceof String)
                && ((String) this.val).startsWith((String) variable.val);
    }

    /**
     * 都是string类型，且以参数为结尾才返回true
     */
    public boolean endsWith(Variable variable) {
        return (this.val instanceof String && variable.val instanceof String)
                && ((String) this.val).endsWith((String) variable.val);
    }

    /**
     * 都是string类型，且包含参数才返回true
     */
    public boolean contains(Variable variable) {
        return (this.val instanceof String && variable.val instanceof String)
                && ((String) this.val).contains((String) variable.val);
    }
}
