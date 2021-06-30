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

package com.tencent.bk.base.dataflow.core.function.base.udaf;


import com.tencent.bk.base.dataflow.core.function.base.IFunction;
import com.tencent.bk.base.dataflow.core.function.base.udf.UDFTypes.UcDataType;
import java.io.Serializable;
import java.util.HashMap;

public abstract class AbstractAggFunction<R, T1 extends AbstractAggFunction> implements IFunction, Serializable {

    // 内置存储中间值，本质上为Map
    // Spark 用于初始化
    // Flink 用于保存累加器累加值
    private UCAggMapBuffer innerBuffer = new UCAggMapBuffer(new HashMap<Integer, Object>());

    public AbstractAggFunction() {
        initialize(innerBuffer);
    }

    /**
     * 获取输入字段类型
     *
     * @return
     */
    public abstract UcDataType[] getInputTypeArray();

    /**
     * 获取buffer类型列表
     *
     * @return
     */
    public abstract UcDataType[] getBufferTypeArray();

    /**
     * 初始化函数，一般用于buffer的初始值设置
     */
    public abstract void initialize(AbstractAggBuffer buffer);

    /**
     * 获取聚合后结果值
     *
     * @return
     */
    public abstract R evaluate(AbstractAggBuffer buffer);
    
    /**
     * UDAF更新操作
     *
     * @param inputValue
     */
    public abstract void update(AbstractAggBuffer buffer, Object... inputValue);

    /**
     * 合并两个UDAF对象的buffer
     *
     * @param buffer1
     * @param buffer2
     */
    public abstract void merge(AbstractAggBuffer buffer1, AbstractAggBuffer buffer2);

    public AbstractAggBuffer getBuffer() {
        return this.innerBuffer;
    }
}
