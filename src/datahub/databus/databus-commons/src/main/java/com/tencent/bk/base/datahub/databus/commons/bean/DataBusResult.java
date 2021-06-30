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

package com.tencent.bk.base.datahub.databus.commons.bean;

import com.tencent.bk.base.datahub.databus.commons.connector.source.BkSourceRecord;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;

public class DataBusResult<T> {

    /**
     * 清洗结果
     */
    private ConvertResult convertResult;
    /**
     * 中间结果（清洗后输出结果）
     */
    private BkSourceRecord<T> intermediateResult;

    /**
     * 输出的数据条数
     */
    private int outputMessageCount;

    /**
     * 输出的消息大小
     */
    private long outputMessageSize;


    public DataBusResult(ConvertResult convertResult, int outputMessageCount, long outputMessageSize) {
        this.convertResult = convertResult;
        this.outputMessageCount = outputMessageCount;
        this.outputMessageSize = outputMessageSize;
    }


    public void setIntermediateResult(BkSourceRecord<T> intermediateResult) {
        this.intermediateResult = intermediateResult;
    }

    public ConvertResult getConvertResult() {
        return convertResult;
    }

    public BkSourceRecord<T> getIntermediateResult() {
        return intermediateResult;
    }

    public int getOutputMessageCount() {
        return outputMessageCount;
    }

    public long getOutputMessageSize() {
        return outputMessageSize;
    }
}
