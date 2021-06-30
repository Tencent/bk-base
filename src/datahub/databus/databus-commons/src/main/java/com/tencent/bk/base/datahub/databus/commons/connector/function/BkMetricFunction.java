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

package com.tencent.bk.base.datahub.databus.commons.connector.function;

import com.tencent.bk.base.datahub.databus.commons.bean.DataBusResult;
import com.tencent.bk.base.datahub.databus.commons.connector.sink.BkMetricSink;
import com.tencent.bk.base.datahub.databus.commons.connector.sink.BkSinkRecord;
import com.tencent.bk.base.datahub.databus.commons.connector.source.BkSourceRecord;
import com.tencent.bk.base.datahub.databus.commons.callback.TaskContextChangeCallback;

public abstract class BkMetricFunction<INPUT, OUTPUT> extends BkMetricSink<INPUT, OUTPUT> implements
        BkFunction<INPUT, OUTPUT>, TaskContextChangeCallback {

    @Override
    public final BkSourceRecord<OUTPUT> process(BkSinkRecord<INPUT> record) throws Exception {
        DataBusResult<OUTPUT> databusResult = this.write(record);//记录度量
        if (databusResult != null) {
            return databusResult.getIntermediateResult();
        }
        return null;
    }

    @Override
    protected final DataBusResult<OUTPUT> doWrite(BkSinkRecord<INPUT> record) {
        return doProcess(record);
    }

    /**
     * 函数执行体
     *
     * @param record 需要处理的消息
     * @return 处理后的中间结果
     */
    protected abstract DataBusResult<OUTPUT> doProcess(BkSinkRecord<INPUT> record);
}
