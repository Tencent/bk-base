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

package com.tencent.bk.base.dataflow.ml.exceptions;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.exceptions.PipelineRuntimeException;
import com.tencent.bk.base.dataflow.ml.metric.DebugConstant;
import com.tencent.bk.base.dataflow.ml.metric.DebugExceptionReporter;
import com.tencent.bk.base.dataflow.ml.metric.ExceptionReporter;
import com.tencent.bk.base.dataflow.ml.metric.MonitorExceptionReporter;
import com.tencent.bk.base.dataflow.ml.topology.ModelTopology;
import java.text.MessageFormat;

public class OneModelException extends PipelineRuntimeException {

    private String code;
    private String message;

    public OneModelException(String code, String nodeId, String... args) {
        super(code, MessageFormat.format(OneModelErrorMessage.getMessage(code), nodeId, args), null);
        this.code = code;
        this.message = MessageFormat.format(OneModelErrorMessage.getMessage(code), nodeId, args);
        this.getReporter().report(nodeId, this.code, this.message, this.message);
    }

    public OneModelException(String code, Throwable cause, String nodeId, String... args) {
        super(code, MessageFormat.format(OneModelErrorMessage.getMessage(code), args), cause);
        this.code = code;
        this.message = MessageFormat.format(OneModelErrorMessage.getMessage(code), args);
        this.getReporter().report(nodeId, this.code, this.message, this.message);
    }

    public OneModelException(String code, String nodeId, ModelTopology topology, String... args) {
        super(code, MessageFormat.format(OneModelErrorMessage.getMessage(code), nodeId, args), null);
        this.code = code;
        this.message = MessageFormat.format(OneModelErrorMessage.getMessage(code), nodeId, args);
        this.getReporter(topology).report(nodeId, this.code, this.message, this.message);
    }

    @Override
    public String getMessage() {
        return "[" + this.code + "] " + this.message;
    }

    public String getCode() {
        return this.code;
    }

    public ExceptionReporter getReporter() {
        if (ConstantVar.RunMode.debug.toString().equalsIgnoreCase(DebugConstant.getRunMode())) {
            return new DebugExceptionReporter(ConstantVar.Role.modeling.toString(),
                    ConstantVar.Component.spark_mllib.toString(), DebugConstant.getTopology());
        } else {
            return new MonitorExceptionReporter(ConstantVar.Role.modeling.toString(),
                    ConstantVar.Component.spark_mllib.toString(), DebugConstant.getTopology());
        }
    }

    public ExceptionReporter getReporter(ModelTopology topology) {
        if (ConstantVar.RunMode.debug.toString().equalsIgnoreCase(topology.getRunMode())) {
            return new DebugExceptionReporter(ConstantVar.Role.modeling.toString(),
                    ConstantVar.Component.spark_mllib.toString(), topology);
        } else {
            return new MonitorExceptionReporter(ConstantVar.Role.modeling.toString(),
                    ConstantVar.Component.spark_mllib.toString(), topology);
        }
    }
}
