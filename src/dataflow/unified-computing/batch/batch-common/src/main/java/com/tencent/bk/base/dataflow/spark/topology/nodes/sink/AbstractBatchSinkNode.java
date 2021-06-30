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

package com.tencent.bk.base.dataflow.spark.topology.nodes.sink;

import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBatchSinkNode extends SinkNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBatchSinkNode.class);

    protected String outputTimeRangeString = "not_available";

    protected boolean enableReservedField = false;
    protected boolean enableStartEndTimeField = false;
    protected String startTimeString = null;
    protected String endTimeString = null;

    protected boolean forceUpdateReservedSchema = false;

    protected boolean enableCallMaintainInterface = false;

    protected boolean enableCallShipperInterface = false;

    public AbstractBatchSinkNode(AbstractBuilder builder) {
        super(builder);
        LOGGER.info(String.format("Build a %s for %s", this.getClass().getSimpleName(), this.nodeId));
    }

    public String getOutputTimeRangeString() {
        return outputTimeRangeString;
    }

    public void setEnableReservedField(boolean enableReservedField) {
        this.enableReservedField = enableReservedField;
    }

    public boolean isReservedFieldEnabled() {
        return this.enableReservedField;
    }

    public void setForceToUpdateReservedSchema(boolean forceUpdateReservedSchema) {
        this.forceUpdateReservedSchema = forceUpdateReservedSchema;
    }

    public boolean isForceUpdateReservedSchema() {
        return forceUpdateReservedSchema;
    }

    public boolean isEnableCallShipperInterface() {
        return this.enableCallShipperInterface;
    }

    public void setEnableCallShipperInterface(boolean value) {
        this.enableCallShipperInterface = value;
    }

    public boolean isEnableCallMaintainInterface() {
        return this.enableCallMaintainInterface;
    }

    public void setEnableCallMaintainInterface(boolean value) {
        this.enableCallMaintainInterface = value;
    }

    public boolean isEnableStartEndTimeField() {
        return this.enableStartEndTimeField;
    }

    public void setEnableStartEndTimeField(boolean enableStartEndTimeField) {
        this.enableStartEndTimeField = enableStartEndTimeField;
    }

    public void setStartTimeString(String startTimeString) {
        this.startTimeString = startTimeString;
    }

    public void setEndTimeString(String endTimeString) {
        this.endTimeString = endTimeString;
    }

    public String getStartTimeString() {
        return this.startTimeString;
    }

    public String getEndTimeString() {
        return this.endTimeString;
    }
}
