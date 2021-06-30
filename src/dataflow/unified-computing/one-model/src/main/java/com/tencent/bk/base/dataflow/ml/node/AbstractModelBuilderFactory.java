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

package com.tencent.bk.base.dataflow.ml.node;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.HdfsTableFormat;
import com.tencent.bk.base.dataflow.core.topo.Node.AbstractBuilder;
import com.tencent.bk.base.dataflow.core.topo.UserDefinedFunctions;
import java.util.Map;

public abstract class AbstractModelBuilderFactory {

    protected long scheduleTime;
    protected String jobType;
    protected Map<String, Object> nodeInfo;

    public AbstractModelBuilderFactory(String jobType, long scheduleTime) {
        this.jobType = jobType;
        this.scheduleTime = scheduleTime;
    }

    protected ConstantVar.HdfsTableFormat getHdfsTableFormat(String format) {
        ConstantVar.HdfsTableFormat hdfsTableDataType = null;
        switch (format) {
            case "iceberg":
                hdfsTableDataType = HdfsTableFormat.iceberg;
                break;
            case "parquet":
                hdfsTableDataType = HdfsTableFormat.parquet;
                break;
            default:
                hdfsTableDataType = HdfsTableFormat.other;
        }
        return hdfsTableDataType;
    }

    public abstract AbstractBuilder getBuilder(Map<String, Object> nodeInfo);

    public abstract Map<String, Object> getConfigMap(ConstantVar.TableType tableType,
            ConstantVar.HdfsTableFormat hdfsTableFormat, ConstantVar.DataNodeType sourceNodeType);

    public UserDefinedFunctions getUserDefinedFunctions() {
        return null;
    }
}
