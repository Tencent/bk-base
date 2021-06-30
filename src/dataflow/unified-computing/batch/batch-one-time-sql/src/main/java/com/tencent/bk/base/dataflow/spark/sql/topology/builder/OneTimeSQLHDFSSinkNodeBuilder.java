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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.spark.TimeFormatConverter$;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchSinglePathOutput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSSinkNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Map;

public class OneTimeSQLHDFSSinkNodeBuilder extends BatchHDFSSinkNode.BatchHDFSSinkNodeBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(OneTimeSQLHDFSSinkNodeBuilder.class);

    private ConstantVar.Role role;

    public OneTimeSQLHDFSSinkNodeBuilder(Map<String, Object> info) {
        super(info);
    }

    @Override
    protected void buildParams() {
        Map<String, Object> outputMap = (Map<String, Object>)this.info.get("output");
        Map<String, String> storageMap = (Map<String, String>)outputMap.get("conf");

        String mode = outputMap.get("mode").toString();
        String format = storageMap.get("data_type");

        String nameService = storageMap.get("name_service");
        String physicalName = storageMap.get("physical_table_name");

        String root = String.format("%s%s", nameService, physicalName);

        if (null != info.get("role")) {
            this.role = ConstantVar.Role.valueOf(info.get("role").toString().toLowerCase());
        }

        if (null != this.role
            && this.role == ConstantVar.Role.queryset) {
            this.hdfsOutput = new BatchSinglePathOutput(root, format, mode);
        } else {
            String partition = info.get("partition").toString();
            String outputPath = pathConstructor.timeToPath(root, partition);
            this.hdfsOutput = new BatchSinglePathOutput(outputPath, format, mode);
            try {
                this.dtEventTimeStamp = TimeFormatConverter$.MODULE$.platformCTDateFormat().parse(partition).getTime();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
        LOGGER.info(String.format("Output paths is: %s, mode: %s, data type: %s",
            this.hdfsOutput.getOutputInfo(), this.hdfsOutput.getMode(), this.hdfsOutput.getFormat()));
    }

    @Override
    public BatchHDFSSinkNode build() {
        this.buildParams();
        BatchHDFSSinkNode sinkNode = new BatchHDFSSinkNode(this);
        if (this.role != ConstantVar.Role.queryset) {
            sinkNode.setEnableCallMaintainInterface(true);
            sinkNode.setEnableReservedField(true);
            sinkNode.setForceToUpdateReservedSchema(true);
        }
        sinkNode.setEnableCallShipperInterface(false);
        return sinkNode;
    }

}
