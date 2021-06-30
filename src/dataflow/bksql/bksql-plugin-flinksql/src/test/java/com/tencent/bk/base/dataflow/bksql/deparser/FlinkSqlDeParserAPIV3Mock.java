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

package com.tencent.bk.base.dataflow.bksql.deparser;

import com.typesafe.config.Config;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import net.sf.jsqlparser.schema.Column;

/**
 * mock generateColumnName fuction as fixed sequence No
 */
public class FlinkSqlDeParserAPIV3Mock extends FlinkSqlDeParserAPIV3 {

    private final AtomicInteger mockUuidID = new AtomicInteger(0);

    public FlinkSqlDeParserAPIV3Mock(Config properties,
                                     String trtTableMetadataUrlPattern,
                                     String staticTableMetadataUrlPattern,
                                     String udfMetadataUrlPattern,
                                     List<String> aggregationCallRefs) {
        super(properties,
                trtTableMetadataUrlPattern,
                staticTableMetadataUrlPattern,
                udfMetadataUrlPattern,
                aggregationCallRefs);
    }

    @Override
    protected String generateColumnName(Column column, String fromTable) {
        return "col_" + String.format("%s_%s_%s", fromTable, getColumnName(column), getMockID());
    }

    protected String getMockID() {
        int seqNo = mockUuidID.incrementAndGet();
        StringBuilder stringBuilder = new StringBuilder("")
                .append(seqNo)
                .append(seqNo)
                .append(seqNo)
                .append(seqNo)
                .append(seqNo)
                .append(seqNo);
        return stringBuilder.toString();
    }
}
