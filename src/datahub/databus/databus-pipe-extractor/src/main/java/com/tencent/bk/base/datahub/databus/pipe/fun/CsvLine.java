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

package com.tencent.bk.base.datahub.databus.pipe.fun;

import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.pipe.NodeFactory;
import com.tencent.bk.base.datahub.databus.pipe.exception.AssignNodeNeededError;
import com.tencent.bk.base.datahub.databus.pipe.exception.BadCsvDataError;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CsvLine extends Fun {

    /**
     * 字符串切分.
     */
    @SuppressWarnings("unchecked")
    public CsvLine(Context ctx, Map<String, Object> config) {
        nodeConf = config;
        nextNodeConf = (Map<String, Object>) nodeConf.get(EtlConsts.NEXT);
        generateNodeLabel();
        this.next = NodeFactory.genNode(ctx, nextNodeConf);
    }

    @Override
    public boolean validateNext() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object execute(Context ctx, Object o) {
        if (o instanceof String) {
            String data = (String) o;
            List<String> nodeResult = csvLine(data);
            ctx.setNodeDetail(label, nodeResult);
            ctx.setNodeOutput(label, nodeResult.getClass().getSimpleName(), nodeResult);
            if (next == null) {
                ctx.setNodeError(label, String.format("%s", AssignNodeNeededError.class.getSimpleName()));
            } else {
                return this.next.execute(ctx, nodeResult);
            }
        } else {
            ctx.setNodeError(label, String.format("%s: %s", BadCsvDataError.class.getSimpleName(), o));
        }

        ctx.setNodeDetail(label, null);
        return null;
    }

    private List<String> csvLine(String s) throws BadCsvDataError {
        Reader in = new StringReader(s);

        try {
            CSVParser parser = new CSVParser(in, CSVFormat.RFC4180);
            List<CSVRecord> list = parser.getRecords();
            for (CSVRecord record : list) {
                List<String> strList = new ArrayList<String>();
                for (String field : record) {
                    strList.add(field);
                }
                return strList;
            }
        } catch (Exception e) {
            throw new BadCsvDataError(e.getMessage());
        }
        return new ArrayList<>();
    }
}
