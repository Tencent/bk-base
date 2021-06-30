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
import com.tencent.bk.base.datahub.databus.pipe.exception.NullDataError;
import com.tencent.bk.base.datahub.databus.pipe.exception.TypeConversionError;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Split extends Fun {

    private char delimiter;
    private static final int MAX_LIMIT = 1001; // 最多切分为1001个字符串
    private int limit;

    /**
     * 字符串切分.
     */
    @SuppressWarnings("unchecked")
    public Split(Context ctx, Map<String, Object> config) {
        nodeConf = config;
        nextNodeConf = (Map<String, Object>) nodeConf.get(EtlConsts.NEXT);
        generateNodeLabel();

        // args 切分参数, 最多2项参数
        // 0: 分隔符
        // 1: 切分个数
        List<Object> args = (List<Object>) nodeConf.get(EtlConsts.ARGS);
        String args0 = ((String) (args.get(0)));
        args0 = args0.replace("\\n", "\n");
        this.delimiter = args0.charAt(0);
        if (args.size() == 1) {
            limit = MAX_LIMIT;
        } else {
            limit = (Integer) ((List<Object>) nodeConf.get(EtlConsts.ARGS)).get(1);
            limit = limit > MAX_LIMIT ? MAX_LIMIT : limit;
        }
        this.next = NodeFactory.genNode(ctx, nextNodeConf);
    }

    @Override
    public boolean validateNext() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object execute(Context ctx, Object o) {
        if (o == null) {
            ctx.setNodeError(label, String.format("%s", NullDataError.class.getSimpleName()));
            ctx.setNodeDetail(label, null);
            return null;
        }
        if (o instanceof String) {
            String data = (String) o;
            List<String> nodeResult = split(data);
            ctx.setNodeDetail(label, nodeResult);
            ctx.setNodeOutput(label, nodeResult.getClass().getSimpleName(), nodeResult);
            if (next == null) {
                ctx.setNodeError(label, String.format("%s", AssignNodeNeededError.class.getSimpleName()));
            } else {
                return this.next.execute(ctx, nodeResult);
            }
        } else {
            ctx.setNodeError(label, String.format("%s: %s -> String", TypeConversionError.class.getSimpleName(),
                    o.getClass().getSimpleName()));
        }
        ctx.setNodeDetail(label, null);
        return null;
    }

    /**
     * 分隔符切分.
     */
    private List<String> split(String s) {
        List<String> array = new ArrayList<String>();
        int count = 1;
        int start = 0;
        int i = 0;
        while ((i = s.indexOf(this.delimiter, start)) > -1) {
            if (count++ >= this.limit) { // 切分个数不大于limit
                break;
            }
            array.add(s.substring(start, i));
            start = i + 1;
        }

        if (start == s.length()) {
            // 最后一个字符是分割符
            array.add("");
        } else {
            array.add(s.substring(start, s.length()));
        }

        return array;
    }
}

