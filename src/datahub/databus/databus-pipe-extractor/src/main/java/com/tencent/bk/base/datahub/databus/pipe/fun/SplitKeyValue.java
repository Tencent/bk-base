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
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitKeyValue extends Fun {

    private String entrySeparator = ",";

    private String keyValueSeparator = ":";

    /**
     * 拆分字符串为map.
     */
    public SplitKeyValue(Context ctx, Map<String, Object> config) {
        nodeConf = config;
        nextNodeConf = (Map<String, Object>) nodeConf.get(EtlConsts.NEXT);
        generateNodeLabel();

        List<Object> args = (List<Object>) nodeConf.get(EtlConsts.ARGS);
        if (args != null && args.size() >= 2) {
            entrySeparator = args.get(0).toString();
            keyValueSeparator = args.get(1).toString();
        }
        this.next = NodeFactory.genNode(ctx, nextNodeConf);
    }

    @Override
    public boolean validateNext() {
        // TODO Auto-generated method stub
        return false;
    }


    /**
     * 将输入的字符串进行拆分，首先按照entrySeparator拆分为多条记录，然后按照keyValueSeparator.
     * 拆分为key和value，将所有的key->value放入map中返回。丢弃没有value的key值。
     *
     * @param ctx 上下文
     * @param o 待处理的字符串
     * @return 包含所有key/value的map
     */
    @Override
    public Object execute(Context ctx, Object o) {
        if (o == null) {
            ctx.setNodeError(label, String.format("%s", NullDataError.class.getSimpleName()));
            ctx.setNodeDetail(label, null);
            return null;
        }

        if (o instanceof String) {
            String input = o.toString();
            Map<String, String> result = new HashMap<>();
            for (String entry : StringUtils.split(input, entrySeparator)) {
                String[] kv = StringUtils.split(entry, keyValueSeparator, 2);
                if (kv.length == 2) {
                    result.put(kv[0], kv[1]);
                }
            }

            ctx.setNodeDetail(label, result);
            ctx.setNodeOutput(label, result.getClass().getSimpleName(), result);
            if (next == null) {
                ctx.setNodeError(label, String.format("%s", AssignNodeNeededError.class.getSimpleName()));
            } else {
                return this.next.execute(ctx, result);
            }

        } else {
            ctx.setNodeError(label, String.format("%s: %s -> String", TypeConversionError.class.getSimpleName(),
                    o.getClass().getSimpleName()));
        }

        ctx.setNodeDetail(label, null);
        return null;
    }
}
