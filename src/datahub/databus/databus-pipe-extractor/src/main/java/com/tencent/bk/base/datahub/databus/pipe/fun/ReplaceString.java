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

import java.util.List;
import java.util.Map;

public class ReplaceString extends Fun {

    // 需要替换的字符串参数列表，单数位的是需要匹配的字符串，双数位的为需要替换成的字符串
    private List<Object> args;

    public ReplaceString(Context ctx, Map<String, Object> config) {
        nodeConf = config;
        nextNodeConf = (Map<String, Object>) nodeConf.get(EtlConsts.NEXT);
        generateNodeLabel();

        this.args = (List<Object>) nodeConf.get(EtlConsts.ARGS);
        this.next = NodeFactory.genNode(ctx, nextNodeConf);
    }


    @Override
    public boolean validateNext() {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * 将字符串的内容进行替换.
     *
     * @param ctx 上下文信息
     * @param o 待处理的字符串
     * @return 内容替换后的字符串
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
            for (int i = 0; i < args.size(); i += 2) {
                String key = args.get(i).toString();
                String value = (i + 1 == args.size()) ? "" : args.get(i + 1).toString();
                input = StringUtils.replace(input, key, value);
            }

            ctx.setNodeDetail(label, input);
            ctx.setNodeOutput(label, input.getClass().getSimpleName(), input);
            if (next == null) {
                ctx.setNodeError(label, String.format("%s", AssignNodeNeededError.class.getSimpleName()));
            } else {
                return this.next.execute(ctx, input);
            }

        } else {
            ctx.setNodeError(label, String.format("%s: %s -> String", TypeConversionError.class.getSimpleName(),
                    o.getClass().getSimpleName()));
        }

        ctx.setNodeDetail(label, null);
        return null;
    }

}
