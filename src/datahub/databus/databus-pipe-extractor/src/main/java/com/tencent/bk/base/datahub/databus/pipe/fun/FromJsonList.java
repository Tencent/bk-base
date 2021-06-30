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
import com.tencent.bk.base.datahub.databus.pipe.exception.BadJsonListError;
import com.tencent.bk.base.datahub.databus.pipe.exception.BadJsonObjectError;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;

import java.io.IOException;
import java.util.Map;

public class FromJsonList extends Fun {

    public FromJsonList(Context ctx, Map<String, Object> config) {
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

    /**
     * 将上一节点输入解析为json list对象
     *
     * @param ctx 上下文
     * @param o 输入的字符串,json list格式
     * @return json list对象
     */
    @Override
    public Object execute(Context ctx, Object o) {
        if (o instanceof String) {
            String data = (String) o;
            try {
                Object nodeResult = JsonUtils.readList(data);
                ctx.setNodeDetail(label, nodeResult); // json解析ok,设置解析结果为当前节点结果
                ctx.setNodeOutput(label, nodeResult.getClass().getSimpleName(), nodeResult);
                if (next == null) {
                    ctx.setNodeError(label, String.format("%s", AssignNodeNeededError.class.getSimpleName()));
                } else {
                    return this.next.execute(ctx, nodeResult);
                }
            } catch (IOException e) {
                ctx.appendBadValues(data, new BadJsonObjectError(e.getMessage()));
                ctx.setNodeError(label, String.format("%s: %s", BadJsonListError.class.getSimpleName(), data));
            }
        } else {
            ctx.setNodeError(label, String.format("%s: %s", BadJsonListError.class.getSimpleName(), o));
        }
        ctx.setNodeDetail(label, null);

        return null;
    }
}
