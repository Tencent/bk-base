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
import com.tencent.bk.base.datahub.databus.pipe.exception.EmptyEtlResultError;
import com.tencent.bk.base.datahub.databus.pipe.exception.NotListDataError;
import com.tencent.bk.base.datahub.databus.pipe.exception.PipeExtractorException;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Iterate extends Fun {

    /**
     * 当前迭代节点的schema.
     */
    Fields thisField;

    /**
     * 遍历器，初始画的额时候schema通过压栈来处理层次关系.
     */
    public Iterate(Context ctx, Map<String, Object> config) {
        nodeConf = config;
        nextNodeConf = (Map<String, Object>) nodeConf.get(EtlConsts.NEXT);
        generateNodeLabel();

        ctx.pushSchemaStack();
        thisField = ctx.getSchema();

        String iterName = EtlConsts.ITERATION_IDX;
        List<Object> args = (List<Object>) nodeConf.get(EtlConsts.ARGS);

        if (args != null && args.size() > 0) {
            iterName = ((String) (args.get(0)));
        }

        this.next = NodeFactory.genNode(ctx, nextNodeConf);

        Field field = new Field(iterName, EtlConsts.INT);
        field.setIsInternal(true);
        ctx.getSchema().append(field);

        ctx.popSchemaStack();
    }

    @Override
    public boolean validateNext() {
        // TODO Auto-generated method stub
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object execute(Context ctx, Object o) {
        if (!(o instanceof List)) {
            ctx.setNodeDetail(label, null);
            ctx.setNodeError(label, String.format("%s: %s", NotListDataError.class.getSimpleName(), o));
            return null;
        }

        // schema压栈.
        ctx.pushValuesStack(thisField);
        Fields savedSchema = ctx.getSchema();
        ctx.setSchema(thisField);

        List<Object> values = ctx.getValues();
        // 设置清洗解析的结果
        ctx.setNodeDetail(label, o);

        int idx = 1; // 列表元素编号。这个会单独赋值给迭代器编号字段
        for (Object item : (List<Object>) o) {
            List<Object> subValues = new ArrayList<Object>();
            ctx.setValues(subValues);
            ctx.incrTotalMsgNum();
            ctx.setNodeOutput(label, item.getClass().getSimpleName(), item);

            try {
                if (next == null) {
                    ctx.setNodeError(label, String.format("%s", AssignNodeNeededError.class.getSimpleName()));
                } else {
                    this.next.execute(ctx, item);
                    subValues.add(idx++);
                    values.add(subValues);
                    ctx.incrSuccessMsgNum();
                }

            } catch (PipeExtractorException e) {
                ctx.appendBadValues(item, e);
                ctx.setNodeError(label, String.format("%s: %s", PipeExtractorException.class.getSimpleName(), o));
            }
        }

        if (values.isEmpty()) {
            //迭代器都失败了，那么这整条message应该无效

            /**
             * 恢复schema的压栈.
             */
            ctx.setSchema(savedSchema);
            ctx.popValuesStack();

            throw new EmptyEtlResultError("iterate all failed");
        }

        ctx.setValues(values);

        /**
         * 恢复schema的压栈.
         */
        ctx.setSchema(savedSchema);
        ctx.popValuesStack();

        return null;
    }
}
