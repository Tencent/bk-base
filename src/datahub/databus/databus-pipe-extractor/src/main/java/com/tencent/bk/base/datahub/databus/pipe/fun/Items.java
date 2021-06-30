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
import com.tencent.bk.base.datahub.databus.pipe.exception.NotMapDataError;
import com.tencent.bk.base.datahub.databus.pipe.exception.PipeExtractorException;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Items extends Fun {

    /**
     * 当前迭代节点的schema.
     */
    Fields thisField;
    boolean iteratorEntry = false;

    /**
     * 遍历器，初始化的时候schema通过压栈来处理层次关系.
     */
    public Items(Context ctx, Map<String, Object> config) {
        nodeConf = config;
        nextNodeConf = (Map<String, Object>) nodeConf.get(EtlConsts.NEXT);
        generateNodeLabel();

        ctx.pushSchemaStack();
        thisField = ctx.getSchema();

        this.next = NodeFactory.genNode(ctx, nextNodeConf);

        List<String> args = (List<String>) nodeConf.get(EtlConsts.ARGS);
        if (args.isEmpty()) {
            iteratorEntry = true;
        } else {
            String keyAssignTo = args.get(0);
            Boolean isDimension = false;
            if (args.size() == 2 && ((String) args.get(1)).equals(EtlConsts.DIMENSION)) {
                isDimension = true;
            }
            Field field = new Field(keyAssignTo, EtlConsts.STRING, isDimension);
            ctx.getSchema().append(field);
        }

        ctx.popSchemaStack();
    }

    @Override
    public boolean validateNext() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object execute(Context ctx, Object o) {
        if (!(o instanceof Map)) {
            ctx.setNodeDetail(label, null);
            ctx.setNodeError(label, String.format("%s: %s", NotMapDataError.class.getSimpleName(), o));
            return null;
        }

        // schema压栈.
        ctx.pushValuesStack();
        Fields savedSchema = ctx.getSchema();
        ctx.setSchema(thisField);

        List<Object> values = ctx.getValues();

        if (iteratorEntry) {
            iterateEntry(ctx, o, values);
        } else {
            iterateValue(ctx, o, values);
        }

        if (values.isEmpty()) {
            //迭代器都失败了，那么这整条message应该无效

            /**
             * 恢复schema的压栈.
             */
            ctx.setSchema(savedSchema);
            ctx.popValuesStack();
            ctx.setNodeError(label, String.format("%s: %s", EmptyEtlResultError.class.getSimpleName(), o));
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

    @SuppressWarnings("unchecked")
    private void iterateEntry(Context ctx, Object o, List<Object> values) {
        Map<String, Object> mapObj = (Map<String, Object>) o;
        if (ctx.getVerifyConf()) {
            // NodeDetail 该值只用于验证清洗配置模式，每次都进行数据转换，太浪费性能，有需要再转换
            List<Map<String, Object>> list = new ArrayList<>();
            for (Map.Entry<String, Object> entry : mapObj.entrySet()) {
                Map<String, Object> valMap = new HashMap<>();
                valMap.put("key", entry.getKey());
                valMap.put("value", entry.getValue());
                list.add(valMap);
            }
            ctx.setNodeDetail(label, list);
        }

        Map<String, Object> valMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : mapObj.entrySet()) {
            List<Object> subValues = new ArrayList<Object>();
            ctx.setValues(subValues);
            ctx.incrTotalMsgNum();

            try {
                valMap.put("key", entry.getKey());
                valMap.put("value", entry.getValue());
                ctx.setNodeOutput(label, valMap.getClass().getSimpleName(), valMap);

                if (next == null) {
                    ctx.setNodeError(label, String.format("%s", AssignNodeNeededError.class.getSimpleName()));
                } else {
                    this.next.execute(ctx, valMap);
                }

                values.add(subValues);

                ctx.incrSuccessMsgNum();
            } catch (PipeExtractorException e) {
                ctx.appendBadValues(mapObj, e);
                ctx.setNodeError(label, String.format("%s: %s", PipeExtractorException.class.getSimpleName(), o));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void iterateValue(Context ctx, Object o, List<Object> values) {
        Map<String, Object> mapObj = (Map<String, Object>) o;
        ctx.setNodeDetail(label, mapObj.values());

        for (Map.Entry<String, Object> entry : mapObj.entrySet()) {
            List<Object> subValues = new ArrayList<Object>();
            ctx.setValues(subValues);
            ctx.incrTotalMsgNum();

            try {
                Object val = entry.getValue();
                ctx.setNodeOutput(label, val.getClass().getSimpleName(), val);

                if (next == null) {
                    ctx.setNodeError(label, String.format("%s", AssignNodeNeededError.class.getSimpleName()));
                } else {
                    this.next.execute(ctx, val);
                }

                subValues.add(entry.getKey());
                values.add(subValues);
                ctx.incrSuccessMsgNum();
            } catch (PipeExtractorException e) {
                ctx.appendBadValues(mapObj, e);
                ctx.setNodeError(label, String.format("%s: %s", PipeExtractorException.class.getSimpleName(), o));
            }
        }
    }
}
