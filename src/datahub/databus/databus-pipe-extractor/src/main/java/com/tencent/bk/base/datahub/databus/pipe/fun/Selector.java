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
import com.tencent.bk.base.datahub.databus.pipe.Node;
import com.tencent.bk.base.datahub.databus.pipe.NodeFactory;
import com.tencent.bk.base.datahub.databus.pipe.exception.ConditionAndBranchNotMatch;
import com.tencent.bk.base.datahub.databus.pipe.fun.logic.expression.Expression;
import com.tencent.bk.base.datahub.databus.pipe.fun.logic.expression.ExpressionFactory;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Selector extends Node {

    private List<Expression> conditions;
    private List<Node> next;
    private List<Object> defaultValues;
    private List<List<Integer>> notContantFieldsList;

    /**
     * 选择节点.
     */
    @SuppressWarnings("unchecked")
    public Selector(Context ctx, Map<String, Object> config) {
        nodeConf = config;
        generateNodeLabel();
        notContantFieldsList = new ArrayList<>();

        List<Map<String, Object>> args = (List<Map<String, Object>>) nodeConf.get(EtlConsts.ARGS);
        List<Map<String, Object>> branches = (List<Map<String, Object>>) nodeConf.get(EtlConsts.NEXT);

        // 执行分支数必须等于条件数，或者比条件数大1
        int diff = branches.size() - args.size();
        if (diff < 0 || diff > 1) {
            throw new ConditionAndBranchNotMatch("next.len - args.len should be 0 or 1, but " + diff);
        }

        next = new ArrayList<>();
        int schemaLen = ctx.getSchema().size();
        List<Object> fieldList = new ArrayList<>();
        // 每个单独分支生成清洗节点，然后合并schema到context
        for (Map<String, Object> branch : branches) {
            Context context = new Context();
            next.add(NodeFactory.genNode(context, branch));
            context.getSchema().forEach(field -> {
                if (!fieldList.contains(field)) {
                    fieldList.add(field);
                }
            });

            // 记录在本分支中不曾不出现的子schema位置，目的是后面设置默认值做特殊处理，避免最后扁平化时因类型不匹配失败
            List<Integer> list = new ArrayList<>();
            for (int idx = 0; idx < fieldList.size(); idx++) {
                Object o = fieldList.get(idx);
                if (o instanceof Fields && !context.getSchema().contain(o)) {
                    list.add(idx + schemaLen);
                }
            }
            notContantFieldsList.add(list);
        }

        // 生成清洗汇总后各字段缺省值
        Fields schema = ctx.getSchema();
        defaultValues = new ArrayList<>();
        for (int i = 0; i < schemaLen; i++) {
            defaultValues.add(null);
        }
        for (Object o : fieldList) {
            if (o instanceof Field) {
                // field 直接填充null
                schema.append((Field) o);
                defaultValues.add(null);
            } else {
                // 子结构，生成一个各字段都为null的缺省值
                // 如果直接为null，会导致最后扁平化时，结果集为空
                schema.append((Fields) o);
                genDef4Fields(defaultValues, (Fields) o);
            }
        }

        // 生成表达式节点
        conditions = new ArrayList<>();
        for (Map<String, Object> arg : args) {
            Expression expr = ExpressionFactory.createExpression(arg);
            conditions.add(expr);
        }
    }

    /**
     * 生成fields对应的一条默认记录值并添加到list中
     *
     * @param list 默认值列表
     * @param fields schema列表
     */
    private void genDef4Fields(List<Object> list, Fields fields) {
        List<Object> newList = new ArrayList<>();
        for (Iterator<Object> iterator = fields.iterator(); iterator.hasNext(); ) {
            Object o = iterator.next();
            if (o instanceof Field) {
                newList.add(null);
            } else {
                genDef4Fields(newList, (Fields) o);
            }
        }
        List<List<Object>> l = new ArrayList<>();
        l.add(newList);
        list.add(l);
    }

    @Override
    public boolean validateNext() {
        return false;
    }

    @Override
    public Object execute(Context ctx, Object o) {
        int idx = 0;
        for (; idx < conditions.size(); idx++) {
            if (conditions.get(idx).execute(o)) {
                // 满足分支idx
                next.get(idx).execute(ctx, o);
                setMissingValue(idx, ctx.getValues());
                return null;
            }
        }
        if (idx < next.size()) {
            // 所有分支不满足，执行配置的else操作
            next.get(idx).execute(ctx, o);
            setMissingValue(idx, ctx.getValues());
            return null;
        }

        // 所有分支不满足，没有配置else操作，填充缺少值
        setMissingValue(ctx.getValues());
        return null;
    }

    /**
     * 为index分支的数据填充缺少的值
     *
     * @param index 分支索引
     * @param value 值列表
     */
    private void setMissingValue(int index, List<Object> value) {
        notContantFieldsList.get(index).forEach(idx -> {
            value.set(idx, defaultValues.get(idx));
        });

        for (int i = value.size(); i < defaultValues.size(); i++) {
            value.add(defaultValues.get(i));
        }
    }

    private void setMissingValue(List<Object> value) {
        int i = value.size();
        if (i == 0) {
            // 没有数据，不填充
            return;
        }

        for (; i < defaultValues.size(); i++) {
            // 填充默认值
            value.add(defaultValues.get(i));
        }
    }
}
