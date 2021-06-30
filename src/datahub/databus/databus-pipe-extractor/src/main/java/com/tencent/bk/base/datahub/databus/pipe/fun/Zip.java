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
import com.tencent.bk.base.datahub.databus.pipe.exception.NotListDataError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 实现两个列表的zip.
 * 限制：当前的实现只支持父结构是object且两个列表在同一个层次
 */
public class Zip extends Fun {

    // 第一个列表key值
    private String firstListName;

    // 第二个列表key值
    private String secondListName;

    /**
     * 字符串切分.
     */
    @SuppressWarnings("unchecked")
    public Zip(Context ctx, Map<String, Object> config) {
        nodeConf = config;
        nextNodeConf = (Map<String, Object>) nodeConf.get(EtlConsts.NEXT);
        generateNodeLabel();

        firstListName = ((String) (((List<Object>) nodeConf.get(EtlConsts.ARGS)).get(0)));
        secondListName = ((String) (((List<Object>) nodeConf.get(EtlConsts.ARGS)).get(1)));
        this.next = NodeFactory.genNode(ctx, nextNodeConf);
    }

    @Override
    public boolean validateNext() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object execute(Context ctx, Object o) {
        if (o instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) o;
            if (map.get(firstListName) instanceof List && map.get(secondListName) instanceof List) {
                List<Object> firstList = (List<Object>) map.get(firstListName);
                List<Object> secondList = (List<Object>) map.get(secondListName);

                List<List<Object>> nodeResult = zipList(firstList, secondList);
                ctx.setNodeDetail(label, nodeResult);
                ctx.setNodeOutput(label, nodeResult.getClass().getSimpleName(), nodeResult);
                if (next == null) {
                    ctx.setNodeError(label, String.format("%s", AssignNodeNeededError.class.getSimpleName()));
                } else {
                    return this.next.execute(ctx, nodeResult);
                }

            }
        }

        ctx.setNodeError(label,
                String.format("%s: %s %s", NotListDataError.class.getSimpleName(), firstListName, secondListName));
        ctx.setNodeDetail(label, null);
        return null;
    }

    /**
     * zip实现函数.
     */
    private List<List<Object>> zipList(List<Object> firstList, List<Object> secondList) {
        List<List<Object>> ret = new ArrayList<List<Object>>();
        for (int i = 0; i < firstList.size(); i++) {
            if (i >= secondList.size()) {
                break;
            }
            ret.add(Arrays.asList(firstList.get(i), secondList.get(i)));
        }

        return ret;
    }
}

