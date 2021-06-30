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

package com.tencent.bk.base.datahub.databus.pipe.access;


import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.pipe.NodeFactory;
import com.tencent.bk.base.datahub.databus.pipe.exception.AccessByIndexFailedError;
import com.tencent.bk.base.datahub.databus.pipe.exception.AssignNodeNeededError;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;

import java.util.List;
import java.util.Map;

public class AccessPos extends Access {

    private Integer index;

    /**
     * 位置赋值.
     */
    @SuppressWarnings("unchecked")
    public AccessPos(Context ctx, Map<String, Object> config) {
        nodeConf = config;
        nextNodeConf = (Map<String, Object>) nodeConf.get(EtlConsts.NEXT);
        generateNodeLabel();

        if (nodeConf.get(EtlConsts.INDEX) instanceof String) {
            index = Integer.parseInt((String) nodeConf.get(EtlConsts.INDEX));
        } else {
            index = (Integer) nodeConf.get(EtlConsts.INDEX);
        }
        next = NodeFactory.genNode(ctx, nextNodeConf);
        defaultType = (String) nodeConf.getOrDefault(EtlConsts.DEFAULT_TYPE, NULL);
        if (!defaultType.equals(NULL)) {
            String value = (String) nodeConf.get(EtlConsts.DEFAULT_VALUE);
            defaultValue = new Field(defaultType, defaultType).castType(value);
        }
    }

    @Override
    public boolean validateNext() {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * 执行函数.
     */
    @SuppressWarnings("unchecked")
    public Object execute(Context ctx, Object o) {
        if (!(o instanceof List) || (defaultType.equals(NULL) && (((List<String>) o).size() <= index || index < 0))) {
            ctx.setNodeDetail(label, null);
            ctx.setNodeError(label, String.format("%s: %s", AccessByIndexFailedError.class.getSimpleName(), index));
            throw new AccessByIndexFailedError("AccessPos failed while trying to access index " + index);
        }

        Object nodeResult;
        if (((List<String>) o).size() > index && index >= 0) {
            nodeResult = ((List<String>) o).get(index);
        } else {
            nodeResult = defaultValue;
        }

        ctx.setNodeDetail(label, nodeResult);
        ctx.setNodeOutput(label, nodeResult.getClass().getSimpleName(), nodeResult);
        if (next == null) {
            ctx.setNodeError(label, String.format("%s", AssignNodeNeededError.class.getSimpleName()));
            ctx.setNodeDetail(label, null); // 默认结果为空
        } else {
            return this.next.execute(ctx, nodeResult);
        }

        return null;
    }
}

