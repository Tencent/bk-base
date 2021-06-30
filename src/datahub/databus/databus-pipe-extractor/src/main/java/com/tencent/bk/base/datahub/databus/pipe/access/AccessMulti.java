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
import com.tencent.bk.base.datahub.databus.pipe.exception.AccessByKeyFailedError;
import com.tencent.bk.base.datahub.databus.pipe.exception.AssignNodeNeededError;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class AccessMulti extends Access {

    private String[] keys;
    private String separator = ",";

    /**
     * 构造函数
     */
    public AccessMulti(Context ctx, Map<String, Object> config) {
        nodeConf = config;
        nextNodeConf = (Map<String, Object>) nodeConf.get(EtlConsts.NEXT);
        generateNodeLabel();

        keys = ((String) nodeConf.get(EtlConsts.KEYS)).split(",");
        separator = (String) nodeConf.get(EtlConsts.SEPARATOR);
        next = NodeFactory.genNode(ctx, nextNodeConf);
    }

    @Override
    public boolean validateNext() {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * 将多个字段使用连接符串在一起，作为一个字符串返回.
     *
     * @param ctx 上下文
     * @param o 输入对象，map类型
     * @return map中多个key所对应的value连接成的字符串
     */
    @Override
    public Object execute(Context ctx, Object o) {
        if (!(o instanceof Map)) {
            ctx.setNodeDetail(label, null);
            ctx.setNodeError(label,
                    String.format("%s: %s", AccessByKeyFailedError.class.getSimpleName(), StringUtils.join(keys, ",")));
            throw new AccessByKeyFailedError(
                    "AccessObj failed while trying to access keys: " + StringUtils.join(keys, ","));
        }

        Map<String, Object> input = (Map<String, Object>) o;
        StringBuilder sb = new StringBuilder();
        for (String key : keys) {
            Object value = input.get(key);
            if (value != null) {
                sb.append(value.toString()).append(separator);
            }
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }

        String nodeResult = sb.toString();
        ctx.setNodeDetail(label, nodeResult);
        ctx.setNodeOutput(label, nodeResult.getClass().getSimpleName(), nodeResult);
        if (next == null) {
            ctx.setNodeError(label, String.format("%s", AssignNodeNeededError.class.getSimpleName()));
        } else {
            return next.execute(ctx, nodeResult);
        }
        return null;
    }
}
