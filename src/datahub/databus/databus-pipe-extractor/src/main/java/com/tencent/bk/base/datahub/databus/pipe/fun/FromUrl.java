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
import com.tencent.bk.base.datahub.databus.pipe.exception.UrlDecodeError;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * 解析数据中URL编码的数据到字典类型数据.
 */
public class FromUrl extends Fun {

    public FromUrl(Context ctx, Map<String, Object> config) {
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
     * 解析url编码的字符串,转换为map结构的key/value对
     *
     * @param ctx 上下文
     * @param o url编码的字符串
     * @return 下个节点处理结果
     */
    @Override
    public Object execute(Context ctx, Object o) {
        if (o instanceof String) {
            String data = (String) o;
            Map<String, String> nodeResult = readMap(ctx, data);
            ctx.setNodeDetail(label, nodeResult);
            ctx.setNodeOutput(label, nodeResult.getClass().getSimpleName(), nodeResult);
            if (next == null) {
                ctx.setNodeError(label, String.format("%s", AssignNodeNeededError.class.getSimpleName()));
            } else {
                return this.next.execute(ctx, nodeResult);
            }
        } else {
            ctx.setNodeError(label, String.format("%s: %s", UrlDecodeError.class.getSimpleName(), o));
            ctx.setNodeDetail(label, null);
            return null;
        }

        return null;
    }

    /**
     * 将url字符串解码,并将其中的key=value进行提取,放入map后返回
     *
     * @param ctx 上下文
     * @param data url字符串
     * @return 包含url中所有key/value的map
     */
    private Map<String, String> readMap(Context ctx, String data) {
        Map<String, String> map = new HashMap<>();
        try {
            if (ctx.etl.encoding == null) {
                data = URLDecoder.decode(data, EtlConsts.UTF8);
            } else {
                data = URLDecoder.decode(data, ctx.etl.encoding);
            }
        } catch (Exception e) {
            ctx.appendBadValues(data, new UrlDecodeError(e.getMessage()));
            ctx.setNodeError(label, String.format("%s: %s", UrlDecodeError.class.getSimpleName(), data));
            return map;
        }

        String[] params = data.split("&");
        for (String param : params) {
            String[] parts = param.split("=");
            String name = parts[0];
            String value = parts.length > 1 ? parts[1] : "";
            map.put(name, value);
        }
        return map;
    }
}

