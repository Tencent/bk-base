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

import java.util.Map;

public abstract class Fun extends Node {

    /**
     * 根据配置文件生成节点树.
     */
    @SuppressWarnings("unchecked")
    public static Node genNode(Context ctx, Map<String, Object> config) {

        String method = (String) config.get(EtlConsts.METHOD);

        switch (method) {
            case EtlConsts.FROM_JSON:
                return new FromJson(ctx, config);

            case EtlConsts.ITERATE:
                return new Iterate(ctx, config);

            case EtlConsts.SELECTOR:
                return new Selector(ctx, config);

            case EtlConsts.POP:
                return new Pop(ctx, config);

            case EtlConsts.SPLIT:
                return new Split(ctx, config);

            case EtlConsts.FROM_URL:
                return new FromUrl(ctx, config);

            case EtlConsts.CSV_LINE:
                return new CsvLine(ctx, config);

            case EtlConsts.FROM_JSON_LIST:
                return new FromJsonList(ctx, config);

            case EtlConsts.REPLACE:
                return new ReplaceString(ctx, config);

            case EtlConsts.SPLITKV:
                return new SplitKeyValue(ctx, config);

            case EtlConsts.ITEMS:
                return new Items(ctx, config);

            case EtlConsts.ZIP:
                return new Zip(ctx, config);

            case EtlConsts.TO_STRING:
                return new ToString(ctx, config);

            case EtlConsts.REGEX_EXTRACT:
                return new RegexExtract(ctx, config);

            default:
                return null;
        }
    }
}

