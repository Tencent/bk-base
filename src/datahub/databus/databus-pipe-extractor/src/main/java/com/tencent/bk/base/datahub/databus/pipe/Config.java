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

package com.tencent.bk.base.datahub.databus.pipe;

import com.tencent.bk.base.datahub.databus.pipe.cal.Cal;
import com.tencent.bk.base.datahub.databus.pipe.convert.Convert;
import com.tencent.bk.base.datahub.databus.pipe.dispatch.Dispatch;
import com.tencent.bk.base.datahub.databus.pipe.dispatch.Expr;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Config {

    /**
     * 分析配置文件，生成清洗提取节点树.
     */
    @SuppressWarnings("unchecked")
    public static Node parse(Context ctx, String jsonStr) throws IOException {
        Map<String, Object> map = JsonUtils.readMap(jsonStr);
        if (map.get(EtlConsts.EXTRACT) != null) {
            return NodeFactory.genNode(ctx, (Map<String, Object>) map.get(EtlConsts.EXTRACT));
        } else {
            return NodeFactory.genNode(ctx, map);
        }
    }

    /**
     * 分析数据计算节点的配置.
     */
    @SuppressWarnings("unchecked")
    public static List<Cal> parseCalculate(Context ctx, String jsonStr) throws IOException {
        Map<String, Object> map = JsonUtils.readMap(jsonStr);
        if (map.get(EtlConsts.CALCULATE) == null) {
            return null;
        }
        List<Cal> cals = new ArrayList<Cal>();

        List<Map<String, Object>> calConf = (List<Map<String, Object>>) map.get(EtlConsts.CALCULATE);

        for (Map<String, Object> conf : calConf) {
            cals.add(new Cal(ctx, conf));
        }
        return cals;
    }

    /**
     * 分析配置文件，生成清洗分发节点树.
     */
    @SuppressWarnings("unchecked")
    public static Map<Integer, Expr> parseDispatch(ETL etl, String jsonStr) throws IOException {
        Map<String, Object> map = JsonUtils.readMap(jsonStr);
        if (map.get(EtlConsts.DISPATCH) == null) {
            return null;
        }
        Map<Integer, Expr> dispatches = new HashMap<Integer, Expr>();

        List<Map<String, Object>> dispatchConf = (List<Map<String, Object>>) map.get(EtlConsts.DISPATCH);

        for (Map<String, Object> conf : dispatchConf) {
            dispatches.put((Integer) conf.get(EtlConsts.DATA_ID),
                    Dispatch.buildExpr(etl, (Map<String, Object>) conf.get(EtlConsts.COND)));
        }
        return dispatches;
    }

    /**
     * 分析配置文件，生成清洗转换节点树.
     */
    @SuppressWarnings("unchecked")
    public static List<Convert> parseConvert(ETL etl, String jsonStr) throws IOException {
        Map<String, Object> map = JsonUtils.readMap(jsonStr);
        if (map.get(EtlConsts.CONVERT) == null) {
            return null;
        }
        List<Convert> converts = new ArrayList<Convert>();

        List<Map<String, Object>> convertConf = (List<Map<String, Object>>) map.get(EtlConsts.CONVERT);

        for (Map<String, Object> conf : convertConf) {
            Convert convert = Convert.buildConvert(conf);
            if (convert != null) {
                converts.add(convert);
            }
        }
        return converts;
    }

    /**
     * 分析配置文件，生成清洗字段扩充节点树.
     */
    @SuppressWarnings("unchecked")
    public static Derive parseDerive(Context ctx, String jsonStr) throws IOException {
        Map<String, Object> map = JsonUtils.readMap(jsonStr);
        if (map.get(EtlConsts.DERIVE) == null) {
            return null;
        }
        return new Derive(ctx, map);
    }

}

