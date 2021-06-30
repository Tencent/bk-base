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

package com.tencent.bk.base.datahub.databus.pipe.cal;

import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CacheMap extends Expr {

    private static final Logger log = LoggerFactory.getLogger(CacheMap.class);
    private CacheMapImpl cache;
    private List<String> keys;
    private String cacheKey;

    //表名，map_column, "k1, k2, k3", "column1, column2, column3"

    /**
     * 遍历器，初始画的额时候schema通过压栈来处理层次关系.
     */
    public CacheMap(Context ctx, Map<String, Object> config) {
        List<Object> args = (List<Object>) config.get(EtlConsts.ARGS);
        keys = new ArrayList<>();

        final String tableName = ((String) (args.get(0))).trim();
        final String mapColumn = ((String) (args.get(1))).trim();
        final String origKeysStr = ((String) (args.get(2))).trim();
        final String keyColsStr = ((String) (args.get(3))).trim();
        for (String key : StringUtils.split(origKeysStr, ",")) {
            keys.add(key.trim());
        }

        if (ctx.ccCacheConfig == null) {
            throw new IllegalArgumentException("can not find config of cc cache");
        }

        String host = ctx.ccCacheConfig.get(EtlConsts.HOST);
        String port = ctx.ccCacheConfig.get(EtlConsts.PORT);
        String user = ctx.ccCacheConfig.get(EtlConsts.USER);
        String pass = ctx.ccCacheConfig.get(EtlConsts.PASSWD);
        String db = ctx.ccCacheConfig.get(EtlConsts.DB);

        this.cache = CacheMapImpl.getInstance(host, port, user, pass, db);
        cacheKey = cache.getCacheKey(tableName, mapColumn, keyColsStr);
        cache.addCache(tableName, mapColumn, keyColsStr);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object execute(String varName, List<Object> oneValue, Fields flattenedSchema) {
        String keysInStr = buildCacheKey(oneValue, flattenedSchema);
        Object value = cache.get(cacheKey, keysInStr);
        if (value == null) {
            return 0;
        }
        return value;
    }

    /**
     * 构建mapping的key,并作为字符串返回。
     *
     * @param oneValue 清洗结果的一条记录
     * @param flattenedSchema 清洗结果的schema信息
     * @return 字符串, 包含指定的keys的值, 用分隔符串起来
     */
    private String buildCacheKey(List<Object> oneValue, Fields flattenedSchema) {
        List<Object> objs = new ArrayList<>(keys.size());
        for (String key : keys) {
            int varIdx = flattenedSchema.fieldIndex(key);
            objs.add(oneValue.get(varIdx));
        }

        return StringUtils.join(objs, CacheMapImpl.SEP);
    }
}
