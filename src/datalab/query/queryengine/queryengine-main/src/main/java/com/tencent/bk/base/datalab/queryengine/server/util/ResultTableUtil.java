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

package com.tencent.bk.base.datalab.queryengine.server.util;

import static com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants.PROCESSING_TYPE_QUERYSET;
import static com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants.PROCESSING_TYPE_SNAPSHOT;
import static com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants.ES_INDEX;
import static com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants.ES_INDEX_PATTERN;

import com.tencent.bk.base.datalab.meta.TableMeta;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.third.MetaApiService;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/**
 * 获取结果表工具类
 */
public class ResultTableUtil {

    private static final Pattern ES_PATTERN = Pattern.compile(ES_INDEX_PATTERN);
    private static final Pattern RESULT_TABLE_PATTERN = Pattern.compile("(\\d+)_\\w+");

    /**
     * 获取es 结果表
     *
     * @param sql es 查询 dsl
     * @return 结果表
     */
    public static String getEsResultTableId(String sql) {
        try {
            Map<String, Object> dslMap = JacksonUtil.convertJson2Map(sql);
            if (dslMap != null) {
                String index = dslMap.get(ES_INDEX)
                        .toString();
                Matcher matcher = ES_PATTERN.matcher(index);
                if (matcher.find()) {
                    return matcher.group(1);
                }
            }
        } catch (Exception e) {
            String msg = "ES查询语法错误，无法解析出Index";
            throw new QueryDetailException(ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR, msg);
        }
        return "";
    }

    /**
     * 获取结果表里的业务 Id
     *
     * @param resultTableId 结果表 Id
     * @return 业务 Id
     */
    public static String extractBizId(String resultTableId) {
        Matcher matcher = RESULT_TABLE_PATTERN.matcher(resultTableId);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    /**
     * 是否存在结果表
     *
     * @param resultTableId 结果表 Id
     * @return 结果表存在时返回 true，否则返回 false
     */
    public static boolean isExistResultTable(String resultTableId) {
        MetaApiService metaApiService = SpringBeanUtil.getBean(MetaApiService.class);
        TableMeta tableMeta = metaApiService.fetchTableFields(resultTableId);
        return !tableMeta.getResultTableName().isEmpty();
    }

    /**
     * 校验结果表名是否符合规范
     *
     * @param resultTableId 结果表 Id
     * @return 符合规范返回 true，否则返回 false
     */
    public static boolean validResultTable(String resultTableId) {
        Matcher tbMacher = RESULT_TABLE_PATTERN.matcher(resultTableId);
        if (!tbMacher.matches()) {
            return false;
        }
        return true;
    }

    /**
     * 是否是非分区表
     *
     * @param resultTableId 结果表 Id
     * @return 结果表是非分区表时返回 true，否则返回 false
     */
    public static boolean isNonPartitionTable(String resultTableId) {
        String rtType = SpringBeanUtil.getBean(MetaApiService.class)
                .fetchTableFields(resultTableId)
                .getProcessingType();
        if (StringUtils.equalsAnyIgnoreCase(rtType, PROCESSING_TYPE_QUERYSET,
                PROCESSING_TYPE_SNAPSHOT)) {
            return true;
        }
        return false;
    }
}
