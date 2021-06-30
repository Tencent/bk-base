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

import static com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants.BKSQL_PROTOCOL_COMMON;
import static com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants.BKSQL_PROTOCOL_CREATE_TABLE_NAME;
import static com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants.BKSQL_PROTOCOL_QUERY_IN_CREATE_AS;
import static com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants.BKSQL_PROTOCOL_QUERY_SOURCE;
import static com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants.BKSQL_PROTOCOL_TABLE_NAMES_WITH_STORAGE;
import static com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants.DEVICE_TYPE_ES;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.INTERFACE_BKSQL_INVOKE_ERROR;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR;

import com.tencent.bk.base.datalab.bksql.core.BKSqlService;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ParsingContext;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import java.util.Map;
import java.util.Set;

public class BkSqlUtil {

    /**
     * 调用 BkSql 服务 获取 select 语句 from 源表
     *
     * @param sql sql 文本
     * @return from 源表
     */
    public static Set<String> getResultTableIds(String sql) {
        try {
            return (Set<String>) SpringBeanUtil.getBean(BKSqlService.class)
                    .convert(BkSqlContants.BKSQL_PROTOCOL_TABLE_NAMES,
                            new ParsingContext(sql, null));
        } catch (Exception e) {
            throw new QueryDetailException(INTERFACE_BKSQL_INVOKE_ERROR, e.getMessage());
        }
    }

    /**
     * 调用 BkSql 服务获取结果表和存储映射关系
     *
     * @param sql sql 文本
     * @return 结果表和存储映射关系
     */
    public static Map<String, String> getResultTableWithStorage(
            String sql) {
        try {
            return (Map<String, String>) SpringBeanUtil.getBean(BKSqlService.class)
                    .convert(BKSQL_PROTOCOL_TABLE_NAMES_WITH_STORAGE,
                            new ParsingContext(sql, null));
        } catch (Exception e) {
            throw new QueryDetailException(INTERFACE_BKSQL_INVOKE_ERROR, e.getMessage());
        }
    }

    /**
     * 调用 BkSql 服务获取 create as 语句里的的 create 表名
     *
     * @param sql sql 文本
     * @return create 表名
     */
    public static String getCreateTableName(String sql) {
        try {
            return (String) SpringBeanUtil.getBean(BKSqlService.class)
                    .convert(BKSQL_PROTOCOL_CREATE_TABLE_NAME,
                            new ParsingContext(sql, null));
        } catch (Exception e) {
            throw new QueryDetailException(INTERFACE_BKSQL_INVOKE_ERROR, e.getMessage());
        }
    }

    /**
     * 调用 BkSql 服务获取 create as 语句里的的 select 源表名
     *
     * @param sql sql 文本
     * @return select 源表名
     */
    public static Set<String> getCreateSourceTableNames(String sql) {
        try {
            String selectSql = getQueryInCreateAs(sql);
            return getResultTableIds(selectSql);
        } catch (Exception e) {
            throw new QueryDetailException(INTERFACE_BKSQL_INVOKE_ERROR, e.getMessage());
        }
    }

    /**
     * 调用 BkSql 服务获取 create as 语句里的 select 语句
     *
     * @param sql sql 文本
     * @return create as 语句里的 select 语句
     */
    public static String getQueryInCreateAs(String sql) {
        try {
            return (String) SpringBeanUtil.getBean(BKSqlService.class)
                    .convert(BKSQL_PROTOCOL_QUERY_IN_CREATE_AS,
                            new ParsingContext(sql, null));
        } catch (Exception e) {
            throw new QueryDetailException(INTERFACE_BKSQL_INVOKE_ERROR, e.getMessage());
        }
    }

    /**
     * 调用 BkSql 服务获取结果表的时间范围
     *
     * @param sql sql 文本
     * @return 结果表的时间范围
     */
    public static Map<String, Map<String, String>> getResultTablePartition(
            String sql) {
        try {
            return (Map<String, Map<String, String>>) SpringBeanUtil.getBean(BKSqlService.class)
                    .convert(BKSQL_PROTOCOL_QUERY_SOURCE,
                            new ParsingContext(sql, null));
        } catch (Exception e) {
            throw new QueryDetailException(INTERFACE_BKSQL_INVOKE_ERROR, e.getMessage());
        }
    }

    /**
     * 调用 BkSql 服务校验 sql 语法
     *
     * @param sql sql 文本
     * @return 校验成功返回 sql，校验失败返回语法异常
     */
    public static String checkSyntax(String sql) {
        try {
            return (String) SpringBeanUtil.getBean(BKSqlService.class)
                    .convert(BKSQL_PROTOCOL_COMMON,
                            new ParsingContext(sql, null));
        } catch (Exception e) {
            throw new QueryDetailException(QUERY_SQL_SYNTAX_ERROR, e.getMessage());
        }
    }

    /**
     * 判断 sql 是否属于 es dsl
     *
     * @param sql sql 文本
     * @param preferStorage 指定的存储
     * @return 属于 dsl 返回 true，否则返回 false
     */
    public static boolean isEsDsl(String sql, String preferStorage) {
        boolean isEsDsl =
                JacksonUtil.readTree(sql) != null && DEVICE_TYPE_ES.equals(preferStorage);
        return isEsDsl;
    }
}
