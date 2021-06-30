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

package com.tencent.bk.base.datalab.queryengine.server.service.impl;

import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datalab.bksql.core.BKSqlService;
import com.tencent.bk.base.datalab.bksql.exception.FailedOnCheckException;
import com.tencent.bk.base.datalab.bksql.exception.MessageLocalizedException;
import com.tencent.bk.base.datalab.bksql.exception.ParseException;
import com.tencent.bk.base.datalab.bksql.exception.TokenMgrException;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ParsingContext;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants;
import com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.enums.StatementTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.ParameterInvalidException;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryAsyncService;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryProxyService;
import com.tencent.bk.base.datalab.queryengine.server.service.SqlAdminService;
import com.tencent.bk.base.datalab.queryengine.server.service.SqlDdlService;
import com.tencent.bk.base.datalab.queryengine.server.service.SqlDmlService;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class QueryProxyServiceImpl implements QueryProxyService {

    @Autowired
    private BKSqlService bkSqlService;

    @Autowired
    private QueryAsyncService queryAsyncService;

    @Autowired
    private SqlDdlService sqlDdlService;

    @Autowired
    private SqlAdminService sqlAdminService;

    @Autowired
    private SqlDmlService sqlDmlService;

    @Override
    public void doProxy(QueryTaskContext queryContext) {
        final QueryTaskInfo qt = queryContext.getQueryTaskInfo();
        final String sql = queryContext.getQueryTaskInfo().getSqlText();
        try {
            boolean isEsDsl = JacksonUtil.readTree(sql) != null
                    && StorageConstants.DEVICE_TYPE_ES.equals(qt.getPreferStorage());
            StatementTypeEnum st;
            if (!isEsDsl) {
                String statementType = (String) bkSqlService
                        .convert(BkSqlContants.BKSQL_PROTOCOL_STATEMENT_TYPE,
                                new ParsingContext(sql, qt.getProperties()));
                st = StatementTypeEnum.valueOf(statementType.toUpperCase(Locale.ENGLISH));
            } else {
                st = StatementTypeEnum.DML_SELECT;
            }
            queryContext.setStatementType(st);
            switch (st) {
                case DML_SELECT:
                case DDL_CTAS:
                    queryAsyncService.query(queryContext);
                    break;
                case DDL_CREATE:
                    sqlDdlService.createResultTable(queryContext);
                    break;
                case SHOW_TABLES:
                    sqlAdminService.showResultTables(queryContext);
                    break;
                case SHOW_SQL:
                    sqlAdminService.showCreateTableSql(queryContext);
                    break;
                case DML_DELETE:
                    sqlDmlService.deleteTable(queryContext);
                    break;
                case DML_UPDATE:
                    sqlDmlService.updateTable(queryContext);
                    break;
                default:
                    throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR,
                            MessageLocalizedUtil
                                    .getMessage("当前sql语句类型 {0} 暂未支持", new Object[]{st.toString()}),
                            ImmutableMap.<String, String>builder().put(ResponseConstants.QUERY_ID, qt.getQueryId())
                                    .build());
            }
        } catch (QueryDetailException | ParameterInvalidException e) {
            throw e;
        } catch (FailedOnCheckException | ParseException e) {
            throw new QueryDetailException(ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR, e.getMessage());
        } catch (TokenMgrException e) {
            throw new QueryDetailException(ResultCodeEnum.QUERY_SQL_TOKEN_ERROR, e.getMessage());
        } catch (MessageLocalizedException e) {
            throw new QueryDetailException(ResultCodeEnum.RESOURCE_NOT_EXISTED, e.getMessage());
        } catch (IllegalArgumentException e) {
            throw new ParameterInvalidException(ResultCodeEnum.PARAM_IS_INVALID, e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.SYSTEM_INNER_ERROR, e.getMessage());
        }
    }
}
