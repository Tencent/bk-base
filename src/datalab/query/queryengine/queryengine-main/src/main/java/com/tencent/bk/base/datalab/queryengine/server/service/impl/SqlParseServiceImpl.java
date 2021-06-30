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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.bk.base.datalab.bksql.core.BKSqlService;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ParsingContext;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants;
import com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.StatementTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.service.SqlParseService;
import com.tencent.bk.base.datalab.queryengine.server.util.BkSqlUtil;
import java.util.Locale;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SqlParseServiceImpl implements SqlParseService {

    @Autowired
    BKSqlService bkSqlService;

    @Override
    public String checkSyntax(String sql) throws Exception {
        return BkSqlUtil.checkSyntax(sql);
    }

    @Override
    public Map<String, Object> getStatementTypeAndResultTables(String sql,
            Map<String, Object> properties)
            throws Exception {
        final Map<String, Object> result = Maps.newHashMap();
        String statementType = (String) bkSqlService
                .convert(BkSqlContants.BKSQL_PROTOCOL_STATEMENT_TYPE,
                        new ParsingContext(sql, properties));
        StatementTypeEnum st = StatementTypeEnum.valueOf(statementType.toUpperCase(Locale.ENGLISH));
        result.put(ResponseConstants.STATEMENT_TYPE, st);
        switch (st) {
            case DML_SELECT:
                result.put(ResponseConstants.RESULT_TABLE_IDS, BkSqlUtil.getResultTableIds(sql));
                break;
            case DML_UPDATE:
                result.put(ResponseConstants.RESULT_TABLE_IDS, Sets.newHashSet(
                        (String) bkSqlService.convert(BkSqlContants.BKSQL_PROTOCOL_UPDATE_TABLE_NAME,
                                new ParsingContext(sql, properties))));
                break;
            case DML_DELETE:
                result.put(ResponseConstants.RESULT_TABLE_IDS, Sets.newHashSet(
                        (String) bkSqlService.convert(BkSqlContants.BKSQL_PROTOCOL_DELETE_TABLE_NAME,
                                new ParsingContext(sql, properties))));
                break;
            case DDL_CTAS:
                result.put(ResponseConstants.RESULT_TABLE_IDS, BkSqlUtil.getCreateSourceTableNames(sql));
                break;
            default:
                result.put(ResponseConstants.RESULT_TABLE_IDS, Sets.newHashSet());
        }
        return result;
    }
}
