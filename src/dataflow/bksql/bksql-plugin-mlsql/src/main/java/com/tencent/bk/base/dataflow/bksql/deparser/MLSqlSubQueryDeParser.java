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

package com.tencent.bk.base.dataflow.bksql.deparser;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.tencent.bk.base.dataflow.bksql.deparser.subdeparser.SubQuerySelectDeParser;
import com.tencent.bk.base.datalab.bksql.deparser.DeParser;
import com.tencent.bk.base.datalab.bksql.util.calcite.deparser.ExpressionDeParser;
import com.tencent.bk.base.datalab.bksql.util.calcite.deparser.StatementDeParser;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.sql.SqlDropModels;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowSql;
import org.apache.calcite.sql.SqlTruncateTable;

public class MLSqlSubQueryDeParser implements DeParser {

    protected boolean hasSubQuery = false;
    protected String operateType = null;
    protected String targetEntityName = null;
    protected boolean subQueryHasJoin = false;

    @JsonCreator
    public MLSqlSubQueryDeParser() {

    }

    @Override
    public Object deParse(SqlNode sqlNode) {
        Map<String, Object> map = new HashMap<>();
        if (sqlNode instanceof SqlShow) {
            operateType = "show";
        } else if (sqlNode instanceof SqlShowSql) {
            operateType = "show";
        } else if (sqlNode instanceof SqlDropModels) {
            operateType = "drop";
        } else if (sqlNode instanceof SqlTruncateTable) {
            operateType = "truncate";
        } else {
            if (sqlNode.getKind() == SqlKind.INSERT) {
                SqlInsert insert = (SqlInsert) sqlNode;
                SqlSelect source = (SqlSelect) insert.getSource();
                sqlNode = source.getFrom();
                operateType = "insert";
                targetEntityName = insert.getTargetTable().toString();
            }
            SubQuerySelectDeParser selectDeParser = new SubQuerySelectDeParser(this.hasSubQuery,
                    this.operateType, this.targetEntityName, this.subQueryHasJoin);
            final StatementDeParser deParser = new StatementDeParser(new ExpressionDeParser(),
                    selectDeParser, new StringBuilder());
            sqlNode.accept(deParser);
            this.hasSubQuery = selectDeParser.isHasSubQuery();
            this.targetEntityName = selectDeParser.getTargetEntityName();
            this.operateType = selectDeParser.getOperateType();
            this.subQueryHasJoin = selectDeParser.isSubQueryHasJoin();
            map.put("sql", deParser.getBuffer().toString());
            map.put("target_name", this.targetEntityName);
        }
        map.put("has_sub_query", this.hasSubQuery);
        map.put("operate_type", this.operateType);
        map.put("sub_query_has_join", this.subQueryHasJoin);
        return map;
    }

    public boolean hasSubQuery() {
        return hasSubQuery;
    }
}
