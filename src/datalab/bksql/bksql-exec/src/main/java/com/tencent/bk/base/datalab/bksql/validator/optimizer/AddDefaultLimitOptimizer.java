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

package com.tencent.bk.base.datalab.bksql.validator.optimizer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.tencent.bk.base.datalab.bksql.util.ReflectUtil;
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.parser.SqlParserPos;

public class AddDefaultLimitOptimizer extends SimpleListenerWalker {

    private final int limit;

    @JsonCreator
    public AddDefaultLimitOptimizer(@JsonProperty("limit") int limit) {
        this.limit = limit;
    }

    @Override
    public void exitQueryNode(SqlCall call) {
        Preconditions.checkArgument(limit > 0, "limit can not be negative");
        call = addDefaultLimit(call);
        super.exitQueryNode(call);
    }

    private SqlCall addDefaultLimit(SqlCall call) {
        if (call instanceof SqlOrderBy) {
            handleSqlOrderBy((SqlOrderBy) call);
        } else if (call instanceof SqlSelect) {
            handleSqlSelect((SqlSelect) call);
        } else if (call instanceof SqlWith) {
            SqlNode body = ((SqlWith) call).body;
            if (body instanceof SqlCall) {
                call = addDefaultLimit((SqlCall) body);
            }
        }
        return call;
    }

    private void handleSqlSelect(SqlSelect call) {
        SqlSelect select = call;
        if (select.getFetch() == null) {
            select.setFetch(
                    SqlLiteral.createExactNumeric(String.valueOf(limit), SqlParserPos.ZERO));
        }
    }

    private void handleSqlOrderBy(SqlOrderBy call) {
        SqlOrderBy select = call;
        if (select.fetch == null) {
            ReflectUtil.modifyFinalField(select.getClass(), "fetch", select,
                    SqlLiteral.createExactNumeric(String.valueOf(limit), SqlParserPos.ZERO));
        }
    }
}
