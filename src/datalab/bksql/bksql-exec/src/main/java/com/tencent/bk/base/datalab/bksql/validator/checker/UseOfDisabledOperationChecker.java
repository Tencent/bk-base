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

package com.tencent.bk.base.datalab.bksql.validator.checker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datalab.bksql.validator.AbstractTriggeredListenerWalker;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateView;

public class UseOfDisabledOperationChecker extends AbstractTriggeredListenerWalker {

    // thread safe
    private static final Map<String, Class<?>> SUPPORTED_OP_MAP =
            ImmutableMap.<String, Class<?>>builder()
                    .put("ALTER", SqlAlter.class)
                    .put("ALTER VIEW", SqlAlter.class)
                    .put("CREATE INDEX", SqlCreate.class)
                    .put("CREATE TABLE", SqlCreateTable.class)
                    .put("CREATE VIEW", SqlCreateView.class)
                    .put("DELETE", SqlDelete.class)
                    .put("DROP", SqlDrop.class)
                    .put("EXECUTE", SqlExecutableStatement.class)
                    .put("INSERT", SqlInsert.class)
                    .put("UPDATE", SqlInsert.class)
                    .put("MERGE", SqlMerge.class)
                    .put("SELECT", SqlSelect.class)
                    .build();

    // thread safe
    private static final Set<Class<?>> SUPPORTED_OPS = Collections
            .unmodifiableSet(new HashSet<>(SUPPORTED_OP_MAP.values()));

    private final Map<Class<?>, String> disabledOps = new HashMap<>();

    @JsonCreator
    public UseOfDisabledOperationChecker(
            @JsonProperty("disabled-operation-list") List<String> disabledOperationList) {
        super(SUPPORTED_OPS);
        for (String opName : disabledOperationList) {
            if (!SUPPORTED_OP_MAP.containsKey(opName)) {
                throw new IllegalArgumentException("operation not found: " + opName);
            }
            disabledOps.put(SUPPORTED_OP_MAP.get(opName), opName);
        }
    }

    @Override
    public void enterObject0(Object node) {
        if (disabledOps.containsKey(node.getClass())) {
            fail("disabled.operation", disabledOps.get(node.getClass()));
        }
    }

    @Override
    public void exitObject0(Object node) {

    }
}
