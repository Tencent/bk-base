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

package com.tencent.bk.base.dataflow.bksql.validator.optimizer;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.tencent.blueking.bksql.validator.SimpleListenerBasedWalker;
import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Map;
import net.sf.jsqlparser.schema.Table;

public class SqlReplaceTableNameOptimizer extends SimpleListenerBasedWalker {

    private static final String PROPERTY_KEY_NEW_TABLE_NAMES = "replace_table_names";
    protected final Config config;
    private final Map<String, String> newTableNames;

    public SqlReplaceTableNameOptimizer(@JacksonInject("properties") Config properties) {
        config = properties;
        newTableNames = initNewTableNames();
    }

    private Map<String, String> initNewTableNames() {
        Map<String, String> retVal = new HashMap<>();
        if (!config.hasPath(PROPERTY_KEY_NEW_TABLE_NAMES)) {
            throw new RuntimeException(String.format("the params must have key %s", PROPERTY_KEY_NEW_TABLE_NAMES));
        }
        Object newTables = this.config.getAnyRef(PROPERTY_KEY_NEW_TABLE_NAMES);
        Map<String, String> tables = (Map<String, String>) newTables;

        for (Map.Entry<String, String> entry : tables.entrySet()) {
            retVal.put(entry.getKey(), entry.getValue());
        }

        return retVal;
    }

    @Override
    public void enterNode(Table inputTable) {
        String name = inputTable.getName();
        if (newTableNames.get(name) != null) {
            inputTable.setName(newTableNames.get(name));
        }
        super.enterNode(inputTable);
    }
}
