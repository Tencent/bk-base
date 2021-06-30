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

package com.tencent.bk.base.datalab.bksql.deparser;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.calcite.sql.SqlDropModels;
import org.apache.calcite.sql.SqlIdentifier;

public class DropTable extends SimpleListenerDeParser {

    private static final String DROP_TABLE_NAME = "drop_table_name";
    private static final String IF_EXISTS = "if_exists";
    private static final String DROP_MODELS = "drop_models";
    private Map<String, Object> deParseMap = Maps.newHashMap();

    @Override
    public void enterDropModelsNode(SqlDropModels node) {
        super.enterDropModelsNode(node);
        SqlIdentifier tableName = node.getName();
        boolean dropModels = node.isDropModels();
        boolean ifExists = node.isIfExists();
        deParseMap.putIfAbsent(DROP_TABLE_NAME, tableName.toString());
        deParseMap.putIfAbsent(IF_EXISTS, ifExists);
        deParseMap.putIfAbsent(DROP_MODELS, dropModels);
    }

    @Override
    protected Object getRetObj() {
        return deParseMap;
    }
}
