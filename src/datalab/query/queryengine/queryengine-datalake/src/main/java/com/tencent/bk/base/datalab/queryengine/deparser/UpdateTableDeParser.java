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

package com.tencent.bk.base.datalab.queryengine.deparser;

import static com.tencent.bk.base.datalab.queryengine.constants.DataLakeConstants.FIELDS;
import static com.tencent.bk.base.datalab.queryengine.constants.DataLakeConstants.UPDATE_ASSIGN;
import static com.tencent.bk.base.datalab.queryengine.constants.DataLakeConstants.UPDTATE_CONDITION;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datahub.iceberg.functions.Assign;
import com.tencent.bk.base.datahub.iceberg.functions.ValFunction;
import com.tencent.bk.base.datalab.bksql.deparser.SimpleListenerDeParser;
import com.tencent.bk.base.datalab.meta.Field;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.iceberg.expressions.Expression;

public class UpdateTableDeParser extends SimpleListenerDeParser {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final Map<String, Object> parsedMap = Maps.newHashMap();
    private static List<Field> fieldList;

    static {
        JSON_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        JSON_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    public UpdateTableDeParser(@JacksonInject("properties")
            Config properties) {
        try {
            fieldList = JSON_MAPPER
                    .readValue(properties.getString(FIELDS), new TypeReference<List<Field>>() {
                    });
        } catch (IOException e) {
            throw new RuntimeException("Failed to extract table schema!");
        }
    }

    @Override
    public void enterUpdateNode(SqlUpdate update) {
        super.enterUpdateNode(update);
        final SqlNodeList updateColumnList = update.getTargetColumnList();
        final SqlNodeList updateExpList = update.getSourceExpressionList();
        final SqlNode whereNode = update.getCondition();
        Map<String, ValFunction> transformerMap = resolveUpdateMap(updateColumnList,
                updateExpList);
        Expression whereExpresion = DeparserUtil.resolveWhereCondition((SqlBasicCall) whereNode, fieldList);
        if (transformerMap == null || transformerMap.size() == 0) {
            throw new RuntimeException("Failed to parse update expression");
        }
        if (whereExpresion == null) {
            throw new RuntimeException("Failed to parse where expression");
        }
        parsedMap.put(UPDATE_ASSIGN, transformerMap);
        parsedMap.put(UPDTATE_CONDITION, whereExpresion);
    }

    private Map<String, ValFunction> resolveUpdateMap(SqlNodeList updateColumnList,
            SqlNodeList updateExpList) {
        Map<String, ValFunction> transformerMap = Maps.newHashMap();
        for (int i = 0; i < updateColumnList.size(); i++) {
            SqlNode columnNode = updateColumnList.get(i);
            SqlNode valueNode = updateExpList.get(i);
            boolean isSimpleExp =
                    columnNode instanceof SqlIdentifier && valueNode instanceof SqlLiteral;
            if (isSimpleExp) {
                String columnName = ((SqlIdentifier) columnNode).getSimple();
                Object columnValue = DeparserUtil.getColumnValue(fieldList, valueNode, columnName);
                if (ObjectUtils.allNotNull(columnName, columnValue)) {
                    transformerMap.put(columnName, new Assign<>(columnValue));
                }
            }
        }
        return transformerMap;
    }

    @Override
    protected Object getRetObj() {
        return parsedMap;
    }
}
