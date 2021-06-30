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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.base.Preconditions;
import com.tencent.bk.base.datalab.bksql.protocol.BaseProtocolPlugin;
import com.tencent.bk.base.datalab.bksql.util.ReflectUtil;
import com.tencent.bk.base.datalab.bksql.util.calcite.deparser.ExpressionDeParser;
import com.tencent.bk.base.datalab.bksql.util.calcite.deparser.SelectDeParser;
import com.tencent.bk.base.datalab.bksql.util.calcite.deparser.StatementDeParser;
import com.tencent.bk.base.datalab.bksql.validator.ASTreeWalker;
import com.typesafe.config.Config;
import java.util.Objects;
import org.apache.calcite.sql.SqlNode;

public class Sql implements DeParser {

    private final ASTreeWalker walker;

    @JsonCreator
    public Sql(
            @JacksonInject("properties")
                    Config properties,
            @JsonProperty(value = "walker")
                    JsonNode walkerConfig
    ) {
        Preconditions.checkNotNull(walkerConfig, "walkerConfig");
        final JsonNode finalWalkerConfig;
        if (Objects.equals(walkerConfig, NullNode.getInstance())) {
            finalWalkerConfig = BaseProtocolPlugin.EMPTY_WALKER_CONFIG;
        } else {
            finalWalkerConfig = walkerConfig;
        }
        walker = ReflectUtil
                .injectPropertiesAndCreate(finalWalkerConfig, ASTreeWalker.class, properties);
    }

    @Override
    public Object deParse(SqlNode sqlNode) {
        walker.walk(sqlNode);
        final StatementDeParser deParser = new StatementDeParser(new ExpressionDeParser(),
                new SelectDeParser(), new StringBuilder());
        sqlNode.accept(deParser);
        return deParser.getBuffer().toString();
    }
}
