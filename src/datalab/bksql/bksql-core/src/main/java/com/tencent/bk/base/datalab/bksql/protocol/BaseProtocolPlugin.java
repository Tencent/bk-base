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

package com.tencent.bk.base.datalab.bksql.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.base.Preconditions;
import com.tencent.bk.base.datalab.bksql.deparser.DeParser;
import com.tencent.bk.base.datalab.bksql.exception.MessageLocalizedException;
import com.tencent.bk.base.datalab.bksql.parser.calcite.ParserHelper;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ParsingContext;
import com.tencent.bk.base.datalab.bksql.util.ReflectUtil;
import com.tencent.bk.base.datalab.bksql.validator.ASTreeWalker;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.Map;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.MapUtils;

/**
 * not thread safe
 */
public class BaseProtocolPlugin implements ProtocolPlugin {

    public static final JsonNode EMPTY_WALKER_CONFIG = JsonNodeFactory.instance
            .objectNode()
            .put("type", "com.tencent.bk.base.datalab.bksql.validator.checker.NoneChecker");
    public static final JsonNode EMPTY_DE_PARSER_CONFIG = JsonNodeFactory.instance
            .objectNode()
            .put("type", ".Ast");

    private final JsonNode defaultWalkerConfig;
    private final JsonNode defaultDeParserConfig;
    private final Map<String, Object> defaultProperties;

    protected BaseProtocolPlugin(JsonNode defaultWalkerConfig,
            JsonNode defaultDeParserConfig,
            Map<String, Object> defaultProperties) {
        Preconditions.checkNotNull(defaultWalkerConfig, "defaultWalkerConfig");
        Preconditions.checkNotNull(defaultDeParserConfig, "defaultDeParserConfig");

        this.defaultWalkerConfig = defaultWalkerConfig;
        this.defaultDeParserConfig = defaultDeParserConfig;
        this.defaultProperties = Collections.unmodifiableMap(defaultProperties);
    }

    @Override
    public final Object convert(ParsingContext context) throws Exception {
        SqlNode parsed = ParserHelper.parse(context.getSql());
        Config properties = extractProperties(context);
        walk(parsed, properties);
        return deParse(parsed, properties);
    }

    private Config extractProperties(ParsingContext context) {
        Map<String, Object> properties;
        if (MapUtils.isEmpty(context.getProperties())) {
            properties = defaultProperties;
        } else {
            properties = context.getProperties();
        }
        return ConfigFactory.parseMap(properties);
    }

    private Object deParse(SqlNode sqlNode, Config properties) {
        DeParser deParser = ReflectUtil
                .injectPropertiesAndCreate(defaultDeParserConfig, DeParser.class, properties);
        return deParse(sqlNode, deParser);
    }

    private Object deParse(SqlNode sqlNode, DeParser deParser) {
        try {
            return deParser.deParse(sqlNode);
        } catch (MessageLocalizedException e) { // fixme treat all exceptions within the same way
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "failed to invoke de-parser within config: " + deParser + ", reason: " + e
                            .getMessage(), e);
        }
    }

    private void walk(SqlNode parsed, Config properties) {
        ASTreeWalker walker = ReflectUtil
                .injectPropertiesAndCreate(defaultWalkerConfig, ASTreeWalker.class, properties);
        walk(parsed, walker);
    }

    private void walk(SqlNode sqlNode, ASTreeWalker walker) {
        try {
            walker.walk(sqlNode);
        } catch (MessageLocalizedException e) { // fixme treat all exceptions within the same way
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "failed to invoke walker within config: " + walker + ", reason: " + e
                            .getMessage(), e);
        }
    }

    @JsonProperty
    public JsonNode getDefaultWalkerConfig() {
        return defaultWalkerConfig;
    }

    @JsonProperty
    public JsonNode getDefaultDeParserConfig() {
        return defaultDeParserConfig;
    }

    @JsonProperty
    public Map<String, Object> getDefaultProperties() {
        return defaultProperties;
    }
}
