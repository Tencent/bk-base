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

package com.tencent.bk.base.datalab.queryengine.validator.optimizer;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.bksql.core.BKSqlService;
import com.tencent.bk.base.datalab.bksql.deparser.Sql;
import com.tencent.bk.base.datalab.bksql.protocol.BaseProtocolPlugin;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ParsingContext;
import com.tencent.bk.base.datalab.bksql.table.BlueKingMetaApiFetcher;
import com.tencent.bk.base.datalab.bksql.table.TableStorageMetaFetcher;
import com.tencent.bk.base.datalab.bksql.util.QuoteUtil;
import com.tencent.bk.base.datalab.bksql.validator.ExpressionReplacementOptimizer;
import com.tencent.bk.base.datalab.meta.StoragesProperty;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.collections.MapUtils;
import org.yaml.snakeyaml.util.UriEncoder;

public class SubQueryIndependentOptimizer extends ExpressionReplacementOptimizer {

    private static final String FEDERATION_CONF = "FEDERATION_CONF";
    private static final String BKSQL_PROTOCOL_PRESTO_ICEBERG = "presto-iceberg";
    private static final String BKSQL_PROTOCOL_PRESTO = "presto";
    private static final String STORAGE_HDFS = "hdfs";
    private static final String PREFER_STORAGE = "prefer_storage";
    private static final String BKSQL_PROTOCOL_TABLE_NAMES_WITH_STORAGE =
            "table-names-with-storage";
    private static final String BKSQL_PROTOCOL_TABLE_NAMES = "table-names";
    private static final String DATA_TYPE_ICEBERG = "iceberg";
    private static final Pattern RESULT_TABLE_PATTERN = Pattern.compile("(\\d+)_(.+)");
    private static final Sql SQL_DEPARSER = new Sql(ConfigFactory.empty(),
            BaseProtocolPlugin.EMPTY_WALKER_CONFIG);
    private final BKSqlService bkSqlService;
    private final TableStorageMetaFetcher tableStorageMetaFetcher;
    @JacksonInject("properties")
    private Config properties;

    @JsonCreator
    public SubQueryIndependentOptimizer(
            @JsonProperty("urlPattern") String urlPattern) {
        this(urlPattern, System.getenv(FEDERATION_CONF));
    }

    public SubQueryIndependentOptimizer(
            String urlPattern, String federationConf) {
        try {
            tableStorageMetaFetcher = BlueKingMetaApiFetcher.forUrl(encodeUrl(urlPattern));
            bkSqlService = new BKSqlService(federationConf);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load bkSqlService");
        }
    }

    @Override
    public void enterSetExpressionNode(SqlBasicCall node) {
        SqlNode leftNode = node.operand(0);
        SqlNode rightNode = node.operand(1);
        registerReplacement(leftNode, getConvertedNode(leftNode));
        registerReplacement(rightNode, getConvertedNode(rightNode));
    }

    @Override
    public void exitSetExpressionNode(SqlBasicCall node) {
        SqlNode leftNode = node.operand(0);
        SqlNode rightNode = node.operand(1);
        if (isSelectOrBasicCall(leftNode) && retrieveReplacement(leftNode) != null) {
            node.setOperand(0, retrieveReplacement(leftNode));
        }
        if (isSelectOrBasicCall(rightNode) && retrieveReplacement(rightNode) != null) {
            node.setOperand(1, retrieveReplacement(rightNode));
        }
    }

    @Override
    public void enterSubSelectNode(SqlSelect call) {
        registerReplacement(call.getFrom(), enterSubSelect(call));
    }

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        registerReplacement(select.getFrom(), enterPlainSelect(select));
    }

    @Override
    public void enterJoinNode(SqlJoin node) {
        SqlNode left = node.getLeft();
        SqlNode right = node.getRight();
        registerReplacement(left, getConvertedNode(left));
        registerReplacement(right, getConvertedNode(right));
    }

    @Override
    public void exitJoinNode(SqlJoin node) {
        SqlNode left = node.getLeft();
        SqlNode right = node.getRight();
        if (isSelectOrBasicCall(left)) {
            if (retrieveReplacement(left) != null) {
                node.setLeft(retrieveReplacement(left));
            }
        }
        if (isSelectOrBasicCall(right)) {
            if (retrieveReplacement(right) != null) {
                node.setRight(retrieveReplacement(right));
            }
        }
    }

    @Override
    protected void replaceSqlSelect(SqlSelect select) {
        SqlNode from = select.getFrom();
        if (isSelect(from) && retrieveReplacement(select.getFrom()) != null) {
            select.setFrom(retrieveReplacement(select.getFrom()));
        }
    }

    /**
     * 获取替换后的 from 节点
     *
     * @param select select 节点
     * @return 替换后的 from 节点
     */
    private SqlNode enterPlainSelect(SqlSelect select) {
        SqlNode from = select.getFrom();
        return getConvertedNode(from);
    }

    /**
     * 获取替换后的节点
     *
     * @param node 需要替换的节点
     * @return 替换后的节点
     */
    private SqlNode getConvertedNode(SqlNode node) {
        SqlKind kind = node.getKind();
        if (kind == SqlKind.AS) {
            return convertAsNode((SqlBasicCall) node);
        } else {
            return convertNode(node);
        }
    }

    /**
     * 替换 as 节点
     *
     * @param node as 节点
     * @return 替换后的 as 节点
     */
    private SqlBasicCall convertAsNode(SqlBasicCall node) {
        SqlBasicCall sqlAs = node;
        SqlNode beforeAs = sqlAs.operand(0);
        SqlNode convertedNode = convertNode(beforeAs);
        sqlAs.setOperand(0, convertedNode);
        return sqlAs;
    }

    /**
     * 替换节点
     *
     * @param node 待替换的节点
     * @return 替换后的节点
     */
    private SqlNode convertNode(SqlNode node) {
        if (!isSelect(node)) {
            return node;
        }
        if (node instanceof SqlSelect && !isPlainSelectNode((SqlSelect) node)) {
            return node;
        }
        if (node instanceof SqlOrderBy && !isPlainOrderByNode((SqlOrderBy) node)) {
            return node;
        }
        SqlNode convertedNode = node;
        try {
            String sql = (String) SQL_DEPARSER.deParse(node);
            Map<String, String> resultTableWithStorage = (Map<String, String>) bkSqlService
                    .convert(BKSQL_PROTOCOL_TABLE_NAMES_WITH_STORAGE,
                            new ParsingContext(sql, null));
            if (MapUtils.isEmpty(resultTableWithStorage)) {
                resultTableWithStorage = getResultTableWithStorageByOneSql(sql);
            }
            for (Entry<String, String> entry : resultTableWithStorage.entrySet()) {
                String resultTableId = entry.getKey();
                String storage = entry.getValue();
                if (!RESULT_TABLE_PATTERN.matcher(resultTableId).matches()) {
                    continue;
                }
                convertedNode = getConvertedSubNodeByBkSql(sql, resultTableId, storage);
                break;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert sub node", e);
        }
        return convertedNode;
    }

    /**
     * 调用 BkSql 服务获取按照子查询各自协议转换后的 SqlNode
     *
     * @param sql 子查询 Sql
     * @param resultTableId 结果表名
     * @param storage 存储类型
     * @return 转换后的 SqlNode
     */
    private SqlNode getConvertedSubNodeByBkSql(String sql, String resultTableId, String storage) {
        SqlNode convertedNode;
        StoragesProperty sp = tableStorageMetaFetcher
                .fetchStorageProperty(QuoteUtil.trimQuotes(resultTableId),
                        storage);
        String dataType = sp.getDataType();
        String convertedProtocol;
        if (STORAGE_HDFS.equalsIgnoreCase(storage) && DATA_TYPE_ICEBERG
                .equalsIgnoreCase(dataType)) {
            convertedProtocol = BKSQL_PROTOCOL_PRESTO_ICEBERG;
        } else if (STORAGE_HDFS.equalsIgnoreCase(storage)) {
            convertedProtocol = BKSQL_PROTOCOL_PRESTO;
        } else {
            convertedProtocol = storage;
        }
        try {
            convertedNode = (SqlNode) bkSqlService
                    .convert(convertedProtocol,
                            new ParsingContext(sql, null));
        } catch (Exception e) {
            throw new RuntimeException("Unable to get converted child nodes through BkSql", e);
        }
        return convertedNode;
    }

    /**
     * 获取 OneSql 中结果表和存储映射
     *
     * @param sql OneSql文本
     * @return OneSql 中结果表和存储映射
     */
    private Map<String, String> getResultTableWithStorageByOneSql(String sql) {
        Map<String, String> resultTableWithStorage = Maps.newHashMap();
        try {
            Set<String> resultTableIds = (Set<String>) bkSqlService
                    .convert(BKSQL_PROTOCOL_TABLE_NAMES,
                            new ParsingContext(sql, null));
            String storage = STORAGE_HDFS;
            if (properties != null) {
                storage = properties.getString(PREFER_STORAGE);
            }
            for (String resultTableId : resultTableIds) {
                resultTableWithStorage.put(resultTableId,
                        storage);
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to get the result table storage mapping in onesql",
                    e);
        }
        return resultTableWithStorage;
    }

    /**
     * 是否是简单 SqlSelect 节点
     *
     * @param node SqlSelect 节点
     * @return true:是,false:不是
     */
    private boolean isPlainSelectNode(SqlSelect node) {
        SqlNode from = node.getFrom();
        if (from.getKind() == SqlKind.AS) {
            SqlBasicCall sqlAs = (SqlBasicCall) from;
            SqlNode beforeAs = sqlAs.operand(0);
            if (!(beforeAs instanceof SqlIdentifier)) {
                return false;
            }
        } else {
            if (!(from instanceof SqlIdentifier)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 是否是简单 SqlOrderBy 节点
     *
     * @param node SqlSelect 节点
     * @return true:是,false:不是
     */
    private boolean isPlainOrderByNode(SqlOrderBy node) {
        return node.query instanceof SqlSelect && isPlainSelectNode((SqlSelect) node.query);
    }

    /**
     * 对请求 URL 字符串进行URL编码
     *
     * @param urlPattern 请求URL
     * @return URL编码后的字符串
     */
    private String encodeUrl(String urlPattern) {
        String[] urlParts = urlPattern.split("\\?");
        String encodedParams = UriEncoder.encode(urlParts[1]);
        return new StringBuffer(urlParts[0])
                .append("?")
                .append(encodedParams)
                .toString();
    }

    /**
     * 判断是否是 SqlSelect、SqlOrderBy 或者 SqlBasicCall 节点类型
     *
     * @param node 节点
     * @return true:是，false:不是
     */
    private boolean isSelectOrBasicCall(SqlNode node) {
        return node instanceof SqlSelect || node instanceof SqlOrderBy
                || node instanceof SqlBasicCall;
    }

    /**
     * 判断是否是 SqlSelect 或者 SqlOrderBy 节点类型
     *
     * @param node 节点
     * @return true:是，false:不是
     */
    private boolean isSelect(SqlNode node) {
        return node instanceof SqlSelect || node instanceof SqlOrderBy;
    }
}
