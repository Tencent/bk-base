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

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.tencent.bk.base.datalab.bksql.table.BlueKingMetaApiFetcher;
import com.tencent.bk.base.datalab.bksql.table.TableStorageMetaFetcher;
import com.tencent.bk.base.datalab.bksql.util.QuoteUtil;
import com.tencent.bk.base.datalab.bksql.util.StorageConstants;
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import com.tencent.bk.base.datalab.meta.StoragesProperty;
import com.typesafe.config.Config;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.util.UriEncoder;

public class BlueKingTableNameOptimizer extends SimpleListenerWalker {

    public static final String DATA_TYPE_ICEBERG = "iceberg";
    public static final String PRESTO_CATALOG_HIVE = "hive";
    public static final String PRESTO_CATALOG_ICEBERG = "iceberg";
    public static final String PREFER_STORAGE = "prefer_storage";
    public static final String FEDERATION = "federation";
    private static final Pattern NUMERIC_PREFIX_PATTERN = Pattern.compile("(\\d+)_(.+)");
    private static final Joiner DOT_JOINER = Joiner.on(".");
    private final TableStorageMetaFetcher tableStorageMetaFetcher;
    private final String targetType;
    @JacksonInject("properties")
    private Config properties;

    @JsonCreator
    public BlueKingTableNameOptimizer(
            @JsonProperty("urlPattern") String urlPattern,
            @JsonProperty("target") String targetType
    ) {
        super();
        Preconditions.checkNotNull(urlPattern);
        Preconditions.checkNotNull(targetType);
        String[] urlParts = urlPattern.split("\\?");
        String encodedParams = UriEncoder.encode(urlParts[1]);
        String newUrl = new StringBuffer(urlParts[0])
                .append("?")
                .append(encodedParams)
                .toString();
        tableStorageMetaFetcher = BlueKingMetaApiFetcher.forUrl(newUrl);
        this.targetType = targetType;
    }

    @Override
    public void enterSubSelectNode(SqlSelect select) {
        super.enterSubSelectNode(select);
        SqlNode fromItem = select.getFrom();
        extractTableNameNode(fromItem);
    }

    @Override
    public void enterPlainSelectNode(SqlSelect select) {
        super.enterPlainSelectNode(select);
        SqlNode fromItem = select.getFrom();
        extractTableNameNode(fromItem);
    }

    private void extractTableNameNode(SqlNode node) {
        if (node instanceof SqlIdentifier) {
            genPhysicalTableName((SqlIdentifier) node);
        } else if (node instanceof SqlBasicCall) {
            SqlBasicCall fromCall = (SqlBasicCall) node;
            if (fromCall.getOperator() instanceof SqlAsOperator) {
                SqlNode beforeAs = fromCall.operand(0);
                if (beforeAs instanceof SqlIdentifier) {
                    genPhysicalTableName((SqlIdentifier) beforeAs);
                } else {
                    extractTableNameNode(beforeAs);
                }
            }
        }
    }

    @Override
    public void enterJoinNode(SqlJoin join) {
        super.enterJoinNode(join);
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        analysisJoinNode(left);
        analysisJoinNode(right);
    }

    /**
     * 解析Join节点
     *
     * @param joinNode Join节点
     */
    private void analysisJoinNode(SqlNode joinNode) {
        if (joinNode.getKind() == SqlKind.IDENTIFIER) {
            genPhysicalTableName((SqlIdentifier) joinNode);
        } else if (joinNode.getKind() == SqlKind.AS) {
            SqlNode beforeAs = ((SqlBasicCall) joinNode).operand(0);
            if (beforeAs instanceof SqlIdentifier) {
                genPhysicalTableName((SqlIdentifier) beforeAs);
            }
        }
    }

    /**
     * 获取物理表名
     *
     * @param fromItem FROM节点
     */
    private void genPhysicalTableName(SqlIdentifier fromItem) {
        if (fromItem == null || fromItem.names.size() == 0) {
            return;
        }
        TableNode tableNode = new TableNode(fromItem, targetType).invoke();
        Matcher tbMacher = NUMERIC_PREFIX_PATTERN.matcher(tableNode.tableName);
        if (!tbMacher.matches()) {
            return;
        }
        String physical = tableStorageMetaFetcher
                .fetchPhysicalTableName(QuoteUtil.trimQuotes(tableNode.tableName),
                        tableNode.storageType);
        StoragesProperty sp = tableStorageMetaFetcher
                .fetchStorageProperty(QuoteUtil.trimQuotes(tableNode.tableName),
                        tableNode.storageType);
        if (StringUtils.isBlank(physical) || sp == null) {
            return;
        }
        String dataType = sp.getDataType();
        if (StorageConstants.HDFS.equals(StringUtils.trim(tableNode.storageType))) {
            if (!DATA_TYPE_ICEBERG.equalsIgnoreCase(dataType)) {
                String[] paths = physical.split("/");
                physical = paths[paths.length - 1];
            }
        }
        if (FEDERATION.equals(StringUtils.trim(targetType))) {
            physical = getFullPhysicalTbNameWithFed(tableNode, physical, dataType);
        } else {
            physical = getFullPhysicalTbNameDirect(tableNode, physical, dataType);
        }
        fromItem.setNames(Lists.newArrayList(physical), Lists.newArrayList(SqlParserPos.ZERO));
    }

    /**
     * 获取直连查询的物理表全名
     *
     * @param tableNode TableNode实例
     * @param physical 物理表名
     * @param dataType 数据类型
     * @return 物理表全名
     */
    private String getFullPhysicalTbNameDirect(TableNode tableNode, String physical,
            String dataType) {
        if (StorageConstants.HDFS.equals(StringUtils.trim(tableNode.storageType))) {
            physical = getHdfsPhysicalTableName(tableNode, physical, dataType);
        } else if (StorageConstants.ES.equals(StringUtils.trim(tableNode.storageType))) {
            physical = "\"" + physical.toLowerCase() + "\"";
        }
        return physical;
    }

    /**
     * 获取联邦查询的物理表全名
     *
     * @param tableNode TableNode实例
     * @param physical 物理表名
     * @param dataType 数据类型
     * @return 物理表全名
     */
    private String getFullPhysicalTbNameWithFed(TableNode tableNode, String physical,
            String dataType) {
        if (StorageConstants.HDFS.equals(StringUtils.trim(tableNode.storageType))) {
            physical = getHdfsPhysicalTableName(tableNode, physical, dataType);
        } else {
            String clusterName = tableStorageMetaFetcher
                    .fetchClusterName(QuoteUtil.trimQuotes(tableNode.tableName),
                            tableNode.storageType);
            String catalogName = tableNode.storageType + "_" + clusterName;
            physical = DOT_JOINER
                    .join(catalogName, physical);
            physical = shouldAddQuotes(physical);
        }
        return physical;
    }

    private String getHdfsPhysicalTableName(TableNode tableNode, String physical, String dataType) {
        String schemaName;
        if (DATA_TYPE_ICEBERG.equalsIgnoreCase(dataType)) {
            physical = DOT_JOINER.join(PRESTO_CATALOG_ICEBERG, physical);
        } else {
            Matcher matcher = NUMERIC_PREFIX_PATTERN.matcher(tableNode.tableName);
            if (matcher.matches()) {
                schemaName = StorageConstants.SCHEMA_PREFIX + matcher.group(1);
                physical = DOT_JOINER
                        .join(PRESTO_CATALOG_HIVE, schemaName, physical);
                physical = shouldAddQuotes(physical);
            }
        }
        return physical;
    }

    /**
     * 表名是否需要添加双引号
     *
     * @param physical 物理表名
     * @return 修改后的物理表名
     */
    private String shouldAddQuotes(String physical) {
        if (StringUtils.isNotBlank(physical)) {
            String[] splitStr = physical.split("\\.", -1);
            for (int i = 0; i < splitStr.length; i++) {
                if (splitStr[i].contains("-")) {
                    splitStr[i] = new StringBuilder("\"").append(splitStr[i]).append("\"")
                            .toString();
                }
            }
            return DOT_JOINER
                    .join(splitStr);
        }
        return physical;
    }

    private class TableNode {

        private final SqlIdentifier table;
        private final String targetType;
        private String tableName;
        private String storageType;

        private TableNode(SqlIdentifier table, String targetType) {
            this.table = table;
            this.targetType = targetType;
        }

        public String getTableName() {
            return tableName;
        }

        public String getStorageType() {
            return storageType;
        }

        public String getTargetType() {
            return targetType;
        }

        TableNode invoke() {
            if (table.names.size() >= 2) {
                tableName = table.names.get(table.names.size() - 2);
                storageType = table.names.get(table.names.size() - 1);

            } else if (table.names.size() == 1) {
                tableName = table.toString();
                storageType = targetType;
                if (properties != null && properties.hasPath(PREFER_STORAGE)) {
                    String preferStorage = properties.getString(PREFER_STORAGE);
                    if (FEDERATION.equalsIgnoreCase(targetType)) {
                        storageType = preferStorage;
                    }
                }
            }
            return this;
        }
    }
}
