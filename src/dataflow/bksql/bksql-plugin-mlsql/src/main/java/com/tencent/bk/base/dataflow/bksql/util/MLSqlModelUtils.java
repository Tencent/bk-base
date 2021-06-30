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

package com.tencent.bk.base.dataflow.bksql.util;

import com.mashape.unirest.http.Unirest;
import com.tencent.bk.base.dataflow.bksql.deparser.MLSqlDeParser;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.ErrorCode;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.MLSqlLocalizedException;
import com.tencent.bk.base.dataflow.bksql.mlsql.model.MLSqlModel;
import com.tencent.bk.base.dataflow.bksql.mlsql.model.MLSqlModelStorage;
import com.tencent.bk.base.dataflow.bksql.mlsql.parser.RelParser;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

public class MLSqlModelUtils {

    private static String metadataUrl;

    public static void initMetadataUrl(String modelMetaUrl) {
        if (StringUtils.isBlank(modelMetaUrl)) {
            throw new MLSqlLocalizedException("miss.config.error",
                    new Object[]{"modelMetaUrlPattern"},
                    MLSqlDeParser.class,
                    ErrorCode.MISS_CONFIG_ERROR);
        }
        if (StringUtils.isBlank(metadataUrl)) {
            metadataUrl = modelMetaUrl;
        }
    }

    /**
     * 根据模型名称获取模型的具体信息
     *
     * @param modelName 模型名称
     * @return 模型对象
     */
    public static MLSqlModel getMLSqlModel(String modelName) {
        //使用元数据得到模型的详情
        try {
            JSONObject object = Unirest.get(metadataUrl + modelName + "/").asJson().getBody().getObject();
            if (object.getBoolean("result")) {
                JSONObject modelObject = object.getJSONObject("data");
                if (modelObject.length() < 1) {
                    return null;
                } else {
                    MLSqlModel model = new MLSqlModel();
                    model.setAlgorithmName(modelObject.getString("algorithm_name"));
                    model.setModelName(modelObject.getString("model_name"));
                    if (modelObject.has("storage")) {
                        MLSqlModelStorage storage = new MLSqlModelStorage();
                        storage.setPath(modelObject.getJSONObject("storage").getString("path"));
                        model.setModelStorage(storage);
                    }
                    return model;
                }
            } else {
                return null;
            }
        } catch (Throwable e) {
            return null;
        }
    }

    /**
     * 输出表名称以参数的形式嵌入至select参数列表内，这里解析出此表名
     *
     * @param select select语句对象
     * @return select列表内create_target_table对应的值
     */
    public static String parseTargetTable(SqlSelect select) {
        SqlNodeList selectList = select.getSelectList();
        for (SqlNode node : selectList) {
            if (!(node instanceof SqlCall)) {
                continue;
            } else {
                SqlCall call = (SqlCall) node;
                if (call.getOperator() != SqlStdOperatorTable.CONCAT) {
                    continue;
                } else {
                    SqlNode keyNode = call.getOperandList().get(1);
                    SqlNode valueNode = call.getOperandList().get(0);
                    if (!(keyNode instanceof SqlLiteral)) {
                        continue;
                    } else {
                        SqlLiteral keyLiteral = (SqlLiteral) keyNode;
                        if (RelParser.MLSQL_CREATE_TARGET_TABLE.equalsIgnoreCase(keyLiteral.getValueAs(String.class))) {
                            SqlLiteral valueLiteral = (SqlLiteral) valueNode;
                            return valueLiteral.getValueAs(String.class);
                        }
                    }
                }
            }
        }
        return null;
    }

    public static void parseConcatNode(SqlNode node, List<SqlCall> concatNodeList) {
        if (!(node instanceof SqlCall)) {
            return;
        }
        SqlCall call = (SqlCall) node;
        if (call.getOperator() == SqlStdOperatorTable.CONCAT) {
            concatNodeList.add(call);
        } else {
            List<SqlNode> basicNodeList = call.getOperandList();
            for (SqlNode subNode : basicNodeList) {
                parseConcatNode(subNode, concatNodeList);
            }
        }

    }

    /**
     * 从SqlNode中解析出具体的模型名称
     *
     * @param sqlNode MLSQL语句对应的SqlNode
     * @return 模型名称
     */
    public static String parseModel(SqlNode sqlNode) {
        SqlSelect select = (SqlSelect) sqlNode;
        SqlNode fromNode = select.getFrom();
        List<SqlNode> nodeList;
        if (fromNode instanceof SqlJoin) {
            // lateral
            SqlJoin join = (SqlJoin) select.getFrom();
            SqlCall as = (SqlCall) join.getRight();
            SqlCall lateral = (SqlCall) as.getOperandList().get(0);
            SqlCall lateralFunc = (SqlCall) lateral.getOperandList().get(0);
            SqlBasicCall call2 = (SqlBasicCall) lateralFunc.getOperandList().get(0);
            nodeList = call2.getOperandList();
        } else {
            nodeList = select.getSelectList().getList();
        }
        for (SqlNode node : nodeList) {
            List<SqlCall> concatNodeList = new ArrayList<>();
            parseConcatNode(node, concatNodeList);
            for (SqlCall call : concatNodeList) {
                List<SqlNode> basicNodeList = call.getOperandList();
                SqlNode keyNode = basicNodeList.get(1);
                SqlNode valueNode = basicNodeList.get(0);
                if (!(keyNode instanceof SqlLiteral)) {
                    continue;
                } else {
                    SqlLiteral keyLiteral = (SqlLiteral) keyNode;
                    if (RelParser.MLSQL_MODEL_NAME.equalsIgnoreCase(keyLiteral.getValueAs(String.class))) {
                        SqlLiteral valueLiteral = (SqlLiteral) valueNode;
                        return valueLiteral.getValueAs(String.class);
                    }
                }
            }
        }
        return null;
    }
}
