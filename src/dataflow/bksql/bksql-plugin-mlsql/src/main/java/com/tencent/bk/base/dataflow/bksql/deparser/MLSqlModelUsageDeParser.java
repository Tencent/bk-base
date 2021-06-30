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

package com.tencent.bk.base.dataflow.bksql.deparser;

import com.tencent.bk.base.dataflow.bksql.task.MLSqlModelUsageTask;
import com.tencent.bk.base.datalab.bksql.deparser.DeParser;
import com.tencent.bk.base.dataflow.bksql.rest.exception.MessageLocalizedExceptionV1;
import com.tencent.bk.base.dataflow.bksql.mlsql.parser.RelParser;
import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateTableFromModel;
import org.apache.calcite.sql.SqlDropModels;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTrainModel;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLSqlModelUsageDeParser implements DeParser {

    private static final Logger logger = LoggerFactory.getLogger(MLSqlModelUsageDeParser.class);

    @Override
    public Object deParse(SqlNode sqlNode) {
        try {
            Resources.setThreadLocale(LocaleHolder.instance().get());
            return deParseV1(sqlNode);
        } catch (MessageLocalizedExceptionV1 e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            Resources.setThreadLocale(null);
        }
    }


    /**
     * 处理SelectList
     *
     * @param selectList select语句中的列表
     * @param object 用于存储解析结果的存储
     */
    public void dealWithSelectList(SqlNodeList selectList, JSONObject object) {
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
                        SqlLiteral valueLiteral = (SqlLiteral) valueNode;
                        if (RelParser.MLSQL_MODEL_NAME.equalsIgnoreCase(keyLiteral.getValueAs(String.class))) {
                            object.put("operate", "train");
                            object.put("type", "model");
                            object.put("entity", valueLiteral.getValueAs(String.class));
                            break;
                        } else if (RelParser.MLSQL_CREATE_TARGET_TABLE
                                .equalsIgnoreCase(keyLiteral.getValueAs(String.class))) {
                            object.put("operate", "run");
                            object.put("type", "table");
                            object.put("entity", valueLiteral.getValueAs(String.class));
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * 解析SqlNode
     *
     * @param sqlNode 经分析生成sqlNode对象
     * @return 解析结果
     */
    public Object deParseV1(SqlNode sqlNode) throws Exception {
        JSONObject object = new JSONObject();
        switch (sqlNode.getKind()) {
            case DROP_TABLE:
                SqlDropModels drop = (SqlDropModels) sqlNode;
                object.put("operate", "drop");
                if (drop.isDropModels()) {
                    object.put("type", "model");
                } else {
                    object.put("type", "table");
                }
                object.put("entity", drop.getName().getSimple());
                break;
            case TRAIN:
                SqlTrainModel train = (SqlTrainModel) sqlNode;
                object.put("operate", "train");
                object.put("type", "model");
                object.put("entity", train.getModelName().getSimple());
                break;
            case SELECT:
            case CREATE_TABLE:
                // create
                SqlSelect select;
                if (sqlNode instanceof SqlCreateTableFromModel) {
                    select = ((SqlCreateTableFromModel) sqlNode).getQuery();
                } else {
                    select = (SqlSelect) sqlNode;
                }

                SqlNodeList selectList = select.getSelectList();
                dealWithSelectList(selectList, object);
                break;
            default:
                // do nothing
                logger.info("don not need to deal ...");
        }
        return new MLSqlModelUsageTask(object.getString("operate"), object.getString("type"),
                object.getString("entity"));
    }
}
