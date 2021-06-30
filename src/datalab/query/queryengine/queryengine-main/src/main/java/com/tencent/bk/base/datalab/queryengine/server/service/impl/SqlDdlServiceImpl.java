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

package com.tencent.bk.base.datalab.queryengine.server.service.impl;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.tencent.bk.base.datalab.bksql.core.BKSqlService;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ParsingContext;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants;
import com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants;
import com.tencent.bk.base.datalab.queryengine.server.dto.ResultTable;
import com.tencent.bk.base.datalab.queryengine.server.dto.ResultTableProperty;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.ParameterInvalidException;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.meta.FieldMeta;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.service.SqlDdlService;
import com.tencent.bk.base.datalab.queryengine.server.third.DataLabApiService;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.SpringBeanUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class SqlDdlServiceImpl implements SqlDdlService {

    public static final String CREATE_TABLE_NAME = "create_table_name";
    public static final String COLUMN_SCHEMA = "column_schema";
    private static final String INTEGER_TYPE = "integer";
    private static final String INT_TYPE = "int";
    @Autowired
    private BKSqlService bkSqlService;

    @Override
    public void createResultTable(QueryTaskContext queryContext) throws Exception {
        final QueryTaskInfo qt = queryContext.getQueryTaskInfo();
        final Map<String, Object> queryProp = qt.getProperties();
        final String noteBookId = (String) queryProp.get(ThirdApiConstants.NOTEBOOK_ID);
        final String cellId = (String) queryProp.get(ThirdApiConstants.CELL_ID);
        final String sql = qt.getSqlText();
        registerResultTable(queryContext, noteBookId, cellId, sql, qt.getCreatedBy());
    }

    /**
     * 结果表注册
     *
     * @param queryTaskContext 查询上下文
     * @param noteBookId 笔记 Id
     * @param cellId 笔记单元 Id
     * @param sql sql 语句
     * @param userName 用户名
     * @throws Exception 运行时异常
     */
    private void registerResultTable(QueryTaskContext queryTaskContext, String noteBookId,
            String cellId, String sql, String userName)
            throws Exception {
        if (StringUtils.isAnyBlank(noteBookId, cellId)) {
            throw new ParameterInvalidException(ResultCodeEnum.PARAM_NOT_COMPLETE,
                    "notebook_id or cell_id can not be null");
        }
        Map<String, Object> result = (Map<String, Object>) bkSqlService
                .convert(BkSqlContants.BKSQL_PROTOCOL_CREATE_TABLE, new ParsingContext(sql, null));
        final String resultTableName = (String) result.get(CREATE_TABLE_NAME);
        final Map<String, String> columnSchema = (Map<String, String>) result.get(COLUMN_SCHEMA);
        ResultTableProperty rtProperty = getResultTableProperty(userName, resultTableName,
                columnSchema);
        DataLabApiService dataLabApiService = SpringBeanUtil
                .getBean(DataLabApiService.class);
        boolean regResult = dataLabApiService.registerResultTable(noteBookId, cellId, rtProperty);
        if (!regResult) {
            log.error("Failed to register resultTableId:{}", resultTableName);
            throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR,
                    MessageLocalizedUtil.getMessage("结果表 {0} 注册失败，请检查输入参数是否正确或结果表是否已注册",
                            new Object[]{resultTableName}));
        }
        queryTaskContext.setCreateTableName(resultTableName);
    }

    /**
     * 构建 ResultTableProperty
     *
     * @param userName 用户名
     * @param resultTableName 结果表名
     * @param columnSchema 结果表 Schema
     * @return ResultTableProperty 实例
     */
    private ResultTableProperty getResultTableProperty(String userName, String resultTableName,
            Map<String, String> columnSchema) {
        ResultTable resultTable = new ResultTable();
        resultTable.setResultTableId(resultTableName);
        resultTable.setFields(getFields(columnSchema));
        //构建ResultTableProperty
        ResultTableProperty rtProperty = new ResultTableProperty();
        rtProperty.setUserName(userName);
        rtProperty.setAuthMethod(BkDataConstants.AUTH_METHOD_USERNAME);
        rtProperty.setResultTableList(ImmutableList.of(resultTable));
        return rtProperty;
    }

    /**
     * 将表结构定义从 map 转换成 list
     *
     * @param columnSchema 原始表结构定义
     * @return 转换成 list 后的表结构定义
     */
    private List<FieldMeta> getFields(Map<String, String> columnSchema) {
        Preconditions.checkArgument(columnSchema != null);
        int index = 0;
        List<FieldMeta> fieldMetaList = Lists.newArrayList();
        FieldMeta fieldMeta;
        for (Map.Entry<String, String> columnDef : columnSchema.entrySet()) {
            String type = columnDef.getValue();
            if (INTEGER_TYPE.equalsIgnoreCase(type)) {
                type = INT_TYPE;
            }
            fieldMeta = FieldMeta.builder()
                    .fieldName(columnDef.getKey())
                    .fieldType(StringUtils.lowerCase(type))
                    .fieldAlias(columnDef.getKey())
                    .fieldIndex(index)
                    .build();
            fieldMetaList.add(fieldMeta);
            index++;
        }
        return fieldMetaList;
    }
}
