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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datahub.iceberg.BkTable;
import com.tencent.bk.base.datahub.iceberg.functions.ValFunction;
import com.tencent.bk.base.datalab.bksql.core.BKSqlService;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ParsingContext;
import com.tencent.bk.base.datalab.meta.Field;
import com.tencent.bk.base.datalab.meta.StoragesProperty;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants;
import com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.service.SqlDmlService;
import com.tencent.bk.base.datalab.queryengine.server.third.AuthApiService;
import com.tencent.bk.base.datalab.queryengine.server.third.MetaApiService;
import com.tencent.bk.base.datalab.queryengine.server.util.DataLakeUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.expressions.Expression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class SqlDmlServiceImpl implements SqlDmlService {

    public static final String AFFECTED_RECORDS = "affected.records";
    public static final String ASSIGN = "assign";
    public static final String CONDITION = "condition";
    public static final String FIELDS = "fields";
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    static {
        JSON_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        JSON_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    @Autowired
    private BKSqlService bkSqlService;

    @Autowired
    private MetaApiService metaApiService;

    @Autowired
    private AuthApiService authApiService;

    @Override
    public void deleteTable(QueryTaskContext queryContext) throws Exception {
        String sql = queryContext.getQueryTaskInfo()
                .getSqlText();
        String resultTableId = (String) bkSqlService
                .convert(BkSqlContants.BKSQL_PROTOCOL_DELETE_TABLE_NAME, new ParsingContext(sql, null));
        StoragesProperty hdfsStorageProps = metaApiService.fetchTableStorages(resultTableId)
                .getStorages()
                .getAdditionalProperties()
                .get(StorageConstants.DEVICE_TYPE_HDFS);
        List<Field> fields = metaApiService.fetchTableFields(resultTableId).getFields();
        //1、权限以及数据类型校验
        checkPermissionAndDataType(resultTableId, hdfsStorageProps.getDataType(),
                BkDataConstants.PIZZA_AUTH_ACTION_DELETE_RT);
        //2、解析 delete 语句，获取 where 条件表达式
        Expression deleteExpression = (Expression) bkSqlService
                .convert(BkSqlContants.BKSQL_PROTOCOL_DATALAKE_DELETE, new ParsingContext(sql,
                        ImmutableMap.of(FIELDS, JSON_MAPPER.writeValueAsString(fields))));
        if (deleteExpression == null) {
            throw new QueryDetailException(ResultCodeEnum.PARAM_IS_INVALID);
        }
        //3、构造 BKTable 实例
        BkTable table = DataLakeUtil.getBkTable(resultTableId);
        table.loadTable();
        //4、调用 BKTable.deleteData 进行数据删除
        Map<String, String> resultMap = table.deleteData(deleteExpression).summary();
        if (resultMap != null) {
            queryContext.setTotalRecords(Long.parseLong(resultMap.get(AFFECTED_RECORDS)));
        }
    }

    @Override
    public void updateTable(QueryTaskContext queryContext) throws Exception {
        String sql = queryContext.getQueryTaskInfo()
                .getSqlText();
        String resultTableId = (String) bkSqlService
                .convert(BkSqlContants.BKSQL_PROTOCOL_UPDATE_TABLE_NAME, new ParsingContext(sql, null));
        StoragesProperty hdfsStorageProps = metaApiService.fetchTableStorages(resultTableId)
                .getStorages()
                .getAdditionalProperties()
                .get(StorageConstants.DEVICE_TYPE_HDFS);
        List<Field> fields = metaApiService.fetchTableFields(resultTableId).getFields();
        //1、权限以及数据类型校验
        checkPermissionAndDataType(resultTableId, hdfsStorageProps.getDataType(),
                BkDataConstants.PIZZA_AUTH_ACTION_UPDATE_RT);
        //2、解析 update 语句，获取 set 更新表达式和 where 条件表达式
        Map<String, Object> deParsedResult = (Map<String, Object>) bkSqlService
                .convert(BkSqlContants.BKSQL_PROTOCOL_DATALAKE_UPDATE, new ParsingContext(sql,
                        ImmutableMap.of(FIELDS, JSON_MAPPER.writeValueAsString(fields))));
        Map<String, ValFunction> assign = (Map<String, ValFunction>) deParsedResult.get(ASSIGN);
        Expression condition = (Expression) deParsedResult.get(CONDITION);
        boolean skipUpdate = condition == null || assign == null || assign.size() == 0;
        if (skipUpdate) {
            throw new QueryDetailException(ResultCodeEnum.PARAM_IS_INVALID);
        }
        //3、构造 BKTable 实例
        BkTable table = DataLakeUtil.getBkTable(resultTableId);
        table.loadTable();
        //4、调用 BKTable.updateData 进行数据更新
        Map<String, String> resultMap = table.updateData(condition, assign).summary();
        if (resultMap != null) {
            queryContext.setTotalRecords(Long.parseLong(resultMap.get(AFFECTED_RECORDS)));
        }
    }

    /**
     * 权限以及数据类型校验
     *
     * @param resultTableId 结果表
     * @param dataType 数据类型
     * @param action 操作类型
     * @throws Exception 执行异常
     */
    private void checkPermissionAndDataType(String resultTableId, String dataType,
            String action) throws Exception {
        //1、校验是否有删除权限
        authApiService.checkAuth(new String[]{resultTableId}, action);
        //2、检查dataType是否是iceberg
        if (!StorageConstants.DATA_TYPE_ICEBERG.equals(dataType)) {
            throw new QueryDetailException(ResultCodeEnum.PARAM_IS_INVALID,
                    MessageLocalizedUtil
                            .getMessage(
                                    "结果表 {0} 不属于iceberg类型，不能对其进行数据删除或者更新操作",
                                    new Object[]{resultTableId}));
        }
    }
}
