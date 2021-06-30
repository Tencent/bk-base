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

package com.tencent.bk.base.datalab.queryengine.server.wrapper;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.meta.StoragesProperty;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.StatementTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.eval.QueryCost;
import com.tencent.bk.base.datalab.queryengine.server.meta.FieldMeta;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskResultTable;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskStage;
import com.tencent.bk.base.datalab.queryengine.server.querydriver.QueryDriver;
import com.tencent.bk.base.datalab.queryengine.server.third.BatchJob;
import com.tencent.bk.base.datalab.queryengine.server.third.NoteBookOutputDetail;
import com.tencent.bk.base.datalab.queryengine.server.third.NoteBookOutputs;
import com.tencent.bk.base.datalab.queryengine.server.third.StorageState;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.Data;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * 查询的上下文
 */
@Data
public class QueryTaskContext {

    private static final long serialVersionUID = 1285387828325532728L;
    /**
     * 查询任务概要信息
     */
    private QueryTaskInfo queryTaskInfo;

    /**
     * 查询阶段信息集
     */
    private Map<String, QueryTaskStage> queryTaskStageMap;

    /**
     * 查询关联的结果表列表
     */
    private List<QueryTaskResultTable> queryTaskResultTableList;

    /**
     * 合法的存储
     */
    private StoragesProperty pickedValidStorage;

    /**
     * 查询关联的结果表列表
     */
    private List<String> resultTableIdList;

    /**
     * 当前存储集群状态
     */
    private StorageState storageState;

    /**
     * 当前存储集群状态
     */
    private String routingType;

    /**
     * 查询驱动
     */
    @JsonIgnore
    private QueryDriver queryDriver;

    /**
     * 物理查询 sql
     */
    private String realSql;

    /**
     * 返回结果集
     */
    private Object resultData;

    /**
     * 返回字段列表
     */
    private List<String> columnOrder;

    /**
     * 查询返回
     */
    private long totalRecords;

    /**
     * tdw 用户名
     */
    private String tdwUserName;

    /**
     * tdw 密码
     */
    private String tdwPassword;

    /**
     * 上一次关联的存储
     */
    private String previousClusterName;

    /**
     * 查询的存储类型
     */
    private String pickedClusterType;

    /**
     * 查询的存储集群名
     */
    private String pickedClusterName;

    /**
     * 查询语句中结果表关联的存储
     */
    private Map<String, StoragesProperty> pickedStorageMap = Maps.newHashMap();

    /**
     * 数据源类型
     */
    private String queryType = CommonConstants.QUERY_TYPE_DIRECT;

    /**
     * 关系数据投影字段列表
     */
    private List<RelDataTypeField> relFields;

    /**
     * 查询字段列表
     */
    private List<FieldMeta> selectFields;

    /**
     * sqlType 类型 oneSql or nativeSql
     */
    private String sqlType;

    /**
     * create as 语句里的表名
     */
    private String createTableName;

    /**
     * 结果表存储路径
     */
    private String dataPath;

    /**
     * sql 语句类型
     */
    private StatementTypeEnum statementType;

    /**
     * 查询资源评估
     */
    private QueryCost queryCost;

    /**
     * 关联的离线任务信息
     */
    private BatchJob batchJob;

    /**
     * 数据源RT选取的数据时间范围映射
     */
    private Map<String, Map<String, String>> sourceRtRangeMap;

    /**
     * 当前请求的语言环境
     */
    private Locale locale;

    /**
     * iDex 任务相关url
     */
    private String idexTaskUrl;

    /**
     * 笔记产出物
     */
    private NoteBookOutputs noteBookOutputs;

    /**
     * 笔记产出物详情
     */
    private NoteBookOutputDetail noteBookOutputDetail;

    /**
     * 查询 sql 是否是 elastic search dsl
     */
    private boolean isEsDsl;

    /**
     * 用户指定的 prefer_storage 参数
     */
    private String preferStorage;
}
