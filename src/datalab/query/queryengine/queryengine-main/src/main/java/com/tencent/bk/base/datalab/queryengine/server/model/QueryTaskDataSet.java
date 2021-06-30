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

package com.tencent.bk.base.datalab.queryengine.server.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.datalab.queryengine.server.base.BaseModel;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 查询结果集存储表
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class QueryTaskDataSet extends BaseModel {

    /**
     * sql查询作业 id
     */
    @JsonProperty(value = "query_id")
    private String queryId;

    /**
     * 结果集所在集群名称
     */
    @JsonProperty(value = "cluster_name")
    private String clusterName = "";

    /**
     * 结果集所在存储类型
     */
    @JsonProperty(value = "cluster_type")
    private String clusterType = "";

    /**
     * 结果集相关属性，json 格式
     */
    @JsonProperty(value = "connection_info")
    private String connectionInfo;

    /**
     * 结果集 schema，json 格式
     */
    @JsonProperty(value = "schema_info")
    private String schemaInfo;

    /**
     * 结果集条数
     */
    @JsonProperty(value = "total_records")
    private Long totalRecords;

    /**
     * 同步到元数据的结果表名
     */
    @JsonProperty(value = "result_table_id")
    private String resultTableId;

    /**
     * 结果表生成类型
     */
    @JsonProperty(value = "result_table_generate_type")
    private String resultTableGenerateType;
}
