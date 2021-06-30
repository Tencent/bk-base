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
 * 查询任务关联的rt快照
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class QueryTaskResultTable extends BaseModel {

    private static final long serialVersionUID = -8001232357738025239L;
    /**
     * 查询任务 id
     */
    @JsonProperty("query_id")
    private String queryId;

    /**
     * 结果表 id
     */
    @JsonProperty("result_table_id")
    private String resultTableId;

    /**
     * 结果表所在存储集群 id
     */
    @JsonProperty("storage_cluster_config_id")
    private Integer storageClusterConfigId;

    /**
     * 存储表名，实际存储的表名
     */
    @JsonProperty("physical_table_name")
    private String physicalTableName;

    /**
     * 集群名称
     */
    @JsonProperty("cluster_name")
    private String clusterName;

    /**
     * 存储类型
     */
    @JsonProperty("cluster_type")
    private String clusterType;

    /**
     * 集群组
     */
    @JsonProperty("cluster_group")
    private String clusterGroup;

    /**
     * 存储集群连接信息
     */
    @JsonProperty("connection_info")
    private String connectionInfo;

    /**
     * 优先级
     */
    @JsonProperty("priority")
    private Integer priority;

    /**
     * 集群版本
     */
    @JsonProperty("version")
    private String version;

    /**
     * 标记这个集群是系统的 bkdata 还是 other
     */
    @JsonProperty("belongs_to")
    private String belongsTo;

    /**
     * 配置信息，保留 json 化的配置
     */
    @JsonProperty("storage_config")
    private String storageConfig;
}