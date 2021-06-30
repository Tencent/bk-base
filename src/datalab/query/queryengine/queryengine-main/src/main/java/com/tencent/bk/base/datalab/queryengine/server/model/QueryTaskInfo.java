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
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 查询任务概要类
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class QueryTaskInfo extends BaseModel {

    private static final long serialVersionUID = 3303159448375695988L;
    /**
     * sql 脚本
     */
    @JsonProperty("sql_text")
    private String sqlText;

    /**
     * 转换后的 sql 脚本
     */
    @JsonProperty("converted_sql_text")
    private String convertedSqlText;

    /**
     * sql 存储
     */
    @JsonProperty("prefer_storage")
    private String preferStorage;

    /**
     * sql 附加属性
     */
    @JsonProperty("properties")
    private Map<String, Object> properties;

    /**
     * sql 查询作业 id
     */
    @JsonProperty("query_id")
    private String queryId;

    /**
     * sql 查询作业类型
     */
    @JsonProperty("query_method")
    private String queryMethod;

    /**
     * 查询提交时间 精读：毫秒
     */
    @JsonProperty("query_start_time")
    private String queryStartTime;

    /**
     * 查询结束时间 精读：毫秒
     */
    @JsonProperty("query_end_time")
    private String queryEndTime;

    /**
     * 查询耗时 单位：毫秒
     */
    @JsonProperty("cost_time")
    private Integer costTime;

    /**
     * 返回记录数
     */
    @JsonProperty("total_records")
    private Long totalRecords;

    /**
     * 读区公共字典表 sql 任务状态 created：已创建， queued：排队，running：正在执行，finished：执行成功，failed：执行失败，canceled：已取消
     */
    @JsonProperty("query_state")
    private String queryState;

    /**
     * sql 资源评估结果
     */
    @JsonProperty("eval_result")
    private String evalResult;

    /**
     * 路由规则
     */
    @JsonProperty("routing_id")
    private int routingId;
}