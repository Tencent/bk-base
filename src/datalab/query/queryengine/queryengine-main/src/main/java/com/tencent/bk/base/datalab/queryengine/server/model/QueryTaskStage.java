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
 * 查询任务阶段类
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class QueryTaskStage extends BaseModel {

    private static final long serialVersionUID = -5028528554932616736L;
    /**
     * 查询任务 id
     */
    @JsonProperty("query_id")
    private String queryId;

    /**
     * 查询阶段序号
     */
    @JsonProperty("stage_seq")
    private int stageSeq;

    /**
     * 查询阶段类型 checkauth：权限校验，checkrt：rt 校验， sqlparse：sql 解析， sqlestimate：sql 评估， sqlrouting：sql 路由，
     * sqlexec：sql 执行
     */
    @JsonProperty("stage_type")
    private String stageType;

    /**
     * 提交时间
     */
    @JsonProperty("stage_start_time")
    private String stageStartTime;

    /**
     * 结束时间
     */
    @JsonProperty("stage_end_time")
    private String stageEndTime;

    /**
     * 耗时 单位：毫秒
     */
    @JsonProperty("stage_cost_time")
    private int stageCostTime;

    /**
     * stage状态 created：准备中，runing：运行中，finished：成功，failed：失败
     */
    @JsonProperty("stage_status")
    private String stageStatus;

    /**
     * 异常信息
     */
    @JsonProperty("error_message")
    private String errorMessage;
}
