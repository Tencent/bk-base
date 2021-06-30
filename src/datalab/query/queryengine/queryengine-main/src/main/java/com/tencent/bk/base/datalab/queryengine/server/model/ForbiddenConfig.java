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
 * 查询禁用类
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ForbiddenConfig extends BaseModel {

    /**
     * 禁用的结果表 id
     */
    @JsonProperty(value = "result_table_id")
    private String resultTableId;

    /**
     * 禁用的用户
     */
    @JsonProperty(value = "user_name")
    private String userName;

    /**
     * 禁用的存储类型
     */
    @JsonProperty(value = "storage_type")
    private String storageType;

    /**
     * 禁用的存储集群 id
     */
    @JsonProperty(value = "storage_cluster_config_id")
    private int storageClusterConfigId;
}
