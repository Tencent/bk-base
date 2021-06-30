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

package com.tencent.bk.base.datalab.queryengine.server.base;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import lombok.Data;

/**
 * model 基类
 */
@Data
public class BaseModel implements Serializable {

    private static final long serialVersionUID = 2657584033202261542L;
    /**
     * 记录id 主键
     */
    @JsonProperty(value = "id")
    private int id;

    /**
     * 创建时间
     */
    @JsonProperty(value = "created_at")
    private String createdAt;

    /**
     * 创建人
     */
    @JsonProperty(value = "created_by")
    private String createdBy;

    /**
     * 修改时间
     */
    @JsonProperty(value = "updated_at")
    private String updatedAt;

    /**
     * 修改人
     */
    @JsonProperty(value = "updated_by")
    private String updatedBy;

    /**
     * 备注信息
     */
    @JsonProperty(value = "description")
    private String description;

    /**
     * 记录是否有效 0：失效，1：有效
     */
    @JsonProperty(value = "active")
    private int active;
}
