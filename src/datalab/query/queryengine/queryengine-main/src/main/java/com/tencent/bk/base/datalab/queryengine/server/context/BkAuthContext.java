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

package com.tencent.bk.base.datalab.queryengine.server.context;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

/**
 * 蓝鲸基础计算平台认证授权信息
 */
@Builder
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BkAuthContext {

    private static final long serialVersionUID = 2597074948281385820L;

    /**
     * 蓝鲸基础计算平台应用编码
     */
    @JsonProperty(value = "app_code")
    private String appCode;

    /**
     * 蓝鲸基础计算平台应用私密 key
     */
    @JsonProperty(value = "app_secret")
    private String appSecret;

    /**
     * 蓝鲸基础计算平台应用编码
     */
    @JsonProperty(value = "bk_app_code")
    private String bkAppCode;

    /**
     * 蓝鲸基础计算平台应用私密 key
     */
    @JsonProperty(value = "bk_app_secret")
    private String bkAppSecret;

    /**
     * 蓝鲸基础计算平台认证方式
     */
    @JsonProperty(value = "bkdata_authentication_method")
    private String bkDataAuthenticationMethod;

    /**
     * 蓝鲸基础计算平台用户名
     */
    @JsonProperty(value = "bk_username")
    private String bkUserName;

    /**
     * 蓝鲸基础计算平台生成的授权码
     */
    @JsonProperty(value = "bkdata_data_token")
    private String bkDataDataToken;
}
