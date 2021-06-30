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

package com.tencent.bk.base.datalab.queryengine.server.util;

import java.io.Serializable;

public class ErrorMessageItem implements Serializable {

    private static final long serialVersionUID = -5749407468855859302L;
    /**
     * 查询异常状态码
     */
    private String code;

    /**
     * 异常信息简介
     */
    private String message;

    /**
     * 异常信息详情
     */
    private String info;

    public ErrorMessageItem(String code, String message, String info) {
        this.code = code;
        this.message = MessageLocalizedUtil.getMessage(message);
        this.info = MessageLocalizedUtil.getMessage(info);
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return MessageLocalizedUtil.getMessage(message);
    }

    public void setMessage(String message) {
        this.message = MessageLocalizedUtil.getMessage(message);
    }

    public String getInfo() {
        return MessageLocalizedUtil.getMessage(info);
    }

    public void setInfo(String info) {
        this.info = MessageLocalizedUtil.getMessage(info);
    }
}
