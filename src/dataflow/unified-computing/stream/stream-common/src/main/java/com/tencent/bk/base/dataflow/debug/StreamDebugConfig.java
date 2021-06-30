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

package com.tencent.bk.base.dataflow.debug;

import java.io.Serializable;

public class StreamDebugConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String debugId;
    private String debugNodeMetricApi;
    private String debugResultDataApi;
    private String debugErrorDataApi;

    public String getDebugId() {
        return debugId;
    }

    public void setDebugId(String debugId) {
        this.debugId = debugId;
    }

    public String getDebugNodeMetricApi() {
        return debugNodeMetricApi;
    }

    public void setDebugNodeMetricApi(String debugNodeMetricApi) {
        this.debugNodeMetricApi = debugNodeMetricApi;
    }

    public String getDebugResultDataApi() {
        return debugResultDataApi;
    }

    public void setDebugResultDataApi(String debugResultDataApi) {
        this.debugResultDataApi = debugResultDataApi;
    }

    public String getDebugErrorDataApi() {
        return debugErrorDataApi;
    }

    public void setDebugErrorDataApi(String debugErrorDataApi) {
        this.debugErrorDataApi = debugErrorDataApi;
    }
}
