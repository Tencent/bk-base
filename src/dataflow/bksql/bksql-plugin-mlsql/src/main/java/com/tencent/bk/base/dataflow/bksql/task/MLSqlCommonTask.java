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

package com.tencent.bk.base.dataflow.bksql.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.ErrorCode;
import org.json.JSONObject;

public class MLSqlCommonTask {

    private boolean result;
    private Object content;
    private String code = ErrorCode.SUCCESS_CODE;

    public MLSqlCommonTask(boolean result, Object content) {
        this.result = result;
        this.content = content;
    }

    public MLSqlCommonTask(boolean result, Object content, String code) {
        this.result = result;
        this.content = content;
        this.code = code;
    }

    @JsonProperty("result")
    public boolean isResult() {
        return result;
    }

    @JsonProperty("content")
    public Object getContent() {
        return this.content;
    }

    @JsonProperty("code")
    public String getCode() {
        return this.code;
    }

    @Override
    public String toString() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            JSONObject convertObject = new JSONObject();
            convertObject.put("result", this.result);
            convertObject.put("content", this.content);
            convertObject.put("code", this.code);
            return convertObject.toString();
        }
    }
}
