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

package com.tencent.bk.base.datalab.bksql.rest.wrapper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class ResponseEntity {

    public final boolean result;
    public final String message;
    public final String code;
    public final Object data;

    @JsonCreator
    public ResponseEntity(@JsonProperty("result") boolean result,
            @JsonProperty("message") String message,
            @JsonProperty("code") String code,
            @JsonProperty("data") Object data) {
        this.result = result;
        this.message = message;
        this.code = code;
        this.data = data;
    }

    private ResponseEntity(Builder builder) {
        this(builder.result, builder.message, builder.code, builder.data);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private boolean result;
        private String message;
        private String code;
        private Object data;

        private Builder() {
        }

        public Builder result(boolean result) {
            this.result = result;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder code(String code) {
            this.code = code;
            return this;
        }

        public Builder data(Object data) {
            this.data = data;
            return this;
        }

        public ResponseEntity create() {
            return new ResponseEntity(this);
        }
    }
}
