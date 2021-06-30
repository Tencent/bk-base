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

package com.tencent.bk.base.dataflow.codecheck.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public abstract class AbstractHttpHandler extends HttpHandler {

    /**
     * service
     *
     * @param request
     * @param response
     */
    public void service(Request request, Response response) throws Exception {
        Method method = request.getMethod();
        if (Method.GET == method) {
            doGet(request, response);
        } else if (Method.POST == method) {
            doPost(request, response);
        } else {
            throw new UnsupportedOperationException("Http Method " + method.toString() + " not support.");
        }
    }

    /**
     * doGet
     *
     * @param request
     * @param response
     */
    public abstract void doGet(Request request, Response response) throws Exception;

    /**
     * doPost
     *
     * @param request
     * @param response
     */
    public abstract void doPost(Request request, Response response) throws Exception;

    /**
     * writeData
     *
     * @param data
     * @param response
     */
    public void writeData(String data, Response response) throws IOException {
        response.setContentType("text/plain");
        response.setContentLength(data.getBytes(StandardCharsets.UTF_8).length);
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write(data);
    }

    /**
     * writeData
     *
     * @param success
     * @param message
     * @param response
     */
    public void writeData(boolean success, String message, Response response) throws IOException {
        writeData(success, message, HttpReturnCode.DEFAULT, "", response);
    }

    /**
     * writeData
     *
     * @param success
     * @param message
     * @param code
     * @param response
     */
    public void writeData(boolean success, String message, String code, Response response) throws IOException {
        writeData(success, message, code, "", response);
    }

    /**
     * writeData
     *
     * @param success
     * @param message
     * @param code
     * @param data
     * @param response
     */
    public void writeData(boolean success, String message, String code, String data, Response response)
            throws IOException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("result", success);
        result.put("message", message);
        result.put("code", code);
        result.put("data", data);
        writeData(JsonUtils.writeValueAsString(result), response);
    }

    /**
     * writeData
     *
     * @param success
     * @param message
     * @param code
     * @param data
     * @param response
     */
    public void writeData(boolean success, String message, String code, Object data, Response response)
            throws IOException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("result", success);
        result.put("message", message);
        result.put("code", code);
        result.put("data", data);
        writeData(JsonUtils.writeValueAsString(result), response);
    }
}

