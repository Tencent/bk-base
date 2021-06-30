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

package com.tencent.bk.base.dataflow.codecheck;

public class MethodParserResult {

    private String fileName;
    private String methodName;
    private int lineNo;
    private String message;
    private String parentInvoker;

    /**
     * MethodParserResult
     *
     * @param fileName
     * @param methodName
     * @param lineNo
     */
    public MethodParserResult(String fileName, String methodName, int lineNo) {
        this.fileName = fileName;
        this.methodName = methodName;
        this.lineNo = lineNo;
        this.message = "";
        this.parentInvoker = "";
    }

    /**
     * MethodParserResult
     *
     * @param fileName
     * @param methodName
     * @param lineNo
     * @param message
     */
    public MethodParserResult(String fileName, String methodName, int lineNo, String message) {
        this.fileName = fileName;
        this.methodName = methodName;
        this.lineNo = lineNo;
        this.message = message;
        this.parentInvoker = "";
    }

    /**
     * MethodParserResult
     *
     * @param fileName
     * @param methodName
     * @param lineNo
     * @param message
     * @param parentInvoker
     */
    public MethodParserResult(String fileName, String methodName, int lineNo, String message, String parentInvoker) {
        this.fileName = fileName;
        this.methodName = methodName;
        this.lineNo = lineNo;
        this.message = message;
        this.parentInvoker = parentInvoker;
    }

    /**
     * getFileName
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * setFileName
     *
     * @param fileName
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * getMethodName
     */
    public String getMethodName() {
        return methodName;
    }

    /**
     * setMethodName
     *
     * @param methodName
     */
    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    /**
     * getLineNo
     */
    public int getLineNo() {
        return lineNo;
    }

    /**
     * setLineNo
     *
     * @param lineNo
     */
    public void setLineNo(int lineNo) {
        this.lineNo = lineNo;
    }

    /**
     * getMessage
     */
    public String getMessage() {
        return message;
    }

    /**
     * setMessage
     *
     * @param message
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * getParentInvoker
     */
    public String getParentInvoker() {
        return parentInvoker;
    }

    /**
     * setParentInvoker
     *
     * @param parentInvoker
     */
    public void setParentInvoker(String parentInvoker) {
        this.parentInvoker = parentInvoker;
    }

    /**
     * toString
     */
    @Override
    public String toString() {
        return "MethodParserResult{"
                + "fileName='" + fileName + '\''
                + ", methodName='" + methodName + '\''
                + ", lineNo=" + lineNo
                + ", message='" + message + '\''
                + ", parentInvoker='" + parentInvoker + '\''
                + '}';
    }
}
