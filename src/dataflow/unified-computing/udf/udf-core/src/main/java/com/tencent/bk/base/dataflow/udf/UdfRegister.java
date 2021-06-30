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

package com.tencent.bk.base.dataflow.udf;

import com.google.common.base.CaseFormat;

public class UdfRegister {

    private static final String udfClassPath = "com.tencent.bk.base.dataflow.udf.codegen.";

    /**
     * Get an instance of a generated user-defined function.Registration for flink user-defined function.
     *
     * @param role stream or batch
     * @param udfName user defined function's name
     * @param language java or python
     * @return instance of a generated user-defined function
     */
    public static Object getUdfInstance(String role, String udfName, String language) {
        String className = getUdfClassName(role, udfName, language);
        try {
            Class clazz = Class.forName(className);
            return clazz.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Not found class " + className, e);
        }
    }

    /**
     * Get a generated user-defined function's class name.
     * Registration for
     *
     * @param role stream or batch
     * @param udfName user defined function's name
     * @param language java or python
     * @return a generated user-defined function's class name
     */
    public static String getUdfClassName(String role, String udfName, String language) {
        String fileName = udfFileNameGenerator(role, udfName, language);
        return udfClassPath + fileName;
    }

    private static String udfFileNameGenerator(String role, String udfName, String language) {
        String rolePrefix;
        String pathPreFix;
        switch (role) {
            case "stream":
                rolePrefix = "Flink";
                pathPreFix = "flink";
                break;
            case "batch":
                rolePrefix = "Hive";
                pathPreFix = "hive";
                break;
            default:
                throw new RuntimeException("Not support role " + role);
        }
        String lanPrefix;
        switch (language) {
            case "java":
                lanPrefix = "Java";
                break;
            case "python":
                lanPrefix = "Py";
                break;
            default:
                throw new RuntimeException("Not support language " + language);
        }
        String standardizedName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, udfName);
        return pathPreFix + "." + rolePrefix + lanPrefix + standardizedName;
    }
}
