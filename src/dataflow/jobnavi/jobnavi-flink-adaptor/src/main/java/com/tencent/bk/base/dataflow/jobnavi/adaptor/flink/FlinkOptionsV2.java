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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.flink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;

public class FlinkOptionsV2 extends FlinkOptions {

    private static final Logger logger = Logger.getLogger(FlinkOptionsV2.class);

    protected Option code;
    protected Option userMainClass;

    FlinkOptionsV2(String paramJson, Configuration config, String typeName) throws IOException {
        super(paramJson, config, typeName);
    }

    @Override
    protected void initOpts() {
        super.initOpts();

        userMainClass = new Option("user_main_class", null);
        opts.put(userMainClass.getName(), userMainClass);

        code = new Option("code", null);
        opts.put(code.getName(), code);
    }

    public Option getUserMainClass() {
        return userMainClass;
    }

    public Option getCode() {
        return code;
    }

    @Override
    protected String[] toFinkArgs(Map<String, String> jobNameAndIdMaps, boolean useSavepoint) throws IOException {
        // 先取消jarFileName option 值，交给 toFlinkArgsForV2 处理。
        // V2 版本支持合并包（用户在线写代码）处理
        Object jarFileValue = this.jarFileName.getValue();
        this.jarFileName.setValue(null);

        try {
            String[] args = super.toFinkArgs(jobNameAndIdMaps, useSavepoint);
            return toFlinkArgsForV2(jarFileValue, args);
        } finally {
            // 恢复回去
            this.jarFileName.setValue(jarFileValue);
        }

    }

    private String[] toFlinkArgsForV2(Object jarFileValue, String[] args) {
        List<String> argsArray = new ArrayList<>();
        String jarFilePath = null;
        if (jarFileValue != null) {
            String path = System.getProperty("JOBNAVI_HOME");
            jarFilePath = path + "/adaptor/" + typeName + "/job/" + jarFileValue.toString();
        }
        if (null != userMainClass.getValue() && null != code.getValue()) {
            // 前面已经打包了，这里用于获取包的具体路径
            String job = jobName.getValue().toString();
            logger.info("v1 jar file : " + jarFilePath);
            jarFilePath = System.getProperty("JOBNAVI_HOME")
                    + "/adaptor/" + typeName + "/opt/" + job + "/" + job + ".jar";
            logger.info("reset jar file for v2 : " + jarFilePath);
        }

        if (null != jarFilePath) {
            argsArray.add("-" + jarFileName.getFlinkOpt());
            argsArray.add(jarFilePath);
        }

        Collections.addAll(argsArray, args);
        logger.info("FlinkOptionsV2:");
        return argsArray.toArray(new String[argsArray.size()]);
    }
}
