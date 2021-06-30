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

package com.tencent.bk.base.dataflow.udf.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import jep.Jep;
import jep.JepConfig;
import jep.JepException;
import jep.MainInterpreter;
import jep.PyConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JepSingleton {

    private static final Logger LOGGER = LoggerFactory.getLogger(JepSingleton.class);

    private static transient ThreadLocal<Jep> jepThreadLocal = new ThreadLocal<Jep>() {
        @Override
        protected Jep initialValue() {
            return null;
        }
    };

    private static transient ThreadLocal<Map<String, Boolean>>
            flagThreadLocal = new ThreadLocal<Map<String, Boolean>>() {
        @Override
        protected Map<String, Boolean> initialValue() {
            return new HashMap<>();
        }
    };

    private static transient boolean tag = false;

    private static class HolderClass {

        private static final JepSingleton instance = new JepSingleton();
    }

    public static void setFlag(String functionName, boolean flag) {
        flagThreadLocal.get().put(functionName, flag);
    }

    public static Boolean getFlag(String functionName) {
        if (flagThreadLocal.get().containsKey(functionName)) {
            return flagThreadLocal.get().get(functionName);
        } else {
            return false;
        }
    }

    /**
     * 获取jep对象
     *
     * @param pythonHome python home
     * @param pythonScriptPath python script path
     * @return jep单例
     */
    public static JepSingleton getInstance(String pythonHome, String pythonScriptPath, String cpythonPackages) {
        if (!tag) {
            try {
                MainInterpreter.setInitParams(new PyConfig().setPythonHome(pythonHome));
            } catch (JepException e) {
                LOGGER.warn("Set init params failed.", e);
            }
            tag = true;
        }
        if (jepThreadLocal.get() == null) {
            LOGGER.info("Init jep instance in " + Thread.currentThread().getId());
            JepConfig jepConfig = new JepConfig();
            jepConfig.addIncludePaths(pythonScriptPath);
            if (StringUtils.isNotBlank(cpythonPackages)) {
                Arrays.stream(cpythonPackages.split(","))
                        .forEach(jepConfig::addSharedModules);
            }
            try {
                jepThreadLocal.set(new Jep(jepConfig));
            } catch (JepException e) {
                e.printStackTrace();
                throw new RuntimeException("Create jep failed.", e);
            }
        }
        return HolderClass.instance;
    }

    public Jep getJep() {
        return jepThreadLocal.get();
    }

    public static void close() {
        if (jepThreadLocal.get() != null) {
            try {
                jepThreadLocal.get().close();
            } catch (JepException e) {
                LOGGER.warn("Failed to close jep.", e);
            }
        }
    }
}
