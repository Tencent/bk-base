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

import com.google.common.base.Splitter;
import com.tencent.bk.base.dataflow.codecheck.util.Constants;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MethodParserGroup {

    public static Logger logger = LoggerFactory.getLogger(MethodParserGroup.class);

    private static volatile MethodParserGroup singleton;
    private final Map<String, MethodParser> parserMap;

    /**
     * MethodParserGroup
     */
    private MethodParserGroup() {
        parserMap = new HashMap<>();
    }

    /**
     * getSingleton
     *
     * @return com.tencent.bkdata.codecheck.MethodParserGroup
     */
    public static MethodParserGroup getSingleton() {
        if (singleton == null) {
            synchronized (MethodParserGroup.class) {
                if (singleton == null) {
                    singleton = new MethodParserGroup();
                }
            }
        }
        return singleton;
    }

    /**
     * addParserGroup
     *
     * @param groupName
     * @param jarLib
     * @param sourceDir
     */
    public void addParserGroup(String groupName, String jarLib, String sourceDir) throws IOException {
        List<String> jarList = Splitter.on(Constants.DELIMITER).trimResults().splitToList(jarLib);
        addParserGroup(groupName, jarList, sourceDir);
    }

    /**
     * addParserGroup
     *
     * @param key
     * @param jarList
     * @param sourceDir
     */
    private void addParserGroup(String key, List<String> jarList, String sourceDir) throws IOException {
        MethodParser methodParser = new MethodParser(jarList, sourceDir);
        methodParser.init();
        if (parserMap.containsKey(key)) {
            parserMap.get(key).clear();
        }
        parserMap.put(key, methodParser);
    }

    /**
     * getParserGroup
     *
     * @param groupName
     * @return com.tencent.bkdata.codecheck.MethodParser
     */
    public MethodParser getParserGroup(String groupName) {
        return parserMap.get(groupName);
    }

    /**
     * listParserGroup
     *
     * @param groupName
     * @return java.util.Map-java.lang.String, java.util.Map
     */
    public Map<String, Map> listParserGroup(String groupName) {
        Map<String, Map> ret = new HashMap<>();
        if (groupName == null || groupName.isEmpty()) {
            for (Map.Entry<String, MethodParser> entry : parserMap.entrySet()) {
                Map<String, Object> parserInfo = new HashMap<>();
                parserInfo.put("lib_dir", entry.getValue().getAdditionalJars());
                parserInfo.put("source_dir", entry.getValue().getSourceDir());
                ret.put(entry.getKey(), parserInfo);
            }
        } else {
            if (parserMap.containsKey(groupName)) {
                MethodParser methodParser = parserMap.get(groupName);
                Map<String, Object> parserInfo = new HashMap<>();
                parserInfo.put("lib_dir", methodParser.getAdditionalJars());
                parserInfo.put("source_dir", methodParser.getSourceDir());
                ret.put(groupName, parserInfo);
            }
        }
        return ret;
    }

    /**
     * deleteParserGroup
     *
     * @param groupName
     */
    public void deleteParserGroup(String groupName) {
        if (parserMap.containsKey(groupName)) {
            logger.info("delete parser_group {}", groupName);
            MethodParser methodParser = parserMap.remove(groupName);
            methodParser.clear();
            methodParser = null;
        }
    }
}
