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

package com.tencent.bk.base.datalab.queryengine.common.configure;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtil {

    private static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

    /**
     * 加载配置文件
     *
     * @param mappingFile 配置文件路径
     * @return 配置Map实例
     */
    public static Map<String, String> loadProperties(String mappingFile) {
        Map<String, String> propMap = null;
        FileInputStream fio = null;
        Properties properties;
        if (StringUtils.isNotBlank(mappingFile)) {
            try {
                fio = new FileInputStream(mappingFile);
                properties = new Properties();
                properties.load(fio);
                propMap = new HashMap<>(16);
                for (Map.Entry kv : properties.entrySet()) {
                    propMap.put((String) kv.getKey(), (String) kv.getValue());
                }
            } catch (FileNotFoundException e) {
                logger.error("mappingFile:{} is not exist! e:{}", mappingFile,
                        ExceptionUtils.getStackTrace(e));
            } catch (IOException e) {
                logger.error("mappingFile:{} init failed!  e:{}", mappingFile,
                        ExceptionUtils.getStackTrace(e));
            } catch (Exception e) {
                logger.error("mappingFile:{} init failed!  e:{}", mappingFile,
                        ExceptionUtils.getStackTrace(e));
            } finally {
                if (fio != null) {
                    try {
                        fio.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            logger.warn("mappingFile is null or empty!");

        }
        return propMap;
    }
}
