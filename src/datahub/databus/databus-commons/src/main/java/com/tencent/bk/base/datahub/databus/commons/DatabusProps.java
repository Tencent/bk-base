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

package com.tencent.bk.base.datahub.databus.commons;

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DatabusProps {

    private static final Logger log = LoggerFactory.getLogger(DatabusProps.class);

    private Properties props = new Properties();

    /**
     * 构造函数（***请勿直接使用，开放构造函数访问权限仅用于测试用***）
     */
    protected DatabusProps() {
        // 首先在jar包里找配置文件databus.properties，如果不存在，则通过系统属性找，如果还不存在，则配置为空
        File propsFile = new File(Consts.DATABUS_PROPERTIES);
        if (!propsFile.exists()) {
            String filename = System.getProperty(Consts.DATABUS_PROPERTIES_FILE_CONFIG);
            if (filename != null) {
                propsFile = new File(filename);
                if (!propsFile.exists()) {
                    LogUtils.warn(log, "cat not find databus.properties file to load!");
                    return;
                }
            }
        }

        LogUtils.info(log, "going to load file {} properties", propsFile.getPath());
        try (InputStream is = new FileInputStream(propsFile)) {
            props.load(is);
        } catch (IOException ioe) {
            LogUtils.warn(log, "failed to load properties from file " + propsFile.getPath(), ioe);
        }
    }

    /**
     * 获取配置属性的值
     *
     * @param key 配置的键值
     * @return 配置属性的值
     */
    public String getProperty(String key) {
        return props.getProperty(key);
    }

    /**
     * 设置配置文件的属性，便于测试
     *
     * @param key 属性的key
     * @param value 属性的值
     */
    public void setProperty(String key, String value) {
        props.setProperty(key, value);
    }

    /**
     * 清理databus props中所有的key/value，便于测试
     */
    public void clearProperty() {
        props.clear();
    }

    /**
     * 获取配置属性的值
     *
     * @param key 配置的键值
     * @param defaultValue 配置属性的默认值，当配置键值不存在时使用
     * @return 配置属性的值
     */
    public String getOrDefault(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    /**
     * 获取配置属性的整型值
     *
     * @param key 配置的键值
     * @param defaultValue 配置属性的默认值，当配置键值不存在时使用
     * @return 配置属性的值
     */
    public int getOrDefault(String key, int defaultValue) {
        String val = props.getProperty(key);
        if (StringUtils.isNoneBlank(val)) {
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException nfe) {
                LogUtils.warn(log, String.format("property %s has value %s, is not integer", key, val), nfe);
            }
        }

        return defaultValue;
    }

    /**
     * 获取配置属性的整型值
     *
     * @param key 配置的键值
     * @param defaultValue 配置属性的默认值，当配置键值不存在时使用
     * @return 配置属性的值
     */
    public long getOrDefault(String key, long defaultValue) {
        String val = props.getProperty(key);
        if (StringUtils.isNoneBlank(val)) {
            try {
                return Long.parseLong(val);
            } catch (NumberFormatException nfe) {
                LogUtils.warn(log, String.format("property %s has value %s, is not long", key, val), nfe);
            }
        }

        return defaultValue;
    }

    /**
     * 获取配置中以固定prefix开头的所有配置项的kv,并将key中的prefix去掉。
     *
     * @param prefix 配置项prefix
     * @return 符合条件的配置项, 放入map中返回
     */
    public Map<String, Object> originalsWithPrefix(String prefix) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String key = entry.getKey().toString();
            if (key.startsWith(prefix) && key.length() > prefix.length()) {
                result.put(key.substring(prefix.length()), entry.getValue());
            }
        }

        return result;
    }

    /**
     * 将配置项转换为map结构返回
     *
     * @return map结构的配置项
     */
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        props.forEach((k, v) -> map.put(k.toString(), v.toString()));
        return map;
    }

    /**
     * 获取配置属性的值，将对应的值拆分为字符串数组
     *
     * @param key 配置的键值
     * @param separator 拆分属性值的分隔符
     * @return 字符串数组
     */
    public String[] getArrayProperty(String key, String separator) {
        String val = getOrDefault(key, "");
        return StringUtils.split(val, separator);
    }

    // 单例模式
    private static class Holder {

        private static final DatabusProps INSTANCE = new DatabusProps();
    }

    /**
     * 获取本单例对象的引用
     *
     * @return DatabusProps对象，单例
     */
    public static DatabusProps getInstance() {
        return Holder.INSTANCE;
    }

}
