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

package com.tencent.bk.base.dataflow.jobnavi.util.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 * jobnavi Configuration
 */
public class Configuration {

    private static final Logger logger = Logger.getLogger(Configuration.class);

    private Properties properties;
    private ClassLoader classLoader;

    {
        classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = Configuration.class.getClassLoader();
        }
    }

    public Configuration(boolean loadDefault) throws IOException {
        properties = new Properties();
        if (loadDefault) {
            InputStream inputStream = null;
            try {
                inputStream = this.getClass().getResourceAsStream("/jobnavi.properties");
                properties.load(inputStream);
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }
    }

    public Configuration(String propertyFile) throws IOException {
        properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = this.getClass().getResourceAsStream("/" + propertyFile);
            properties.load(inputStream);
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }


    public Configuration(InputStream is) throws IOException {
        properties = new Properties();
        properties.load(is);
    }

    public Configuration(Properties properties) {
        this.properties = properties;
    }

    public void setString(String name, String value) {
        properties.setProperty(name, value);
    }


    /**
     * Get value of the <code>name</code> property as <code>String</code>.
     *
     * @param name property name.
     * @return property value of <code>String</code>.
     */
    public String getString(String name) {
        Object value = get(name);
        if (value == null) {
            return null;
        }
        return value.toString().trim();
    }

    /**
     * Get value of the <code>name</code> property as <code>String</code>.
     * If no such property is specified then default value is returned.
     *
     * @param name property name.
     * @param defaultValue The default value
     * @return property value of <code>String</code>, or default value.
     */
    public String getString(String name, String defaultValue) {
        Object value = get(name);
        if (value == null) {
            return defaultValue;
        }
        return value.toString().trim();
    }


    /**
     * Get value of the <code>name</code> property as <code>int</code>.
     * If no such property is specified then default value is returned.
     *
     * @param name property name.
     * @param defaultValue The default value
     * @return property value of <code>int</code>,or default value.
     */
    public int getInt(String name, int defaultValue) {
        String value = getString(name);
        if (value == null) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    /**
     * Get value of the <code>name</code> property as <code>int</code>.
     * If no such property is specified then 0 is returned.
     *
     * @param name property name.
     * @return property value of <code>int</code>,or 0.
     */
    public int getInt(String name) {
        String value = getString(name);
        if (value == null) {
            return 0;
        }
        return Integer.parseInt(value);
    }

    /**
     * Get value of the <code>name</code> property as <code>long</code>.
     * If no such property is specified then default value is returned.
     *
     * @param name property name.
     * @param defaultValue The default value
     * @return property value of <code>long</code>,or default value.
     */
    public long getLong(String name, long defaultValue) {
        String value = getString(name);
        if (value == null) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }

    /**
     * Get value of the <code>name</code> property as <code>long</code>.
     * If no such property is specified then 0 is returned.
     *
     * @param name property name.
     * @return property value of <code>long</code>,or 0.
     */
    public long getLong(String name) {
        String value = getString(name);
        if (value == null) {
            return 0;
        }
        return Long.parseLong(value);
    }

    /**
     * Get value of the <code>name</code> property as <code>boolean</code>.
     * If no such property is specified then default value is returned.
     *
     * @param name property name.
     * @param defaultValue The default value
     * @return property value of <code>boolean</code>,or default value.
     */
    public boolean getBoolean(String name, boolean defaultValue) {
        String value = getString(name);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }


    /**
     * Get value of the <code>name</code> property as <code>boolean</code>.
     * If no such property is specified then false is returned.
     *
     * @param name property name.
     * @return property value of <code>boolean</code>,or false
     */
    public boolean getBoolean(String name) {
        String value = getString(name);
        return value != null && Boolean.parseBoolean(value);
    }

    /**
     * Get value of the <code>name</code> property as <code>double</code>.
     * If no such property is specified then default value is returned.
     *
     * @param name property name.
     * @param defaultValue The default value
     * @return property value of <code>double</code>,or default value.
     */
    public double getDouble(String name, double defaultValue) {
        String value = getString(name);
        if (value == null) {
            return defaultValue;
        }
        return Double.parseDouble(value);
    }


    /**
     * Get value of the <code>name</code> property as <code>double</code>.
     * If no such property is specified then 0 is returned.
     *
     * @param name property name.
     * @return property value of <code>double</code>,or 0
     */
    public double getDouble(String name) {
        String value = getString(name);
        if (value == null) {
            return 0;
        }
        return Double.parseDouble(value);
    }


    private Object get(String param) {
        return properties.get(param);
    }


    /**
     * print all properties
     */
    public void printProperties() {
        for (Map.Entry entry : properties.entrySet()) {
            logger.info(entry.getKey().toString() + "=" + entry.getValue().toString());
        }
    }
}
