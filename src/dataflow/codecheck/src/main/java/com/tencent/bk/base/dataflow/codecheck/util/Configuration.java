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

import com.typesafe.config.Config;
import java.io.File;
import java.time.Duration;
import java.util.Properties;
import pl.touk.tscreload.Reloadable;
import pl.touk.tscreload.TscReloadableConfigFactory;

public class Configuration {

    private static final String DEFAULT_CONFIG_DIRECTORY = "conf";
    private static final String DEFAULT_CONFIG_FILENAME = "codecheck.conf";

    /**
     * 配置自动刷新时间，默认为10秒
     */
    private static final int CONFIG_AUTO_REFRESH_INTERVAL = 10;

    private static final File configFile = null;
    private static volatile Configuration singleton;
    private Reloadable<Config> config = null;

    private Configuration() {
        config = TscReloadableConfigFactory.parseFile(getConfigurationFile(),
                Duration.ofSeconds(CONFIG_AUTO_REFRESH_INTERVAL));
    }

    /**
     * getSingleton
     *
     * @return com.tencent.bkdata.codecheck.util.Configuration
     */
    public static Configuration getSingleton() {
        if (singleton == null) {
            synchronized (Configuration.class) {
                if (singleton == null) {
                    singleton = new Configuration();
                }
            }
        }
        return singleton;
    }

    /**
     * toProperties
     *
     * @param config
     * @return java.util.Properties
     */
    public static Properties toProperties(Config config) {
        Properties properties = new Properties();
        config.entrySet().forEach(e -> properties.setProperty(e.getKey(), config.getString(e.getKey())));
        return properties;
    }

    private File getConfigurationFile() {
        if (configFile != null) {
            if (configFile.exists()) {
                return configFile;
            } else {
                throw new RuntimeException("The configuration directory '" + configFile.toPath()
                        + "', specified in the args, does not exist.");
            }
        }
        File codecheckDefaultDir = new File(getProjectRootDir() + "/" + DEFAULT_CONFIG_DIRECTORY);
        final File ipConfigFile = new File(codecheckDefaultDir, DEFAULT_CONFIG_FILENAME);
        if (!ipConfigFile.exists()) {
            throw new RuntimeException("The codecheck config file '" + ipConfigFile
                    + "' (" + codecheckDefaultDir + ") does not exist.");
        }
        return ipConfigFile;
    }

    private String getProjectRootDir() {
        String jarPath = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        return jarPath.substring(0, jarPath.lastIndexOf("/", jarPath.lastIndexOf("/") - 1) + 1);
    }

    /**
     * getHost
     *
     * @return java.lang.String
     */
    public String getHost() {
        try {
            return config.currentValue().getString(Constants.HOST);
        } catch (Exception ex) {
            return Constants.DEFAULT_HOST;
        }
    }

    /**
     * getPort
     *
     * @return int
     */
    public int getPort() {
        try {
            return config.currentValue().getInt(Constants.PORT);
        } catch (Exception ex) {
            return Constants.DEFAULT_PORT;
        }
    }

    /**
     * getRegexMode
     *
     * @return java.lang.String
     */
    public String getRegexMode() {
        try {
            return config.currentValue().getString(Constants.REGEX_MODE);
        } catch (Exception ex) {
            return Constants.DEAFAULT_REGEX_MODE;
        }
    }

    /**
     * getReloadConfigSec
     *
     * @return int
     */
    public int getReloadConfigSec() {
        try {
            return config.currentValue().getInt(Constants.RELOAD_CONFIG_SEC);
        } catch (Exception ex) {
            return Constants.DEFAULT_RELOAD_CONFIG_SEC;
        }
    }

    /**
     * getValue
     *
     * @param key
     * @param defaultValue
     * @return java.lang.String
     */
    public String getValue(String key, String defaultValue) {
        try {
            return config.currentValue().getString(key);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    /**
     * getValue
     *
     * @param key
     * @param defaultValue
     * @return int
     */
    public int getValue(String key, int defaultValue) {
        try {
            return config.currentValue().getInt(key);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    /**
     * getDBConfig
     *
     * @return java.util.Properties
     */
    public Properties getDBConfig() {
        try {
            return toProperties(config.currentValue().getConfig("db"));
        } catch (Exception ex) {
            return null;
        }
    }
}
