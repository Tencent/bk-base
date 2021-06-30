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

package com.tencent.bk.base.datalab.bksql.core;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.PatternFilenameFilter;
import com.tencent.bk.base.datalab.bksql.protocol.ProtocolPlugin;
import com.tencent.bk.base.datalab.bksql.protocol.ProtocolPluginFactory;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ParsingContext;
import java.io.File;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BKSqlService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BKSqlService.class);

    private final ProtocolPluginFactory protocolPluginFactory = new ProtocolPluginFactory();

    public BKSqlService() throws IOException {
        this("");
    }

    public BKSqlService(String configDirectory) throws IOException {
        protocolPluginFactory.loadFromClasses();
        if (StringUtils.isNotEmpty(configDirectory)) {
            File dir = new File(configDirectory);
            if (!(dir.exists() && dir.isDirectory())) {
                throw new IllegalArgumentException(
                        "config directory path " + configDirectory + " does not denote a folder");
            }
            File[] files = dir.listFiles(new PatternFilenameFilter(".*\\.ya?ml"));
            Preconditions.checkNotNull(files);
            for (File file : files) {
                Preconditions.checkNotNull(file);
                protocolPluginFactory.loadFromFile(file);
            }
        }
    }

    public Object convert(String type, ParsingContext context) throws Exception {
        Preconditions
                .checkArgument(!Strings.isNullOrEmpty(type), "param: type cannot be null or empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(context.getSql()),
                "param: sql cannot be null or empty");
        ProtocolPlugin protocolPlugin = protocolPluginFactory.forType(type);
        return protocolPlugin.convert(context);
    }

    public boolean isRegistered(String type) {
        return protocolPluginFactory.isAvailableType(type);
    }

    @Override
    public void close() throws Exception {

    }

    public Object protocols() {
        return protocolPluginFactory.registeredPlugins();
    }

    public Object protocol(String protocolName) {
        return protocolPluginFactory.pluginConfig(protocolName);
    }
}
