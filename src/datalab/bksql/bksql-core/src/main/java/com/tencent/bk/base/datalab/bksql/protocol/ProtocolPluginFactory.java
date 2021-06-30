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

package com.tencent.bk.base.datalab.bksql.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import com.google.common.base.Preconditions;
import com.tencent.bk.base.datalab.bksql.preloading.PreLoading;
import com.tencent.bk.base.datalab.bksql.util.ProtocolPluginLoader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtocolPluginFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolPluginFactory.class);
    private final Reflections reflections = ProtocolPluginLoader.getPluginReflections();
    private final Map<String, ProtocolPlugin> pluginTypeMapping = new ConcurrentHashMap<>();

    public ProtocolPluginFactory() {
    }

    /**
     * 通过class文件来加载plugin
     */
    public void loadFromClasses() {
        Set<Class<? extends ProtocolPlugin>> classes = reflections
                .getSubTypesOf(ProtocolPlugin.class);
        Set<Class<? extends ProtocolPlugin>> filtered = new HashSet<>();
        for (Class<? extends ProtocolPlugin> clazz : classes) {
            if (clazz.isAnnotationPresent(ProtocolType.class)) {
                filtered.add(clazz);
            }
        }
        for (Class<? extends ProtocolPlugin> clazz : filtered) {
            try {
                ProtocolType protocolType = clazz.getAnnotation(ProtocolType.class);
                String type = protocolType.value();
                checkNotDuplicated(type);
                ProtocolPlugin inst = clazz.newInstance();
                register(type, inst);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("failed to create instance for class: " + clazz, e);
            }
        }
    }

    public List<String> registeredPlugins() {
        return new ArrayList<>(pluginTypeMapping.keySet());
    }

    public ProtocolPlugin pluginConfig(String name) {
        if (!pluginTypeMapping.containsKey(name)) {
            throw new IllegalArgumentException("plugin not found: " + name);
        }
        return pluginTypeMapping.get(name);
    }

    /**
     * 通过配置文件来加载plugin
     *
     * @param file 配置文件File对象
     * @throws IOException 运行时异常
     */
    public void loadFromFile(File file) throws IOException {
        YAMLFactory factory = new YAMLFactory();
        factory.enable(YAMLParser.Feature.READ_INT_AS_STRINGS);
        factory.enable(YAMLParser.Feature.READ_FLOAT_AS_STRINGS);
        factory.enable(YAMLParser.Feature.READ_BOOL_AS_STRINGS);
        factory.enable(YAMLParser.Feature.READ_NULL_AS_STRINGS);
        ObjectMapper mapper = new ObjectMapper(factory);
        Map<String, JsonCreatedProtocolPlugin> pluginMap = mapper
                .readValue(file, new TypeReference<Map<String, JsonCreatedProtocolPlugin>>() {
                });
        if (pluginMap != null) {
            pluginMap.forEach((type, plugin) -> {
                checkNotDuplicated(type);
                register(type, plugin);
            });
        }
    }

    private void register(String type, ProtocolPlugin plugin) {
        validateTypeName(type);
        pluginTypeMapping.put(type, plugin);
        LOG.info("Protocol plugin registered: " + type);
    }

    private void validateTypeName(String typeName) {
        for (char c : typeName.toCharArray()) {
            if (!Character.isAlphabetic(c)
                    && !Character.isDigit(c)
                    && c != '-') {
                throw new IllegalArgumentException(
                        "illegal protocol plugin type name: " + typeName);
            }
        }
    }

    private void checkNotDuplicated(String type) {
        if (pluginTypeMapping.containsKey(type)) {
            throw new RuntimeException("protocol type " + type + " already exists");
        }
    }

    public ProtocolPlugin forType(String protocolType) {
        return pluginTypeMapping.get(protocolType);
    }

    public boolean isAvailableType(String type) {
        return pluginTypeMapping.containsKey(type);
    }

    private static final class JsonCreatedProtocolPlugin extends BaseProtocolPlugin {

        @JsonCreator
        JsonCreatedProtocolPlugin(@JsonProperty("walker") JsonNode walkerConfig,
                @JsonProperty("de-parser") JsonNode deParserConfig,
                @JsonProperty("pre-loading") JsonNode preLoadingConfig,
                @JsonProperty("properties") Map<String, Object> properties) {
            super(
                    withDefault(walkerConfig, EMPTY_WALKER_CONFIG),
                    withDefault(deParserConfig, EMPTY_DE_PARSER_CONFIG),
                    withDefault(properties, Collections.emptyMap())
            );
            // 异步预热：pre-loading
            doPreLoading(preLoadingConfig);
        }

        private static <T> T withDefault(T t, T de) {
            Preconditions.checkNotNull(de);
            if (t == null) {
                return de;
            }
            if (t.equals(NullNode.getInstance())) {
                return de;
            }
            return t;
        }

        private static void doPreLoading(final JsonNode preLoadingConfig) {
            if (null != preLoadingConfig && !preLoadingConfig.isNull()) {
                // Asynchronous
                new Thread(() -> {
                    try {
                        PreLoading preLoading = new ObjectMapper().readValue(
                                new TreeTraversingParser(preLoadingConfig), PreLoading.class);
                        if (null != preLoading) {
                            preLoading.preLoading();
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                        throw new RuntimeException("Preload failed!", e);
                    }
                }).start();
            }
        }
    }
}
