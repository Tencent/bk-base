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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.sparkstreaming;

import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class SparkStreamingOptions {

    public static final String DEPLOY_MODE_DEFAULT_VALUE = "cluster";
    public static final String MASTER_DEFAULT_VALUE = "yarn";
    public static final String TYPE_PYSPARK = "pyspark";
    public static final String TYPE_JAR = "jar";
    private static final Logger logger = Logger.getLogger(SparkStreamingOptions.class);
    private Map<String, Option> opts;

    private Option name;
    private Option type;
    private Option master;
    private Option deployMode;
    private Option queue;
    private Option jars;
    private Option pyFiles;
    private Option primaryResource;
    private Option mainArgs;
    private Option driverCores;
    private Option driverMemory;
    private Option numExecutors;
    private Option executorCores;
    private Option executorMemory;
    private Option clusterGroupId;
    private Option resourceCheck;
    private Option resourceGroupId;
    private Option geogAreaCode;

    private final String scheduleId;
    private final String paramJson;
    private final Properties config;
    private final String adaptorType;

    private String typeValue = null;
    private String primaryResourceValue = null;
    private String mainArgsValue = null;


    SparkStreamingOptions(String scheduleId, String paramJson, Properties config, String adaptorType) {
        logger.info("param:" + paramJson);
        this.config = config;
        this.paramJson = paramJson;
        this.scheduleId = scheduleId;
        this.adaptorType = adaptorType;
        initOpts();
        initValue();
    }

    private void initOpts() {
        opts = new LinkedHashMap<>();

        type = new Option("type", null);
        opts.put(type.getName(), type);

        name = new Option("name", "--name");
        opts.put(name.getName(), name);

        master = new Option("master", "--master", MASTER_DEFAULT_VALUE);
        opts.put(master.getName(), master);

        deployMode = new Option("deployMode", "--deploy-mode", DEPLOY_MODE_DEFAULT_VALUE);
        opts.put(deployMode.getName(), deployMode);

        queue = new Option("queue", "--queue");
        opts.put(queue.getName(), queue);

        jars = new Option("jars", "--jars");
        opts.put(jars.getName(), jars);

        pyFiles = new Option("pyFiles", "--py-files");
        opts.put(pyFiles.getName(), pyFiles);

        primaryResource = new Option("primaryResource", null);
        opts.put(primaryResource.getName(), primaryResource);

        mainArgs = new Option("mainArgs", null);
        opts.put(mainArgs.getName(), mainArgs);

        driverCores = new Option("driverCores", "--driver-cores");
        opts.put(driverCores.getName(), driverCores);

        driverMemory = new Option("driverMemory", "--driver-memory");
        opts.put(driverMemory.getName(), driverMemory);

        numExecutors = new Option("numExecutors", "--num-executors");
        opts.put(numExecutors.getName(), numExecutors);

        executorCores = new Option("executorCores", "--executor-cores");
        opts.put(executorCores.getName(), executorCores);

        executorMemory = new Option("executorMemory", "--executor-memory");
        opts.put(executorMemory.getName(), executorMemory);

        clusterGroupId = new Option("cluster_group_id", null);
        opts.put(clusterGroupId.getName(), clusterGroupId);

        resourceCheck = new Option("resource_check", null);
        opts.put(resourceCheck.getName(), resourceCheck);

        resourceGroupId = new Option("resource_group_id", null);
        opts.put(resourceGroupId.getName(), resourceGroupId);

        geogAreaCode = new Option("geog_area_code", null);
        opts.put(geogAreaCode.getName(), geogAreaCode);
    }

    private void initValue() {
        Map<String, Object> param = JsonUtils.readMap(paramJson);
        for (Map.Entry<String, Object> entry : param.entrySet()) {
            Option o = opts.get(entry.getKey());
            if (o != null) {
                o.setValue(entry.getValue());
            }
        }
    }

    public Option getQueue() {
        return queue;
    }

    public Option getClusterGroupId() {
        return clusterGroupId;
    }

    public Option getDriverMemory() {
        return driverMemory;
    }

    public Option getNumExecutors() {
        return numExecutors;
    }

    public Option getExecutorCores() {
        return executorCores;
    }

    public Option getExecutorMemory() {
        return executorMemory;
    }

    public Option getResourceCheck() {
        return resourceCheck;
    }

    public Option getResourceGroupId() {
        return resourceGroupId;
    }

    public Option getGeogAreaCode() {
        return geogAreaCode;
    }

    String toSparkStreamingArgs() throws IOException {

        Map<String, String> sparkStreamingOptsMap = new HashMap<>();
        for (Option o : opts.values()) {
            String value = o.getValue() == null ? null : o.getValue().toString();
            String name = o.getName();
            sparkStreamingOptsMap.put(name, value);
        }
        logger.info("sparkStreaming opts: " + JsonUtils.writeValueAsString(sparkStreamingOptsMap));

        List<String> args = new ArrayList<>();

        setOptions(args);

        setAdaptorConf(typeValue, args);

        setAdaptorJar(primaryResourceValue, args);

        setMainArgsValue(mainArgsValue, args);

        StringBuilder argStr = new StringBuilder();
        for (String arg : args) {
            argStr.append(arg).append(" ");
        }
        logger.info("sparkStreaming args is: " + argStr);

        return argStr.toString();
    }

    private void setOptions(List<String> args) {
        for (Option o : opts.values()) {
            if (type.getName().equals(o.getName()) && o.getValue() != null) {
                typeValue = o.getValue().toString();
            } else if (primaryResource.getName().equals(o.getName()) && o.getValue() != null) {
                primaryResourceValue = o.getValue().toString();
            } else if (mainArgs.getName().equals(o.getName()) && o.getValue() != null) {
                mainArgsValue = o.getValue().toString();
            } else if (name.getName().equals(o.getName())) {
                args.add(o.getYarnOpt());
                args.add(scheduleId);
                args.add("--conf");
                args.add("spark.app.name=" + scheduleId);
            } else if (StringUtils.isNotEmpty(o.getYarnOpt()) && o.getValue() != null) {
                args.add(o.getYarnOpt());
                args.add(o.getValue().toString());
            }
        }
    }

    private void setAdaptorConf(String typeValue, List<String> args) {
        if (typeValue != null) {
            if (TYPE_PYSPARK.equalsIgnoreCase(typeValue)) {
                String path = System.getProperty("JOBNAVI_HOME");
                String pySparkConf = path + "/adaptor/" + adaptorType + "/conf/python-spark.conf";
                // pySpark extra properties
                args.add("--properties-file");
                args.add(pySparkConf);
            } else if (TYPE_JAR.equalsIgnoreCase(typeValue)) {
                String path = System.getProperty("JOBNAVI_HOME");
                String jarSparkConf = path + "/adaptor/" + adaptorType + "/conf/jar-spark.conf";
                // jar(for Java / Scala apps) extra properties
                args.add("--properties-file");
                args.add(jarSparkConf);
            }
        }
    }

    private void setAdaptorJar(String primaryResourceValue, List<String> args) {
        if (primaryResourceValue != null) {
            String path = System.getProperty("JOBNAVI_HOME");
            String jarFilePath = path + "/adaptor/" + adaptorType + "/bin/" + primaryResourceValue;
            args.add(jarFilePath);
        }
    }

    private void setMainArgsValue(String mainArgsValue, List<String> args) {
        if (mainArgsValue != null) {
            args.add(Base64.encodeBase64String(mainArgsValue.getBytes(StandardCharsets.UTF_8)));
        }
    }
}
