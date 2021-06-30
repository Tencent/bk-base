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

import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;

public class FlinkOptions {

    private static final Logger logger = Logger.getLogger(FlinkOptions.class);

    protected Map<String, Option> opts;

    protected Option jobName;
    protected Option mode;
    protected Option useSavepoint;
    protected Option parallelism;
    protected Option savepointPath;
    protected Option jarFileName;
    protected Option argument;
    protected Option license;
    protected Option slot;
    protected Option taskManagerMemory;
    protected Option container;
    protected Option yarnQueue;
    protected Option applicationId;
    protected Option jobManangerMemory;
    protected Option resourceCheck;
    protected Option clusterGroupId;
    protected Option resourceGroupId;
    protected Option geogAreaCode;

    protected String paramJson;
    protected Configuration config;
    protected String typeName;

    protected String jarFileValue = null;
    protected String argumentValue = null;
    protected String licenseValue = null;
    protected String jobId = null;

    FlinkOptions(String paramJson, Configuration config) throws IOException {
        this(paramJson, config, "stream");
    }

    FlinkOptions(String paramJson, Configuration config, String typeName) throws IOException {
        logger.info("param:" + paramJson);
        this.config = config;
        this.paramJson = paramJson;
        this.typeName = typeName;
        initOpts();
        initValue();
    }

    protected void initOpts() {
        opts = new LinkedHashMap<>();

        jobName = new Option("job_name", null);
        opts.put(jobName.getName(), jobName);

        mode = new Option("mode", null);
        opts.put(mode.getName(), mode);

        useSavepoint = new Option("use_savepoint", null);
        useSavepoint.setValue(true);
        opts.put(useSavepoint.getName(), useSavepoint);

        parallelism = new Option("parallelism", "p");
        opts.put(parallelism.getName(), parallelism);

        savepointPath = new Option("savepoint_path", "s");
        opts.put(savepointPath.getName(), savepointPath);

        jarFileName = new Option("jar_file_name", "j");
        opts.put(jarFileName.getName(), jarFileName);

        argument = new Option("argument", null);
        opts.put(argument.getName(), argument);

        license = new Option("license", null);
        opts.put(license.getName(), license);

        slot = new Option("slot", "ys");
        opts.put(slot.getName(), slot);

        taskManagerMemory = new Option("task_manager_memory", "ytm");
        opts.put(taskManagerMemory.getName(), taskManagerMemory);

        jobManangerMemory = new Option("job_manager_memory", "yjm");
        opts.put(jobManangerMemory.getName(), jobManangerMemory);

        container = new Option("container", "yn");
        opts.put(container.getName(), container);

        yarnQueue = new Option("yarn_queue", "yqu");
        opts.put(yarnQueue.getName(), yarnQueue);

        applicationId = new Option("application_id", null);
        opts.put(applicationId.getName(), applicationId);

        resourceCheck = new Option("resource_check", null);
        opts.put(resourceCheck.getName(), resourceCheck);

        clusterGroupId = new Option("cluster_group_id", null);
        opts.put(clusterGroupId.getName(), clusterGroupId);

        resourceGroupId = new Option("resource_group_id", null);
        opts.put(resourceGroupId.getName(), resourceGroupId);

        geogAreaCode = new Option("geog_area_code", null);
        opts.put(geogAreaCode.getName(), geogAreaCode);
    }

    protected void initValue() {
        Map<String, Object> param = JsonUtils.readMap(paramJson);
        for (Map.Entry<String, Object> entry : param.entrySet()) {
            Option o = opts.get(entry.getKey());
            if (o != null) {
                o.setValue(entry.getValue());
            }
        }
    }

    protected void buildSavepoint() throws IOException {
        if (mode.getValue() != null && "yarn-session".equals(mode.getValue().toString())) {
            return;
        }

        if (Boolean.parseBoolean(useSavepoint.getValue().toString()) && savepointPath.getValue() == null) {
            String savepointRootPath = config.getString("state.savepoints.dir", "hdfs:///app/flink/savepoints")
                    + jobName.getValue().toString();
            String savepointPathStr = FlinkSavepoint.getSavepoint(savepointRootPath);
            savepointPath.setValue(savepointPathStr);
        }
    }

    /**
     * get flink save point root path
     *
     * @return
     */
    public String getSavepointRootPath() {
        if (savepointPath.getValue() == null) {
            return config.getString("state.savepoints.dir", "hdfs:///app/flink/savepoints") + jobName.getValue()
                    .toString();
        } else {
            return savepointPath.getValue().toString();
        }
    }


    Option getOption(String name) {
        return opts.get(name);
    }

    Option getMode() {
        return mode;
    }

    void setModeValue(String modeValue) {
        mode.setValue(modeValue);
    }

    public Option getJobName() {
        return jobName;
    }

    Option getSavepointPath() {
        return savepointPath;
    }

    public Option getSlot() {
        return slot;
    }

    public Option getYarnQueue() {
        return yarnQueue;
    }

    public Option getJobManangerMemory() {
        return jobManangerMemory;
    }

    public Option getUseSavepoint() {
        return useSavepoint;
    }

    public Option getParallelism() {
        return parallelism;
    }

    public Option getTaskManagerMemory() {
        return taskManagerMemory;
    }

    public Option getContainer() {
        return container;
    }

    public Option getResourceCheck() {
        return resourceCheck;
    }

    public Option getJarFileName() {
        return jarFileName;
    }

    public String getTypeName() {
        return typeName;
    }

    public Option getClusterGroupId() {
        return clusterGroupId;
    }

    public Option getResourceGroupId() {
        return resourceGroupId;
    }

    public Option getGeogAreaCode() {
        return geogAreaCode;
    }

    String[] toFinkArgs(Map<String, String> jobNameAndIdMaps, boolean useSavepoint) throws IOException {
        if (useSavepoint) {
            buildSavepoint();
        }

        Map<String, String> flinkOptsMap = new HashMap<>();
        for (Option o : opts.values()) {
            String value = o.getValue() == null ? null : o.getValue().toString();
            String name = o.getName();
            flinkOptsMap.put(name, value);
        }
        logger.info("flink opts: " + JsonUtils.writeValueAsString(flinkOptsMap));

        List<String> args = new ArrayList<>();

        setOptions(jobNameAndIdMaps, args);

        addJarToArgs(this.jarFileValue, args);

        writeArgumentValue(this.argumentValue, args);

        addLicenseValueToArgs(this.licenseValue, args);

        if (this.jobId != null) {
            args.add(this.jobId);
        }

        StringBuilder argStr = new StringBuilder();
        for (String arg : args) {
            argStr.append(arg).append(" ");
        }
        logger.info("flink args is: " + argStr);

        return args.toArray(new String[args.size()]);
    }

    private void setOptions(
            Map<String, String> jobNameAndIdMaps,
            List<String> args) {
        for (Option o : opts.values()) {
            if (jarFileName.getName().equals(o.getName()) && o.getValue() != null) {
                jarFileValue = o.getValue().toString();
            } else if (argument.getName().equals(o.getName()) && o.getValue() != null) {
                argumentValue = o.getValue().toString();
            } else if (license.getName().equals(o.getName()) && o.getValue() != null) {
                licenseValue = o.getValue().toString();
            } else if (jobName.getName().equals(o.getName()) && jobName.getValue() != null
                    && jobNameAndIdMaps != null) {
                jobId = jobNameAndIdMaps.get(jobName.getValue().toString());
            } else if (StringUtils.isNotEmpty(o.getFlinkOpt()) && o.getValue() != null) {
                args.add("-" + o.getFlinkOpt());
                args.add(o.getValue().toString());
            }
        }
    }

    private void addJarToArgs(String jarFileValue, List<String> args) {
        if (jarFileValue != null) {
            String path = System.getProperty("JOBNAVI_HOME");
            String jarFilePath = path + "/adaptor/stream/job/" + jarFileValue;
            args.add("-" + jarFileName.getFlinkOpt());
            args.add(jarFilePath);
        }
    }

    private void writeArgumentValue(String argumentValue, List<String> args) throws IOException {
        if (argumentValue != null) {
            logger.info("argumentValue1 " + argumentValue);
            String jsonPath = MessageFormat.format("/tmp/flink-{0}.json", jobName.getValue().toString());
            File jsonFile = new File(jsonPath);
            if (!jsonFile.createNewFile()) {
                logger.info("json file:" + jsonFile + " already exist");
            }
            try (FileOutputStream fileOutputStream = new FileOutputStream(jsonFile, false);
                    OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream,
                            StandardCharsets.UTF_8);
                    BufferedWriter out = new BufferedWriter(outputStreamWriter)) {
                out.write(argumentValue);
                out.flush();
            }
            logger.info("The argumentValue json file is written to complete with the path of " + jsonPath);
            args.add(jsonPath);
        }
    }

    private void addLicenseValueToArgs(String licenseValue, List<String> args) {
        if (licenseValue != null) {
            licenseValue = licenseValue.replace("\\", "\\\\");
            licenseValue = licenseValue.replace("\"", "\\\"");
            licenseValue = licenseValue.replace("`", "\\`");
            licenseValue = "\"" + licenseValue + "\"";
            logger.info("licenseValue " + licenseValue);
            args.add(licenseValue);
        }
    }
}
