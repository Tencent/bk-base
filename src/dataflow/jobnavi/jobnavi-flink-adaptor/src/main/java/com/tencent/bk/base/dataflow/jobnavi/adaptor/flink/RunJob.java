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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.client.cli.RunOptions;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.ProgramMissingJobException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.log4j.Logger;

public class RunJob {

    private static final Logger logger = Logger.getLogger(RunJob.class);

    private ClusterClient client;
    private String applicationId;

    /**
     * Creates a {@link ClusterClient} object from the given command line options and other parameters.
     *
     * @throws Exception
     */
    public JobSubmissionResult submit(
            String[] args, Configuration config, String configurationDirectory) throws Exception {

        // Get the custom command-line (e.g. Standalone/Yarn/Mesos)
        List<CustomCommandLine<?>> customCommandLines = CliFrontend
                .loadCustomCommandLines(config, configurationDirectory);
        Options customCommandLineOptions = new Options();
        for (CustomCommandLine<?> customCommandLine : customCommandLines) {
            customCommandLine.addGeneralOptions(customCommandLineOptions);
            customCommandLine.addRunOptions(customCommandLineOptions);
        }
        final Options commandOptions = CliFrontendParser.getRunCommandOptions();
        final Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);
        final CommandLine commandLine = CliFrontendParser.parse(commandLineOptions, args, true);

        final RunOptions runOptions = new RunOptions(commandLine);

        final PackagedProgram program;
        try {
            logger.info("Building program from JAR file");
            program = buildProgram(runOptions);
        } catch (FileNotFoundException e) {
            throw new CliArgsException("Could not build the program from JAR file.", e);
        }

        CustomCommandLine<?> customCommandLine = getActiveCustomCommandLine(commandLine, customCommandLines);
        try {
            return runProgram(customCommandLine, commandLine, runOptions, program);
        } finally {
            // 删除临时下周的UDF jar文件
            if (program instanceof BkDataPackagedProgram) {
                ((BkDataPackagedProgram) program).deleteUdfLibraries();
            }
        }
    }


    /**
     * Creates a {@link ClusterClient} object from the given command line options and other parameters.
     *
     * @throws Exception
     */
    public void submitYarnCluster(
            String[] args, Configuration config, String configurationDirectory, String jobName) throws Exception {

        // Get the custom command-line (e.g. Standalone/Yarn/Mesos)
        List<CustomCommandLine<?>> customCommandLines = CliFrontend
                .loadCustomCommandLines(config, configurationDirectory);
        Options customCommandLineOptions = new Options();
        for (CustomCommandLine<?> customCommandLine : customCommandLines) {
            customCommandLine.addGeneralOptions(customCommandLineOptions);
            customCommandLine.addRunOptions(customCommandLineOptions);
        }
        final Options commandOptions = CliFrontendParser.getRunCommandOptions();
        final Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);
        final CommandLine commandLine = CliFrontendParser.parse(commandLineOptions, args, true);

        final RunOptions runOptions = new RunOptions(commandLine);

        final PackagedProgram program;
        try {
            logger.info("Building program from JAR file");
            program = buildProgram(runOptions);
        } catch (FileNotFoundException e) {
            throw new CliArgsException("Could not build the program from JAR file.", e);
        }
        CustomCommandLine customCommandLine = getActiveCustomCommandLine(commandLine, customCommandLines);
        final ClusterDescriptor clusterDescriptor = customCommandLine.createClusterDescriptor(commandLine);
        logger.info("Descriptor is " + clusterDescriptor.getClass().getSimpleName());
        if (clusterDescriptor instanceof AbstractYarnClusterDescriptor) {
            ((AbstractYarnClusterDescriptor) clusterDescriptor).setName(jobName);
        }

        int parallelism = runOptions.getParallelism() == -1 ? 1 : runOptions.getParallelism();

        final JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, config, parallelism);

        final ClusterSpecification clusterSpecification = customCommandLine.getClusterSpecification(commandLine);
        client = clusterDescriptor.deployJobCluster(
                clusterSpecification,
                jobGraph,
                true);
        applicationId = client.getClusterId().toString();
        logger.info("Job has been submitted with JobID " + jobGraph.getJobID());
        try {
            client.shutdown();
        } catch (Exception e) {
            logger.error("Could not properly shut down the client.", e);
        }
    }


    private PackagedProgram buildProgram(ProgramOptions options)
            throws FileNotFoundException, ProgramInvocationException {
        String[] programArgs = options.getProgramArgs();
        String jarFilePath = options.getJarFilePath();
        List<URL> classpaths = options.getClasspaths();

        if (jarFilePath == null) {
            throw new IllegalArgumentException("The program JAR file was not specified.");
        }

        File jarFile = new File(jarFilePath);

        // Check if JAR file exists
        if (!jarFile.exists()) {
            throw new FileNotFoundException("JAR file does not exist: " + jarFile);
        } else if (!jarFile.isFile()) {
            throw new FileNotFoundException("JAR file is not a file: " + jarFile);
        }

        // Get assembler class
        String entryPointClass = options.getEntryPointClassName();

        PackagedProgram program = entryPointClass == null
                ? new BkDataPackagedProgram(jarFile, classpaths, programArgs) :
                new BkDataPackagedProgram(jarFile, classpaths, entryPointClass, programArgs);

        program.setSavepointRestoreSettings(options.getSavepointRestoreSettings());

        return program;
    }

    private <T> JobSubmissionResult runProgram(CustomCommandLine<T> customCommandLine, CommandLine commandLine,
            RunOptions runOptions, PackagedProgram program) throws FlinkException, ProgramInvocationException {
        final ClusterDescriptor<T> clusterDescriptor = customCommandLine.createClusterDescriptor(commandLine);

        T clusterId = customCommandLine.getClusterId(commandLine);
        if (clusterId != null) {
            client = clusterDescriptor.retrieve(clusterId);
        } else {
            // also in job mode we have to deploy a session cluster because the job
            // might consist of multiple parts (e.g. when using collect)
            final ClusterSpecification clusterSpecification = customCommandLine.getClusterSpecification(commandLine);
            client = clusterDescriptor.deploySessionCluster(clusterSpecification);
        }

        try {
            client.setPrintStatusDuringExecution(runOptions.getStdoutLogging());
            client.setDetached(true);

            int userParallelism = runOptions.getParallelism();
            if (client.getMaxSlots() != -1 && userParallelism == -1) {
                logger.info("Using the parallelism provided by the remote cluster ("
                        + client.getMaxSlots() + "). "
                        + "To use another parallelism, set it at the ./bin/flink client.");
                userParallelism = client.getMaxSlots();
            } else if (ExecutionConfig.PARALLELISM_DEFAULT == userParallelism) {
                userParallelism = 1;
            }

            return executeProgram(program, client, userParallelism);
        } finally {
            if (clusterId == null && !client.isDetached()) {
                // terminate the cluster only if we have started it before and if it's not detached
                try {
                    client.shutDownCluster();
                } catch (final Exception e) {
                    logger.error("Could not properly terminate the Flink cluster.", e);
                }
            }

            try {
                client.shutdown();
            } catch (Exception e) {
                logger.error("Could not properly shut down the client.", e);
            }
        }
    }


    private JobSubmissionResult executeProgram(PackagedProgram program, ClusterClient<?> client, int parallelism)
            throws ProgramMissingJobException, ProgramInvocationException {
        logger.info("Starting execution of program");

        final JobSubmissionResult result = client.run(program, parallelism);
        applicationId = client.getClusterId().toString();

        if (null == result) {
            throw new ProgramMissingJobException(
                    "No JobSubmissionResult returned, please make sure you called ExecutionEnvironment.execute()");
        }

        if (result.isJobExecutionResult()) {
            logger.info("Program execution finished");
            JobExecutionResult execResult = result.getJobExecutionResult();
            logger.info("Job with JobID " + execResult.getJobID() + " has finished.");
            logger.info("Job Runtime: " + execResult.getNetRuntime() + " ms");
            Map<String, Object> accumulatorsResult = execResult.getAllAccumulatorResults();
            if (accumulatorsResult.size() > 0) {
                logger.info("Accumulator Results: ");
                logger.info(AccumulatorHelper.getResultsFormatted(accumulatorsResult));
            }
        } else {
            logger.info("Job has been submitted with JobID " + result.getJobID());
        }
        return result;
    }


    public String getApplicationId() {
        return applicationId;
    }

    public ClusterClient getClient() {
        return client;
    }

    private CustomCommandLine<?> getActiveCustomCommandLine(CommandLine commandLine,
            List<CustomCommandLine<?>> customCommandLines) {
        for (CustomCommandLine<?> cli : customCommandLines) {
            if (cli.isActive(commandLine)) {
                return cli;
            }
        }
        throw new IllegalStateException("No command-line ran.");
    }

    private static class BkDataPackagedProgram extends PackagedProgram {

        private static final Logger logger = Logger.getLogger(BkDataPackagedProgram.class);
        protected List<String> udfTempLibraries;
        protected boolean isInited = false;
        protected ProgramInvocationException exception;

        BkDataPackagedProgram(File jarFile, List<URL> classpaths, String... args)
                throws ProgramInvocationException {
            super(jarFile, classpaths, null, args);
            if (null != exception) {
                throw exception;
            }
        }

        BkDataPackagedProgram(File jarFile, List<URL> classpaths, @Nullable String entryPointClassName,
                String... args) throws ProgramInvocationException {
            super(jarFile, classpaths, entryPointClassName, args);
            if (null != exception) {
                throw exception;
            }
        }

        private static void deleteUdfTempLibraries(List<String> tempLibraries) {
            for (String f : tempLibraries) {
                if (!new File(f).delete()) {
                    logger.warn("temp library:" + f + " may not deleted");
                }
            }
        }

        private synchronized void initUdf() {
            if (isInited) {
                return;
            }
            isInited = true;
            String[] args = getArguments();
            if (null == args || args.length == 0) {
                return;
            }
            String arg = args[0];
            this.udfTempLibraries = new ArrayList<>();
            String jobConfigStr = null;
            File f = new File(arg);
            if (f.isFile() && f.exists()) {
                try (FileInputStream fileInputStream = new FileInputStream(f)) {
                    jobConfigStr = IOUtils.toString(fileInputStream);
                } catch (IOException e) {
                    logger.error("Read job config error '" + f.getAbsolutePath() + "'.", e);
                    exception = new ProgramInvocationException(
                            "read bkdata jobConfig error '" + f.getAbsolutePath() + "'.", e);
                }
            } else {
                jobConfigStr = arg;
            }
            Map<String, Object> job = readMap(jobConfigStr);
            // 根据旧的uc配置加载udf jar包
            downloadUDFJarV1(job);

            // 根据新的uc配置加载udf jar包
            downloadUDFJarV2(job);

            logger.info("udfTempLibraries:" + Arrays.toString(udfTempLibraries.toArray()));
            // 加载udf lib到当前system classloader，供client提交job使用。
            for (String udfTempLibrary : udfTempLibraries) {
                try {
                    URLClassLoader sysloader = (URLClassLoader) RunJob.class.getClassLoader();
                    Class sysClass = URLClassLoader.class;
                    Method method = sysClass.getDeclaredMethod("addURL", URL.class);
                    method.setAccessible(true);
                    method.invoke(sysloader, new File(udfTempLibrary).toURI().toURL());
                } catch (Throwable t) {
                    logger.error("Could not add URL to system classloader", t);
                    exception = new ProgramInvocationException("Error, could not add URL to system classloader", t);
                }
            }
        }

        private void downloadUDFJarV2(Map<String, Object> job) {
            Map<String, Object> jobnaviConf = job != null ? (Map<String, Object>) job.get("jobnavi") : null;
            if (null != jobnaviConf) {
                Random rnd = new Random();
                List<String> config = (List<String>) jobnaviConf.get("udf_path");
                if (null != config && !config.isEmpty()) {
                    for (String oneHdfsPath : config) {
                        String name = FilenameUtils.getName(oneHdfsPath);
                        File tempFile;
                        try {
                            tempFile = File.createTempFile(rnd.nextInt(Integer.MAX_VALUE) + "_", name);
                            tempFile.deleteOnExit();
                            HdfsUtil.downloadFromHdfs(oneHdfsPath, tempFile.getAbsolutePath());
                            this.udfTempLibraries.add(tempFile.getAbsolutePath());
                        } catch (IOException e) {
                            logger.error("An I/O error occurred while creating temporary file to udf library '"
                                    + oneHdfsPath + "'.", e);
                            exception = new ProgramInvocationException(
                                    "An I/O error occurred while creating temporary file to bkdata udf library '"
                                            + oneHdfsPath + "'.", e);
                        }
                    }
                }
            }
        }

        private void downloadUDFJarV1(Map<String, Object> job) {
            Map<String, Object> udf = job != null ? (Map<String, Object>) job.get("udf") : null;
            if (null != udf) {
                Random rnd = new Random();
                List<Object> config = (List<Object>) udf.get("config");
                if (null != config && !config.isEmpty()) {
                    for (Object o : config) {
                        Map<String, Object> udfConfig = (Map<String, Object>) o;
                        if (null != udfConfig.get("hdfs_path")) {
                            String hdfsPath = udfConfig.get("hdfs_path").toString();
                            String name = FilenameUtils.getName(hdfsPath);
                            File tempFile;
                            try {
                                tempFile = File.createTempFile(rnd.nextInt(Integer.MAX_VALUE) + "_", name);
                                tempFile.deleteOnExit();
                                HdfsUtil.downloadFromHdfs(hdfsPath, tempFile.getAbsolutePath());
                                this.udfTempLibraries.add(tempFile.getAbsolutePath());
                            } catch (IOException e) {
                                logger.error("An I/O error occurred while creating temporary file to udf library '"
                                        + hdfsPath + "'.", e);
                                exception = new ProgramInvocationException(
                                        "An I/O error occurred while creating temporary file to bkdata udf library '"
                                                + hdfsPath + "'.", e);
                            }
                        }
                    }
                }
            }
        }

        @Override
        public List<URL> getAllLibraries() {
            List<URL> jars = super.getAllLibraries();
            // 父类构造方法会调用该函数，所以在这里进行初始化。
            this.initUdf();
            // 添加UDF jar
            for (String udfTempLibrary : udfTempLibraries) {
                try {
                    jars.add(new File(udfTempLibrary).toURI().toURL());
                } catch (Exception e) {
                    throw new RuntimeException("Error, could not add udf jar file URL", e);
                }
            }
            return jars;
        }

        public void deleteUdfLibraries() {
            deleteUdfTempLibraries(this.udfTempLibraries);
            this.udfTempLibraries.clear();
        }

        /**
         * read json map from string
         *
         * @param json
         * @return
         */
        private Map<String, Object> readMap(String json) {
            if (StringUtils.isBlank(json)) {
                return new HashMap<>();
            }
            try {
                return new ObjectMapper().readValue(json, HashMap.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
