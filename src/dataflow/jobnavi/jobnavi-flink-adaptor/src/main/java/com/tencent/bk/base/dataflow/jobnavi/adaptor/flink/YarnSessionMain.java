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

import java.io.File;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.log4j.Logger;

public class YarnSessionMain {

    private static final Logger logger = Logger.getLogger(YarnSessionMain.class);

    /**
     * flink yarn session submit main entry
     * @param args
     */
    public static void main(String[] args) {
        try {
            String configPath = args[4];
            File configDirectory = new File(configPath);
            logger.info("Using configuration directory " + configDirectory.getAbsolutePath());
            Configuration config = GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath());

            String[] flinkArg = Arrays.copyOfRange(args, 6, args.length);
            FlinkYarnSessionCli cli = new FlinkYarnSessionCli(config, configPath, "y", "yarn");
            Options options = new Options();
            cli.addGeneralOptions(options);
            cli.addRunOptions(options);

            CommandLineParser parser = new PosixParser();
            CommandLine cmd;
            try {
                cmd = parser.parse(options, flinkArg);
            } catch (Exception e) {
                logger.error("parse param error.", e);
                throw e;
            }
            AbstractYarnClusterDescriptor yarnDescriptor = cli.createClusterDescriptor(cmd);
            String sessionName = args[3];
            yarnDescriptor.setName(sessionName);
            final ClusterSpecification clusterSpecification = cli.getClusterSpecification(cmd);
            ClusterClient<?> client = yarnDescriptor.deploySessionCluster(clusterSpecification);
            final LeaderConnectionInfo connectionInfo = client.getClusterConnectionInfo();
            logger.info(
                    "Flink JobManager is now running on "
                            + connectionInfo.getHostname() + ':' + connectionInfo.getPort()
                            + " with leader id " + connectionInfo.getLeaderSessionID() + '.');
            logger.info("JobManager Web Interface: " + client.getWebInterfaceURL());
            client.waitForClusterToBeReady();
            String applicationId = client.getClusterId().toString();
            String webInterfaceUrl = client.getWebInterfaceURL();
            System.out.println(applicationId);
            System.out.println(webInterfaceUrl);
        } catch (Throwable e) {
            logger.error("submit yarn session error.", e);
            System.exit(1);
        }
    }
}
