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

package com.tencent.bk.base.datahub.cache.ignite;

import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SparkIgCache {

    private static final Logger log = LoggerFactory.getLogger(SparkIgCache.class);

    private static final String CLUSTER_NAME = "cluster.name";
    private static final String CLUSTER_NAME_DFT = "default";
    private static final String ZK_DOMAIN = "zk.domain";
    private static final String ZK_DOMAIN_DFT = "localhost";
    private static final String ZK_PORT = "zk.port";
    private static final String ZK_PORT_DFT = "2181";
    private static final String ZK_ROOT_PATH = "zk.root.path";
    private static final String ZK_ROOT_PATH_DFT = "/";
    private static final String SESSION_TIMEOUT = "session.timeout";
    private static final String SESSION_TIMEOUT_DFT = "30000";
    private static final String JOIN_TIMEOUT = "join.timeout";
    private static final String JOIN_TIMEOUT_DFT = "10000";
    private static final String USER = "user";
    private static final String USER_DFT = "";
    private static final String PASSWORD = "password";
    private static final String PASSWORD_DFT = "";
    private static final String TEMPLATE_FILE = "ignite.conf.template";


    private String confFile = "";
    private boolean useFile = true;
    private String confStr = "";

    /**
     * 构造函数
     *
     * @param igniteConfFile ignite的配置文件
     */
    public SparkIgCache(String igniteConfFile) throws InitException {
        confFile = igniteConfFile;
        try {
            confStr = FileUtils.readFileToString(new File(confFile), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.warn("failed to read ignite config file {}", confFile);
            throw new InitException("failed to read ignite config file " + confFile, e);
        }
    }

    /**
     * 构造函数，通过传入ignite集群的链接信息，使用模板文件构建ignite的配置文件
     *
     * @param connInfo ignite集群的链接配置信息
     * @param useFile 是否生成配置文件，落地到磁盘上
     */
    public SparkIgCache(Map<String, Object> connInfo, boolean useFile) throws InitException {
        this.useFile = useFile;
        // 对于可能为数值类型的配置项，调用toString方法
        Map<String, Object> root = new HashMap<>();
        root.put(CLUSTER_NAME, connInfo.getOrDefault(CLUSTER_NAME, CLUSTER_NAME_DFT));
        root.put(ZK_DOMAIN, connInfo.getOrDefault(ZK_DOMAIN, ZK_DOMAIN_DFT));
        root.put(ZK_PORT, connInfo.getOrDefault(ZK_PORT, ZK_PORT_DFT).toString());
        root.put(ZK_ROOT_PATH, connInfo.getOrDefault(ZK_ROOT_PATH, ZK_ROOT_PATH_DFT));
        root.put(SESSION_TIMEOUT, connInfo.getOrDefault(SESSION_TIMEOUT, SESSION_TIMEOUT_DFT).toString());
        root.put(JOIN_TIMEOUT, connInfo.getOrDefault(JOIN_TIMEOUT, JOIN_TIMEOUT_DFT).toString());
        root.put(USER, connInfo.getOrDefault(USER, USER_DFT).toString());
        root.put(PASSWORD, connInfo.getOrDefault(PASSWORD, PASSWORD_DFT).toString());

        buildIgniteConfig(root);
    }

    /**
     * 生成ignite集群的配置
     *
     * @param root 生成配置所需的属性
     */
    private void buildIgniteConfig(Map<String, Object> root) throws InitException {
        // 在当前目录获取配置模板，替换其中的变量，存储为ignite集群配置文件
        Configuration cfg = new Configuration();
        cfg.setClassForTemplateLoading(this.getClass(), "/");
        cfg.setObjectWrapper(new DefaultObjectWrapper());
        try {
            if (useFile) {
                buildFileConfig(cfg, root);
            } else {
                // 生成配置文件的内容，无需落地到磁盘上。
                StringWriter stringWriter = new StringWriter();
                Template template = cfg.getTemplate(TEMPLATE_FILE);
                template.process(root, stringWriter);
                confStr = stringWriter.toString();
                log.info("ignite config created by template file {}: {}", TEMPLATE_FILE, confStr);
            }
        } catch (Exception e) {
            log.warn("failed to build ignite config with {}", root);
            throw new InitException("failed to build ignite config", e);
        }
    }

    /**
     * 生成ignite集群配置文件，落地到磁盘上。
     *
     * @param cfg 模板文件配置
     * @param root 配置熟悉值
     * @throws IOException IO异常
     * @throws TemplateException 模板处理异常
     */
    private void buildFileConfig(Configuration cfg, Map<String, Object> root) throws IOException, TemplateException {
        // 生成配置文件，落地到磁盘上。
        String dt = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date(System.currentTimeMillis()));
        String fn = String.format("ignite-%s-%s.xml", root.get(CLUSTER_NAME), dt);
        File file = new File(fn);
        try (
                FileOutputStream fos = new FileOutputStream(file);
                OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
                Writer out = new BufferedWriter(osw)
        ) {
            Template template = cfg.getTemplate(TEMPLATE_FILE);
            template.process(root, out);
            out.flush();
            out.close();
            log.info("ignite config file {} created with template file {}", file.getAbsoluteFile(), TEMPLATE_FILE);
            confFile = file.getAbsolutePath();
            confStr = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        }
    }

    /**
     * 构建关联spark的ignite上下文
     *
     * @param spark spark的session
     * @return ignite上下文，已关联spark session
     */
    public JavaIgniteContext<Object, BinaryObjectImpl> buildIgniteContext(SparkSession spark) {
        // 创建JavaIgniteContext
        JavaIgniteContext<Object, BinaryObjectImpl> igniteContext = new JavaIgniteContext<>(
                JavaSparkContext.fromSparkContext(spark.sparkContext()), new SerializedJavaIgniteConfig(confStr));
        log.info("java ignite context <Object, BinaryObjectImpl> build by conf file {}", confFile);
        return igniteContext;
    }

    /**
     * 进程关系前，调用此方法将ignite的worker关闭。
     *
     * @param ctx ignite上下文
     */
    public void closeContext(JavaIgniteContext ctx) {
        ctx.close(true);
    }

    /**
     * 构造spark使用的dataset数据集。注：需要使用配置文件的方式构造SparkIgCache实例。
     *
     * @param spark spark的session
     * @param cacheName 缓存名称（表名称）
     * @param keyFields 主键列表，逗号分隔
     * @return dataset数据集
     */
    public Dataset<Row> constructDataset(SparkSession spark, String cacheName, String keyFields) {
        if (!useFile) {
            throw new RuntimeException("confFile is empty as useFile is false, unable to create spark data frame!");
        }
        Dataset<Row> df = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE()) //Data source type.
                .option(IgniteDataFrameSettings.OPTION_TABLE(), cacheName) //Table to read.
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), confFile) //Ignite config.
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), keyFields)
                .option(IgniteDataFrameSettings.OPTION_STREAMER_ALLOW_OVERWRITE(), false)
                .load();
        log.info("spark dataset created for {} with keys {}", cacheName, keyFields);

        return df;
    }

    /**
     * 构造spark使用的RDD数据集
     *
     * @param igniteContext ignite上下文
     * @param cacheName 缓存名称
     * @param schema 缓存的schema
     * @return JavaRDD数据集
     */
    public JavaRDD<Row> constructRdd(JavaIgniteContext<Object, BinaryObjectImpl> igniteContext, String cacheName,
            StructType schema) {
        JavaIgniteRDD<Object, BinaryObjectImpl> cache = igniteContext.fromCache(cacheName).withKeepBinary();

        // 创建Row类型的RDD
        JavaRDD<Row> rowRdd = cache.map((Function<Tuple2<Object, BinaryObjectImpl>, Row>) record -> {
            List<Object> fieldObjList = new ArrayList<>();
            for (StructField field : schema.fields()) {
                fieldObjList.add(record._2().field(field.name()));
            }
            return RowFactory.create(fieldObjList.toArray(new Object[0]));
        });

        log.info("{}: ignite cache with schema {} loaded", cacheName, schema.toString());
        return rowRdd;
    }

    /**
     * toString方法，用于debug
     *
     * @return 包含对象内容的字符串
     */
    @Override
    public String toString() {
        return String.format("[useFile=%s, confFile=%s, confStr=%s]", useFile, confFile, confStr);
    }
}