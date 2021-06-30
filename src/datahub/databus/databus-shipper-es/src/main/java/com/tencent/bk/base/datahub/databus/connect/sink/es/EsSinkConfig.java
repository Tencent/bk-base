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


package com.tencent.bk.base.datahub.databus.connect.sink.es;


import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.utils.ConnUtils;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class EsSinkConfig extends AbstractConfig {

    public static final String TYPE_NAME_CONFIG = "type.name";
    public static final String ES_INDEX_PREFIX_CONFIG = "es.index.prefix";
    public static final String ES_CLUSTER_NAME_CONFIG = "es.cluster.name";
    public static final String ES_CLUSTER_VERSION_CONFIG = "es.cluster.version";
    public static final String ES_HOSTS_CONFIG = "es.host";
    public static final String ES_HTTP_PORT_CONFIG = "es.http.port";
    public static final int ES_7_BIG_VERSION = 7;

    private static final String ELASTICSEARCH_GROUP = "Elasticsearch";
    private static final String CONNECTOR_GROUP = "Connector";

    private static final String TYPE_NAME_DOC = "The type to use for each index.";
    private static final String TYPE_NAME_DISPLAY = "Type Name";

    // "index":"es_index", "es.cluster.name":"elasticsearch", "es.host":"localhost", "es.http.port":"9200",
    private static final String ES_INDEX_PREFIX_DOC = "data sink into es is indexed by date, add this value before "
            + "the date to construct index name.";
    private static final String ES_INDEX_PREFIX_DEFAULT = "";
    private static final String ES_INDEX_PREFIX_DISPLAY = "ES index name prefix.";

    private static final String ES_CLUSTER_NAME_DOC = "the cluster name of the ElasticSearch.";
    private static final String ES_CLUSTER_NAME_DEFAULT = "elasticsearch";
    private static final String ES_CLUSTER_NAME_DISPLAY = "ES cluster name.";

    private static final String ES_CLUSTER_VERSION_DOC = "the cluster version of the ElasticSearch.";
    private static final String ES_CLUSTER_VERSION_DEFAULT = "5.4.1";
    private static final String ES_CLUSTER_VERSION_DISPLAY = "ES cluster version.";

    private static final String ES_CLUSTER_ENABLE_PLAINTEXTPWD = "es.cluster.enable.PlaintextPwd";
    private static final String ES_ENABLE_PLAINTEXTPWD = "es.enable.plaintext.pwd";
    private static final String ES_CLUSTER_ENABLE_PLAINTEXTPWD_DOC = "the cluster is enable plaintext password.";
    private static final String ES_CLUSTER_ENABLE_PLAINTEXTPWD_DISPLAY = "the cluster is enable plaintext password.";

    private static final String ES_CLUSTER_ENABLE_AUTH = "es.cluster.enable.auth";
    private static final String ES_ENABLE_AUTH = "es.enable.auth";
    private static final String ES_CLUSTER_ENABLE_AUTH_DOC = "the cluster is enable auth.";
    private static final String ES_CLUSTER_ENABLE_AUTH_DISPLAY = "the cluster is enable auth.";

    private static final String ES_CLUSTER_USERNAME = "es.cluster.username";
    private static final String ES_HTTP_AUTH_USER = "es.http.auth.user";
    private static final String ES_CLUSTER_USERNAME_DOC = "the cluster username.";
    private static final String ES_CLUSTER_USERNAME_DISPLAY = "the cluster username.";

    private static final String ES_CLUSTER_PASSWORD = "es.cluster.password";
    private static final String ES_HTTP_AUTH_PWD = "es.http.auth.pwd";
    private static final String ES_CLUSTER_PASSWORD_DOC = "the cluster password.";
    private static final String ES_CLUSTER_PASSWORD_DISPLAY = "the cluster password.";

    private static final String ES_HOSTS_DOC = "the elastic master host name of the nodes in ElasticSearch cluster";
    private static final String ES_HOSTS_DEFAULT = "localhost";
    private static final String ES_HOSTS_DISPLAY = "ES Host Name.";

    private static final String ES_HTTP_PORT_DOC = "the http port of the nodes in ElasticSearch cluster.";
    private static final String ES_HTTP_PORT_DEFAULT = "9200";
    private static final String ES_HTTP_PORT_DISPLAY = "ES http port.";

    public static ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TYPE_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TYPE_NAME_DOC,
                    ELASTICSEARCH_GROUP, 1, ConfigDef.Width.SHORT, TYPE_NAME_DISPLAY)
            .define(ES_INDEX_PREFIX_CONFIG, ConfigDef.Type.STRING, ES_INDEX_PREFIX_DEFAULT, ConfigDef.Importance.HIGH,
                    ES_INDEX_PREFIX_DOC, ELASTICSEARCH_GROUP, 2, ConfigDef.Width.SHORT, ES_INDEX_PREFIX_DISPLAY)
            .define(ES_CLUSTER_NAME_CONFIG, ConfigDef.Type.STRING, ES_CLUSTER_NAME_DEFAULT, ConfigDef.Importance.HIGH,
                    ES_CLUSTER_NAME_DOC, ELASTICSEARCH_GROUP, 3, ConfigDef.Width.SHORT, ES_CLUSTER_NAME_DISPLAY)
            .define(ES_HOSTS_CONFIG, ConfigDef.Type.STRING, ES_HOSTS_DEFAULT, ConfigDef.Importance.HIGH, ES_HOSTS_DOC,
                    ELASTICSEARCH_GROUP, 4, ConfigDef.Width.LONG, ES_HOSTS_DISPLAY)
            .define(ES_HTTP_PORT_CONFIG, ConfigDef.Type.INT, ES_HTTP_PORT_DEFAULT, ConfigDef.Importance.HIGH,
                    ES_HTTP_PORT_DOC, ELASTICSEARCH_GROUP, 5, ConfigDef.Width.SHORT, ES_HTTP_PORT_DISPLAY)
            .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.GROUP_ID_DOC,
                    CONNECTOR_GROUP, 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
            .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.CONNECTOR_NAME_DOC, CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM,
                    BkConfig.CONNECTOR_NAME_DISPLAY)
            .define(BkConfig.RT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.RT_ID_DOC,
                    CONNECTOR_GROUP, 3, ConfigDef.Width.LONG, BkConfig.RT_ID_DISPLAY)
            .define(ES_CLUSTER_VERSION_CONFIG, ConfigDef.Type.STRING, ES_CLUSTER_VERSION_DEFAULT,
                    ConfigDef.Importance.HIGH, ES_CLUSTER_VERSION_DOC, ELASTICSEARCH_GROUP, 3, ConfigDef.Width.SHORT,
                    ES_CLUSTER_VERSION_DISPLAY)
            .define(ES_CLUSTER_ENABLE_AUTH, ConfigDef.Type.BOOLEAN, Boolean.valueOf(false), ConfigDef.Importance.HIGH,
                    ES_CLUSTER_ENABLE_AUTH_DOC, ELASTICSEARCH_GROUP, 3, ConfigDef.Width.SHORT,
                    ES_CLUSTER_ENABLE_AUTH_DISPLAY)
            .define(ES_CLUSTER_ENABLE_PLAINTEXTPWD, ConfigDef.Type.BOOLEAN, Boolean.valueOf(false),
                    ConfigDef.Importance.HIGH, ES_CLUSTER_ENABLE_PLAINTEXTPWD_DOC, ELASTICSEARCH_GROUP, 3,
                    ConfigDef.Width.SHORT, ES_CLUSTER_ENABLE_PLAINTEXTPWD_DISPLAY)
            .define(ES_CLUSTER_USERNAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, ES_CLUSTER_USERNAME_DOC,
                    ELASTICSEARCH_GROUP, 3, ConfigDef.Width.SHORT, ES_CLUSTER_USERNAME_DISPLAY)
            .define(ES_CLUSTER_PASSWORD, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, ES_CLUSTER_PASSWORD_DOC,
                    ELASTICSEARCH_GROUP, 3, ConfigDef.Width.SHORT, ES_CLUSTER_PASSWORD_DISPLAY);


    public final String typeName;
    public final String esIndexPrefix;
    public final String esClusterName;
    public final String esHost;
    public final int esHttpPort;
    public final String cluster;
    public final String connector;
    public final String rtId;
    public final String version;
    public final boolean enableAuth;
    public final boolean enablePlaintextPwd;
    public final String authUser;
    public final String authPassword;

    public EsSinkConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        this.cluster = getString(BkConfig.GROUP_ID);
        this.connector = getString(BkConfig.CONNECTOR_NAME);
        this.rtId = getString(BkConfig.RT_ID);
        this.typeName = getString(TYPE_NAME_CONFIG).toLowerCase();
        this.esIndexPrefix = rtId.toLowerCase();
        this.esClusterName = getString(ES_CLUSTER_NAME_CONFIG);
        this.esHost = getString(ES_HOSTS_CONFIG);
        this.esHttpPort = getInt(ES_HTTP_PORT_CONFIG);
        this.version = getString(ES_CLUSTER_VERSION_CONFIG);
        Map<String, String> clusterProps = BasicProps.getInstance().getClusterProps();
        this.enableAuth = props.containsKey(ES_CLUSTER_ENABLE_AUTH) ? getBoolean(ES_CLUSTER_ENABLE_AUTH).booleanValue()
                : Boolean.parseBoolean(clusterProps.getOrDefault(ES_ENABLE_AUTH, Boolean.FALSE.toString()));
        this.enablePlaintextPwd =
                props.containsKey(ES_CLUSTER_ENABLE_PLAINTEXTPWD) ? getBoolean(ES_CLUSTER_ENABLE_PLAINTEXTPWD)
                        .booleanValue()
                        : Boolean.parseBoolean(
                                clusterProps.getOrDefault(ES_ENABLE_PLAINTEXTPWD, Boolean.FALSE.toString()));
        this.authUser = props.containsKey(ES_CLUSTER_USERNAME) ? getString(ES_CLUSTER_USERNAME)
                : clusterProps.getOrDefault(ES_HTTP_AUTH_USER, "");
        String pwd = props.containsKey(ES_CLUSTER_PASSWORD) ? getString(ES_CLUSTER_PASSWORD)
                : clusterProps.getOrDefault(ES_HTTP_AUTH_PWD, "");
        this.authPassword = this.enablePlaintextPwd ? pwd : ConnUtils.decodePass(pwd);
    }

}
