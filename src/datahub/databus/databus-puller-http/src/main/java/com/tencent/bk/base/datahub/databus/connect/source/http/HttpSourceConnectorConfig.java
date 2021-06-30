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

package com.tencent.bk.base.datahub.databus.connect.source.http;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class HttpSourceConnectorConfig extends AbstractConfig {

    protected static final String END_TIME_FIELD = "source.end.time.field";
    protected static final String START_TIME_FIELD = "source.start.time.field";
    protected static final String HTTP_URL = "source.http.url";
    protected static final String PERIOD_SECOND = "source.period.second";
    protected static final String HTTP_METHOD = "source.http.method";
    protected static final String TIME_FORMAT = "source.time.format";
    protected static final String PULL_HTTP_RETRY_TIMES = "source.retry.times";
    // 目标kafka相关配置项定义
    protected static final String DEST_KAFKA_BS = "source.dest.kafka.bs";
    protected static final String DEST_TOPIC = "source.dest.topic";

    // Config definition
    public static final ConfigDef CONFIG = new ConfigDef()
            .define(BkConfig.DATA_ID, Type.INT, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "")
            .define(END_TIME_FIELD, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "")
            .define(START_TIME_FIELD, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "")
            .define(HTTP_URL, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "")
            .define(BkConfig.CONNECTOR_NAME, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "")
            .define(PERIOD_SECOND, Type.INT, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "")
            .define(HTTP_METHOD, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "")
            .define(TIME_FORMAT, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "")
            .define(DEST_KAFKA_BS, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "")
            .define(DEST_TOPIC, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "")
            .define(PULL_HTTP_RETRY_TIMES, Type.INT, 3, Importance.HIGH, "");

    protected final String destKafkaBs;
    protected final String destTopic;
    protected final int dataId;
    protected final String endTimeField;
    protected final String startTimeField;
    protected final String httpUrl;
    protected final int periodSecond;
    protected final Method httpMethod;
    protected final String timeFormat;
    protected final int retryTimes;

    public HttpSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
        destKafkaBs = getString(DEST_KAFKA_BS);
        destTopic = getString(DEST_TOPIC);
        dataId = this.getInt(BkConfig.DATA_ID);
        endTimeField = this.getString(END_TIME_FIELD);
        startTimeField = this.getString(START_TIME_FIELD);
        httpUrl = this.getString(HTTP_URL);
        periodSecond = this.getInt(PERIOD_SECOND);
        httpMethod = Method.valueOf(this.getString(HTTP_METHOD));
        timeFormat = this.getString(TIME_FORMAT);
        retryTimes = this.getInt(PULL_HTTP_RETRY_TIMES);
    }

    public enum Method {
        get, post
    }


}
