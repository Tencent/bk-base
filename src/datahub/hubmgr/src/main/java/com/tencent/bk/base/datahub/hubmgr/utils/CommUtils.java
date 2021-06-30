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

package com.tencent.bk.base.datahub.hubmgr.utils;

import static com.tencent.bk.base.datahub.databus.commons.Consts.API_DNS_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_DATAMANAGE_DNS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_DMONITOR_METRICS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_DMONITOR_METRICS_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_META_DNS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_META_RT_INFO;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_META_RT_INFO_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_QUERY_METRIC;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_QUERY_METRIC_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.MAX_MSG_LENGTH;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.STORAGES;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.errors.ApiException;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.telnet.TelnetClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommUtils {

    public static final Pattern TODELETE_TABLE = Pattern.compile("^([\\w_]+)_([0-9]+)_(todelete_[0-9]{14})$");
    public static final Pattern NORMAL_TABLE = Pattern.compile("^([\\w_]+)_([0-9]+)$");
    public static final String CK_JOB_EXEC_ERROR = "CK_JOB_EXEC_ERROR";
    public static final int DEF_SLEEP_TIME_MS = 1000;

    private static final Logger log = LoggerFactory.getLogger(CommUtils.class);

    /**
     * 获取当前的时间字符串
     *
     * @return 当前时间字符串
     */
    public static String getDatetime() {
        return getDateString("yyyy-MM-dd HH:mm:ss", System.currentTimeMillis());
    }

    /**
     * 获取指定时间戳的时间字符串
     *
     * @param tm 时间戳，毫秒
     * @return 时间字符串
     */
    public static String getDatetime(long tm) {
        return getDateString("yyyy-MM-dd HH:mm:ss", tm);
    }

    /**
     * 获取日期字符串，格式yyyy-MM-dd
     *
     * @param tm 时间戳，毫秒
     * @return 日期字符串，e.g. 2019-08-01
     */
    public static String getDate(long tm) {
        return getDateString("yyyy-MM-dd", tm);
    }

    /**
     * 获取指定格式的日期
     *
     * @param format 日期格式
     * @param ts 时间戳，毫秒
     * @return 日期字符串
     */
    public static String getDateString(String format, long ts) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(new Date(ts));
    }

    /**
     * 获取当天的起始时间戳，单位为秒。
     *
     * @return 当天起始时间戳，秒。
     */
    public static long getTodayStartTs() {
        Calendar cal = new GregorianCalendar();
        cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DATE), 0, 0, 0);
        return cal.getTimeInMillis() / 1000;
    }

    /**
     * 获取时间串对应的时间戳，毫秒
     *
     * @param format 时间格式
     * @param time 时间字符串
     * @return 时间戳，毫秒
     */
    public static long getTimestamp(String format, String time) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            return sdf.parse(time).getTime();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    /**
     * 获取dns地址
     * @param dnsName dns名称
     * @return dns地址，字符串。类似 xx.xx.xx:xxx
     */
    public static String getDns(String dnsName) {
        DatabusProps props = DatabusProps.getInstance();
        String defaultDns = props.getOrDefault(Consts.API_DNS, API_DNS_DEFAULT);

        return props.getOrDefault(dnsName, defaultDns);
    }

    /**
     * 通过datamanage的接口查询打点数据
     *
     * @param database 数据库
     * @param sql sql语句
     * @return 查询结果
     * @throws ApiException 异常
     */
    public static ApiResult queryMetrics(String database, String sql) throws ApiException {
        DatabusProps props = DatabusProps.getInstance();
        // 获取总线集群中的任务列表
        String apiDns = getDns(API_DATAMANAGE_DNS);
        String path = props.getOrDefault(API_QUERY_METRIC, API_QUERY_METRIC_DEFAULT);
        Map<String, String> params = new HashMap<>();
        params.put("database", database);
        params.put("sql", sql);

        String response = HttpUtils.post(String.format("http://%s%s", apiDns, path), params);
        return JsonUtils.parseApiResult(response);
    }

    /**
     * 获取rt的meta元信息，包含对应的存储信息。
     *
     * @param rtId 结果表id
     * @return rt元信息的map
     */
    public static Map<String, String> getRtMeta(String rtId) {
        DatabusProps props = DatabusProps.getInstance();
        String apiDns = getDns(API_META_DNS);
        String apiPath = props.getOrDefault(API_META_RT_INFO, API_META_RT_INFO_DEFAULT);
        String url = String.format("http://%s%s%s/?related=storages", apiDns, apiPath, rtId);
        Map<String, String> result = new HashMap<>();

        try {
            Map<String, Object> data = (Map<String, Object>) CommUtils.parseGetApiResult(url);
            data.forEach((k, v) -> result.put(k, v == null ? null : v.toString()));
            Map<String, Map<String, Object>> storages = (Map<String, Map<String, Object>>) data.get(STORAGES);
            storages.forEach((k1, v1) -> v1.forEach(
                    (k2, v2) -> result
                            .put(String.format("%s.%s.%s", STORAGES, k1, k2), v2 == null ? null : v2.toString())));
        } catch (Exception e) {
            log.warn("failed to get meta info for " + rtId, e);
        }

        return result;
    }

    /**
     * 截取异常堆栈信息
     *
     * @param throwable 异常
     * @param length 截取长度
     * @return 异常堆栈
     */
    public static String getStackTrace(Throwable throwable, int length) {
        StringWriter sw = new StringWriter();

        String message = "";
        try (PrintWriter pw = new PrintWriter(sw);) {
            throwable.printStackTrace(pw);
            message = sw.toString();
        } catch (Exception e) {
            LogUtils.info(log, "print stackTrace is error", e.getMessage());
            message = e.getMessage();
        }
        return message.length() > length ? message.substring(0, length) : message;
    }

    /**
     * 截取1000异常堆栈信息
     *
     * @param throwable 异常
     * @return 异常堆栈
     */
    public static String getStackTrace(Throwable throwable) {
        return getStackTrace(throwable, 1000);
    }

    /**
     * 通过http get请求url，并解析返回的json格式数据，将data的内容返回。
     *
     * @param url api地址
     * @return api response中data的内容
     */
    public static Object parseGetApiResult(String url) {
        ObjectMapper om = new ObjectMapper();
        try {
            HashMap res = om.readValue(new URL(url), HashMap.class);
            if (res.get("result").equals(true) && res.containsKey("data")) {
                return res.get("data");
            } else {
                log.warn("{} got bad response: {}", url, res);
            }
        } catch (Exception ignore) {
            log.warn("failed to read api result " + url, ignore);
        }

        throw new RuntimeException("get api result failed for " + url);
    }

    /**
     * 将指标数据上报到数据管理
     *
     * @param messagesBody 数据指标信息
     */
    public static void reportMetrics(Map<String, Object> messagesBody) {
        DatabusProps props = DatabusProps.getInstance();
        String apiDns = CommUtils.getDns(API_DATAMANAGE_DNS);
        String metricsReportPath = props.getOrDefault(API_DMONITOR_METRICS, API_DMONITOR_METRICS_DEFAULT);
        String restUrl = String.format("http://%s%s", apiDns, metricsReportPath);
        try {
            if (!HttpUtils.postAndCheck(restUrl, messagesBody)) {
                LogUtils.warn(log, "Post message {} to {} failed!", JsonUtils.toJson(messagesBody), restUrl);
            }
        } catch (Exception e) {
            //抓住未知异常
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.DATABUS_API_ERR, log,
                    String.format("Post metric message failed! url %s, params %s", restUrl, messagesBody), e);
        }
    }

    /**
     * 根据后缀过滤目录下配置文件
     *
     * @return 文件路径
     */
    public static List<String> filterFile(String filePath, String suffix) {
        List<String> subFiles = new ArrayList<>();
        File file = new File(filePath);
        if (file.isDirectory()) {
            File[] tempList = file.listFiles();

            if (tempList != null) {
                for (File subFile : tempList) {
                    if (subFile.isFile() && subFile.getName().endsWith(suffix)) {
                        subFiles.add(subFile.getPath());
                    }
                }
            }
        }
        return subFiles;
    }

    /**
     * 根据iceberg表的完整表名称转换出result_table_id的值
     *
     * @param fullTableName 完整的表名称
     * @return result_table_id的值
     */
    public static String parseResultTableId(String fullTableName) {
        int idx = fullTableName.lastIndexOf(".");
        if (idx == -1) {
            return fullTableName;
        } else {
            String tableName = fullTableName.substring(idx + 1);
            // 部分表名称格式为xxx_591_todelete_20201215170921，为待删除表
            Matcher matcher = TODELETE_TABLE.matcher(tableName);

            if (matcher.find()) {
                return matcher.group(3) + "_" + matcher.group(2) + "_" + matcher.group(1);
            } else {
                matcher = NORMAL_TABLE.matcher(tableName);

                if (matcher.find()) {
                    return matcher.group(2) + "_" + matcher.group(1);
                } else {
                    LogUtils.warn(log, "bad full table name {}, unable to parse rt id", fullTableName);
                    return tableName;
                }
            }
        }
    }

    /**
     * 获取指定时间字符串和今天相差的天数。如果是负值，则此时间字符串是未来时间。
     *
     * @param datetimeStr 时间字符串，格式yyyyMMddHHmmss，其中HHmmss可选。
     * @return 时间字符串和今天相差的天数
     */
    public static int daysBeforeToday(String datetimeStr) {
        if (datetimeStr.length() < 8) {
            LogUtils.warn(log, "bad datetime string {}, unable to calc days before today.", datetimeStr);
            return 0;
        }

        String year = datetimeStr.substring(0, 4);
        String month = datetimeStr.substring(4, 6);
        String day = datetimeStr.substring(6, 8);
        LocalDate delDate = LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), Integer.parseInt(day));
        Period period = Period.between(delDate, LocalDate.now());

        return period.getYears() * 365 + period.getMonths() * 30 + period.getDays();
    }

    /**
     * 解析过期时间配置，转换为具体的天数
     *
     * @param expires 过期时间配置
     * @return 过期时间配置对应的天数
     */
    public static int parseExpiresDays(String expires) {
        int days = -1;
        try {
            days = Integer.parseInt(expires);
        } catch (NumberFormatException e) {
            if (expires.length() > 1) {
                try {
                    int base = Integer.parseInt(expires.substring(0, expires.length() - 1));
                    switch (expires.charAt(expires.length() - 1)) {
                        case 'd':
                        case 'D':
                            days = base;
                            break;

                        case 'w':
                        case 'W':
                            days = 7 * base;
                            break;

                        case 'm':
                        case 'M':
                            days = 30 * base;
                            break;

                        case 'y':
                        case 'Y':
                            days = 365 * base;
                            break;

                        default:
                            LogUtils.info(log, "unable to parse expire days {}", expires);
                    }
                } catch (NumberFormatException ignore) {
                    LogUtils.info(log, "unable to parse expire days {}", expires);
                }
            } else {
                LogUtils.info(log, "unable to parse expire days {}", expires);
            }
        }

        return days;
    }

    /***
     * 检查节点可用性。 检查连通性，端口探活
     * @param retryTimes 节点列表
     * @param host 节点
     * @param port 端口
     */
    public static boolean checkAvailable(int retryTimes, String host, int port) throws InterruptedException {
        boolean available = false;
        TelnetClient telnet = new TelnetClient();
        try {
            telnet.connect(host, port);
            available = true;
        } catch (Exception ex) {
            LogUtils.error(CK_JOB_EXEC_ERROR, log, "{} is not reachable, retryTimes: {}, exception: {}",
                    host, retryTimes, getStackTrace(ex));
        } finally {
            disconnect(telnet);
        }

        // 如果不可用，需要多次重试，若还是不可用，则需要关闭该节点
        if (!available && retryTimes > 0) {
            Thread.sleep(DEF_SLEEP_TIME_MS);
            available = checkAvailable(--retryTimes, host, port);
        }

        return available;
    }

    /**
     * 关闭telnet连接
     *
     * @param telnet telnet 对象
     */
    private static void disconnect(TelnetClient telnet) {
        if (telnet != null) {
            try {
                telnet.disconnect();
            } catch (IOException ioe) {
                LogUtils.error(CK_JOB_EXEC_ERROR, log, "{} telnet client disconnect error, exception: {}",
                        telnet.getRemoteAddress(), ioe.getMessage());
            }
        }
    }

    /**
     * 获取tsdb字段信息
     *
     * @param msg message信息
     * @param uid uid
     */
    public static String tsdbFields(String msg, String uid) {
        int idx = Math.min(msg.length(), MAX_MSG_LENGTH);
        String message = msg.substring(0, idx);

        return String.format("uid=\"%s\",message=\"%s\"",
                StringUtils.replace(uid, "\"", "|"),
                StringUtils.replace(message, "\"", "|"));
    }
}
