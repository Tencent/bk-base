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

package com.tencent.bk.base.datahub.databus.commons.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;
import com.tencent.bk.base.datahub.databus.commons.errors.ApiException;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtils {

    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);


    /**
     * 通过http接口获取resultTable的相关配置信息,放在map中返回。
     *
     * @param rtId resultTableId
     * @return 包含rt配置信息的map
     */
    public static Map<String, String> getRtInfo(String rtId) {
        String url = BasicProps.getInstance().getRtInfoUrl() + "?rt_id=" + rtId;
        return getApiResultAsMap(url);
    }


    /**
     * 通过http接口获取某个总线集群的配置信息,包含相关kafka地址、rest端口、topic名称、
     * consumer相关配置信息等
     *
     * @param clusterName 集群名称
     * @return 集群配置
     */
    public static Map<String, String> getClusterConfig(String clusterName) {
        String url = BasicProps.getInstance().getClusterInfoUrl() + "?cluster_name=" + clusterName;
        return getApiResultAsMap(url);
    }


    /**
     * 通过http接口获取resultTable的固化节点处理配置
     *
     * @param rtId resultTableId
     * @return 包含rt固化节点配置信息的map
     */
    public static Map<String, String> getDatanodeConfig(String rtId) {
        String url = BasicProps.getInstance().getDatanodeInfoUrl() + rtId + "/";
        return getApiResultAsMap(url);
    }

    /**
     * 通过http接口获取dataids到topics的映射关系,找到指定的dataid应该写入的kafka topic名称。
     *
     * @param dataIds dataids的列表,逗号分隔
     * @return dataids到topics的映射
     */
    public static Map<String, String> getDataidsTopics(String dataIds) {
        String url = BasicProps.getInstance().getDataidsTopicsUrl();
        return getApiResultAsMap(url);
    }

    /**
     * 获取离线任务的信息
     *
     * @param rtId result_table_id
     * @param type 存储类型
     * @return 接口返回结果
     */
    public static Map<String, String> getOfflineTaskInfo(String rtId, String type) {
        String url = BasicProps.getInstance().getOfflineTaskInfoUrl() + "?rt_id=" + rtId + "&storage_type=" + type;
        return getApiResultAsMap(url);
    }

    /**
     * 按照存储类型，获取离线任务列表
     *
     * @param type 存储类型
     * @return 离线任务列表
     */
    public static Map<String, String> getOfflineTasks(String type) {
        String url = BasicProps.getInstance().getOfflineTasksUrl() + "?storage_type=" + type;
        return getApiResultAsMap(url);
    }

    /**
     * 更新离线任务的进度
     *
     * @param rtId result_table_id
     * @param dataDir 数据文件的目录
     * @param type 存储类型
     * @param status 处理进度
     * @return 接口返回结果
     */
    public static Map<String, String> updateOfflineTaskInfo(String rtId, String dataDir, String type, String status) {
        String url = BasicProps.getInstance().getUpdateOfflineTaskUrl() + "?rt_id=" + rtId + "&data_dir=" + dataDir
                + "&storage_type=" + type + "&status_update=" + status;
        return getApiResultAsMap(url);
    }

    /**
     * 标记离线任务为完成
     *
     * @param rtId result_table_id
     * @param dataDir 数据文件的目录
     * @param type 存储类型
     * @return 接口返回结果
     */
    public static Map<String, String> markOfflineTaskFinish(String rtId, String dataDir, String type) {
        String url = BasicProps.getInstance().getFinishOfflineTaskUrl() + "?rt_id=" + rtId + "&data_dir=" + dataDir
                + "&storage_type=" + type;
        return getApiResultAsMap(url);
    }

    /**
     * 调用接口，记录总线存储事件
     *
     * @param rtId rtId
     * @param storage 存储类型
     * @param eventType 事件类型
     * @param eventValue 事件值
     * @return 是否添加成功 true/false
     */
    public static boolean addDatabusStorageEvent(String rtId, String storage, String eventType, String eventValue) {
        Map<String, String> params = new HashMap<>();
        params.put(Consts.RESULT_TABLE_ID, rtId);
        params.put(Consts.STORAGE, storage);
        params.put(Consts.EVENT_TYPE, eventType);
        params.put(Consts.EVENT_VALUE, eventValue);
        String url = BasicProps.getInstance().getAddDatabusStorageEventUrl();
        return postAndCheck(url, params);
    }

    /**
     * 通过http接口获取hdfs导入任务列表
     *
     * @return hdfs的导入任务列表
     */
    public static List<Map<String, Object>> getHdfsImportTasks() {
        String url = BasicProps.getInstance().getEarliestHdfsImportTaskUrl();
        return getApiResultAsListMap(url);
    }

    /**
     * 通过http接口获取指定区域的hdfs导入任务列表
     *
     * @return hdfs的导入任务列表
     */
    public static List<Map<String, Object>> getHdfsImportTasks(String geogArea) {
        String url = BasicProps.getInstance().getEarliestHdfsImportTaskUrl() + "?geog_area=" + geogArea;
        return getApiResultAsListMap(url);
    }

    /**
     * 通过http接口更新hdfs导入任务的状态
     *
     * @param rtId result table id
     * @param dataDir 数据目录
     * @param status 任务状态
     * @param finished 是否结束
     * @return true/false, 更新成功与否
     */
    public static boolean updateHdfsImportTaskStatus(long id, String rtId, String dataDir, String status,
            boolean finished) {
        Map<String, Object> params = new HashMap<>();
        params.put(Consts.ID, id);
        params.put(Consts.RT_ID, rtId);
        params.put(Consts.DATA_DIR, dataDir);
        params.put(Consts.STATUS_UPDATE, status);
        String finish = finished ? "true" : "false";
        params.put(Consts.FINISH, finish);
        String url = BasicProps.getInstance().getUpdateHdfsImportTaskUrl();

        return postAndCheck(url, params);
    }

    /**
     * 通过http接口更新hdfs导入任务的状态
     *
     * @param rtId result table id
     * @param deltaDate 偏移天数
     * @return true/false, 更新成功与否
     */
    public static boolean updateHdfsMaintainDate(String rtId, long deltaDate) {
        String url =
                BasicProps.getInstance().getUpdateHdfsMaintainDateUrl() + rtId + "/maintain/?delta_day=" + deltaDate;
        try {
            String result = get(url);
            LogUtils.info(log, "url {}, result {}", url, result);
            ApiResult apiResult = JsonUtils.parseApiResult(result);
            if (apiResult.isResult()) {
                return true;
            }
            LogUtils.warn(log, "bad response..." + result);
        } catch (Exception e) {
            LogUtils.warn(log, "request {} failed", url);
        }
        return false;
    }

    /**
     * 通过接口获取kafka集群对应的机器IP列表
     *
     * @param kafkaBsServers kafka集群的地址
     * @return 集群机器的IP列表
     */
    public static List<String> getKafkaHosts(String kafkaBsServers) {
        List<String> hosts = new ArrayList<>();
        try {
            String[] arr = StringUtils.split(kafkaBsServers, ".");
            String url = BasicProps.getInstance().getConsulServiceUrl() + "?service_name=" + arr[0] + "&idc=" + arr[2];
            String result = get(url);
            LogUtils.debug(log, "url {}, result {}", url, result);

            ApiResult apiResult = JsonUtils.parseApiResult(result);
            if (apiResult.isResult()) {
                List<Map<String, Object>> jsonArr = (List<Map<String, Object>>) apiResult.getData();
                jsonArr.forEach(m -> hosts.add(m.get("IP").toString()));
            } else {
                LogUtils.warn(log, "bad response..." + result);
            }
        } catch (Exception ignore) {
            LogUtils.warn(log, "failed to get kafka hosts for {}", kafkaBsServers);
        }

        return hosts;
    }

    /**
     * 通过GET方法请求http接口，将返回结果解析为ApiResult对象
     *
     * @param restUrl http的资源地址，通过GET方法请求
     * @return ApiResult对象
     * @throws ApiException API请求异常
     */
    public static ApiResult getApiResult(String restUrl) throws ApiException {
        String response = get(restUrl);
        LogUtils.debug(log, "request url {}, response {}", restUrl, response);
        return JsonUtils.parseApiResult(response);
    }

    /**
     * 请求pizza的接口,解析返回结果,将返回结果中的data包含的内容转换为map对象返回。
     * 当返回结果为不成功时,抛出异常。
     *
     * @param restUrl 请求的url
     * @return map对象, 包含接口返回值data得内容。
     */
    public static Map<String, String> getApiResultAsMap(String restUrl) {
        try {
            String result = get(restUrl);
            LogUtils.debug(log, "request url {}, response {}", restUrl, result);
            return parseApiResult(result);
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_RESPONSE, restUrl + " get bad response",
                    e);
            throw new ConnectException("exception during request handling...", e);
        }
    }

    /**
     * 通过get方式请求指定的url，将返回结果解析为map的list结构并返回。
     *
     * @param restUrl 请求的url地址
     * @return map的list结果
     */
    public static List<Map<String, Object>> getApiResultAsListMap(String restUrl) {
        try {
            String result = get(restUrl);
            LogUtils.debug(log, "request url {}, response {}", restUrl, result);
            return parseApiResultAsListMap(result);
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_RESPONSE, restUrl + " get bad response",
                    e);
            throw new ConnectException("exception during request handling...", e);
        }
    }

    /**
     * 将api返回结果进行解析，解析为map的列表，然后返回
     *
     * @param response api的返回结果
     * @return map的列表
     * @throws IOException 异常
     */
    public static List<Map<String, Object>> parseApiResultAsListMap(String response) {
        ApiResult result = JsonUtils.parseApiResult(response);
        if (result.isResult()) {
            try {
                return (List<Map<String, Object>>) result.getData();
            } catch (Exception e) {
                LogUtils.warn(log, "bad api response result: " + response, e);
            }
        }

        // bad response
        throw new ConnectException("bad api response result: " + response);
    }

    /**
     * 解析API返回的结果，将结果中data字段里的内容作为map结构解析返回
     *
     * @param response API返回的结果字符串
     * @return API返回结果中data字段转换为Map结构的数据
     * @throws IOException 异常
     */
    public static Map<String, String> parseApiResult(String response) throws IOException {
        JsonNode json = JsonUtils.MAPPER.readTree(response);
        if (json.get("result").asBoolean()) {
            json = json.get("data");
            Map<String, String> map = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> iter = json.fields();
            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                JsonNode jsonNode = entry.getValue();
                String valueString = jsonNode instanceof ObjectNode ? jsonNode.toString() : jsonNode.asText();
                map.put(entry.getKey(), valueString);
            }

            return map;
        } else {
            LogUtils.warn(log, "bad response... {}", response);
            throw new ConnectException("bad response " + response);
        }
    }

    /**
     * Post请求，并验证post返回的结果
     *
     * @param url post的url地址
     * @param params post的参数
     * @return true/false，是否请求成功
     */
    public static boolean postAndCheck(String url, Map params) {
        try {
            String res = post(url, params);
            ApiResult result = JsonUtils.parseApiResult(res);
            if (result.isResult()) {
                return true;
            } else {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.DATABUS_API_ERR, log,
                        "Bad databus API response. url {}, params {}, response {}", url, params, res);
            }
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.DATABUS_API_ERR,
                    String.format("Bad databus API call. url %s, params %s", url, params), e);
        }
        return false;
    }

    /**
     * Post data with params in a map. return the response string.
     *
     * @param request request url
     * @param param request params in map
     * @return the response content
     */
    public static String post(String request, Map param) {
        try {
            String body = JsonUtils.toJson(param);
            return post(request, body);
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.JSON_FORMAT_ERR,
                    "Failed to compose a json string to post in http! " + param.toString(), e);
            throw new ConnectException("Failed to compose a json string to post in http! ", e);
        }
    }

    /**
     * Post a json string to the request url, and return the response content in a string.
     *
     * @param request the request url
     * @param body the request body in a json format
     * @return the response content
     */
    public static String post(String request, String body) {
        HttpURLConnection connection = prepareConnection(request, "POST", body, null);
        String result = "";
        DataOutputStream wr = null;
        try {
            wr = new DataOutputStream(connection.getOutputStream());
            wr.writeBytes(body);
            wr.flush();
            wr.close();
            result = handleResponse(connection);
            connection.disconnect();
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_RESPONSE,
                    String.format("Failed to post data in http request! %s %s", request, body), e);
            throw new ConnectException("Failed to post data in http request! ", e);
        } finally {
            if (null != wr) {
                try {
                    wr.close();
                } catch (IOException e) {
                    LogUtils.warn(log, "Failed to close IO!", e);
                }
            }
        }

        return result;
    }

    /**
     * Return the http response content for a url by using the http get method.
     *
     * @param request request url
     * @return the response content
     */
    public static String get(String request) {
        HttpURLConnection connection = prepareConnection(request, "GET", "", null);
        String result = handleResponse(connection);
        connection.disconnect();
        return result;
    }

    /**
     * Return the http response content for a url by using the http get method with headers
     *
     * @param request request url
     * @param headers headers
     * @return the response content
     */
    public static String get(String request, Map<String, String> headers) {
        HttpURLConnection connection = prepareConnection(request, "GET", "", headers);
        String result = handleResponse(connection);
        connection.disconnect();
        return result;
    }

    /**
     * Construct the connection object based on request url, method and params.
     * This connection uses json as data format.
     *
     * @param request the request url
     * @param method the http request method
     * @param params the params to use in request
     * @param headers headers
     * @return the connection object
     */
    private static HttpURLConnection prepareConnection(String request, String method, String params,
            Map<String, String> headers) {
        HttpURLConnection connection;
        try {
            URL url = new URL(request);
            connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setInstanceFollowRedirects(false);
            connection.setRequestMethod(method);
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("charset", "utf-8");
            if (headers != null && !headers.isEmpty()) {
                headers.forEach(connection::setRequestProperty);
            }
            connection.setUseCaches(false);
            connection.setConnectTimeout(3000);
            connection.setReadTimeout(600000);
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HTTP_CONNECTION_FAIL,
                    String.format("Failed to create http connection to handle request!! %s %s %s", request, method,
                            params), e);
            throw new ConnectException("Failed to create http connection to handle request!!", e);
        }

        return connection;
    }

    /**
     * Handling the response, get out the response content and return. For bad responses, just throw a runtime
     * exception.
     *
     * @param connection the http url connection
     * @return the response content
     */
    private static String handleResponse(HttpURLConnection connection) {
        StringBuilder sb = new StringBuilder();
        int responseCode = 0;
        BufferedReader reader = null;
        try {
            responseCode = connection.getResponseCode();
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF8"));
            String line = "";
            while (null != (line = reader.readLine())) {
                sb.append(line);
            }
            if (responseCode < 200 || responseCode >= 300) { // bad response
                String errorMsg = "Bad response! response code: " + responseCode + "  response: " + sb.toString();
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.BAD_RESPONSE, log, errorMsg);
                throw new ConnectException(errorMsg);
            }
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_RESPONSE,
                    "IOException during response handling!!", e);
            throw new ConnectException("IOException during response handling!!", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ignore) {
                    LogUtils.debug(log, "failed to close buffered reader!", ignore);
                }
            }
        }

        return sb.toString();
    }

    /**
     * Post data with params in a map. return the response string.
     *
     * @param request request url
     * @param param request params in map
     * @return the response content
     */
    public static String postSSL(String request, Map param) {
        try {
            String body = JsonUtils.toJson(param);
            return doSSLPost(request, body);
        } catch (IOException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.JSON_FORMAT_ERR,
                    "Failed to compose a json string to post in http! " + param.toString(), e);
            throw new ConnectException("Failed to compose a json string to post in http! ", e);
        }
    }

    /**
     * 发送带证书的post 请求
     *
     * @param request 请求的地址
     * @param body 请求的内容
     * @return 返回的结果
     */
    private static String doSSLPost(String request, String body) {
        CloseableHttpClient httpClient = null;
        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            // Trust own CA and all self-signed certs
            SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(keyStore, new TrustSelfSignedStrategy())
                    .build();
            // Allow SSL protocol only
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, null, null,
                    new AllowAllHostnameVerifier());
            httpClient = HttpClients.custom().setSSLSocketFactory(sslsf)
                    .setSchemePortResolver(new DefaultSchemePortResolver()).build();
            // 创建httpPost参数
            HttpPost httpPost = new HttpPost(request);
            ContentType contentType = ContentType.create("application/json", Consts.UTF8);
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setEntity(new StringEntity(body, contentType));
            // 处理response
            String response = httpClient.execute(httpPost, new BasicResponseHandler());
            httpClient.close();
            return response;
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.BAD_RESPONSE,
                    String.format("Failed to post data in http request! %s %s", request, body), e);
            throw new ConnectException("Failed to post data in http request! ", e);
        } finally {
            if (null != httpClient) {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    LogUtils.warn(log, "Failed to close IO!", e);
                }
            }
        }

    }
}
