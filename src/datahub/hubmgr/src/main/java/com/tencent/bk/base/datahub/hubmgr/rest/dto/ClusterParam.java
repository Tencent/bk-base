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

package com.tencent.bk.base.datahub.hubmgr.rest.dto;


import com.tencent.bk.base.datahub.cache.CacheConsts;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import java.util.Arrays;
import java.util.HashMap;
import javax.validation.constraints.NotBlank;


public class ClusterParam {

    // 服务端连接参数，默认格式为 ip:port;user=xxxx;password=xxxx;
    @NotBlank
    public String url;

    @NotBlank
    public String clusterName;

    public ClusterParam(String clusterName, String url) {
        this.clusterName = clusterName;
        this.url = url;
    }

    public ClusterParam() {
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    // 获取连接参数
    public String getUrl() {
        return url;
    }

    // 获取host
    public String getHost() {
        return url != null ? url.split(":")[0] : CacheConsts.IGNITE_HOST_DFT;
    }

    /**
     * 获取端口
     *
     * @return 端口
     */
    public String getPort() {
        String[] urlArray = url.split(";");
        if (urlArray.length < 1) {
            return CacheConsts.IGNITE_PORT_DFT;
        }
        String[] paramArray = Arrays.copyOfRange(urlArray, 0, 1);
        return paramArray[0].split(":").length > 1 ? paramArray[0].split(":")[1] : CacheConsts.IGNITE_PORT_DFT;
    }

    /**
     * 获取用户
     *
     * @return 用户
     */
    public String getUser() {
        String user = getParams("user");
        return user == null ? CacheConsts.IGNITE_USER_DFT : user;
    }

    /**
     * sdk 会先解密，所以需要再次加密
     *
     * @return 加密密码
     */
    public String getPassword() {
        String pass = getParams("password");
        return pass == null ? CacheConsts.IGNITE_PASS_DFT : pass;
    }

    private String getParams(String key) {
        String[] urlArray = url.split(";");
        if (urlArray.length == 1) {
            return null;
        }
        String[] paramArray = Arrays.copyOfRange(urlArray, 1, urlArray.length);
        HashMap<String, String> params = new HashMap<>();
        for (String param : paramArray) {
            String[] keyValue = param.split("=", 2);
            if (keyValue.length > 1) {
                params.put(keyValue[0], keyValue[1]);
            }
        }
        return params.get(key);
    }

    /**
     * 获取集群配置
     *
     * @return 返回集群配置
     */
    public HashMap<String, String> getConf() {
        HashMap<String, String> conf = new HashMap<>();
        conf.put("ignite.host", getHost());
        conf.put("ignite.port", getPort());
        conf.put("ignite.user", getUser());
        conf.put("ignite.pass", getPassword());
        conf.put("cache.primary", CacheConsts.IGNITE);
        conf.put("ignite.cluster", getClusterName());
        conf.put("instance.key", DatabusProps.getInstance().getOrDefault(Consts.INSTANCE_KEY, ""));
        return conf;
    }

    public void setUrl(String url) {
        this.url = url;
    }

}
