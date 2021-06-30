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


import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.errors.KeyGenException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public class ConnUtils {

    private static final Logger log = LoggerFactory.getLogger(ConnUtils.class);

    /**
     * 将密文的密码解密，返回原始密码
     *
     * @param cipherText 密文
     * @return 解密后的明文
     */
    public static String decodePass(String cipherText) {
        if (StringUtils.isNotBlank(cipherText)) {
            Map<String, String> clusterProps = BasicProps.getInstance().getClusterProps();
            if (clusterProps.containsKey(Consts.INSTANCE_KEY)) {
                String instanceKey = clusterProps.get(Consts.INSTANCE_KEY);
                String rootKey = clusterProps.getOrDefault(Consts.ROOT_KEY, "");
                String keyIV = clusterProps.getOrDefault(Consts.KEY_IV, "");
                try {
                    return new String(KeyGen.decrypt(cipherText, rootKey, keyIV, instanceKey),
                            StandardCharsets.ISO_8859_1);
                } catch (KeyGenException ex) {
                    LogUtils.warn(log, "decode pass failed! key: " + instanceKey + " cipher: " + cipherText, ex);
                }
            }
        }

        return cipherText;
    }

    /**
     * 初始化数据库连接对象并返回。当创建时发生异常时,等待retryBackoffMs时间,再重试。
     *
     * @param connUrl 数据库连接url
     * @param connUser 数据库账号
     * @param connPass 数据库密码
     * @param retryBackoffMs 重试等待时间(ms) (最多等待15s,超出时放弃创建数据库连接,返回null)
     * @return 数据库连接对象
     */
    public static Connection initConnection(String connUrl, String connUser, String connPass, long retryBackoffMs) {
        if (retryBackoffMs > 15000) { // 超出15s间隔时,直接放回null
            return null;
        }
        try {
            if (connUrl.contains(Consts.JDBC_CRATE)) {
                return DriverManager.getConnection(connUrl);
            } else if (connUrl.contains(Consts.JDBC_POSTGRESQL)) {
                Class.forName("org.postgresql.Driver");
                return DriverManager.getConnection(connUrl, connUser, connPass);
            } else {
                Class.forName("com.mysql.jdbc.Driver");
                return DriverManager.getConnection(connUrl, connUser, connPass);
            }
        } catch (SQLException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR,
                    "Failed to initialize connection for pool! " + connUrl + " " + connPass, e);
            try {
                Thread.sleep(retryBackoffMs);
            } catch (InterruptedException ignore) {
                LogUtils.warn(log, "get interrupted exception!", e);
            }
            return initConnection(connUrl, connUser, connPass, retryBackoffMs * 2);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    /**
     * 关闭数据库连接
     *
     * @param conn 数据库连接对象
     */
    public static void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignore) {
                LogUtils.warn(log, "get sql exception!", ignore);
            }
        }
    }
}
