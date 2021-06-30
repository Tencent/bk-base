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

package com.tencent.bk.base.dataflow.jobnavi.util.socket;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.SecureRandom;

public class NetUtil {

    /***
     *  true:already in using  false:not using
     * @param port
     */
    public static boolean isLocalPortBind(int port) {
        boolean flag = true;
        try {
            flag = isPortBind("127.0.0.1", port);
        } catch (Exception e) {
            //nothing to do
        }
        return flag;
    }

    /***
     *  true:already in using  false:not using
     * @param host
     * @param port
     * @throws UnknownHostException
     */
    public static boolean isPortBind(String host, int port) throws IOException {
        boolean flag = false;
        InetAddress theAddress = InetAddress.getByName(host);
        Socket socket = null;
        try {
            socket = new Socket(theAddress, port);
            flag = true;
        } catch (IOException e) {
            //nothing to do
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
        return flag;
    }

    public static int getUnboundPort(int minPort, int maxPort, int maxRetry) throws NaviException {
        SecureRandom secureRandom = new SecureRandom();
        for (int i = 0; i < maxRetry; i++) {
            int port = secureRandom.nextInt(maxPort - minPort) + minPort;
            if (!isLocalPortBind(port)) {
                return port;
            }
        }
        throw new NaviException("Port bind error after retry " + maxRetry + " times.");
    }
}
