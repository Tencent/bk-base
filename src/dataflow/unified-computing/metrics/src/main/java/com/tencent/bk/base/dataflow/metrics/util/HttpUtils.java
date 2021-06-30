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

package com.tencent.bk.base.dataflow.metrics.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class HttpUtils {

  /**
   * send http post request
   *
   * @param httpUrl
   * @param body    user data to send
   * @return http post result
   */
  public static String post(String httpUrl, String body) throws Exception {
    HttpURLConnection connection = null;
    try {
      URL url = new URL(httpUrl);
      connection = (HttpURLConnection) url.openConnection();
      connection.setDoOutput(true);
      connection.setDoInput(true);
      connection.setInstanceFollowRedirects(false);
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Accept", "application/json");
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setRequestProperty("charset", "utf-8");
      connection.setRequestProperty("maxPostSize", "0");
      connection.setRequestProperty("Content-Length", String.valueOf(body.getBytes(StandardCharsets.UTF_8).length));
      connection.setUseCaches(false);
      connection.setConnectTimeout(10000);
      connection.setReadTimeout(10000);
      try (DataOutputStream dos = new DataOutputStream(connection.getOutputStream())) {
        dos.write(body.getBytes(StandardCharsets.UTF_8));
        dos.flush();
      }
      return handleResponse(connection);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  /**
   * handle http post response
   *
   * @param connection
   * @throws Exception
   */
  private static String handleResponse(HttpURLConnection connection) throws Exception {
    int responseCode = connection.getResponseCode();
    if (responseCode >= 200 && responseCode < 300) {
      return readAll(connection.getInputStream());
    } else {
      throw new HttpError(responseCode, readAll(connection.getErrorStream()));
    }
  }

  /**
   * read http response msg
   *
   * @param inputStream
   * @return http response msg as a string
   * @throws Exception
   */
  private static String readAll(InputStream inputStream) throws Exception {
    if (null == inputStream) {
      return null;
    }
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF8"));
      StringBuilder stringBuilder = new StringBuilder();
      String line;
      while (null != (line = reader.readLine())) {
        stringBuilder.append(line);
        stringBuilder.append("\n");
      }
      return stringBuilder.toString();
    } finally {
      inputStream.close();
    }
  }

  public static class HttpError extends RuntimeException {
    public final int responseCode;
    public final String output;

    /**
     * HttpError
     *
     * @param responseCode
     * @param output
     */
    public HttpError(int responseCode, String output) {
      super("[" + responseCode + "]: " + output);
      this.responseCode = responseCode;
      this.output = output;
    }

    /**
     * toString
     *
     * @return java.lang.String
     */
    @Override
    public String toString() {
      return "Error{" + "responseCode=" + responseCode + ", output='" + output + '\'' + '}';
    }
  }
}
