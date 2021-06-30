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

import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class FlinkOptionsTest {

  @Test
  public void toFinkArgsForYarnCluster() throws IOException {
    Configuration config = null;
    Map<String, String> jobNameAndIdMaps = new HashMap<>();
    String paramJson = "{"
            + "\"slot\": 1, "
            + "\"use_savepoint\": false, "
            + "\"container\": 1, "
            + "\"parallelism\": 1, "
            + "\"job_manager_memory\": 1024, "
            + "\"task_manager_memory\": 2048, "
            + "\"jar_file_name\": \"flink-code-0.0.1.jar\", "
            + "\"custom_jar_file\": \"hdfs://xxx/xx2/sdk/user-code-0.0.1.jar\", "
            + "\"yarn_queue\": \"root.xxx\", "
            + "\"mode\": \"yarn-cluster\"}";
    Map<String, Object> map = JsonUtils.readMap(paramJson);
    map.put("argument", "");
    Map<String, Object> schedule = new HashMap<>();
    schedule.put("extra_info", JsonUtils.writeValueAsString(map));
    FlinkOptions options = new FlinkOptions(paramJson, config);
    String[] args = options.toFinkArgs(jobNameAndIdMaps, false);
    StringBuilder runJobArg = new StringBuilder();
    for (String arg : args) {
      runJobArg.append(arg).append(" ");
    }
    Assert.assertEquals(
            "-p 1 -ys 1 -ytm 2048 -yjm 1024 -yn 1 -yqu root.xxx -j null/adaptor/stream/job/flink-code-0.0.1.jar",
            runJobArg.toString().trim());
  }

  @Test
  public void toFinkArgsForYarnSession() throws IOException {
    Configuration config = null;
    Map<String, String> jobNameAndIdMaps = new HashMap<>();
    String paramJson = "{\n"
            + "    \"slot\":1,\n"
            + "    \"task_manager_memory\":1024,\n"
            + "    \"yarn_queue\":\"root.xx.xx.xx.session\",\n"
            + "    \"container\":1,\n"
            + "    \"job_manager_memory\":1024\n"
            + "}";
    FlinkOptions options = new FlinkOptions(paramJson, config);
    String[] args = options.toFinkArgs(jobNameAndIdMaps, false);
    StringBuilder runJobArg = new StringBuilder();
    for (String arg : args) {
      // 去掉yarn的前缀
      if (arg.startsWith("-y") && arg.length() > 2) {
        arg = "-" + arg.substring(2);
      }
      runJobArg.append(arg).append(" ");
    }
    Assert.assertEquals("-s 1 -tm 1024 -jm 1024 -n 1 -qu root.xx.xx.xx.session",
            runJobArg.toString().trim());
  }
}
