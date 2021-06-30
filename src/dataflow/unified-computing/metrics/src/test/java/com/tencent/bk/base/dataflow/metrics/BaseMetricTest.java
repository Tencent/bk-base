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

package com.tencent.bk.base.dataflow.metrics;

import com.tencent.bk.base.dataflow.metrics.util.TimeUtils;

import java.util.HashMap;
import java.util.Map;

public class BaseMetricTest {
  /**
   * BaseMetricTest main
   *
   * @param args
   */
  public static void main(String[] args) throws InterruptedException {
    // 构建BaseMetric对象
    // periodicRepost -> 是否自动上报指标数据：
    //      true： 自动按照配置对间隔上报到kafka（使用方无需关心上报过程）
    //      false： 用户手动触发上报到kafka，同时可获取到指标序列化后到字符串
    BaseMetric baseMetric = new BaseMetric(1, null, null, "console");

    // 提供2种单例实现，满足不同的需求场景
//        BaseMetric baseMetric = SingletonBaseMetric.getInstance();
//        BaseMetric baseMetric = ThreadLocalSingletonBaseMetric.getInstance();

    // ===================================================================
    // 【可选】：添加metric前缀信息，序列化后会将指标信息附带在该前缀之后
    // 只对Counter生效，对Literal无效
    baseMetric.setCountersPrefix("metrics.data_monitor");
    baseMetric.setLiteralsPrefix("metrics.data_monitor");
    baseMetric.setGaugesPrefix("metrics.data_monitor");

    // ===================================================================
    // 【可选】：添加自定义信息，比如非数值类型的信息
    // "time": 1589018238,
    // "infoa": "xxxinfo",
    // "outter_key" : {
    //     "inner_key1" : "inner_value1",
    //     "inner_key2" : 2
    // }
    baseMetric.literal("time").setLiteral(new TimeUtils(null));
    baseMetric.literal("infoa").setLiteral("xxxinfo");
    baseMetric.literal("outter_key.inner_key1").setLiteral("inner_value1");
    baseMetric.literal("outter_key.inner_key2").setLiteral(2);

    // ===================================================================
    // 申请普通数值型counter，对counter进行加减操作
    // "flink" : {
    //    "taskmanager" : {
    //      "testcounter1" : 100,
    //      "testcounter2" : 200
    //    }
    //  }
    Counter counter1 = baseMetric.counter("flink.taskmanager.testcounter1");
    counter1.inc(100);
    Counter counter2 = baseMetric.counter("flink.taskmanager.testcounter2");
    counter2.inc(200);

    // ===================================================================
    // 非法counter名称，不能和已有的counter名称前缀匹配（按'.'分隔）;
    // 下述 counter3 == null;
    Counter counter3 = baseMetric.counter("flink.taskmanager");

    // ===================================================================
    // 可选：上述附带额外"标签"信息的counter，也可以通过下面的counter+literal来实现
    Counter counter5 = baseMetric.counter("flink.jobmanager.cpu2.cnt");
    baseMetric.literal("flink.jobmanager.cpu2.tags.code1").setLiteral("reason1");
    baseMetric.literal("flink.jobmanager.cpu2.tags.code2").setLiteral("reason2");
    counter5.inc(400);

    // ===================================================================
    // 生成手动进行reset类型的Counter
    Counter counter6 = baseMetric.counter("spark.executor.attempt");
    counter6.setResetWhenSerialized(false);
    counter6.inc(500);
    // 以下两种reset方式等价
    baseMetric.resetCounter("spark.executor.attempt");
    counter6.reset();

    Map<String, String> testMap = new HashMap<>();
    testMap.put("a", "b");
    testMap.put("c", "d");

    /*Gauge<Long> gauge = baseMetric.gauge("test.gauge.size", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return (long) testMap.size();
      }
    });*/


    // ===================================================================
    // 使用方只用关心对应的counter的使用，
    // 如果自动上报打开：上报操作后台线程进行，且上报同时对自动reset类型的counter进行reset
    // 如果自动上报未打开：手动调用获取指标序列化数据，且触发上报，同时对自动reset类型的counter进行reset
    for (int i = 0; i < 20; ++i) {
      // xxx 业务逻辑处理
      // ...
      counter1.inc(1);
      counter2.inc(2);
      counter5.inc(5);
      counter6.inc(6);
      Thread.sleep(1234);
    }

    // ===================================================================
    // 手动获取metric序列化的字符串（并且触发上报）
    //Map<String, String> msg = baseMetric.getMetricsStringAndReport();
    Thread.sleep(2000);

    // ===================================================================
    // 关闭metric
    // 需手动调用，释放内部相关资源
    baseMetric.close();
  }

}
