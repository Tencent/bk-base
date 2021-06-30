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

import java.util.Map;

public class DataPlatformMetricTest {
  /**
   * DataPlatformMetricTest main
   *
   * @param args
   */
  public static void main(String[] args) throws InterruptedException {
    // 构建DataPlatformMetric对象
    // periodicRepostIntervalSec：上报指标数据间隔（单位：秒）
    //      >0： 自动按照配置对间隔上报到kafka（使用方无需关心上报过程）
    //      <=0： 使用方手动触发上报到kafka，同时可获取到指标序列化后到字符串
    DataPlatformMetric.Builder builder = new DataPlatformMetric.Builder(
        1, null, null, "console");
    DataPlatformMetric dataPlatformMetric = builder
        .module("stream")
        .component("jobmanager")
        .cluster("cluster")
        .storageHost("127.0.0.1")
        .storagePort(1000)
        .physicalTag("topology_id", "topology-xx")
        .physicalTag("task_id", 1)
        .logicalTag("result_table_id", "rt1")
        .logicalTag("task_id", 1)
        .customTag("ip", "0.0.0.0")
        .customTag("port", "8888")
        .customTag("task", 1)
        .build();

    // ============data_loss.input============
    dataPlatformMetric.getInputTotalCntCounter().inc(3000);
    dataPlatformMetric.getInputTotalCntIncrementCounter().inc(400);
    dataPlatformMetric.getInputTagsCounter("rt1", 1580009090).inc(100);
    dataPlatformMetric.getInputTagsCounter("batch_rt1", 1580006090, 1580009090)
        .inc(100);
    dataPlatformMetric.getInputSegmentsCounter("rt1").inc(500);
    dataPlatformMetric.getInputSegmentsCounter("rt2").inc(1000);
    dataPlatformMetric.getInputSegmentsCounter("rt3").inc(1500);

    // ============data_loss.output============
    dataPlatformMetric.getOutputTotalCntCounter().inc(3000);
    dataPlatformMetric.getOutputTotalCntIncrementCounter().inc(4000);
    dataPlatformMetric.getOutputCkpDropCounter().inc(5);
    dataPlatformMetric.getOutputTagsCounter("rt1", 1580019239).inc(100);
    dataPlatformMetric.getOutputTagsCounter("batch_rt1", 1580016239, 1580019239)
        .inc(100);
    dataPlatformMetric.getOutputCkpDropTagsCounter("rt1", 1580019239).inc(2);
    dataPlatformMetric.getOutputSegmentsCounter("rt1").inc(500);
    dataPlatformMetric.getOutputSegmentsCounter("rt2").inc(1000);
    dataPlatformMetric.getOutputSegmentsCounter("rt3").inc(1500);

    // ============data_loss.data_drop============
    dataPlatformMetric.getDataDropTagsCounter("drop_code1", "数据解析失败").inc(5);

    // ============data_delay============
    dataPlatformMetric.getDelayWindowTimeCounter().inc(60);
    dataPlatformMetric.getDelayWaitingTimeCounter().inc(30);
    dataPlatformMetric.setDelayMinDelayLiteral(157000000);
    dataPlatformMetric.setDelayMaxDelayLiteral(158000000);

    // ============data_profiling============
    dataPlatformMetric.getDataMalformedCounter("field1", "数据结构异常").inc(1000);

    // ============resource_monitor============
    dataPlatformMetric.counter("metrics.resource_monitor.stream_flink_mem.total").inc(1000);
    dataPlatformMetric.counter("metrics.resource_monitor.stream_flink_mem.used").inc(800);
    dataPlatformMetric.literal("metrics.resource_monitor.stream_flink_mem.tags.thread_id")
        .setLiteral("thread1");

    // ============custom_metrics============
    dataPlatformMetric.counter("metrics.custom_metrics.stream_flink_custom_metric.field1").inc(100);
    dataPlatformMetric.counter("metrics.custom_metrics.stream_flink_custom_metric.field2").inc(200);
    dataPlatformMetric.literal("metrics.custom_metrics.stream_flink_custom_metric.tags.tag1")
        .setLiteral("val");

    Thread.sleep(3000);

    // ============手动上报，建议普通场景无需使用，仅限特殊场景使用============
    Map<String, String> res = dataPlatformMetric.getMetricsStringAndReport();
    for (Map.Entry<String, String> entry : res.entrySet()) {
      System.out.println("=====manually report:\n" + entry.getValue());
    }

    // ============关闭metric============
    dataPlatformMetric.close();
  }
}
