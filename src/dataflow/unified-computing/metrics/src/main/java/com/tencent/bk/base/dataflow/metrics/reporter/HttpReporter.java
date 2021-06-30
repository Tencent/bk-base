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

package com.tencent.bk.base.dataflow.metrics.reporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.tencent.bk.base.dataflow.metrics.Counter;
import com.tencent.bk.base.dataflow.metrics.Gauge;
import com.tencent.bk.base.dataflow.metrics.Literal;
import com.tencent.bk.base.dataflow.metrics.MetricFilter;
import com.tencent.bk.base.dataflow.metrics.MetricsModule;
import com.tencent.bk.base.dataflow.metrics.registry.MetricRegistry;
import com.tencent.bk.base.dataflow.metrics.util.MapUtils;
import com.tencent.bk.base.dataflow.metrics.util.HttpUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * console reporter
 */
public class HttpReporter extends AbstractScheduledReporter {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final String reporterNamePrefix = "http_reporter";
  private final String url;

  /**
   * HttpReporter
   *
   * @param filter
   * @param url
   * @param periodicReportFlag
   */
  public HttpReporter(MetricFilter filter,
                      String url,
                      boolean periodicReportFlag) {
    super(reporterNamePrefix, filter, periodicReportFlag);
    this.url = url;
    // custom metric serialization, and register into json mapper
    MetricsModule module = new MetricsModule(filter);
    mapper.registerModule(module);
    mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
//    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          stop();
        } catch (Exception ex) {
          // do nothing
        }
      }
    }));
  }

  @Override
  public void stop() {
    super.stop();
  }

  @SuppressWarnings("rawtypes")
  public String getMetricsStringAndReport(SortedMap<String, Counter> counters,
                                          SortedMap<String, Literal> literals,
                                          SortedMap<String, Gauge> gauges) {
    String msgContentStr = "";
    Map<String, Object> resultMap = new LinkedHashMap<>();
    MapUtils.generateNestedMap(literalPrefix, literals, resultMap);
    MapUtils.generateNestedMap(counterPrefix, counters, resultMap);
    MapUtils.generateNestedMap(gaugePrefix, gauges, resultMap);

    try {
      msgContentStr = mapper.writeValueAsString(resultMap);
      HttpUtils.post(url, msgContentStr);
    } catch (Exception ex) {
      // do nothing
    }
    return msgContentStr;
  }

  @Override
  public Map<String, String> getMetricsStringAndReport(Map<String, MetricRegistry> registryMap) {
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, MetricRegistry> entry : registryMap.entrySet()) {
      String oneRes = getMetricsStringAndReport(
          entry.getValue().getCounters(),
          entry.getValue().getLiterals(),
          entry.getValue().getGauges());
      result.put(entry.getKey(), oneRes);
    }
    return result;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void report(SortedMap<String, Counter> counters,
                     SortedMap<String, Literal> literals,
                     SortedMap<String, Gauge> gauges) {
    try {
      String msgContentStr = getMetricsStringAndReport(counters, literals, gauges);
      HttpUtils.post(url, msgContentStr);
    } catch (Exception e) {
      // do nothing
    }
  }

  @Override
  public void report(Map<String, MetricRegistry> registryMap) {
    for (Map.Entry<String, MetricRegistry> entry : registryMap.entrySet()) {
      report(entry.getValue().getCounters(),
          entry.getValue().getLiterals(),
          entry.getValue().getGauges());
    }
  }
}