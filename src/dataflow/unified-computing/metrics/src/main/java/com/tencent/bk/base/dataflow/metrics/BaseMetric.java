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

import com.tencent.bk.base.dataflow.metrics.registry.MetricRegistry;
import com.tencent.bk.base.dataflow.metrics.reporter.ConsoleReporter;
import com.tencent.bk.base.dataflow.metrics.reporter.HttpReporter;
import com.tencent.bk.base.dataflow.metrics.reporter.KafkaReporter;
import com.tencent.bk.base.dataflow.metrics.reporter.AbstractScheduledReporter;
import com.tencent.bk.base.dataflow.metrics.util.Constants;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class BaseMetric {
  protected static AbstractScheduledReporter reporter;
  protected MetricRegistry metricRegistry;

  /**
   * BaseMetric
   */
  public BaseMetric() {
    this(Constants.DEFAULT_PERIODIC_REPORT_SEC);
  }

  /**
   * 构造BaseMetric
   *
   * @param periodicReportIntervalSec 定期上报间隔，单位：秒，小于或等于0时，不自动上报
   */
  public BaseMetric(int periodicReportIntervalSec) {
    this(periodicReportIntervalSec,
        Constants.DEFAULT_KAFKA_HOSTS, Constants.DEFAULT_TOPIC, Constants.REPORTER_TYPE_KAFKA);
  }

  /**
   * 构造BaseMetric
   *
   * @param periodicReportIntervalSec 定期上报间隔，单位：秒，小于或等于0时，不自动上报
   * @param targetUrl                 kafka 类型：上报的 kafka host ； http 类型： url
   * @param topic                     上报的topic
   */
  public BaseMetric(int periodicReportIntervalSec,
                    String targetUrl, String topic) {
    this(periodicReportIntervalSec, targetUrl, topic, Constants.DEFAULT_REPORTER_TYPE);
  }

  /**
   * 构造BaseMetric
   *
   * @param periodicReportIntervalSec 定期上报间隔，单位：秒，小于或等于0时，不自动上报
   * @param targetUrl                 kafka 类型：上报的 kafka host ； http 类型： url
   * @param topic                     上报的topic
   * @param reportType                数据上报目的端的类型：kafka / http / console (only for debug)
   */
  public BaseMetric(int periodicReportIntervalSec,
                    String targetUrl, String topic, String reportType) {
    metricRegistry = new MetricRegistry();

    synchronized (BaseMetric.class) {
      if (reporter == null) {
        if (reportType.equalsIgnoreCase(Constants.REPORTER_TYPE_KAFKA)) {
          // init reporter
          Properties reporterConfigProperties = loadDefaultKafkaConfig(targetUrl);
          if (topic == null || topic.isEmpty()) {
            topic = Constants.DEFAULT_TOPIC;
          }
          reporter = new KafkaReporter(
              MetricFilter.ALL,
              topic,
              reporterConfigProperties,
              periodicReportIntervalSec > 0);
        } else if (reportType.equalsIgnoreCase(Constants.REPORTER_TYPE_HTTP)) {
          reporter = new HttpReporter(
              MetricFilter.ALL,
              targetUrl,
              periodicReportIntervalSec > 0);
        } else {
          reporter = new ConsoleReporter(
              MetricFilter.ALL,
              periodicReportIntervalSec > 0);
        }

        if (periodicReportIntervalSec > 0) {
          reporter.start(periodicReportIntervalSec, TimeUnit.SECONDS);
        }
      }
    }

    reporter.addMetricRegistry(metricRegistry);

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          shutdown();
        } catch (Exception ex) {
          // do nothing
        }
      }
    }));
  }

  /**
   * 设置所有 counter 的前缀，全局生效
   *
   * @param prefix 所有 counter 前缀字符串
   **/
  public void setCountersPrefix(String prefix) {
    reporter.setCountersPrefix(prefix);
  }

  /**
   * 设置所有 literal 的前缀，全局生效
   *
   * @param prefix 所有 literal 前缀字符串
   */
  public void setLiteralsPrefix(String prefix) {
    reporter.setLiteralsPrefix(prefix);
  }

  /**
   * 设置所有 gauge 的前缀，全局生效
   *
   * @param prefix 所有 gauge 前缀字符串
   */
  public void setGaugesPrefix(String prefix) {
    reporter.setGaugesPrefix(prefix);
  }

  /**
   * 关闭 BaseMetric
   */
  public void close() {
    if (reporter != null) {
      reporter.removeMetricRegistry(metricRegistry.getMetricRegistryName());
    }
  }

  /**
   * 关闭 BaseMetric
   */
  public void shutdown() {
    if (reporter != null) {
      reporter.close();
    }
  }

  /**
   * 删除指定的 metric
   *
   * @param name 指定 metric 名称
   **/
  public void removeMetric(String name) {
    metricRegistry.remove(name);
  }

  /**
   * 清理所有 metrics
   **/
  public void clearMetrics() {
    metricRegistry.removeMatching(MetricFilter.ALL);
  }

  /**
   * 清理指定类型的 metric
   *
   * @param filter 特定类型的过滤器，starsWith, endsWith, contains三种
   **/
  public void clearMetrics(MetricFilter filter) {
    metricRegistry.removeMatching(filter);
  }

  /**
   * 重置指定名称的 counter 为0
   *
   * @param name 指定 counter 名称
   **/
  public void resetCounter(String name) {
    metricRegistry.resetCounter(name);
  }

  /**
   * 重置所有的 counter 为0
   **/
  public void resetCounters() {
    metricRegistry.resetCounters(MetricFilter.ALL);
  }

  private Properties loadDefaultKafkaConfig(String kafkaHosts) {
    Properties properties = new Properties();
    if (kafkaHosts == null || kafkaHosts.isEmpty()) {
      kafkaHosts = Constants.DEFAULT_KAFKA_HOSTS;
    }
    properties.put("bootstrap.servers", kafkaHosts);
    properties.put("acks", "1");
    properties.put("retries", 5);
    properties.put("max.in.flight.requests.per.connection", 5);
    properties.put("batch.size", 1048576);
    properties.put("linger.ms", 500);
    properties.put("buffer.memory", 52428800);
    properties.put("max.request.size", 52428800);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return properties;
  }

  /**
   * 获取指定名称的 counter
   *
   * @param name 指定 counter 名称
   * @return com.tencent.bk.base.metrics.Counter
   **/
  public Counter counter(int name) {
    return counter(String.valueOf(name));
  }

  /**
   * 获取指定名称的 counter
   *
   * @param name 指定 counter 名称
   * @return com.tencent.bk.base.metrics.Counter
   **/
  public Counter counter(String name) {
    return metricRegistry.counter(name.trim());
  }

  /**
   * 获取指定名称的 literal
   *
   * @param name 指定 literal 名称
   * @return com.tencent.bk.base.metrics.Literal
   **/
  public Literal literal(int name) {
    return literal(String.valueOf(name));
  }

  /**
   * 获取指定名称的 literal
   *
   * @param name 指定 literal 名称
   * @return com.tencent.bk.base.metrics.Literal
   **/
  public Literal literal(String name) {
    return metricRegistry.literal(name.trim());
  }

  /***
   * 获取指定名称的 gauge
   *
   * @param name  指定 gauge 名称
   * @param gauge 指定 gauge
   * @return com.tencent.bk.base.metrics.Gauge
   **/
  @SuppressWarnings("rawtypes")
  public Gauge gauge(String name, Gauge gauge) {
    return metricRegistry.register(name.trim(), gauge);
  }

  /**
   * 获取指定名称的 gauge
   *
   * @param name  指定 gauge 名称
   * @param gauge 指定 gauge
   * @return com.tencent.bk.base.metrics.Gauge
   **/
  @SuppressWarnings("rawtypes")
  public Gauge gauge(int name, Gauge gauge) {
    return gauge(String.valueOf(name), gauge);
  }

  /**
   * 获取所有 metric 的格式化字符串，并同时后台进行上报
   *
   * @return java.util.Map_java.lang.String, java.lang.String
   **/
  public Map<String, String> getMetricsStringAndReport() {
    return reporter.getMetricsStringAndReport();
  }

  private String getCurrentRegistryName() {
    return metricRegistry.getMetricRegistryName();
  }

  /**
   * 获取当前 metric 的格式化字符串，并同时后台进行上报
   *
   * @return java.util.Map_java.lang.String, java.lang.String
   **/
  public String getCurrentMetricsStringAndReport() {
    return reporter.getMetricsStringAndReport(getCurrentRegistryName());
  }
}
