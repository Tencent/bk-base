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
import com.tencent.bk.base.dataflow.metrics.registry.MetricRegistry;
import com.tencent.bk.base.dataflow.metrics.util.Constants;
import com.tencent.bk.base.dataflow.metrics.util.TimeDeltaUtils;

import java.util.HashMap;
import java.util.Map;

public class DataPlatformMetric extends BaseMetric {

  /**
   * 构造默认的 DataPlatformMetric
   */
  public DataPlatformMetric() {
    this(Constants.DEFAULT_PERIODIC_REPORT_SEC);
  }

  /**
   * 构造上报间隔为 periodicRepostIntervalSec 的 DataPlatformMetric
   *
   * @param periodicReportIntervalSec 定期上报间隔，单位：秒，小于或等于0时，不自动上报
   */
  public DataPlatformMetric(int periodicReportIntervalSec) {
    this(periodicReportIntervalSec, Constants.DEFAULT_KAFKA_HOSTS, Constants.DEFAULT_TOPIC);
  }

  /**
   * 构造 DataPlatformMetric
   *
   * @param periodicReportIntervalSec 定期上报间隔，单位：秒，小于或等于0时，不自动上报
   * @param kafkaHosts                kafka 类型：上报的 kafka host ； http 类型： url
   * @param topic                     上报数据的 kafka topic 名称
   */
  public DataPlatformMetric(int periodicReportIntervalSec, String kafkaHosts, String topic) {
    this(periodicReportIntervalSec, kafkaHosts, topic, Constants.DEFAULT_REPORTER_TYPE);
  }

  /**
   * 构造 DataPlatformMetric
   *
   * @param periodicReportIntervalSec 定期上报间隔，单位：秒，小于或等于0时，不自动上报
   * @param kafkaHosts                kafka 类型：上报的 kafka host ； http 类型： url
   * @param topic                     上报数据的 kafka topic 名称
   * @param reporterType              数据上报目的端的类型：kafka / http / console (only for debug)
   */
  public DataPlatformMetric(int periodicReportIntervalSec, String kafkaHosts, String topic, String reporterType) {
    super(periodicReportIntervalSec, kafkaHosts, topic, reporterType);
  }

  /**
   * 关闭 DataPlatformMetric
   **/
  public void close() {
    setIsEndLiteral(true);
    super.close();
  }

  /**
   * 获取输入总数据量 counter : data_monitor.data_loss.input.total_cnt
   * <pre>
   * "data_loss": {
   *      "input": {
   *          "total_cnt": 3000
   *      }
   * }
   * </pre>
   *
   * @return com.tencent.bk.base.metrics.Counter
   **/
  public Counter getInputTotalCntCounter() {
    Counter counter = metricRegistry.counter("metrics.data_monitor.data_loss.input.total_cnt");
    counter.setResetWhenSerialized(false);
    return counter;
  }

  /**
   * 获取输入增量数据量 counter : data_monitor.data_loss.input.total_cnt_increment
   * <pre>
   * "data_loss": {
   *      "input": {
   *          "total_cnt_increment": 3000
   *      }
   * }
   * </pre>
   *
   * @return com.tencent.bk.base.metrics.Counter
   **/
  public Counter getInputTotalCntIncrementCounter() {
    return metricRegistry.counter("metrics.data_monitor.data_loss.input.total_cnt_increment");
  }

  /**
   * 获取输入 tag 信息 counter : metrics.data_monitor.data_loss.input.tags.tagName
   * <pre>
   * "data_loss": {
   *      "input": {
   *          "tags": {
   *              "tagName": 100
   *          }
   *      }
   * }
   * </pre>
   *
   * @param tagName tag名称
   * @return com.tencent.bk.base.metrics.Counter
   **/
  public Counter getInputTagsCounter(String tagName) {
    return metricRegistry.counter("metrics.data_monitor.data_loss.input.tags." + tagName);
  }

  /**
   * 获取输入 tag 信息 counter : metrics.data_monitor.data_loss.input.tags.tagPrefix|timeStamp
   * <pre>
   * "data_loss": {
   *      "input": {
   *          "tags": {
   *              "tagPrefix_timeStamp": 100
   *          }
   *      }
   * }
   * </pre>
   *
   * @param tagPrefix tag前缀
   * @param timeStamp 时间戳，unix时间，精度：秒
   * @return com.tencent.bk.base.metrics.Counter
   **/
  public Counter getInputTagsCounter(String tagPrefix,
                                     long timeStamp) {
    return metricRegistry.counter("metrics.data_monitor.data_loss.input.tags." + tagPrefix + "_"
        + timeStamp);
  }

  /**
   * 获取输入 tag 信息 counter : metrics.data_monitor.data_loss.input.tags.tagPrefix:timeStamp1_timeStamp2
   * <pre>
   * "data_loss": {
   *      "input": {
   *          "tags": {
   *              "tagPrefix:timeStamp1_timeStamp2": 100
   *          }
   *      }
   * }
   * </pre>
   *
   * @param tagPrefix  tag前缀
   * @param timeStamp1 时间戳，unix时间，精度：秒
   * @param timeStamp2 时间戳，unix时间，精度：秒
   * @return com.tencent.bk.base.metrics.Counter
   **/
  public Counter getInputTagsCounter(String tagPrefix,
                                     long timeStamp1, long timeStamp2) {
    return metricRegistry.counter("metrics.data_monitor.data_loss.input.tags." + tagPrefix + ":"
        + timeStamp1 + "_" + timeStamp2);
  }

  /**
   * 批量设置输入 tag 信息 counter : metrics.data_monitor.data_loss.input.tags.{ key = value }
   * <pre>
   * "data_loss": {
   *      "input": {
   *          "tags": {
   *              "key": value
   *          }
   *      }
   * }
   * </pre>
   *
   * @param tags {tag_key, tag_value}
   **/
  public void setInputTagsCounter(Map<String, Long> tags) {
    for (Map.Entry<String, Long> entry : tags.entrySet()) {
      metricRegistry.counter("metrics.data_monitor.data_loss.input.tags." + entry.getKey()).inc(entry.getValue());
    }
  }

  /**
   * 获取输入 segment 信息 counter : metrics.data_monitor.data_loss.input.segments.segmentName
   * <pre>
   * "data_loss": {
   *      "input": {
   *          "segments": {
   *              "segmentName": value
   *          }
   *      }
   * }
   * </pre>
   *
   * @param segmentName segment名称
   * @return com.tencent.bk.base.metrics.Counter
   **/
  public Counter getInputSegmentsCounter(String segmentName) {
    return metricRegistry.counter("metrics.data_monitor.data_loss.input.segments." + segmentName);
  }

  /**
   * 批量设置输入 segment 信息 counter : metrics.data_monitor.data_loss.input.segments.{ key = value }
   * <pre>
   * "data_loss": {
   *      "input": {
   *          "segments": {
   *              "key": value
   *          }
   *      }
   * }
   * </pre>
   *
   * @param segments {segment_key, segment_value}
   **/
  public void setInputSegmentsCounter(Map<String, Long> segments) {
    for (Map.Entry<String, Long> entry : segments.entrySet()) {
      metricRegistry.counter("metrics.data_monitor.data_loss.input.segments."
          + entry.getKey()).inc(entry.getValue());
    }
  }

  // data_monitor/data_loss/output

  /**
   * 获取输出总数据量 counter : metrics.data_monitor.data_loss.output.total_cnt
   * <pre>
   * "data_loss": {
   *      "output": {
   *          "total_cnt": 3000
   *      }
   * }
   * </pre>
   *
   * @return com.tencent.bk.base.metrics.Counter
   **/
  public Counter getOutputTotalCntCounter() {
    Counter counter = metricRegistry.counter("metrics.data_monitor.data_loss.output.total_cnt");
    counter.setResetWhenSerialized(false);
    return counter;
  }

  /**
   * 获取输出增量数据量 counter : metrics.data_monitor.data_loss.output.total_cnt_increment
   * <pre>
   * "data_loss": {
   *      "output": {
   *          "total_cnt_increment": 3000
   *      }
   * }
   * </pre>
   *
   * @return com.tencent.bk.base.metrics.Counter
   **/
  public Counter getOutputTotalCntIncrementCounter() {
    return metricRegistry.counter("metrics.data_monitor.data_loss.output.total_cnt_increment");
  }

  /**
   * 获取输出 tag 信息 counter : metrics.data_monitor.data_loss.output.tags.tagName
   * <pre>
   * "data_loss": {
   *      "output": {
   *          "tags": {
   *              "tagName": value
   *          }
   *      }
   * }
   * </pre>
   *
   * @param tagName tag名称
   * @return com.tencent.bk.base.metrics.Counter
   **/
  public Counter getOutputTagsCounter(String tagName) {
    return metricRegistry.counter("metrics.data_monitor.data_loss.output.tags." + tagName);
  }

  /**
   * 获取输出 tag 信息 counter : metrics.data_monitor.data_loss.output.tags.tagPrefix_timeStamp
   * <pre>
   * "data_loss": {
   *      "output": {
   *          "tags": {
   *              "tagPrefix_timeStamp": value
   *          }
   *      }
   * }
   * </pre>
   *
   * @param tagPrefix tag名称前缀
   * @param timeStamp 时间戳，unix时间，单位：秒
   * @return com.tencent.bk.base.metrics.Counter
   */
  public Counter getOutputTagsCounter(String tagPrefix, long timeStamp) {
    return metricRegistry.counter("metrics.data_monitor.data_loss.output.tags." + tagPrefix + "_" + timeStamp);
  }

  /**
   * 获取输出 tag 信息 counter : metrics.data_monitor.data_loss.output.tags.tagPrefix:timeStamp1_timeStamp2
   * <pre>
   * "data_loss": {
   *      "output": {
   *          "tags": {
   *              "tagPrefix:timeStamp1_timeStamp2": value
   *          }
   *      }
   * }
   * </pre>
   *
   * @param tagPrefix  tag名称前缀
   * @param timeStamp1 时间戳，unix时间，单位：秒
   * @param timeStamp2 时间戳，unix时间，单位：秒
   * @return com.tencent.bk.base.metrics.Counter
   */
  public Counter getOutputTagsCounter(String tagPrefix,
                                      long timeStamp1, long timeStamp2) {
    return metricRegistry.counter("metrics.data_monitor.data_loss.output.tags." + tagPrefix + ":"
        + timeStamp1 + "|" + timeStamp2);
  }

  /**
   * 批量设置输出 tag 信息 counter : metrics.data_monitor.data_loss.output.tags.key
   * <pre>
   * "data_loss": {
   *      "output": {
   *          "tags": {
   *              "key": value
   *          }
   *      }
   * }
   * </pre>
   *
   * @param tags {tag_key, tag_value}
   */
  public void setOutputTagsCounter(Map<String, Long> tags) {
    for (Map.Entry<String, Long> entry : tags.entrySet()) {
      metricRegistry.counter("metrics.data_monitor.data_loss.output.tags."
          + entry.getKey()).inc(entry.getValue());
    }
  }

  /**
   * 获取输出 checkpoint 丢弃 counter : metrics.data_monitor.data_loss.output.ckp_drop
   * <pre>
   * "data_loss": {
   *      "output": {
   *          "ckp_drop": 5
   *      }
   * }
   * </pre>
   *
   * @return com.tencent.bk.base.metrics.Counter
   */
  public Counter getOutputCkpDropCounter() {
    return metricRegistry.counter("metrics.data_monitor.data_loss.output.ckp_drop");
  }

  /**
   * 获取输出 checkpoint 丢弃 tag 相关 counter : metrics.data_monitor.data_loss.output.ckp_drop_tags.tagName
   * <pre>
   * "data_loss": {
   *      "output": {
   *          "ckp_drop_tags": {
   *              "tagName": value
   *          }
   *      }
   * }
   * </pre>
   *
   * @param tagName tag名称
   * @return com.tencent.bk.base.metrics.Counter
   */
  public Counter getOutputCkpDropTagsCounter(String tagName) {
    return metricRegistry.counter("metrics.data_monitor.data_loss.output.ckp_drop_tags." + tagName);
  }

  /**
   * 获取输出 checkpoint 丢弃 tag 相关 counter : metrics.data_monitor.data_loss.output.ckp_drop_tags.tagPrefix_timeStamp
   * <pre>
   * "data_loss": {
   *      "output": {
   *          "ckp_drop_tags": {
   *              "tagPrefix_timeStamp": value
   *          }
   *      }
   * }
   * </pre>
   *
   * @param tagPrefix tag前缀名称
   * @param timeStamp 时间戳，unix时间，单位：秒
   * @return com.tencent.bk.base.metrics.Counter
   */
  public Counter getOutputCkpDropTagsCounter(String tagPrefix,
                                             long timeStamp) {
    return metricRegistry.counter("metrics.data_monitor.data_loss.output.ckp_drop_tags." + tagPrefix
        + "_" + timeStamp);
  }

  /**
   * 批量设置输出 checkpoint 丢弃 tag 相关 counter : metrics.data_monitor.data_loss.output.ckp_drop_tags.key
   * <pre>
   * "data_loss": {
   *      "output": {
   *          "ckp_drop_tags": {
   *              "key1": value1,
   *              "key2": value2
   *          }
   *      }
   * }
   * </pre>
   *
   * @param tags {tag_key, tag_value}
   */
  public void setOutputCkpDropTagsCounter(Map<String, Long> tags) {
    for (Map.Entry<String, Long> entry : tags.entrySet()) {
      metricRegistry.counter("metrics.data_monitor.data_loss.output.ckp_drop_tags."
          + entry.getKey()).inc(entry.getValue());
    }
  }

  /**
   * 获取输出 segment 相关 counter : metrics.data_monitor.data_loss.output.segments.segmentName
   * <pre>
   * "data_loss": {
   *      "output": {
   *          "segments": {
   *              "segmentName": value
   *          }
   *      }
   * }
   * </pre>
   *
   * @param segmentName
   * @return com.tencent.bk.base.metrics.Counter
   */
  public Counter getOutputSegmentsCounter(String segmentName) {
    return metricRegistry.counter("metrics.data_monitor.data_loss.output.segments." + segmentName);
  }

  /**
   * 批量设置输出 segment 信息 counter : metrics.data_monitor.data_loss.output.segments.{ key = value }
   * <pre>
   * "data_loss": {
   *      "output": {
   *          "segments": {
   *              "key": value
   *          }
   *      }
   * }
   * </pre>
   *
   * @param segments {segment_key, segment_value}
   **/
  public void setOutputSegmentsCounter(Map<String, Long> segments) {
    for (Map.Entry<String, Long> entry : segments.entrySet()) {
      metricRegistry.counter("metrics.data_monitor.data_loss.output.segments."
          + entry.getKey()).inc(entry.getValue());
    }
  }

  // data_monitor/data_loss/drop

  /**
   * 获取输出丢弃 counter : metrics.data_monitor.data_loss.data_drop.dropCode.cnt
   * <pre>
   * "data_loss": {
   *      "data_drop": {
   *          "dropCode": {
   *              "cnt": 5,
   *              "reason": "dropReason"
   *          }
   *      }
   * }
   * </pre>
   *
   * @param dropCode   dropCode
   * @param dropReason dropReason
   * @return com.tencent.bk.base.metrics.Counter
   */
  public Counter getDataDropTagsCounter(String dropCode, String dropReason) {
    Counter counter = metricRegistry.counter("metrics.data_monitor.data_loss.data_drop." + dropCode + ".cnt");
    metricRegistry.literal("metrics.data_monitor.data_loss.data_drop." + dropCode + ".reason")
        .setLiteral(dropReason);
    return counter;
  }

  /**
   * 设置输出丢弃 counter: metrics.data_monitor.data_loss.data_drop.dropCode.cnt
   * <pre>
   * "data_loss": {
   *      "data_drop": {
   *          "dropCode": {
   *              "cnt": dropCnt,
   *              "reason": "dropReason"
   *          }
   *      }
   * }
   * </pre>
   *
   * @param dropCode   dropCode
   * @param dropReason dropReason
   * @param dropCnt    dropCnt
   */
  public void setDataDropTagsCounter(String dropCode, String dropReason, long dropCnt) {
    metricRegistry.counter("metrics.data_monitor.data_loss.data_drop." + dropCode + ".cnt").inc(dropCnt);
    metricRegistry.literal("metrics.data_monitor.data_loss.data_drop." + dropCode + ".reason")
        .setLiteral(dropReason);
  }

  // data_monitor/data_delay

  /**
   * 获取数据延迟窗口时间 counter : metrics.data_monitor.data_delay.window_time
   * <pre>
   * "data_delay": {
   *      "window_time": 60
   * }
   * </pre>
   *
   * @return com.tencent.bk.base.metrics.Counter
   */
  public Counter getDelayWindowTimeCounter() {
    return metricRegistry.counter("metrics.data_monitor.data_delay.window_time");
  }

  /**
   * 设置数据延迟窗口时间 counter : metrics.data_monitor.data_delay.window_time
   * <pre>
   * "data_delay": {
   *      "window_time": 60
   * }
   * </pre>
   */
  public void setDelayWindowTimeCounter(long windowTime) {
    metricRegistry.counter("metrics.data_monitor.data_delay.window_time").inc(windowTime);
  }

  /**
   * 获取数据延迟等待时间 counter : metrics.data_monitor.data_delay.waiting_time
   * <pre>
   * "data_delay": {
   *      "waiting_time": 30
   * }
   * </pre>
   *
   * @return com.tencent.bk.base.metrics.Counter
   */
  public Counter getDelayWaitingTimeCounter() {
    return metricRegistry.counter("metrics.data_monitor.data_delay.waiting_time");
  }

  /**
   * 设置数据延迟等待时间 counter : metrics.data_monitor.data_delay.waiting_time
   * <pre>
   * "data_delay": {
   *      "waiting_time": 30
   * }
   * </pre>
   */
  public void setDelayWaitingTimeCounter(long waitingTime) {
    metricRegistry.counter("metrics.data_monitor.data_delay.waiting_time").inc(waitingTime);
  }

  /**
   * 设置最小延迟相关 literal: metrics.data_monitor.data_delay.min_delay.data_time
   * <pre>
   * "data_delay": {
   *      "min_delay": {
   *          "output_time": 1577808000,
   *          "data_time": dataTime,
   *          "delay_time" : 0
   *      }
   * }
   * </pre>
   *
   * @param dataTime 数据时间 dataTime , unix时间戳，单位：秒
   *                 <p>输出时间 outputTime , unix时间戳，单位：秒，无需手动设置</p>
   */
  public void setDelayMinDelayLiteral(long dataTime) {
    TimeUtils timeUtils = new TimeUtils(null);
    TimeDeltaUtils timeDeltaUtils = new TimeDeltaUtils(timeUtils, dataTime);
    metricRegistry.literal("metrics.data_monitor.data_delay.min_delay.data_time").setLiteral(dataTime);
    metricRegistry.literal("metrics.data_monitor.data_delay.min_delay.output_time").setLiteral(timeUtils);
    metricRegistry.literal("metrics.data_monitor.data_delay.min_delay.delay_time").setLiteral(timeDeltaUtils);
  }

  /**
   * 设置最小延迟相关 literal: metrics.data_monitor.data_delay.min_delay.data_time
   * <pre>
   * "data_delay": {
   *      "min_delay": {
   *          "output_time": outputTime,
   *          "data_time": dataTime,
   *          "delay_time" : 0
   *      }
   * }
   * </pre>
   *
   * @param dataTime   数据时间 dataTime , unix时间戳，单位：秒
   * @param outputTime 输出时间 outputTime , unix时间戳，单位：秒
   */
  public void setDelayMinDelayLiteral(long dataTime, long outputTime) {
    metricRegistry.literal("metrics.data_monitor.data_delay.min_delay.data_time").setLiteral(dataTime);
    metricRegistry.literal("metrics.data_monitor.data_delay.min_delay.output_time").setLiteral(outputTime);
    metricRegistry.literal("metrics.data_monitor.data_delay.min_delay.delay_time")
        .setLiteral(outputTime - dataTime);
  }

  /**
   * 设置最大延迟相关 literal : metrics.data_monitor.data_delay.max_delay.data_time
   * <pre>
   * "data_delay": {
   *      "max_delay": {
   *          "output_time": 1577808000,
   *          "data_time": dataTime,
   *          "delay_time" : 0
   *      }
   * }
   * </pre>
   *
   * @param dataTime 数据时间 dataTime , unix时间戳，单位：秒
   *                 <p>输出时间 outputTime , unix时间戳，单位：秒，无需手动设置</p>
   */
  public void setDelayMaxDelayLiteral(long dataTime) {
    TimeUtils timeUtils = new TimeUtils(null);
    TimeDeltaUtils timeDeltaUtils = new TimeDeltaUtils(timeUtils, dataTime);
    metricRegistry.literal("metrics.data_monitor.data_delay.max_delay.data_time").setLiteral(dataTime);
    metricRegistry.literal("metrics.data_monitor.data_delay.max_delay.output_time").setLiteral(timeUtils);
    metricRegistry.literal("metrics.data_monitor.data_delay.max_delay.delay_time").setLiteral(timeDeltaUtils);
  }

  /**
   * 设置最大延迟相关 literal : metrics.data_monitor.data_delay.max_delay.data_time
   * <pre>
   * "data_delay": {
   *      "max_delay": {
   *          "output_time": outputTime,
   *          "data_time": dataTime,
   *          "delay_time" : 0
   *      }
   * }
   * </pre>
   *
   * @param dataTime   数据时间 dataTime , unix时间戳，单位：秒
   * @param outputTime 输出时间 outputTime , unix时间戳，单位：秒
   */
  public void setDelayMaxDelayLiteral(long dataTime, long outputTime) {
    metricRegistry.literal("metrics.data_monitor.data_delay.max_delay.data_time").setLiteral(dataTime);
    metricRegistry.literal("metrics.data_monitor.data_delay.max_delay.output_time").setLiteral(outputTime);
    metricRegistry.literal("metrics.data_monitor.data_delay.max_delay.delay_time")
        .setLiteral(outputTime - dataTime);
  }

  // data_profiling

  /**
   * 获取数据质量相关 counter: metrics.data_profiling.data_structure.data_malformed.field.cnt
   * <pre>
   * "data_profiling": {
   *      "data_structure": {
   *          "data_malformed": {
   *              "field": {
   *                  "cnt": 1000,
   *                  "reason": "reason"
   *              }
   *          }
   *      }
   * }
   * </pre>
   *
   * @param field  field
   * @param reason reason
   * @return com.tencent.bk.base.metrics.Counter
   */
  public Counter getDataMalformedCounter(String field, String reason) {
    Counter counter = metricRegistry.counter("metrics.data_profiling.data_structure.data_malformed."
        + field + ".cnt");
    metricRegistry.literal("metrics.data_profiling.data_structure.data_malformed." + field + ".reason")
        .setLiteral(reason);
    return counter;
  }

  /**
   * 设置数据质量相关 counter: metrics.data_profiling.data_structure.data_malformed.field.cnt
   * <pre>
   * "data_profiling": {
   *      "data_structure": {
   *          "data_malformed": {
   *              "field": {
   *                  "cnt": 1000,
   *                  "reason": "reason"
   *              }
   *          }
   *      }
   * }
   * </pre>
   *
   * @param field  field
   * @param reason reason
   * @param cnt    cnt
   */
  public void setDataMalformedCounter(String field, String reason, long cnt) {
    metricRegistry.counter("metrics.data_profiling.data_structure.data_malformed." + field + ".cnt").inc(cnt);
    metricRegistry.literal("metrics.data_profiling.data_structure.data_malformed." + field + ".reason")
        .setLiteral(reason);
  }

  private void setIsEndLiteral(boolean isEnd) {
    metricRegistry.literal("is_end").setLiteral(isEnd);
  }

  public static class Builder {
    private static final String DEFAULT_VERSION = "3.3";
    // init
    private int periodicReportIntervalSec;
    private String kafkaHosts;
    private String topic;
    private String reporterType;
    // version
    private String version;
    // seq_no
//    private long seq_no = 0;
    // is_end
    private boolean isEnd = false;

    // info
    private String module;
    private String component;
    private String cluster;

    // storage
    private String storageType;
    private int storageId;
    private String clusterName;
    private String clusterType;
    private String storageHost;
    private int storagePort;

    // physical_tag
    private Map<String, Object> physicalTagMap;

    // logical_tag
    private Map<String, Object> logicalTagMap;

    // custom_tags
    private Map<String, Object> customTagMap;

    private String name;


    public Builder() {
      this(Constants.DEFAULT_PERIODIC_REPORT_SEC, Constants.DEFAULT_KAFKA_HOSTS, Constants.DEFAULT_TOPIC);
    }

    public Builder(int periodicReportIntervalSec) {
      this(periodicReportIntervalSec, Constants.DEFAULT_KAFKA_HOSTS, Constants.DEFAULT_TOPIC);
    }

    public Builder(int periodicReportIntervalSec, String kafkaHosts, String topic) {
      this.periodicReportIntervalSec = periodicReportIntervalSec;
      this.kafkaHosts = kafkaHosts;
      this.topic = topic;
      this.reporterType = Constants.DEFAULT_REPORTER_TYPE;
      this.version = DEFAULT_VERSION;
    }

    public Builder(int periodicReportIntervalSec, String kafkaHosts, String topic, String reporterType) {
      this.periodicReportIntervalSec = periodicReportIntervalSec;
      this.kafkaHosts = kafkaHosts;
      this.topic = topic;
      this.reporterType = reporterType;
      this.version = DEFAULT_VERSION;
    }

    public Builder periodicReportIntervalSec(int periodicReportIntervalSec) {
      this.periodicReportIntervalSec = periodicReportIntervalSec;
      return this;
    }

    public Builder kafkaHosts(String kafkaHosts) {
      this.kafkaHosts = kafkaHosts;
      return this;
    }

    public Builder topic(String topic) {
      this.topic = topic;
      return this;
    }

    public Builder reporterType(String reporterType) {
      this.reporterType = reporterType;
      return this;
    }

    public Builder version(String topic) {
      this.topic = topic;
      return this;
    }

    public Builder module(String module) {
      this.module = module;
      return this;
    }

    public Builder component(String component) {
      this.component = component;
      return this;
    }

    public Builder cluster(String cluster) {
      this.cluster = cluster;
      return this;
    }

    public Builder storageType(String storageType) {
      this.storageType = storageType;
      return this;
    }

    public Builder storageId(int storageId) {
      this.storageId = storageId;
      return this;
    }

    public Builder clusterType(String clusterType) {
      this.clusterType = clusterType;
      return this;
    }

    public Builder clusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    public Builder storageHost(String storageHost) {
      this.storageHost = storageHost;
      return this;
    }

    public Builder storagePort(int storagePort) {
      this.storagePort = storagePort;
      return this;
    }

    public Builder physicalTag(String key, Object value) {
      if (this.physicalTagMap == null) {
        this.physicalTagMap = new HashMap<>();
      }
      this.physicalTagMap.put(key, value);
      return this;
    }

    public Builder removePhysicalTag(String key) {
      if (this.physicalTagMap != null) {
        this.physicalTagMap.remove(key);
      }
      return this;
    }

    public Builder clearPhysicalTag(String key) {
      if (this.physicalTagMap != null) {
        this.physicalTagMap.clear();
      }
      return this;
    }

    public Builder logicalTag(String key, Object value) {
      if (this.logicalTagMap == null) {
        this.logicalTagMap = new HashMap<>();
      }
      this.logicalTagMap.put(key, value);
      return this;
    }

    public Builder removeLogicalTag(String key) {
      if (this.logicalTagMap != null) {
        this.logicalTagMap.remove(key);
      }
      return this;
    }

    public Builder clearLogicalTag(String key) {
      if (this.physicalTagMap != null) {
        this.physicalTagMap.clear();
      }
      return this;
    }

    public Builder customTag(String key, Object value) {
      if (this.customTagMap == null) {
        this.customTagMap = new HashMap<>();
      }
      this.customTagMap.put(key, value);
      return this;
    }

    public Builder removeCustomTag(String key) {
      if (this.customTagMap != null) {
        this.customTagMap.remove(key);
      }
      return this;
    }

    public Builder clearCustomTag(String key) {
      if (this.physicalTagMap != null) {
        this.physicalTagMap.clear();
      }
      return this;
    }

    @SuppressWarnings("rawtypes")
    public Builder physicalTagMap(Map physicalTagMap, boolean removePreviousItems) {
      if (this.physicalTagMap == null) {
        this.physicalTagMap = new HashMap<>();
      } else if (removePreviousItems) {
        this.physicalTagMap.clear();
      }
      this.physicalTagMap.putAll(physicalTagMap);
      return this;
    }

    @SuppressWarnings("rawtypes")
    public Builder logicalTagMap(Map logicalTagMap, boolean removePreviousItems) {
      if (this.logicalTagMap == null) {
        this.logicalTagMap = new HashMap<>();
      } else if (removePreviousItems) {
        this.logicalTagMap.clear();
      }
      this.logicalTagMap.putAll(logicalTagMap);
      return this;
    }

    @SuppressWarnings("rawtypes")
    public Builder customTagMap(Map customTagMap, boolean removePreviousItems) {
      if (this.customTagMap == null) {
        this.customTagMap = new HashMap<>();
      } else if (removePreviousItems) {
        this.customTagMap.clear();
      }
      this.customTagMap.putAll(customTagMap);
      return this;
    }

    public Builder dimension(String dimension) {
      if (this.name == null || this.name.isEmpty()) {
        this.name = dimension.trim();
      } else {
        this.name = this.name + "." + dimension.trim();
      }
      return this;
    }

    /**
     * build
     *
     * @return com.tencent.bk.base.metrics.DataPlatformMetric
     */
    public DataPlatformMetric build() {
      DataPlatformMetric dataPlatformMetric = new DataPlatformMetric(
          periodicReportIntervalSec, kafkaHosts, topic, reporterType);
      MetricRegistry metricRegistry = dataPlatformMetric.metricRegistry;
      // time
      metricRegistry.literal("time").setLiteral(new TimeUtils(null));
      // version
      metricRegistry.literal("version").setLiteral(version);
      // seq_no
      Counter seqNoCounter = metricRegistry.counter("seq_no");
      seqNoCounter.setResetWhenSerialized(false);
      seqNoCounter.setIncWhenSerialized(true);
      // is_end
      metricRegistry.literal("is_end").setLiteral(isEnd);
      // info
      metricRegistry.literal("info.module").setLiteral(module);
      metricRegistry.literal("info.component").setLiteral(component);
      metricRegistry.literal("info.cluster").setLiteral(cluster);
      buildStorage(metricRegistry);
      buildPhysicalTag(metricRegistry);
      buildLogicalTag(metricRegistry);
      buildCustomTag(metricRegistry);
      // metrics
      // --> data_monitor
      //      --> data_loss
      //          --> input
      //              --> tags
      //              --> total_cnt
      //              --> total_cnt_increment
      //          --> output
      //              --> tags
      //              --> total_cnt
      //              --> total_cnt_increment
      //              --> ckp_drop
      //              --> ckp_drop_tags
      //          --> data_drop
      //              --> drop_code1
      //      --> data_delay
      //          --> window_time
      //          --> waiting_time
      //          --> min_delay
      //              --> output_time
      //              --> data_time
      //              --> delay_time
      //          --> max_delay
      //              --> output_time
      //              --> data_time
      //              --> delay_time
      // --> data_profiling
      //      --> data_structure
      //          --> data_malformed

      return dataPlatformMetric;
    }

    private void buildCustomTag(MetricRegistry metricRegistry) {
      // custom_tag
      for (Map.Entry<String, Object> entry : customTagMap.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();
        metricRegistry.literal("info.custom_tags." + key).setLiteral(value);
      }
    }

    private void buildLogicalTag(MetricRegistry metricRegistry) {
      // logical_tag
      StringBuilder logicalTagValuesSb = new StringBuilder();
      for (Map.Entry<String, Object> entry : logicalTagMap.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();
        logicalTagValuesSb.append(value.toString()).append("|");
        metricRegistry.literal("info.logical_tag.desc." + key).setLiteral(value);
      }
      logicalTagValuesSb.deleteCharAt(logicalTagValuesSb.length() - 1);
      metricRegistry.literal("info.logical_tag.tag").setLiteral(logicalTagValuesSb.toString());
    }

    private void buildPhysicalTag(MetricRegistry metricRegistry) {
      // physical_tag
      StringBuilder physicalTagValuesSb = new StringBuilder();
      for (Map.Entry<String, Object> entry : physicalTagMap.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();
        physicalTagValuesSb.append(value.toString()).append("|");
        metricRegistry.literal("info.physical_tag.desc." + key).setLiteral(value);
      }
      physicalTagValuesSb.deleteCharAt(physicalTagValuesSb.length() - 1);
      metricRegistry.literal("info.physical_tag.tag").setLiteral(physicalTagValuesSb.toString());
    }

    private void buildStorage(MetricRegistry metricRegistry) {
      // storage
      if (storageType != null && !storageType.isEmpty()) {
        metricRegistry.literal("info.storage.storage_type").setLiteral(storageType);
        metricRegistry.literal("info.storage.storage_id").setLiteral(storageId);
      }
      if (clusterType != null && !clusterType.isEmpty()) {
        metricRegistry.literal("info.storage.cluster_name").setLiteral(clusterName);
        metricRegistry.literal("info.storage.cluster_type").setLiteral(clusterType);
      }
      if (storageHost != null && !storageHost.isEmpty()) {
        metricRegistry.literal("info.storage.storage_host").setLiteral(storageHost);
        metricRegistry.literal("info.storage.storage_port").setLiteral(storagePort);
      }
    }
  }
}
