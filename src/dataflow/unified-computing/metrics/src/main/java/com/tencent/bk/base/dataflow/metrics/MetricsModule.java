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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tencent.bk.base.dataflow.metrics.util.TimeUtils;
import com.tencent.bk.base.dataflow.metrics.registry.MetricRegistry;
import com.tencent.bk.base.dataflow.metrics.util.TimeDeltaUtils;

import java.io.IOException;
import java.util.Arrays;

public class MetricsModule extends Module {
  static final Version VERSION = new Version(4, 0, 0, "", "io.dropwizard.metrics", "metrics-json");
  protected final MetricFilter filter;

  public MetricsModule() {
    this(MetricFilter.ALL);
  }

  public MetricsModule(MetricFilter filter) {
    this.filter = filter;
  }

  @Override
  public String getModuleName() {
    return "metrics";
  }

  @Override
  public Version version() {
    return VERSION;
  }

  @Override
  public void setupModule(SetupContext context) {
    context.addSerializers(new SimpleSerializers(Arrays.asList(
        new TimeUtilsSerializer(),
        new TimeDeltaUtilsSerializer(),
        new LiteralSerializer(),
        new CounterSerializer(),
        new GaugeSerializer(),
        new MetricRegistrySerializer(filter)
    )));
  }

  private static class TimeUtilsSerializer extends StdSerializer<TimeUtils> {

    private static final long serialVersionUID = 1L;

    private TimeUtilsSerializer() {
      super(TimeUtils.class);
    }

    @Override
    public void serialize(TimeUtils timeUtils,
                          JsonGenerator json,
                          SerializerProvider provider) throws IOException {
      String timeFormats = timeUtils.getTimeFormat();
      if (timeFormats != null && !timeFormats.isEmpty()) {
        json.writeString(timeUtils.getCurrentDataTime());
      } else {
        json.writeNumber(timeUtils.getCurrentEpochSecond());
      }
    }
  }

  private static class TimeDeltaUtilsSerializer extends StdSerializer<TimeDeltaUtils> {

    private static final long serialVersionUID = 1L;

    private TimeDeltaUtilsSerializer() {
      super(TimeDeltaUtils.class);
    }

    @Override
    public void serialize(TimeDeltaUtils timeDeltaUtils,
                          JsonGenerator json,
                          SerializerProvider provider) throws IOException {
      json.writeNumber(timeDeltaUtils.getDeltaSecond());
    }
  }

  private static class LiteralSerializer extends StdSerializer<Literal> {

    private static final long serialVersionUID = 1L;

    private LiteralSerializer() {
      super(Literal.class);
    }

    @Override
    public void serialize(Literal literal,
                          JsonGenerator json,
                          SerializerProvider provider) throws IOException {
      json.writeObject(literal.getLiteral());
    }
  }

  private static class CounterSerializer extends StdSerializer<Counter> {

    private static final long serialVersionUID = 1L;

    private CounterSerializer() {
      super(Counter.class);
    }

    @Override
    public void serialize(Counter counter,
                          JsonGenerator json,
                          SerializerProvider provider) throws IOException {

      boolean resetWhenSerialized = counter.isResetWhenSerialized();
      json.writeNumber(resetWhenSerialized ? counter.getCountAndReset() : counter.getCount());
      if (counter.isIncWhenSerialized()) {
        counter.inc();
      }
    }
  }

  private static class GaugeSerializer extends StdSerializer<Gauge> {

    private static final long serialVersionUID = 1L;

    private GaugeSerializer() {
      super(Gauge.class);
    }

    @Override
    public void serialize(Gauge gauge,
                          JsonGenerator json,
                          SerializerProvider provider) throws IOException {
      json.writeObject(gauge.getValue());
    }
  }

  private static class MetricRegistrySerializer extends StdSerializer<MetricRegistry> {

    private static final long serialVersionUID = 1L;

    private final MetricFilter filter;

    private MetricRegistrySerializer(MetricFilter filter) {
      super(MetricRegistry.class);
      this.filter = filter;
    }

    @Override
    public void serialize(MetricRegistry registry,
                          JsonGenerator json,
                          SerializerProvider provider) throws IOException {
      json.writeStartObject();
      json.writeStringField("version", VERSION.toString());
      json.writeObjectField("counters", registry.getCounters(filter));
      json.writeObjectField("literals", registry.getLiterals(filter));
      json.writeObjectField("gauges", registry.getGauges(filter));
      json.writeEndObject();
    }
  }
}
