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

package com.tencent.bk.base.dataflow.metrics.registry;

import com.tencent.bk.base.dataflow.metrics.Counter;
import com.tencent.bk.base.dataflow.metrics.Gauge;
import com.tencent.bk.base.dataflow.metrics.Literal;
import com.tencent.bk.base.dataflow.metrics.Metric;
import com.tencent.bk.base.dataflow.metrics.MetricFilter;
import com.tencent.bk.base.dataflow.metrics.MetricSet;
import com.tencent.bk.base.dataflow.metrics.util.Trie;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class MetricRegistry implements MetricSet {
  private final ConcurrentMap<String, Metric> metrics;
  private final List<MetricRegistryListener> listeners;
  private final Trie metricsNameTrie;
  private String metricRegistryName;

  /**
   * Creates a new {@link MetricRegistry}.
   */
  public MetricRegistry() {
    this.metrics = buildMap();
    this.listeners = new CopyOnWriteArrayList<>();
    this.metricsNameTrie = new Trie();
    this.metricRegistryName = UUID.randomUUID().toString();
  }

  /**
   * Concatenates elements to form a dotted name, eliding any null values or empty strings.
   *
   * @param name  the first element of the name
   * @param names the remaining elements of the name
   * @return {@code name} and {@code names} concatenated by periods
   */
  public static String name(String name, String... names) {
    final StringBuilder builder = new StringBuilder();
    append(builder, name);
    if (names != null) {
      for (String s : names) {
        append(builder, s);
      }
    }
    return builder.toString();
  }

  /**
   * Concatenates a class name and elements to form a dotted name, eliding any null values or
   * empty strings.
   *
   * @param klass the first element of the name
   * @param names the remaining elements of the name
   * @return {@code klass} and {@code names} concatenated by periods
   */
  public static String name(Class<?> klass, String... names) {
    return name(klass.getName(), names);
  }

  private static void append(StringBuilder builder, String part) {
    if (part != null && !part.isEmpty()) {
      if (builder.length() > 0) {
        builder.append('.');
      }
      builder.append(part);
    }
  }

  /**
   * get metric registry's name
   */
  public String getMetricRegistryName() {
    return metricRegistryName;
  }

  /**
   * set metric registry's name
   */
  public void setMetricRegistryName(String metricRegistryName) {
    this.metricRegistryName = metricRegistryName;
  }

  /**
   * Creates a new {@link ConcurrentMap} implementation for use inside the registry. Override this
   * to create a {@link MetricRegistry} with space- or time-bounded metric lifecycles, for
   * example.
   *
   * @return a new {@link ConcurrentMap}
   */
  protected ConcurrentMap<String, Metric> buildMap() {
    return new ConcurrentHashMap<>();
  }

  /**
   * Given a {@link Metric}, registers it under the given name.
   *
   * @param name   the name of the metric
   * @param metric the metric
   * @param <T>    the type of the metric
   * @return {@code metric}
   * @throws IllegalArgumentException if the name is already registered or metric variable is null
   */
  @SuppressWarnings("unchecked")
  public <T extends Metric> T register(String name, T metric) throws IllegalArgumentException {

    if (metric == null) {
      throw new NullPointerException("metric == null");
    }

    if (metric instanceof MetricRegistry) {
      final MetricRegistry childRegistry = (MetricRegistry) metric;
      final String childName = name;
      childRegistry.addListener(new MetricRegistryListener() {
        @Override
        public void onGaugeAdded(String name, Gauge<?> gauge) {
          register(name(childName, name), gauge);
        }

        @Override
        public void onGaugeRemoved(String name) {
          remove(name(childName, name));
        }

        @Override
        public void onCounterAdded(String name, Counter counter) {
          register(name(childName, name), counter);
        }

        @Override
        public void onCounterRemoved(String name) {
          remove(name(childName, name));
        }

        @Override
        public void onLiteralAdded(String name, Literal literal) {
          register(name(childName, name), literal);
        }

        @Override
        public void onLiteralRemoved(String name) {

        }
      });
    } else if (metric instanceof MetricSet) {
      registerAll(name, (MetricSet) metric);
    } else {
      final Metric existing = metrics.putIfAbsent(name, metric);
      if (existing == null) {
        onMetricAdded(name, metric);
      } else {
        throw new IllegalArgumentException("A metric named " + name + " already exists");
      }
    }
    return metric;
  }

  /**
   * Given a metric set, registers them.
   *
   * @param metrics a set of metrics
   * @throws IllegalArgumentException if any of the names are already registered
   */
  public void registerAll(MetricSet metrics) throws IllegalArgumentException {
    registerAll(null, metrics);
  }

  /**
   * Given a metric set, registers them with the given prefix prepended to their names.
   *
   * @param prefix  a name prefix
   * @param metrics a set of metrics
   * @throws IllegalArgumentException if any of the names are already registered
   */
  public void registerAll(String prefix, MetricSet metrics) throws IllegalArgumentException {
    for (Map.Entry<String, Metric> entry : metrics.getMetrics().entrySet()) {
      if (entry.getValue() instanceof MetricSet) {
        registerAll(name(prefix, entry.getKey()), (MetricSet) entry.getValue());
      } else {
        register(name(prefix, entry.getKey()), entry.getValue());
      }
    }
  }

  /**
   * Return the {@link Counter} registered under this name; or create and register
   * a new {@link Counter} if none is registered.
   *
   * @param name the name of the metric
   * @return a new or pre-existing {@link Counter}
   */
  public Counter counter(String name) {
    return getOrAdd(name, MetricBuilder.COUNTERS);
  }

  /**
   * Return the {@link Counter} registered under this name; or create and register
   * a new {@link Counter} using the provided MetricSupplier if none is registered.
   *
   * @param name     the name of the metric
   * @param supplier a MetricSupplier that can be used to manufacture a counter.
   * @return a new or pre-existing {@link Counter}
   */
  public Counter counter(String name, final MetricSupplier<Counter> supplier) {
    return getOrAdd(name, new MetricBuilder<Counter>() {
      @Override
      public Counter newMetric() {
        return supplier.newMetric();
      }

      @Override
      public boolean isInstance(Metric metric) {
        return Counter.class.isInstance(metric);
      }
    });
  }

  /**
   * Return the {@link Literal} registered under this name; or create and register
   * a new {@link Literal} if none is registered.
   *
   * @param name the name of the metric
   * @return a new or pre-existing {@link Literal}
   */
  public Literal literal(String name) {
    return getOrAdd(name, MetricBuilder.LITERALS);
  }

  /**
   * Return the {@link Literal} registered under this name; or create and register
   * a new {@link Literal} using the provided MetricSupplier if none is registered.
   *
   * @param name     the name of the metric
   * @param supplier a MetricSupplier that can be used to manufacture a counter.
   * @return a new or pre-existing {@link Literal}
   */
  public Literal literal(String name, final MetricSupplier<Literal> supplier) {
    return getOrAdd(name, new MetricBuilder<Literal>() {
      @Override
      public Literal newMetric() {
        return supplier.newMetric();
      }

      @Override
      public boolean isInstance(Metric metric) {
        return Literal.class.isInstance(metric);
      }
    });
  }

  /**
   * Return the {@link Gauge} registered under this name; or create and register
   * a new {@link Gauge} if none is registered.
   *
   * @param name the name of the metric
   * @return a new or pre-existing {@link Gauge}
   */
  @SuppressWarnings("rawtypes")
  public Gauge gauge(String name, final MetricSupplier<Gauge> supplier) {
    return getOrAdd(name, new MetricBuilder<Gauge>() {
      @Override
      public Gauge newMetric() {
        return supplier.newMetric();
      }

      @Override
      public boolean isInstance(Metric metric) {
        return Gauge.class.isInstance(metric);
      }
    });
  }


  /**
   * Removes the metric with the given name.
   *
   * @param name the name of the metric
   * @return whether or not the metric was removed
   */
  public boolean remove(String name) {
    final Metric metric = metrics.remove(name);
    if (metric != null) {
      onMetricRemoved(name, metric);
      return true;
    }
    return false;
  }

  /**
   * Removes all metrics which match the given filter.
   *
   * @param filter a filter
   */
  public void removeMatching(MetricFilter filter) {
    Iterator<Map.Entry<String, Metric>> iterator = metrics.entrySet().iterator();
    Map.Entry<String, Metric> entry;
    while (iterator.hasNext()) {
      entry = iterator.next();
      if (filter.matches(entry.getKey(), entry.getValue())) {
        iterator.remove();
      }
    }

//    for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
//      if (filter.matches(entry.getKey(), entry.getValue())) {
//        remove(entry.getKey());
//      }
//    }
  }

  /**
   * Adds a {@link MetricRegistryListener} to a collection of listeners that will be notified on
   * metric creation.  Listeners will be notified in the order in which they are added.
   * <b>N.B.:</b> The listener will be notified of all existing metrics when it first registers.
   *
   * @param listener the listener that will be notified
   */
  public void addListener(MetricRegistryListener listener) {
    listeners.add(listener);

    for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
      notifyListenerOfAddedMetric(listener, entry.getValue(), entry.getKey());
    }
  }

  /**
   * Removes a {@link MetricRegistryListener} from this registry's collection of listeners.
   *
   * @param listener the listener that will be removed
   */
  public void removeListener(MetricRegistryListener listener) {
    listeners.remove(listener);
  }

  /**
   * Returns a set of the names of all the metrics in the registry.
   *
   * @return the names of all the metrics
   */
  public SortedSet<String> getNames() {
    return Collections.unmodifiableSortedSet(new TreeSet<>(metrics.keySet()));
  }

  /**
   * Returns a map of all the {@link Counter} in the registry and their names.
   *
   * @return all the counters in the registry
   */
  public SortedMap<String, Counter> getCounters() {
    return getCounters(MetricFilter.ALL);
  }

  /**
   * Returns a map of all the counters in the registry and their names which match the given
   * filter.
   *
   * @param filter the metric filter to match
   * @return all the counters in the registry
   */
  public SortedMap<String, Counter> getCounters(MetricFilter filter) {
    return getMetrics(Counter.class, filter);
  }

  /**
   * Returns a map of all the {@link Literal} in the registry and their names.
   *
   * @return all the counters in the registry
   */
  public SortedMap<String, Literal> getLiterals() {
    return getLiterals(MetricFilter.ALL);
  }

  /**
   * Returns a map of all the literals in the registry and their names which match the given
   * filter.
   *
   * @param filter the metric filter to match
   * @return all the counters in the registry
   */
  public SortedMap<String, Literal> getLiterals(MetricFilter filter) {
    return getMetrics(Literal.class, filter);
  }

  /**
   * Returns a map of all the {@link Gauge} in the registry and their names.
   *
   * @return all the counters in the registry
   */
  public SortedMap<String, Gauge> getGauges() {
    return getGauges(MetricFilter.ALL);
  }

  /**
   * Returns a map of all the gauges in the registry and their names which match the given
   * filter.
   *
   * @param filter the metric filter to match
   * @return all the counters in the registry
   */
  public SortedMap<String, Gauge> getGauges(MetricFilter filter) {
    return getMetrics(Gauge.class, filter);
  }

  /**
   * reset counter with specified name to zero
   *
   * @param name
   */
  public void resetCounter(String name) {
    Counter counter = counter(name);
    counter.reset();
  }

  /**
   * reset all counter with specified filter to zero
   *
   * @param filter MetricFilter, startswith / endswith / contains
   */
  public void resetCounters(MetricFilter filter) {
    SortedMap<String, Counter> counters = getMetrics(Counter.class, filter);
    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      entry.getValue().reset();
    }
  }


  @SuppressWarnings("unchecked")
  private <T extends Metric> T getOrAdd(String name, MetricBuilder<T> builder) {
    String[] scopeWordArray = name.trim().split("\\.");

    if (!metricsNameTrie.containsIntermediateNode(scopeWordArray)) {
      final Metric metric = metrics.get(name);
      if (builder.isInstance(metric)) {
        return (T) metric;
      } else if (metric == null) {
        try {
          T newMetric = register(name, builder.newMetric());
          metricsNameTrie.insert(scopeWordArray);
          return newMetric;
        } catch (IllegalArgumentException e) {
          final Metric added = metrics.get(name);
          if (builder.isInstance(added)) {
            return (T) added;
          }
        }
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  protected <T extends Metric> SortedMap<String, T> getMetrics(Class<T> klass, MetricFilter filter) {
    final TreeMap<String, T> timers = new TreeMap<>();
    for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
      if (klass.isInstance(entry.getValue()) && filter.matches(entry.getKey(),
          entry.getValue())) {
        timers.put(entry.getKey(), (T) entry.getValue());
      }
    }
    return Collections.unmodifiableSortedMap(timers);
  }

  /**
   * get all metrics as a map of string to metric
   *
   * @return java.util.Map_java.lang.String, com.tencent.bk.base.metrics.Metric
   */
  @Override
  public Map<String, Metric> getMetrics() {
    return Collections.unmodifiableMap(metrics);
  }

  private void onMetricAdded(String name, Metric metric) {
    for (MetricRegistryListener listener : listeners) {
      notifyListenerOfAddedMetric(listener, metric, name);
    }
  }

  private void notifyListenerOfAddedMetric(MetricRegistryListener listener, Metric metric, String name) {
    if (metric instanceof Gauge) {
      listener.onGaugeAdded(name, (Gauge<?>) metric);
    } else if (metric instanceof Counter) {
      listener.onCounterAdded(name, (Counter) metric);
    } else if (metric instanceof Literal) {
      listener.onLiteralAdded(name, (Literal) metric);
    } else {
      throw new IllegalArgumentException("Unknown metric type: " + metric.getClass());
    }
  }

  private void onMetricRemoved(String name, Metric metric) {
    for (MetricRegistryListener listener : listeners) {
      notifyListenerOfRemovedMetric(name, metric, listener);
    }
  }

  private void notifyListenerOfRemovedMetric(String name, Metric metric, MetricRegistryListener listener) {
    if (metric instanceof Gauge) {
      listener.onGaugeRemoved(name);
    } else if (metric instanceof Counter) {
      listener.onCounterRemoved(name);
    }
    if (metric instanceof Literal) {
      listener.onLiteralRemoved(name);
    } else {
      throw new IllegalArgumentException("Unknown metric type: " + metric.getClass());
    }
  }

  @FunctionalInterface
  public interface MetricSupplier<T extends Metric> {
    T newMetric();
  }

  /**
   * A quick and easy way of capturing the notion of default metrics.
   */
  private interface MetricBuilder<T extends Metric> {
    MetricBuilder<Counter> COUNTERS = new MetricBuilder<Counter>() {
      @Override
      public Counter newMetric() {
        return new Counter();
      }

      @Override
      public boolean isInstance(Metric metric) {
        return Counter.class.isInstance(metric);
      }
    };

    MetricBuilder<Literal> LITERALS = new MetricBuilder<Literal>() {
      @Override
      public Literal newMetric() {
        return new Literal();
      }

      @Override
      public boolean isInstance(Metric metric) {
        return Literal.class.isInstance(metric);
      }
    };

    T newMetric();

    boolean isInstance(Metric metric);
  }

}
