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

import com.tencent.bk.base.dataflow.metrics.Counter;
import com.tencent.bk.base.dataflow.metrics.Gauge;
import com.tencent.bk.base.dataflow.metrics.Literal;
import com.tencent.bk.base.dataflow.metrics.MetricFilter;
import com.tencent.bk.base.dataflow.metrics.registry.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The abstract base class for all scheduled reporters (i.e., reporters which process a registry's
 * metrics periodically).
 */
public abstract class AbstractScheduledReporter implements Reporter {

  protected static String counterPrefix;
  protected static String literalPrefix;
  protected static String gaugePrefix;
  private static final Logger LOG = LoggerFactory.getLogger(AbstractScheduledReporter.class);
  private static final AtomicInteger FACTORY_ID = new AtomicInteger();
  protected Map<String, MetricRegistry> registryMap;
  protected ScheduledExecutorService executor = null;
  protected boolean shutdownExecutorOnStop;
  protected ScheduledFuture<?> scheduledFuture;
  protected MetricFilter filter;
  protected boolean periodicReport = true;

  /**
   * Creates a new {@link AbstractScheduledReporter} instance.
   *
   * @param name   the reporter's name
   * @param filter the filter for which metrics to report
   */
  protected AbstractScheduledReporter(String name,
                                      MetricFilter filter,
                                      boolean periodicReport) {
    this(name, filter, null, periodicReport);
  }

  /**
   * Creates a new {@link AbstractScheduledReporter} instance.
   *
   * @param name     the reporter's name
   * @param filter   the filter for which metrics to report
   * @param executor the executor to use while scheduling reporting of metrics.
   */
  protected AbstractScheduledReporter(String name,
                                      MetricFilter filter,
                                      ScheduledExecutorService executor,
                                      boolean periodicReport) {
    this(name, filter, executor, true, periodicReport);
  }

  /**
   * Creates a new {@link AbstractScheduledReporter} instance.
   *
   * @param name                   the reporter's name
   * @param filter                 the filter for which metrics to report
   * @param executor               the executor to use while scheduling reporting of metrics.
   * @param shutdownExecutorOnStop if true, then executor will be stopped in same time with this reporter
   */
  protected AbstractScheduledReporter(String name,
                                      MetricFilter filter,
                                      ScheduledExecutorService executor,
                                      boolean shutdownExecutorOnStop,
                                      boolean periodicReport) {
    this.filter = filter;
    this.periodicReport = periodicReport;
    if (periodicReport) {
      this.executor = executor == null ? createDefaultExecutor(name) : executor;
    }
    this.shutdownExecutorOnStop = shutdownExecutorOnStop;
  }

  private static ScheduledExecutorService createDefaultExecutor(String name) {
    return Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(name + '-'
        + FACTORY_ID.incrementAndGet()));
  }

  /**
   * addMetricRegistry
   *
   * @param registry
   */
  public void addMetricRegistry(MetricRegistry registry) {
    if (registryMap == null) {
      registryMap = new ConcurrentHashMap<>();
    }
    registryMap.put(registry.getMetricRegistryName(), registry);
  }

  /**
   * removeMetricRegistry
   *
   * @param metricRegistryName
   */
  public void removeMetricRegistry(String metricRegistryName) {
    if (registryMap == null) {
      return;
    }
    registryMap.remove(metricRegistryName);
  }

  /**
   * Starts the reporter polling at the given period.
   *
   * @param period the amount of time between polls
   * @param unit   the unit for {@code period}
   */
  public void start(long period, TimeUnit unit) {
    LocalDateTime now = LocalDateTime.now();
    long initialDelay = period;
    if (unit == TimeUnit.SECONDS && period >= 60) {
      LocalDateTime roundCeiling = now.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
      initialDelay = ChronoUnit.SECONDS.between(now, roundCeiling);
    }

    start(initialDelay, period, unit);
  }

  /**
   * Starts the reporter polling at the given period with the specific runnable action.
   * Visible only for testing.
   */
  synchronized void start(long initialDelay, long period, TimeUnit unit, Runnable runnable) {
    if (this.scheduledFuture != null) {
      throw new IllegalArgumentException("Reporter already started");
    }

    this.scheduledFuture = executor.scheduleAtFixedRate(runnable, initialDelay, period, unit);
  }

  /**
   * Starts the reporter polling at the given period.
   *
   * @param initialDelay the time to delay the first execution
   * @param period       the amount of time between polls
   * @param unit         the unit for {@code period} and {@code initialDelay}
   */
  public synchronized void start(long initialDelay, long period, TimeUnit unit) {
    if (this.periodicReport) {
      start(initialDelay, period, unit, () -> {
        try {
          report();
        } catch (Throwable ex) {
          LOG.error("Exception thrown from {}#report. Exception was suppressed.", this.getClass().getSimpleName(), ex);
        }
      });
    }
  }

  /**
   * Stops the reporter and if shutdownExecutorOnStop is true then shuts down its thread of execution.
   * Uses the shutdown pattern from http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html
   */
  public void stop() {
    if (shutdownExecutorOnStop && executor != null) {
      executor.shutdown(); // Disable new tasks from being submitted
    }

    if (periodicReport) {
      try {
        report(); // Report metrics one last time
      } catch (Exception e) {
        LOG.warn("Final reporting of metrics failed.", e);
      }
    }

    if (shutdownExecutorOnStop && executor != null) {
      try {
        // Wait a while for existing tasks to terminate
        if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
          executor.shutdownNow(); // Cancel currently executing tasks
          // Wait a while for tasks to respond to being cancelled
          if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
            LOG.warn("ScheduledExecutorService did not terminate.");
          }
        }
      } catch (InterruptedException ie) {
        // (Re-)Cancel if current thread also interrupted
        executor.shutdownNow();
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
    } else {
      // The external manager (like JEE container) responsible for lifecycle of executor
      cancelScheduledFuture();
    }
  }

  private synchronized void cancelScheduledFuture() {
    if (this.scheduledFuture == null) {
      // was never started
      return;
    }
    if (this.scheduledFuture.isCancelled()) {
      // already cancelled
      return;
    }
    // just cancel the scheduledFuture and exit
    this.scheduledFuture.cancel(false);
  }

  /**
   * Stops the reporter and shuts down its thread of execution.
   */
  @Override
  public void close() {
    stop();
  }

  /**
   * getMetricsStringAndReport
   *
   * @return java.util.Map_java.lang.String, java.lang.String
   */
  public Map<String, String> getMetricsStringAndReport() {
    synchronized (this) {
      Map<String, String> result = new HashMap<>();
      for (Map.Entry<String, MetricRegistry> entry : registryMap.entrySet()) {
        MetricRegistry registry = entry.getValue();
        String msg = getMetricsStringAndReport(
            registry.getCounters(filter),
            registry.getLiterals(filter),
            registry.getGauges(filter));
        result.put(entry.getKey(), msg);
      }
      return result;
    }
  }

  /**
   * getMetricsStringAndReport
   *
   * @param name
   * @return java.lang.String
   */
  public String getMetricsStringAndReport(String name) {
    synchronized (this) {
      String resultStr = null;
      for (Map.Entry<String, MetricRegistry> entry : registryMap.entrySet()) {
        if (entry.getKey().equals(name)) {
          MetricRegistry registry = entry.getValue();
          resultStr = getMetricsStringAndReport(
              registry.getCounters(filter),
              registry.getLiterals(filter),
              registry.getGauges(filter));
        }
      }
      return resultStr;
    }
  }

  /**
   * getMetricsStringAndReport
   *
   * @param counters
   * @param literals
   * @param gauges
   * @return java.lang.String
   */
  @SuppressWarnings("rawtypes")
  public abstract String getMetricsStringAndReport(SortedMap<String, Counter> counters,
                                                   SortedMap<String, Literal> literals,
                                                   SortedMap<String, Gauge> gauges);

  /**
   * getMetricsStringAndReport
   *
   * @param registryMap
   * @return java.util.Map_java.lang.String, java.lang.String
   */
  public abstract Map<String, String> getMetricsStringAndReport(Map<String, MetricRegistry> registryMap);

  /**
   * Report the current values of all metrics in the registry.
   */
  public void report() {
    synchronized (this) {
      for (Map.Entry<String, MetricRegistry> entry : registryMap.entrySet()) {
        MetricRegistry registry = entry.getValue();
        report(registry.getCounters(filter), registry.getLiterals(filter), registry.getGauges(filter));
      }
    }
  }

  /**
   * Called periodically by the polling thread. Subclasses should report all the given metrics.
   */
  @SuppressWarnings("rawtypes")
  public abstract void report(SortedMap<String, Counter> counters,
                              SortedMap<String, Literal> literals,
                              SortedMap<String, Gauge> gauges);

  /**
   * report
   *
   * @param registryMap
   */
  public abstract void report(Map<String, MetricRegistry> registryMap);

  protected boolean isShutdownExecutorOnStop() {
    return shutdownExecutorOnStop;
  }

  /**
   * setCountersPrefix
   *
   * @param prefixStr
   */
  public void setCountersPrefix(String prefixStr) {
    counterPrefix = prefixStr;
  }

  /**
   * setLiteralsPrefix
   *
   * @param prefixStr
   */
  public void setLiteralsPrefix(String prefixStr) {
    literalPrefix = prefixStr;
  }

  /**
   * setGaugesPrefix
   *
   * @param prefixStr
   */
  public void setGaugesPrefix(String prefixStr) {
    gaugePrefix = prefixStr;
  }

  /**
   * A simple named thread factory.
   */
  @SuppressWarnings("NullableProblems")
  private static class NamedThreadFactory implements ThreadFactory {
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    private NamedThreadFactory(String name) {
      final SecurityManager s = System.getSecurityManager();
      this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
      this.namePrefix = "metrics-" + name + "-thread-";
    }

    /**
     * newThread
     *
     * @param r
     * @return java.lang.Thread
     */
    @Override
    public Thread newThread(Runnable r) {
      final Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
      t.setDaemon(true);
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      return t;
    }
  }
}

