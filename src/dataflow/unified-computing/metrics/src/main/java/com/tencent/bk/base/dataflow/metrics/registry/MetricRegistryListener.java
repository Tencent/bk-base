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

import java.util.EventListener;

/**
 * Listeners for events from the registry.  Listeners must be thread-safe.
 */
public interface MetricRegistryListener extends EventListener {
  void onGaugeAdded(String name, Gauge<?> gauge);

  void onGaugeRemoved(String name);

  void onCounterAdded(String name, Counter counter);

  void onCounterRemoved(String name);

  void onLiteralAdded(String name, Literal counter);

  void onLiteralRemoved(String name);

  /**
   * A no-op implementation of {@link MetricRegistryListener}.
   */
  abstract class AbstractBaseListener implements MetricRegistryListener {
    @Override
    public void onGaugeAdded(String name, Gauge<?> gauge) {
    }

    @Override
    public void onGaugeRemoved(String name) {
    }

    @Override
    public void onCounterAdded(String name, Counter counter) {
    }

    @Override
    public void onCounterRemoved(String name) {
    }

    @Override
    public void onLiteralAdded(String name, Literal counter) {
    }

    @Override
    public void onLiteralRemoved(String name) {
    }
  }
}

