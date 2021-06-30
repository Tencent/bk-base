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

import java.util.concurrent.atomic.LongAdder;

/**
 * Counter ： 数值类指标
 */
public class Counter implements Counting, Metric {
  private final LongAdder count;
  private boolean resetWhenSerialized = true;
  private boolean incWhenSerialized = false;

  /**
   * Counter
   */
  public Counter() {
    this.count = new LongAdder();
  }

  /**
   * 构造 Counter
   *
   * @param resetWhenSerialized 序列化后（如输出指标时），是否自动重置为0
   */
  public Counter(boolean resetWhenSerialized) {
    this.count = new LongAdder();
    this.resetWhenSerialized = resetWhenSerialized;
  }

  /**
   * 构造 Counter
   *
   * @param resetWhenSerialized 序列化后（如输出指标时），是否自动重置为0
   * @param incWhenSerialized   序列化后（如输出指标时），是否自动+1（隐含不自动重置为0）
   */
  public Counter(boolean resetWhenSerialized, boolean incWhenSerialized) {
    this.count = new LongAdder();
    if (incWhenSerialized) {
      resetWhenSerialized = false;
    }
    this.resetWhenSerialized = resetWhenSerialized;
    this.incWhenSerialized = incWhenSerialized;
  }

  /**
   * Increment the counter by one.
   */
  public void inc() {
    inc(1);
  }

  /**
   * Increment the counter by {@code n}.
   *
   * @param n the amount by which the counter will be increased
   */
  public void inc(long n) {
    count.add(n);
  }

  /**
   * Decrement the counter by one.
   */
  public void dec() {
    dec(1);
  }

  /**
   * Decrement the counter by {@code n}.
   *
   * @param n the amount by which the counter will be decreased
   */
  public void dec(long n) {
    count.add(-n);
  }

  /**
   * Returns the counter's current value.
   *
   * @return the counter's current value
   */
  @Override
  public long getCount() {
    return count.sum();
  }

  /**
   * 获取 counter 的最新值返回，并重置为0
   *
   * @return long
   */
  public long getCountAndReset() {
    return count.sumThenReset();
  }

  /**
   * 重置 counter 为0
   */
  public void reset() {
    count.reset();
  }

  /**
   * 判断当 counter 序列化时是否重置
   *
   * @return boolean
   */
  public boolean isResetWhenSerialized() {
    return resetWhenSerialized;
  }

  /**
   * 设置当 counter 序列化时是否重置
   *
   * @param resetWhenSerialized
   */
  public void setResetWhenSerialized(boolean resetWhenSerialized) {
    this.resetWhenSerialized = resetWhenSerialized;
  }

  /**
   * 判断当 counter 序列化时是否自增（这种情况下，不重置）
   *
   * @return boolean
   */
  public boolean isIncWhenSerialized() {
    return incWhenSerialized;
  }

  /**
   * 设置当 counter 序列化时是否自增（这种情况下，不重置）
   *
   * @param incWhenSerialized
   */
  public void setIncWhenSerialized(boolean incWhenSerialized) {
    this.incWhenSerialized = incWhenSerialized;
  }
}
