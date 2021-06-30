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

package com.tencent.bk.base.dataflow.debug;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class LimitQueue<E> implements Queue<E>, Serializable {

    private static final long serialVersionUID = 1L;

    // 队列长度
    private int limit;
    private Queue<E> queue = new LinkedList<E>();

    public LimitQueue(int limit) {
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }

    public Queue<E> getQueue() {
        return queue;
    }

    /**
     * 入列
     *
     * @param e e
     * @return
     */
    @Override
    public boolean offer(E e) {
        if (queue.size() >= limit) {
            queue.poll();
        }
        return queue.offer(e);
    }

    /**
     * 出队
     *
     * @return
     */
    @Override
    public E poll() {
        return queue.poll();
    }

    @Override
    public boolean add(E e) {
        return queue.add(e);
    }

    @Override
    public E element() {
        return queue.element();
    }

    @Override
    public E peek() {
        return queue.peek();
    }

    @Override
    public boolean isEmpty() {
        return queue.size() == 0;
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public boolean remove(Object o) {
        return queue.remove(o);
    }

    @Override
    public E remove() {
        return queue.remove();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return queue.addAll(c);
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public void clear() {
        queue.clear();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return queue.containsAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return queue.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return queue.retainAll(c);
    }
}
