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

package com.tencent.bk.base.dataflow.metrics.util;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class MapUtils {
  /**
   * merge
   *
   * @param lhs
   * @param rhs
   * @return java.util.Map_K, V
   */
  public static <K, V> Map<K, V> merge(final Map<K, V> lhs, final Map<K, V> rhs) {
    Map<K, V> result = null;
    if (lhs == null || lhs.size() == 0) {
      result = copy(rhs);
    } else if (rhs == null || rhs.size() == 0) {
      result = copy(lhs);
    } else {
      result = copy(lhs);
      result.putAll(rhs);
    }
    return result;
  }

  /**
   * copy
   *
   * @param source
   * @return java.util.Map_K, V
   */
  public static <K, V> Map<K, V> copy(final Map<K, V> source) {
    if (source == null) {
      return null;
    }
    final Map<K, V> result = new HashMap<K, V>();
    result.putAll(source);
    return result;
  }

  /**
   * generateNestedMap
   *
   * @param map
   * @param path
   * @param value
   */
  @SuppressWarnings("unchecked")
  public static void generateNestedMap(Map<String, Object> map, String path, Object value) {
    int start = 0;
    int end = path.indexOf('.', start);
    while (end != -1 && map != null) {
      map = (Map<String, Object>) map.computeIfAbsent(path.substring(start, end),
          k -> new HashMap<String, Object>());
      start = end + 1;
      end = path.indexOf('.', start);
    }
    if (map != null) {
      map.put(path.substring(start), value);
    }
  }

  /**
   * generateNestedMap
   *
   * @param map
   * @param result
   * @return java.util.Map_java.lang.String,? extends java.lang.Object
   */
  public static Map<String, ? extends Object> generateNestedMap(
      Map<String, ? extends Object> map,
      Map<String, Object> result) {
    for (Map.Entry<String, ? extends Object> entry : map.entrySet()) {
      String path = entry.getKey();
      Object value = entry.getValue();
      generateNestedMap(result, path, value);
    }
    return result;
  }

  /**
   * generateNestedMap
   *
   * @param prefix
   * @param map
   * @param result
   * @return java.util.Map_java.lang.String,? extends java.lang.Object
   */
  public static Map<String, ? extends Object> generateNestedMap(
      String prefix,
      Map<String, ? extends Object> map,
      Map<String, Object> result) {
    for (Map.Entry<String, ? extends Object> entry : map.entrySet()) {
      String path = entry.getKey();
      if (prefix != null && !prefix.isEmpty()) {
        if (prefix.trim().charAt(prefix.length() - 1) != '.') {
          path = prefix + "." + entry.getKey();
        } else {
          path = prefix + entry.getKey();
        }
      }
      Object value = entry.getValue();
      generateNestedMap(result, path, value);
    }
    return result;
  }

  /**
   * Generic function to construct a new TreeMap from HashMap
   *
   * @param hashMap
   * @return java.util.Map_K, V
   */
  public static <K, V> Map<K, V> getTreeMap(Map<K, V> hashMap) {
    Map<K, V> treeMap = new TreeMap<>();
    treeMap.putAll(hashMap);
    return treeMap;
  }

}
