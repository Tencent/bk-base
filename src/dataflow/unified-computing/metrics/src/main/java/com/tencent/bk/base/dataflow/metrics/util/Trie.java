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

/**
 * trie 树
 */
public class Trie {
  private TrieNode root;

  /**
   * Trie
   */
  public Trie() {
    root = new TrieNode();
  }

  /**
   * 往 trie 树中按数组顺序插入一系列节点
   *
   * @param words 待插入一系列节点
   */
  public void insert(String[] words) {
    TrieNode current = root;

    for (String word : words) {
      if (current != null) {
        current = current.getChildren().computeIfAbsent(word, c -> new TrieNode(word));
      }
    }
    if (current != null) {
      current.setLeaf(true);
    }
  }

  /**
   * 查询是否存在中间节点匹配（非leaf节点匹配）
   *
   * @param words 待匹配一系列节点
   * @return boolean
   */
  public boolean containsIntermediateNode(String[] words) {
    TrieNode current = root;

    for (int i = 0; i < words.length; i++) {
      String ch = words[i];
      TrieNode node = current.getChildren().get(ch);
      if (node == null) {
        return false;
      }
      current = node;
    }
    return !current.isLeaf();
  }

  /**
   * 查询 trie 树是否为空
   *
   * @return boolean
   */
  public boolean isEmpty() {
    return root == null;
  }

}