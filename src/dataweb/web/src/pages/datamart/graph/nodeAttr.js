/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

/* eslint-disable camelcase */
export default {
  methods: {
    label_plus_minus(locations) {
      // 用于血缘节点判断加减号展示,dp节点不显示加减号
      const locations_dict = {};

      for (const each_location of locations) {
        locations_dict[each_location.id] = each_location;
      }
      for (const each_location of locations) {
        if (each_location.direction === 'OUTPUT' && each_location.type !== 'data_processing') {
          if (each_location.has_children) {
            let is_child_in_graph = false;
            for (const each_children of each_location.children_list) {
              if (Object.keys(locations_dict).indexOf(each_children) !== -1) {
                is_child_in_graph = true;
                break;
              }
            }
            if (is_child_in_graph) {
              each_location.display_plus = false;
              each_location.display_minus = true;
            } else {
              each_location.display_plus = true;
              each_location.display_minus = false;
            }
          } else {
            each_location.display_plus = false;
            each_location.display_minus = false;
          }
        } else if (each_location.direction === 'INPUT' && each_location.type !== 'data_processing') {
          if (each_location.has_parent) {
            let is_parent_in_graph = false;
            for (const each_parent of each_location.parent_list) {
              if (Object.keys(locations_dict).indexOf(each_parent) !== -1) {
                is_parent_in_graph = true;
                break;
              }
            }
            if (is_parent_in_graph) {
              each_location.display_plus = false;
              each_location.display_minus = true;
            } else {
              each_location.display_plus = true;
              each_location.display_minus = false;
            }
          } else {
            each_location.display_plus = false;
            each_location.display_minus = false;
          }
        } else {
          each_location.display_plus = false;
          each_location.display_minus = false;
        }
      }
    },

    delete_node(cur_node_id, node_id, locations, lines, direction) {
      // 递归删除所有的子节点和边
      const node = this.find_node(node_id, locations);
      // 判断node是不是字典
      if (node === null || (node !== null && !(node instanceof Object))) {
        return;
      }
      if (direction === 'INPUT') {
        for (const parent_id of node.parent_list) {
          this.delete_node(cur_node_id, parent_id, locations, lines, direction);
        }
      } else {
        for (const child_id of node.children_list) {
          this.delete_node(cur_node_id, child_id, locations, lines, direction);
        }
      }

      if (node_id === cur_node_id) {
        return;
      }

      for (let i = locations.length - 1; i >= 0; i--) {
        if (node_id === locations[i].id) {
          locations.splice(i, 1);
        }
      }

      if (direction === 'INPUT') {
        for (let i = lines.length - 1; i >= 0; i--) {
          if (node_id === lines[i].source.id) {
            lines.splice(i, 1);
          }
        }
      } else {
        for (let i = lines.length - 1; i >= 0; i--) {
          if (node_id === lines[i].target.id) {
            lines.splice(i, 1);
          }
        }
      }

      return;
    },

    find_node(node_id, locations) {
      // 从locations中找到node_id对应的节点
      let result = null;
      for (const node of locations) {
        if (node.id === node_id) {
          result = node;
          break;
        }
      }
      return result;
    },
  },
};
