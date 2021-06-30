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

/* eslint-disable */
class FlowNodeToolKit {
  constructor(flowGraphData, costumeWidth = 615, dataflow_width = 1372, drag_left = null, drag_top = null, direction = null, clickNodeID = null) {
    this.dataflow_width = dataflow_width;
    this.levelPrefix = 'level_';
    this.offsetYCache = {};
    this.graphData = JSON.parse(JSON.stringify(flowGraphData));
    this.getNodeConfigByNodeName = () => {
      return { tptGroup: 'default' };
    };
    this.nodeConfig = {
      offsetX: 500,
      offsetY: 500,
      nodeMarginX: 0,
      nodeMarginY: 10,
      /** 不同类型节点的宽高设置  */
      group: {
        'graph-round': {
          width: 168,
          height: 34,
        },
        'graph-ractangle': {
          width: 168,
          height: 34,
        },
        'graph-square': {
          width: 168,
          height: 34,
        },
        default: {
          width: 131,
          height: 34,
        },
        root: {
          width: 166,
          height: 44,
        },
      },
      // 每根线之间的长度
      lineWidth: 40,
    };
    this.nodeTree = this.__initNodeTree();
    this.nodeTree[0]['direction'] = 'right';
    // this.__digui(this.nodeTree[0])
    /** 记录每层叶子节点数据以及最大叶子节点 */
    this.nodeTreeAttrs = this.__computedPerLevelNodeCount(this.nodeTree);

    /** 计算节点位置 */
    // this.__computedNodePosition(this.nodeTree, 0, this.nodeConfig.offsetX, this.nodeConfig.offsetY)
    this.__computedNodePosition(this.nodeTree, 0, costumeWidth, 20, costumeWidth, 20);
    this.drag_left = drag_left ? drag_left : 0;
    this.drag_top = drag_top ? drag_top : 0;
    if (drag_left && drag_left != 0) {
      if (drag_top && drag_top != 0) {
        this.__computedNodePosition(this.nodeTree, 0, costumeWidth - drag_left, 20 - drag_top, costumeWidth - drag_left, 20 - drag_top);
        this.drag_left = drag_left;
      } else {
        this.__computedNodePosition(this.nodeTree, 0, costumeWidth - drag_left, 20, costumeWidth - drag_left, 20);
        this.drag_left = drag_left;
      }
    }
    let minx, maxx, miny, maxy;
    let xytmp = this.getMinMaxPositionXY();
    minx = xytmp.min_x;
    maxx = xytmp.max_x;
    miny = xytmp.min_y;
    maxy = xytmp.max_y;
    this.direction = direction;
    this.minXAboveZero = 0;
    this.minYAboveZero = 0;
    this.offset = 0;
    // 获取做自适应之前当前点击节点的坐标
    let cur_x_before = null;
    let cur_y_before = null;
    let cur_node = this.__findNodePosition(clickNodeID);
    if (cur_node) {
      cur_x_before = cur_node.cur_x;
      cur_y_before = cur_node.cur_y;
    }
    if (direction == 'left') {
      if (minx < 16) {
        this.setMinPositionXAboveZero(minx);
        xytmp = this.getMinMaxPositionXY();
        minx = xytmp.min_x;
        maxx = xytmp.max_x;
        miny = xytmp.min_y;
        maxy = xytmp.max_y;
      }
      if (this.drag_left + minx < 0) {
        this.offset = -(drag_left + minx) + 18;
      }
      this.minXAboveZero = minx;
    }
    // else if (direction == 'right')
    else {
      if (minx < 16) {
        this.setMinPositionXAboveZero(minx);
        xytmp = this.getMinMaxPositionXY();
        minx = xytmp.min_x;
        maxx = xytmp.max_x;
        miny = xytmp.min_y;
        maxy = xytmp.max_y;
      }
      // this.offset是带方向的，画布该向左挪的时候，offset就是为负
      this.offset = this.setMaxPositionXWithinBoundary(maxx);
      this.minXAboveZero = minx;
    }
    // else {
    //     this.setMaxPositionXWithinBoundary(maxx)
    //     xytmp = this.getMinMaxPositionXY()
    //     minx = xytmp.min_x
    //     maxx = xytmp.max_x
    //     miny = xytmp.min_y
    //     maxy = xytmp.max_y
    //     this.setMinPositionXAboveZero(minx)
    // }
    if (clickNodeID) {
      // 1.保证展开节点的最上面节点是在画布范围以内
      if (miny < 12) {
        this.setMinPositionYAboveZero(miny);
      }
      // 2.保证当前节点的子节点的孩子节点在窗口范围以内
      let clickNodeXYTmp = this.__findNodeYMaxMin(clickNodeID);
      let miny_cur_child = clickNodeXYTmp.min_y;
      let maxy_cur_child = clickNodeXYTmp.max_y;
      let offsetY_tmp = 0;
      if (miny_cur_child < 12) {
        // 1）保证当前节点的子节点的最小值在画布范围以内
        offsetY_tmp = this.setMinPositionYAboveZero(miny_cur_child);
      }
      maxy_cur_child += offsetY_tmp;
      // this.offsetY是带方向的，画布该向上挪的时候，offset就是为负
      // 2）保证当前节点的子节点的最大值在窗口范围以内
      this.offsetY = this.setMaxPositionYWithinBoundary(maxy_cur_child);
      xytmp = this.getMinMaxPositionXY();
      minx = xytmp.min_x;
      maxx = xytmp.max_x;
      miny = xytmp.min_y;
      maxy = xytmp.max_y;
      this.minYAboveZero = miny;
    }
    // 获取做自适应之后当前点击节点的坐标
    let cur_x_after = null;
    let cur_y_after = null;
    cur_node = this.__findNodePosition(clickNodeID);
    if (cur_node) {
      cur_x_after = cur_node.cur_x;
      cur_y_after = cur_node.cur_y;
    }
    // 将最左边节点拖拽到窗口最右侧/将最右边节点拖拽到最左侧，点击+，出现的节点跳动问题
    // 如果当前的节点在树形视图的左侧，只需要判断当前节点的坐标是不是在窗口右侧
    // 如果当前的节点在树形视图的右侧，只需要判断当前节点的坐标是不是在窗口
    let offset_set_cur_back = this.setCurNodeBack(cur_x_after);
    this.offset += offset_set_cur_back;
  }

  __digui(tree_node) {
    // 递归将构建出来的树的每一个节点加上direction属性
    if (!tree_node || !Object.keys(tree_node).includes('childNodes') || tree_node['childNodes'].length == 0) {
      return;
    }

    if (tree_node['id'] == 'sentiment' || tree_node['id'] == 'game') {
      tree_node['direction'] = 'left';
    }
    // 如果当前节点count为0，则对应孩子节点都标记父节点为0的属性
    for (let child of tree_node['childNodes']) {
      if (tree_node['id'] == 'game' || tree_node['id'] == 'sentiment' || tree_node['direction'] == 'left') {
        child['direction'] = 'left';
      } else {
        if (child['id'] == 'sentiment' || child['id'] == 'game') {
          child['direction'] = 'left';
        } else {
          child['direction'] = 'right';
        }
      }
      this.__digui(child);
    }
  }

  //根据ID寻找到该节点
  __findNode(nodeId, tree_node) {
    //确保该节点不是空
    if (!tree_node) {
      return null;
    }

    //判断该节点是否是目标节点
    if (tree_node['id'] == nodeId) {
      return tree_node;
    }

    //判断当前节点有孩子节点
    if (!Object.keys(tree_node).includes('childNodes') || tree_node['childNodes'].length == 0) {
      return null;
    }

    //遍历孩子节点
    for (let child of tree_node['childNodes']) {
      if (child['id'] == nodeId) {
        return child;
      } else {
        //递归孩子节点
        let node = this.__findNode(nodeId, child);
        //如果找到了节点则返回
        if (node != null) {
          return node;
        }
      }
    }
    //该节点及其子孩子节点都是不是目标节点
    return null;
  }

  //根据id寻找它所有孩子节点最大最小y坐标
  __findNodeYMaxMin(nodeId) {
    //初始化
    let node = null;
    //根据ID寻找到目标节点
    node = this.__findNode(nodeId, this.nodeTree[0]);

    //没有找到该节点
    if (node == null) {
      return null;
    }

    let min_y = 5000;
    let max_y = -5000;

    //先处理节点自己本身，防止它没有孩子节点情况发生
    const calcPosition = node.position;
    if (calcPosition.y > max_y) {
      max_y = calcPosition.y;
    }
    if (calcPosition.y < min_y) {
      min_y = calcPosition.y;
    }

    //如果有孩子节点则遍历它的孩子节点，找到最大最小y值
    if (Object.keys(node).includes('childNodes') && node['childNodes'].length != 0) {
      for (let child of node.childNodes) {
        const calcPosition = child.position;
        if (calcPosition.y > max_y) {
          max_y = calcPosition.y;
        }
        if (calcPosition.y < min_y) {
          min_y = calcPosition.y;
        }
      }
    }
    return { min_y: min_y, max_y: max_y, cur_x: node.position.x, cur_y: node.position.y };
  }

  //记录点击的点的信息
  __findNodePosition(nodeId) {
    //初始化
    let node = null;
    //根据ID寻找到目标节点
    node = this.__findNode(nodeId, this.nodeTree[0]);

    if (node) {
      return { cur_x: node.position.x, cur_y: node.position.y };
    }
    return null;
  }

  __initNodeTree() {
    let nodeTree = Object.values(this.__getRootNode());
    const nodesWithoutLine = this.__getNodeWithoutLines();
    nodeTree.push(...nodesWithoutLine);

    nodeTree = nodeTree.filter(node => this.__getNodeConfigById(node.id));
    let cacheNode = {};
    this.__drawNodeTree(nodeTree, 0, '', cacheNode);
    this.__deleteRepeatNode(nodeTree, cacheNode);
    return nodeTree;
  }

  /** 生成节点树 */
  __drawNodeTree(rootNodes, level, pathPrefix, cacheNode = {}) {
    rootNodes.forEach(node => {
      const location = this.__getNodeConfigById(node.id);
      if (location) {
        let currentPrefix = pathPrefix;
        currentPrefix = !level ? `root#${node.id}` : `${currentPrefix}|${node.id}`;
        node['direction'] = location.direction;
        if (!cacheNode[node.id]) {
          cacheNode[node.id] = {
            absPath: currentPrefix,
            level: level,
            delete: [],
          };
        } else {
          if (cacheNode[node.id].level > level) {
            cacheNode[node.id].delete.push(currentPrefix);
          } else {
            cacheNode[node.id].delete.push(cacheNode[node.id].absPath);
            cacheNode[node.id].level = level;
            cacheNode[node.id].absPath = currentPrefix;
          }
        }
        node['groupType'] = this.__getFlowNodeGroup(location) || 'default';
        node.childNodes = this.__getChildNodesBySourceNode(node.id);
        node.childNodes && this.__drawNodeTree(node.childNodes, level + 1, currentPrefix, cacheNode);
      } else {
        console.warn(`找不到节点: from function: __drawNodeTree, file: flowNodeToolkit.js, node id: ${node.id}`);
      }
    });
  }

  __getFlowNodeGroup(location) {
    let tptGroup = '';
    if (location && location.tptGroup) {
      tptGroup = location && location.tptGroup;
    } else {
      if (this.getNodeConfigByNodeName && typeof this.getNodeConfigByNodeName === 'function') {
        const config = this.getNodeConfigByNodeName(location.type) || { tptGroup: 'default' };
        tptGroup = config.tptGroup;
      }
    }
    return tptGroup;
  }

  __deleteRepeatNode(nodeTree, cacheNode) {
    Object.keys(cacheNode).forEach(key => {
      if (cacheNode[key].delete.length) {
        cacheNode[key].delete.forEach(path => {
          this._deleteNode(nodeTree, path);
        });
      }
    });
  }

  _deleteNode(nodeTree, path) {
    let node = {};
    const directories = path.split('|');
    const deep = directories.length - 1;
    directories.forEach((dir, index) => {
      if (node) {
        const nodeid = /^root#/.test(dir) ? dir.replace(/^root#/, '') : dir;
        if (index < deep) {
          node = /^root#/.test(dir) ? nodeTree.find(item => item.id === nodeid) : (node.childNodes && node.childNodes.find(item => item.id === nodeid)) || null;
        } else {
          /^root#/.test(dir)
            ? nodeTree.splice(
              nodeTree.findIndex(item => item.id === nodeid),
              1
            )
            : node.childNodes &&
            node.childNodes.splice(
              node.childNodes.findIndex(item => item.id === nodeid),
              1
            );
        }
      }
    });
  }

  /** 筛选所有根节点 */
  __getRootNode() {
    return this.graphData.lines
      .filter(line => !this.graphData.lines.some(_line => _line.target.id === line.source.id))
      .map(line => line.source)
      .reduce((pre, next) => Object.assign(pre, { [next.id]: JSON.parse(JSON.stringify(next)) }), {});
  }

  /** 查找单独节点（根节点，没有连线） */
  __getNodeWithoutLines() {
    return this.graphData.locations.filter(location => !this.graphData.lines.some(line => line.target.id === location.id || line.source.id === location.id)).map(location => Object.assign({}, { id: location.id, node_id: location.node_id }));
  }

  /** 根据SourceID获取子节点 */
  __getChildNodesBySourceNode(nodeId) {
    return this.graphData.lines.filter(line => line.source.id === nodeId).map(line => JSON.parse(JSON.stringify(line.target)));
  }

  /** 计算左右两边每个叶子节点位置
   * @param Array nodes 叶子节点
   * @param Number level  所在层级
   * @param Number offsetX X轴偏移
   * @param Number preNodeOffsetY 基于同级别上一节点Y轴偏移
   */
  __computedNodePosition(nodes, level, offsetXLeft, preNodeOffsetYLeft, offsetXRight, preNodeOffsetYRight) {
    let currentLevel = level;
    nodes.forEach(node => {
      if (node.leftHeight || node.rightHeight) {
        if (node.leftHeight > node.rightHeight) {
          offsetXRight = offsetXRight + this.nodeConfig.group['root'].width + this.nodeConfig.lineWidth;
          node.direction = 'left';
          preNodeOffsetYRight = preNodeOffsetYRight + (node.leftHeight - node.rightHeight) / 2;
        } else {
          offsetXLeft = offsetXLeft - this.nodeConfig.group['default'].width - this.nodeConfig.lineWidth;
          node.direction = 'right';
          preNodeOffsetYLeft = preNodeOffsetYLeft + (node.rightHeight - node.leftHeight) / 2;
        }
      }
      const positionConfig = this.nodeConfig.group[node.groupType];
      const hasOneMoreChild = node.childNodes && node.childNodes.length > 1;
      let nodePositionLeft = preNodeOffsetYLeft;
      let nodePositionRight = preNodeOffsetYRight;
      let nextOffsetXLeft = offsetXLeft;
      let nextOffsetXRight = offsetXRight;
      if (node.direction == 'left') {
        nodePositionLeft = node.height / 2 + preNodeOffsetYLeft + (hasOneMoreChild ? positionConfig.height / 4 : 0);
        node['position'] = {
          x: offsetXLeft + this.nodeConfig.group[node.groupType].width / 2,
          y: nodePositionLeft + this.nodeConfig.group[node.groupType].height / 2,
        };
        // 最快的解决办法是直接-171
        nextOffsetXLeft = offsetXLeft - this.nodeConfig.group['default'].width - this.nodeConfig.lineWidth;
        // nextOffsetXLeft = offsetXLeft - positionConfig.width - this.nodeConfig.lineWidth
        node.childNodes && this.__computedNodePosition(node.childNodes, currentLevel + 1, nextOffsetXLeft, preNodeOffsetYLeft, nextOffsetXRight, preNodeOffsetYRight);
        preNodeOffsetYLeft += node.height;
      } else {
        nodePositionRight = node.height / 2 + preNodeOffsetYRight + (hasOneMoreChild ? positionConfig.height / 4 : 0);
        node['position'] = {
          x: offsetXRight + this.nodeConfig.group[node.groupType].width / 2,
          y: nodePositionRight + this.nodeConfig.group[node.groupType].height / 2,
        };
        nextOffsetXRight = offsetXRight + positionConfig.width + this.nodeConfig.lineWidth;
        node.childNodes && this.__computedNodePosition(node.childNodes, currentLevel + 1, nextOffsetXLeft, preNodeOffsetYLeft, nextOffsetXRight, preNodeOffsetYRight);
        preNodeOffsetYRight += node.height;
      }
    });
  }

  /** 清洗节点位置，合并同一节点根据不同父级计算所得位置 */
  __getCalcNodePosition() {
    let calcNodePosition = {};
    function cleanNodePosition(nodes) {
      nodes.forEach(node => {
        if (!calcNodePosition[node.id]) {
          calcNodePosition[node.id] = node.position;
        } else {
          calcNodePosition[node.id] = {
            x: node.position.x > calcNodePosition[node.id].x ? node.position.x : calcNodePosition[node.id].x,
            y: node.position.y > calcNodePosition[node.id].y ? node.position.y : calcNodePosition[node.id].y,
          };
        }

        node.childNodes && cleanNodePosition(node.childNodes);
      });
    }

    cleanNodePosition(this.nodeTree);
    return calcNodePosition;
  }

  __getNodeConfigById(id) {
    return this.graphData.locations.find(location => location.id === id);
  }

  __getNodeHeight(groupType) {
    groupType = groupType || 'default';
    return this.nodeConfig.nodeMarginY + this.nodeConfig.group[groupType].height;
  }

  /** 获取每个层级的节点数量和高度 */
  __computedPerLevelNodeCount(nodeTree) {
    let treeDeepReords = {};
    const self = this;

    /** 统计每层叶子节点数量
     * @param nodeRecodes 记录叶子节点属性
     * @param level 上一层级数
     * @param childNodes 子节点
     */
    function getLevelNodesCount(nodeRecodes, level, childNodes) {
      let currentLevel = level + 1;
      childNodes.forEach(child => {
        if (child.childNodes && child.childNodes.length) {
          getLevelNodesCount(nodeRecodes, currentLevel, child.childNodes);

          child['level'] = currentLevel;
          /** 计算当前节点的子节点的总高度 */
          child['height'] = child.childNodes.reduce((pre, curr) => {
            const currHeight = curr.height || self.__getNodeHeight(curr.groupType);
            return pre + currHeight;
          }, 0);
        } else {
          child['height'] = self.__getNodeHeight(child.groupType);
        }
      });

      /** Begin ====> 计算当前层级所有节点总高度 */
      let currentCount = childNodes.length;
      const currentHeight = childNodes.reduce((pre, curr) => {
        const currHeight = curr.height || self.__getNodeHeight(curr.groupType);
        return pre + currHeight;
      }, 0);
      const currentKey = `${self.levelPrefix}${currentLevel}`;
      if (currentCount) {
        Object.assign(nodeRecodes, {
          [currentKey]: nodeRecodes[currentKey]
            ? {
              count: nodeRecodes[currentKey].count + currentCount,
              height: nodeRecodes[currentKey].height + currentHeight,
            }
            : { count: currentCount, height: currentHeight },
        });
        currentCount = nodeRecodes[currentKey].count;
        nodeRecodes.max.count < currentCount && Object.assign(nodeRecodes.max, { level: currentLevel, count: currentCount });
      }
      /** End */
    }

    nodeTree.forEach(root => {
      const currentLevel = 0;
      const currentKey = `${self.levelPrefix}${currentLevel}`;
      const defaultHeight = self.__getNodeHeight(root.groupType);
      treeDeepReords[root.id] = {
        max: {
          level: 0,
          count: 1,
        },
        height: defaultHeight,
        [currentKey]: 1,
      };
      getLevelNodesCount(treeDeepReords[root.id], 0, root.childNodes);

      let currentLeftHeight = root.childNodes.reduce((pre, curr) => {
        // curr.height当前某个孩子节点的高度
        // self.__getNodeHeight(root.groupType)默认高度
        const currHeight = curr.height || self.__getNodeHeight(root.groupType);
        if (curr.direction == 'left') {
          return pre + currHeight;
        } else {
          return pre;
        }
      }, 0);

      let currentRightHeight = root.childNodes.reduce((pre, curr) => {
        // curr.height当前某个孩子节点的高度
        // self.__getNodeHeight(root.groupType)默认高度
        const currHeight = curr.height || self.__getNodeHeight(root.groupType);
        if (curr.direction != 'left') {
          return pre + currHeight;
        } else {
          return pre;
        }
      }, 0);
      root['leftHeight'] = currentLeftHeight;
      root['rightHeight'] = currentRightHeight;
      let currentHeight = null;
      currentHeight = currentLeftHeight > currentRightHeight ? currentLeftHeight : currentRightHeight;
      currentHeight = currentHeight > defaultHeight ? currentHeight : defaultHeight;
      treeDeepReords[root.id].height = currentHeight;
      root['height'] = currentHeight;
      root['level'] = 0;
    });

    return treeDeepReords;
  }

  __assignLocation(location) {
    const nodeConf = this.getNodeConfigByNodeName(location.type);
    let assign = {};
    if (!location.dataType) {
      assign.dataType = location.type;
    }

    if (!location.node_type) {
      assign.node_type = location.type;
    }

    if (!location.tptGroup) {
      assign.tptGroup = nodeConf.tptGroup;
    }

    if (!location.endPoints) {
      assign.endPoints = nodeConf.endPoints;
    }

    return Object.assign(location, assign);
  }

  getOffset() {
    return { offset: this.offset, offsetY: this.offsetY, direction: this.direction };
  }

  updateFlowGraph() {
    const offsetX = this.getNodeOffset();
    const position = this.__getCalcNodePosition();
    this.graphData.locations.forEach(location => {
      const calcPosition = position[location.id];
      if (calcPosition) {
        location.x = calcPosition.x + offsetX;
        location.y = calcPosition.y;
      }

      location = this.__assignLocation(location);
    });

    // this.graphData.lines.forEach(line => {
    //     line.source.arrow = 'Right'
    //     line.target.arrow = 'Left'
    // })
    //  判断当前节点有多少输出的边
    for (let each_tree of this.nodeTree) {
      this.getFromValue(each_tree, this.graphData.locations);
    }
    return this.graphData;
  }

  getTotalHigh() {
    const position = this.__getCalcNodePosition();
    let min = 5000;
    let max = -5000;
    this.graphData.locations.forEach(location => {
      const calcPosition = position[location.id];
      if (calcPosition.y > max) {
        max = calcPosition.y;
      }
      if (calcPosition.y < min) {
        min = calcPosition.y;
      }
    });
    let totalHight = max - min;
    if (totalHight < 0) {
      totalHight = 0;
    }
    return this.minYAboveZero ? totalHight + 111 + this.minYAboveZero : totalHight + 111;
  }

  getTotalWidth() {
    // 每次返回的宽度应该是所有节点的宽度 + 最左边节点的x值
    let position = this.__getCalcNodePosition();
    let min = 5000;
    let max = -5000;
    this.graphData.locations.forEach(location => {
      let calcPosition = position[location.id];
      if (calcPosition.x > max) {
        max = calcPosition.x;
      }
      if (calcPosition.x < min) {
        min = calcPosition.x;
      }
    });
    let totalWidth = max - min;
    if (totalWidth < 0) {
      totalWidth = 0;
    }
    return this.minXAboveZero > 0 ? totalWidth + 250 + this.minXAboveZero : totalWidth + 250;
  }

  getFromValue(node, graphData) {
    node['from'] = 0;
    // debugger
    // 如果node节点有childnodes这个属性
    if (Object.keys(node).includes('childNodes')) {
      node['from'] = node.childNodes.length;
      let childNodes = node.childNodes;
      childNodes.forEach(child => {
        this.getFromValue(child, graphData);
      });
    }
    for (let each_location of graphData) {
      if (each_location.id == node.id) {
        each_location.from = node.from;
        break;
      }
    }
  }

  getMinMaxPositionXY() {
    const position = this.__getCalcNodePosition();
    let min_x = 5000,
      min_y = 5000,
      max_x = -5000,
      max_y = -5000,
      nodeData = '';
    this.graphData.locations.forEach(location => {
      const calcPosition = position[location.id];
      if (calcPosition.x < min_x) {
        min_x = calcPosition.x;
        nodeData = location;
      }
      if (calcPosition.x > max_x) {
        max_x = calcPosition.x;
        nodeData = location;
      }
      if (calcPosition.y < min_y) {
        min_y = calcPosition.y;
      }
      if (calcPosition.y > max_y) {
        max_y = calcPosition.y;
      }
    });
    return { min_x, max_x, min_y, max_y, nodeData };
  }

  getNodeOffset() {
    // 节点的偏移量
    let offsetX = 0;
    // 画布内边距
    const margin = 16;
    const graphWidth = document.getElementById('dfv2-padding').clientWidth;
    const { nodeData, min_x, max_x } = this.getMinMaxPositionXY();
    // 展开direction为left的节点时，判断min_x是否处于画布之外，否处判断max_x是否处于画布之外
    if (this.direction === 'left') {
      if (min_x < margin || min_x - this.nodeConfig.group[nodeData.tptGroup].width / 2 < margin) {
        offsetX = this.nodeConfig.group[nodeData.tptGroup].width / 2 + margin;
      }
    } else {
      if (max_x > graphWidth || max_x + this.nodeConfig.group[nodeData.tptGroup].width / 2 > graphWidth) {
        offsetX = -(max_x + this.nodeConfig.group[nodeData.tptGroup].width - graphWidth - margin);
      }
    }
    return offsetX;
  }

  setMinPositionXAboveZero(min) {
    let offset = 0;
    let position = {};
    if (this.drag_left < 0) {
      if (min + this.drag_left < 0) {
        offset = Math.abs(min) + 50 - this.drag_left;
      }
    } else {
      if (min < 0) {
        offset = Math.abs(min) + 50;
      } else if (min < 16) {
        offset = Math.abs(min) + 30;
      }
    }
    if (offset != 0) {
      position = this.__getCalcNodePosition();
      this.graphData.locations.forEach(location => {
        let calcPosition = position[location.id];
        calcPosition.x += offset;
      });
    }
    return offset;
  }

  setMinPositionYAboveZero(min) {
    let offset = 0;
    let position = {};
    if (this.drag_top < 0) {
      if (min + this.drag_top < 0) {
        offset = Math.abs(min) + 30 - this.drag_top;
      }
    } else {
      if (min < 0) {
        offset = Math.abs(min) + 30;
      } else if (min < 12) {
        offset = Math.abs(min) + 10;
      }
    }
    if (offset != 0) {
      position = this.__getCalcNodePosition();
      this.graphData.locations.forEach(location => {
        let calcPosition = position[location.id];
        calcPosition.y += offset;
      });
    }
    return offset;
  }

  setCurNodeBack(cur_x) {
    // 将特殊情况下节点挪到原来的位置（通过挪画布实现）
    // 如果当前的节点在树形视图的左侧，只需要判断当前节点的坐标是不是在窗口右侧
    // 如果当前的节点在树形视图的右侧，只需要判断当前节点的坐标是不是在窗口
    let offset = 0;
    if (this.direction == 'left') {
      // 节点在window右边，向左挪画布
      if (cur_x + this.nodeConfig.group.default.width + this.drag_left > this.dataflow_width) {
        offset = -(cur_x + this.nodeConfig.group.default.width - this.dataflow_width + this.drag_left + 10);
      }
    } else {
      if (cur_x + this.drag_left < 0) {
        offset = -(cur_x + this.drag_left) + 10;
      }
    }
    return offset;
  }

  setMaxPositionXWithinBoundary(max) {
    let offset = 0;
    if (max + this.nodeConfig.group.default.width + this.drag_left > this.dataflow_width) {
      offset = -(max + this.nodeConfig.group.default.width + 15 - this.dataflow_width + this.drag_left);
    }
    return offset;
  }

  setMaxPositionYWithinBoundary(max) {
    let offset = 0;
    if (max + this.nodeConfig.group.default.height + this.drag_top > 726) {
      offset = -(max + this.nodeConfig.group.default.height + 15 - 726 + this.drag_top);
    }
    // let position = {}
    // if (max + this.nodeConfig.group.default.height > 726) {
    //     offset = -(max + this.nodeConfig.group.default.height - 726 + 50)
    //     position = this.__getCalcNodePosition()
    //     this.graphData.locations.forEach(location => {
    //         let calcPosition = position[location.id]
    //         calcPosition.y += offset
    //     })
    // }
    return offset;
  }
}

export default FlowNodeToolKit;
