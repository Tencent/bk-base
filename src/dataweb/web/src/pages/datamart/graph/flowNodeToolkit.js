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

/* eslint-disable*/
class FlowNodeToolKit {
  constructor(flowGraphData, getNodeConfigByNodeName = null) {
    this.levelPrefix = 'level_';
    this.offsetYCache = {};
    this.graphData = JSON.parse(JSON.stringify(flowGraphData));
    this.getNodeConfigByNodeName = getNodeConfigByNodeName;
    this.nodeConfig = {
      offsetX: 30,
      offsetY: 0,
      nodeMarginX: 0,
      nodeMarginY: 30,
      /** 不同类型节点的宽高设置  */
      group: {
        'graph-round': {
          width: 168,
          height: 64,
        },
        'graph-ractangle': {
          width: 168,
          height: 44,
        },
        'graph-square': {
          width: 168,
          height: 60,
        },
        default: {
          width: 168,
          height: 60,
        },
      },
      lineWidth: 100,
    };
    this.nodeTree = this.__initNodeTree();

    /** 记录每层叶子节点数据以及最大叶子节点 */
    this.nodeTreeAttrs = this.__computedPerLevelNodeCount(this.nodeTree);

    /** 计算节点位置 */
    this.__computedNodePosition(this.nodeTree, 0, this.nodeConfig.offsetX, this.nodeConfig.offsetY);
  }

  __initNodeTree() {
    let nodeTree = Object.values(this.__getRootNode());
    const nodesWithoutLine = this.__getNodeWithoutLines();
    nodeTree.push(...nodesWithoutLine);
    nodeTree = nodeTree.filter(node => this.__getNodeConfigById(node.id));
    const cacheNode = {};
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

        if (!cacheNode[node.id]) {
          cacheNode[node.id] = {
            absPath: currentPrefix,
            level,
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
        node.groupType = this.__getFlowNodeGroup(location);
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
      .reduce(
        (pre, next) =>
          Object.assign(pre, {
            [next.id]: JSON.parse(JSON.stringify(next)),
          }),
        {}
      );
  }

  /** 查找单独节点（根节点，没有连线） */
  __getNodeWithoutLines() {
    return this.graphData.locations.filter(location => !this.graphData.lines.some(line => line.target.id === location.id || line.source.id === location.id)).map(location => Object.assign({}, { id: location.id, node_id: location.node_id }));
  }

  /** 根据SourceID获取子节点 */
  __getChildNodesBySourceNode(nodeId) {
    return this.graphData.lines.filter(line => line.source.id === nodeId).map(line => JSON.parse(JSON.stringify(line.target)));
  }

  /** 计算每个叶子节点位置
   * @param Array nodes 叶子节点
   * @param Number level  所在层级
   * @param Number offsetX X轴偏移
   * @param Number preNodeOffsetY 基于同级别上一节点Y轴偏移
   */
  __computedNodePosition(nodes, level, offsetX, preNodeOffsetY) {
    const currentLevel = level;
    nodes.forEach(node => {
      const positionConfig = this.nodeConfig.group[node.groupType];
      const hasOneMoreChild = node.childNodes && node.childNodes.length > 1;
      const nodePosition = node.height / 2 + preNodeOffsetY + (hasOneMoreChild ? positionConfig.height / 4 : 0);
      node.position = {
        x: offsetX,
        y: nodePosition,
      };
      const nextOffsetX = offsetX + positionConfig.width + this.nodeConfig.lineWidth;
      node.childNodes && this.__computedNodePosition(node.childNodes, currentLevel + 1, nextOffsetX, preNodeOffsetY);
      preNodeOffsetY += node.height;
    });
  }

  /** 清洗节点位置，合并同一节点根据不同父级计算所得位置 */
  __getCalcNodePosition() {
    const calcNodePosition = {};

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
    return this.nodeConfig.nodeMarginY + this.nodeConfig.group[groupType].height;
  }

  /** 获取每个层级的节点数量和高度 */
  __computedPerLevelNodeCount(nodeTree) {
    const treeDeepReords = {};
    const self = this;

    /** 统计每层叶子节点数量
     * @param nodeRecodes 记录叶子节点属性
     * @param level 上一层级数
     * @param childNodes 子节点
     */
    function getLevelNodesCount(nodeRecodes, level, childNodes) {
      const currentLevel = level + 1;
      childNodes.forEach(child => {
        if (child.childNodes && child.childNodes.length) {
          getLevelNodesCount(nodeRecodes, currentLevel, child.childNodes);

          child.level = currentLevel;
          /** 计算当前节点的子节点的总高度 */
          child.height = child.childNodes.reduce((pre, curr) => {
            const currHeight = curr.height || self.__getNodeHeight(curr.groupType);
            return pre + currHeight;
          }, 0);
        } else {
          child.height = self.__getNodeHeight(child.groupType);
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

      let currentHeight = root.childNodes.reduce((pre, curr) => {
        const currHeight = curr.height || self.__getNodeHeight(root.groupType);
        return pre + currHeight;
      }, 0);

      currentHeight = currentHeight > defaultHeight ? currentHeight : defaultHeight;
      treeDeepReords[root.id].height = currentHeight;
      root.height = currentHeight;
      root.level = 0;
    });

    return treeDeepReords;
  }

  __assignLocation(location) {
    const nodeConf = this.getNodeConfigByNodeName(location.type);
    const assign = {};
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

  updateFlowGraph() {
    const position = this.__getCalcNodePosition();
    this.graphData.locations.forEach(location => {
      const calcPosition = position[location.id];
      if (calcPosition) {
        location.x = calcPosition.x;
        location.y = calcPosition.y;
      }

      location = this.__assignLocation(location);
    });

    this.graphData.lines.forEach(line => {
      line.source.arrow = 'Right';
      line.target.arrow = 'Left';
    });
    return this.graphData;
  }
}

export default FlowNodeToolKit;
