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

/* eslint-disable no-unused-vars */
/* eslint-disable no-undef */
/* eslint-disable no-var */
/* eslint-disable no-restricted-properties */
class FlowToolkit {
  constructor(instance) {
    this.flowInstance = instance;
  }

  getBestArrow(line) {
    const sourceEndpoints = this.flowInstance.getEndpoints(line.source.id);
    const targetEndpoints = this.flowInstance.getEndpoints(line.target.id);
    let compareArrow = [];
    sourceEndpoints.forEach((source) => {
      targetEndpoints.forEach((target) => {
        compareArrow.push({
          sArrow: source.anchor.type,
          tArrow: target.anchor.type,
          distance:
                        Math.pow(source.canvas.offsetLeft - target.canvas.offsetLeft, 2)
                        + Math.pow(source.canvas.offsetTop - target.canvas.offsetTop, 2),
        });
      });
    });
    compareArrow = compareArrow.sort(this.compare('distance'));
    return compareArrow[0];
  }

  /**
   * 获取最优连线点
   */
  setBestArrow(id, nodes, lines) {
    const ids = id;
    const locations = nodes;
    locations.forEach((d) => {
      if (d.id === ids) {
        nodeIds = d.node_id;
      }
    });
    var nodes = this.getNodeById(ids, lines);
    // 父子节点
    const parentNode = nodes.parent;
    const { children } = nodes;
    // 父子节点和当前节点的endpoints
    const parentNodeEndpoints = {};
    const childNodeEndpoints = {};
    const currentNodeEndpoints = this.flowInstance.getEndpoints(ids);
    parentNode.forEach((p) => {
      parentNodeEndpoints[p] = this.flowInstance.getEndpoints(p);
    });
    children.forEach((p) => {
      childNodeEndpoints[p] = this.flowInstance.getEndpoints(p);
    });

    // 获取父节点的最优连接点
    let tempArr = [];
    parentNode.forEach((pN) => {
      tempArr = [];
      currentNodeEndpoints.forEach((c) => {
        parentNodeEndpoints[pN].forEach((p) => {
          tempArr.push({
            cArrow: c.anchor.type,
            pArrow: p.anchor.type,
            distance:
                            Math.pow(c.canvas.offsetLeft - p.canvas.offsetLeft, 2)
                            + Math.pow(c.canvas.offsetTop - p.canvas.offsetTop, 2),
          });
        });
        tempArr = tempArr.sort(this.compare('distance'));
      });
      const theBestArrowGroup = tempArr[0];
      lines.forEach((line, index) => {
        if (line.sourceId === pN && line.targetId === ids) {
          instance.detach(line);
          const options = {
            source: [pN, theBestArrowGroup.pArrow],
            target: [ids, theBestArrowGroup.cArrow],
          };
          this.flowInstance.setLine(options);
        }
      });
    });

    // 获取子节点的最优连接点
    let tempArr2 = [];
    children.forEach((cN) => {
      tempArr2 = [];
      currentNodeEndpoints.forEach((c) => {
        childNodeEndpoints[cN].forEach((p) => {
          tempArr2.push({
            cArrow: c.anchor.type,
            pArrow: p.anchor.type,
            distance:
                            Math.pow(c.canvas.offsetLeft - p.canvas.offsetLeft, 2)
                            + Math.pow(c.canvas.offsetTop - p.canvas.offsetTop, 2),
          });
        });
        tempArr2 = tempArr2.sort(this.compare('distance'));
      });
      const theBestArrowGroup = tempArr2[0];
      lines.forEach((line) => {
        if (line.sourceId === ids && line.targetId === cN) {
          instance.detach(line);
          const options = {
            source: [ids, theBestArrowGroup.cArrow],
            target: [cN, theBestArrowGroup.pArrow],
          };
          this.flowInstance.setLine(options);
        }
      });
    });
  }

  compare(property) {
    return function (obj1, obj2) {
      const value1 = obj1[property];
      const value2 = obj2[property];
      return value1 - value2; // 升序
    };
  }

  getNodeById(id, lines) {
    const nodes = {
      parent: [],
      children: [],
    };

    lines.forEach((element) => {
      if (element.target.id === id) {
        nodes.parent.push(element.source.id);
      } else if (element.source.id === id) {
        nodes.children.push(element.target.id);
      }
    });
    return nodes;
  }
}

export default FlowToolkit;
