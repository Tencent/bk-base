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

class NodeLinkRules {
  constructor(rules, nodeConfig) {
    this.rules = rules;
    this.isRulesLoaded = true;
    this.validate = {};
    this.lines = [];
    this.locations = [];
    this.nodeNameMap = {};

    this.initNodeNameMap(nodeConfig);
  }

  initNodeNameMap(config) {
    config.forEach((group) => {
      group.instances.forEach((item) => {
        this.nodeNameMap[item.node_type_instance_name] = item.node_type_instance_alias;
      });
    });
  }
  validateLinkNode(source, target, lines = [], locations = []) {
    this.lines = lines;
    this.locations = locations;
    this.groupRuleTargets = this.rules.group_rules.map(item => item.downstream_instance);
    this.validate = {
      success: true,
      msg: '',
    };
    this.linkToTarget = this.lines.filter(line => line.target.id === target.id);
    this.sourceNodeIds = this.linkToTarget.reduce((pre, cur) => {
      pre.push(cur.source.id);
      return pre;
    }, []);
    this.sourceNodes = this.locations.filter(node => this.sourceNodeIds.includes(node.id));
    if (this.isSameNode(source, target)) {
      this.validate = {
        success: false,
        msg: '不能连接当前节点',
      };

      return this.validate;
    }

    if (!this.isAvailableNodeType(source)) {
      this.validate = {
        success: false,
        msg: `【${this.nodeNameMap[source.instanceTypeName]}】 不支持连接到任何节点`,
      };

      return this.validate;
    }
    !this.isGroupRules(source, target) && this.validateNodeRules(source, target);
    return this.validate;
  }
  /** 根据数据源节点类型，获取相连接的数目 */
  getSourceTypeLinkCount(sourceType) {
    const count = this.linkToTarget.filter((line) => {
      const node = this.sourceNodes.find(node => node.id === line.source.id);
      return node.instanceTypeName === sourceType;
    }).length;
    return count + 1;
  }
  /** 校验节点规则 */
  validateNodeRules(source, target) {
    const rule = this.getCurrentLinkRule(source);
    return this.isAvailableChildNode(rule, target, source) && this.isAvailableUpstream(rule, target, source);
  }
  /** 校验组合规则 */
  validateGroupRules(source, target, rule, upstreamNodes) {
    // let legalCombination = []
    // const rules = this.rules.group_rules.filter(rule => {
    //     if (rule.downstream_instance === target.instanceTypeName) {
    //         legalCombination.push(Object.keys(rule.upstream_max_link_limit).join(','))
    //         return true
    //     }
    // })

    // const result = rules.some(rule => {
    //     const upstreamLimit = rule.upstream_max_link_limit

    //     console.log(this.getSourceTypeLinkCount(source.instanceTypeName))

    //     if (!Object.keys(upstreamLimit).includes(source.instanceTypeName)
    //         || upstreamLimit[source.instanceTypeName] < this.getSourceTypeLinkCount(source.instanceTypeName)
    //         || Object.keys(upstreamLimit) < this.sourceNodes.length) {
    //         return false
    //     } else {
    //         const allowedKeys = Object.keys(upstreamLimit)
    //         const sourceNodesTypes = this.sourceNodes.map(item => item.instanceTypeName)
    //         sourceNodesTypes.push(source.instanceTypeName)

    //         for (let i = sourceNodesTypes.length - 1; i >=0; i--) {
    //             const temp = sourceNodesTypes[i]
    //             for (let j = allowedKeys.length - 1; j >= 0; j--) {
    //                 const temp2 = allowedKeys[j]
    //                 if (temp === temp2) {
    //                     sourceNodesTypes.splice(i, 1)
    //                     allowedKeys.splice(j, 1)
    //                     break
    //                 }
    //             }
    //         }

    //         return sourceNodesTypes.length === 0
    //     }
    // })

    for (const [key, value] of Object.entries(rule.upstream_max_link_limit)) {
      if (value < upstreamNodes[key]) {
        this.validate.msg = `上游${key}节点的个数最大为${value}`;
        this.validate.success = false;
        break;
      }
      this.validate.success = true;
    }

    return this.validate.success;
  }
  handleGroupRules(rule, target, upstreamNodes) {
    const status = rule.downstream_instance === target.instanceTypeName;
    return status && this.isArrayEqual(Object.keys(upstreamNodes), Object.keys(rule.upstream_max_link_limit));
  }

  isGroupRules(source, target) {
    this.sourceNodes.push(source);
    const upstreamNodes = {};

    this.sourceNodes.forEach((source) => {
      if (Object.prototype.hasOwnProperty.call(upstreamNodes, source.instanceTypeName)) {
        upstreamNodes[source.instanceTypeName] += 1;
        return;
      }
      upstreamNodes[source.instanceTypeName] = 1;
    });
    const groupRule = this.rules.group_rules.find(rule => this.handleGroupRules(rule, target, upstreamNodes));
    // const groupRule = this.rules.group_rules.find(
    //     rule =>
    //         rule.downstream_instance === target.instanceTypeName
    // && this.isArrayEqual(Object.keys(upstreamNodes), Object.keys(rule.upstream_max_link_limit))
    // );

    if (this.groupRuleTargets.includes(target.instanceTypeName) && groupRule) {
      this.validateGroupRules(source, target, groupRule, upstreamNodes);
      return true;
    }

    return false;

    // const sourceNodesTypes = [...new Set(this.sourceNodes.map(item => item.instanceTypeName))]
    // if (!this.groupRuleTargets.includes(target.instanceTypeName)
    //     || (sourceNodesTypes.length === 1 && source.instanceTypeName === sourceNodesTypes[0])
    // ) {
    //     return false
    // }
    // return true
  }

  isArrayEqual(arr1, arr2) {
    if (arr1.length !== arr2.length) {
      return false;
    }
    return arr1.every(item => arr2.includes(item));
  }

  /** 连线开始、结束是否为同一节点 */
  isSameNode(source, target) {
    return source.id === target.id;
  }

  /** 校验父节点是否有配置连线规则 */
  isAvailableNodeType(source) {
    return Object.prototype.hasOwnProperty.call(this.rules.node_rules, source.instanceTypeName);
  }

  /** 获取当前节点的连线规则 */
  getCurrentLinkRule(node) {
    return this.rules.node_rules[node.instanceTypeName];
  }

  /** 检查是否是有效的子节点 */
  isAvailableChildNode(rule, target, source) {
    if (!Object.prototype.hasOwnProperty.call(rule, target.instanceTypeName)) {
      console.log(Object.keys(rule));
      const allowedNodes = Object.keys(rule)
        .map(item => (this.nodeNameMap[item] ? `【${this.nodeNameMap[item]}】` : null))
        .filter(item => item !== null)
        .join(',');
      this.validate = {
        success: false,
        msg: `目前仅支持【${this.nodeNameMap[source.instanceTypeName]}】连线至${allowedNodes}`,
      };
    }

    return this.validate.success;
  }
  getDownstreamRule(acc, cur) {
    return acc.downstream_link_limit[1] > cur.downstream_link_limit[1] ? acc : cur;
  }
  /**
     * 检查父节点是否满足连线规则（父节点数据量是否超出配置）
     */
  isAvailableUpstream(rule, target, source) {
    let downstreamRule;
    if (Object.keys(rule[target.instanceTypeName]).length > 1) {
      // downstreamRule = Object.values(rule[target.instanceTypeName]).reduce((acc, cur) =>
      //     acc.downstream_link_limit[1] > cur.downstream_link_limit[1] ? acc : cur
      // );
      downstreamRule = Object.values(rule[target.instanceTypeName]).reduce(this.getDownstreamRule);
    } else {
      downstreamRule = rule[target.instanceTypeName].default;
    }
    const maxUpstream = downstreamRule.upstream_link_limit[1];
    const maxDownstream = downstreamRule.downstream_link_limit[1];

    const linkedCount = (this.lines.filter(line => line.target.id === target.id) || []).length;
    console.log(this.locations.find(node => node.instanceTypeName === target.instanceTypeName));
    const linkedDownStreamCount = this.lines.filter((line) => {
      const node = (line.source.id === source.id && this.locations.find(node => node.id === line.target.id));
      return node.instanceTypeName === target.instanceTypeName;
    }).length;

    if (linkedCount >= maxUpstream) {
      this.validate = {
        success: false,
        msg: `【${this.nodeNameMap[target.instanceTypeName]}】最大支持连线数量为：${maxUpstream}`,
      };
    } else if (linkedDownStreamCount >= maxDownstream) {
      this.validate = {
        success: false,
        msg: `【${this.nodeNameMap[source.instanceTypeName]}】
                最多连接${maxDownstream}个【${this.nodeNameMap[target.instanceTypeName]}】类型节点`,
      };
    }

    return this.validate.success;
  }

  /** 检查 */
}

export default NodeLinkRules;
