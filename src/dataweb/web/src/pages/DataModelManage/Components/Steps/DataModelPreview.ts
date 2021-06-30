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

import { generateId } from '@/common/js/util';
import BkFlow from '@blueking/bkflow.js/lib/index';
import { Component, Emit, Prop, PropSync, Ref, Vue, Watch } from 'vue-property-decorator';
import { confirmModelOverview, getModelOverview } from '../../Api';
import { DataModelManage, IStepsManage } from './IStepsManage';

let treeInstance = null;
@Component
export default class DataModelPreview extends DataModelManage.IStepsManage {
  /**
     * 节点头部配置
     */
  public tableSetting = {
    dimension_table: {
      title: '',
      icon: 'icon-dimension-model',
      nameKey: 'model_name',
      aliasKey: 'model_alias',
    },
    fact_table: {
      title: '主表',
      icon: 'icon-fact-model',
      nameKey: 'model_name',
      aliasKey: 'model_alias',
    },
    calculation_atom: {
      title: '指标统计口径',
      icon: 'icon-statistic-caliber',
      nameKey: 'calculation_atom_name',
      aliasKey: 'calculation_atom_alias',
    },
    indicator: {
      title: '指标',
      icon: 'icon-quota',
      nameKey: 'indicator_name',
      aliasKey: 'indicator_alias',
    },
  };

  /**
     * 节点类型配置
     */
  public nodeSetting = {
    dimension: {
      icon: 'icon-dimens',
      color: '#4BC7AD',
    },
    primary_key: {
      icon: 'icon-key-line',
      color: '#3A84FF',
    },
    measure: {
      icon: 'icon-measure-line',
      color: '#F1CB56',
    },
  };

  public isPreviewLoading = false;

  public collpse = ['calculation_atom', 'indicator', 'fact_table', 'dimension_table'];

  public nodes: IDataModelManage.IModelList[] = [];

  public lines: any[] = [];

  public nodeTreeData: any[] = [];

  public hasDimensionNode = false;

  public hasCalculationAtomNode = false;

  public hasIndicatorNode = false;

  public activeCalculationAtom = '';

  public activeIndicator = '';

  public activeDimensionTable = '';

  public activeFactTable = '';

  public dimensionNode: object = {};

  public tips = {
    isShow: false,
    instance: null,
    content: {
      name: '',
      alias: '',
    },
  };
  private uid = generateId('dataModelManager_preview_canvas_');

  /**
     * 获取模型预览数据
     */
  public getModelOverviewData() {
    this.isPreviewLoading = true;
    getModelOverview(this.modelId)
      .then(res => {
        res.setDataFn(data => {
          this.$set(this, 'nodes', data.nodes || []);
          this.$set(this, 'lines', data.lines || []);
          // 判断是否有关联维度节点
          const dimensionTable = data.nodes.find(node => node.node_type === 'dimension_table');
          this.hasDimensionNode = !!dimensionTable;
          dimensionTable && this.$set(this, 'dimensionNode', dimensionTable);
          // 判断是否有统计口径节点
          const calculationTable = data.nodes.find(node => node.node_type === 'calculation_atom');
          this.hasCalculationAtomNode = !!calculationTable;
          // 判断是否有指标节点
          const indicatorTable = data.nodes.find(node => node.node_type === 'indicator');
          this.hasIndicatorNode = !!indicatorTable;
          // 处理字段排序 主键 => 维度 => 度量
          const fieldCategoryValue = {
            dimension: 2,
            measure: 3,
          };
          // 处理节点fields
          this.nodes.forEach(node => {
            const fields = node.fields || [];
            const extendFields = fields.filter(item => item.is_extended_field);
            const renderFields = node.fields
              .filter(item => !item.is_extended_field)
              .map(field => Object.assign({}, field, {
                extends: extendFields.filter(extend => extend.join_field_name === field.field_name),
              })
              );
            renderFields.sort((next, prev) => {
              if (next.field_name === '__time__') {
                return 1;
              }
              if (prev.field_name === '__time__') {
                return -1;
              }
              if (next.is_primary_key && prev.is_primary_key) {
                return 0;
              }
              if (next.is_primary_key && !prev.is_primary_key) {
                return -1;
              }
              if (!next.is_primary_key && prev.is_primary_key) {
                return 1;
              }
              return fieldCategoryValue[next.field_category] - fieldCategoryValue[prev.field_category];
            });
            node.render_fields = renderFields;
          });
          // 格式化画布数据
          this.nodeTreeData = this.getNodeTreeData();
          // 设置展开节点
          this.hasCalculationAtomNode && (this.activeCalculationAtom = calculationTable.node_id);
          this.hasIndicatorNode && (this.activeIndicator = indicatorTable.node_id);
          this.hasDimensionNode && (this.activeDimensionTable = dimensionTable.node_id);
          const factTable = data.nodes.find(node => node.node_type === 'fact_table');
          this.activeFactTable = factTable?.node_id;
          // 绘制画布
          this.renderNodeTree();
        });
      })
      ['finally'](() => {
        this.isPreviewLoading = false;
      });
  }

  /**
     * 获取树节点数据
     */
  public getNodeTreeData(): any[] {
    const nodeData = [];
    const nodeMap = this.nodes.reduce((map, node) => {
      map[node.node_id] = node;
      return map;
    }, {});
    const lineMap = this.lines.reduce((map, line) => {
      map[line.from] ? map[line.from].push(line.to) : (map[line.from] = [line.to]);
      return map;
    }, {});
    const rootNodeName = this.hasDimensionNode ? 'dimension_table' : 'fact_table';
    for (const node of this.nodes) {
      const nodeKeys = lineMap[node.node_id];
      const childNodes = [];
      if (node.node_type === rootNodeName) {
        node.children = [];
        if (nodeKeys?.length) {
          for (const key of nodeKeys) {
            nodeMap[key] && node.children.push(nodeMap[key]);
          }
        }
        nodeData.push(node);
        continue;
      }
      if (nodeKeys?.length) {
        for (const key of nodeKeys) {
          nodeMap[key] && childNodes.push(nodeMap[key]);
        }
        node.children ? node.children.push(...childNodes) : (node.children = childNodes);
      }
    }
    return nodeData;
  }

  /**
     * 渲染节点头部
     * @param node
     */
  public renderHeader(node) {
    const info = this.tableSetting[node.node_type];
    if (node.node_type === 'fact_table' && node.model_type === 'dimension_table') {
      info.icon = 'icon-dimension-model';
    }
    return (
      (info.title ? `<span class="type">${info.title}</span>` : '')
            + `<div class="node-header">
                <i class="header-icon ${info.icon}"></i>
                <div class="info" title="${node[info.nameKey]}（${node[info.aliasKey]}）">
                    <span class="info-text text-overflow">${node[info.nameKey]}</span>
                    <span class="info-text text-overflow">${node[info.aliasKey]}</span>
                </div>
                ${this.collpse.includes(node.node_type) ? '<i class="toggle-icon icon-angle-double-up"></i>' : ''}
            </div>`
    );
  }

  /**
     * 渲染节点内容
     * @param field
     */
  public renderItem(field: IDataModelManage.IMasterTableField) {
    const key = field.is_primary_key ? 'primary_key' : field.field_category;
    const info = this.nodeSetting[key] || {};
    const extendFields = field['extends'] || [];
    const curFieldName = field.field_name === '__time__' ? '--' : field.field_name;
    const curFieldAlias = field.field_alias ? `(${field.field_alias})` : '';
    // 扩展字段处理
    if (extendFields.length) {
      let extendItems = `
                <div class="node-item join-item" title="${curFieldName} ${curFieldAlias}">
                    <i class="item-icon ${info.icon}"></i>
                    <span class="text-overflow">${curFieldName} ${curFieldAlias}</span>
                </div>
            `;
      for (const item of extendFields) {
        const showFieldName = item.field_name === '__time__' ? '--' : item.field_name;
        const showFieldAlias = item.field_alias ? `(${item.field_alias})` : '';
        const curKey = item.is_primary_key ? 'primary_key' : item.field_category;
        const curInfo = this.nodeSetting[curKey] || {};
        extendItems += `
                    <div class="node-item extend-item" title="${showFieldName} ${showFieldAlias}">
                        <i class="item-icon ${curInfo.icon}"></i>
                        <span class="text-overflow">${showFieldName} ${showFieldAlias}</span>
                    </div>
                `;
      }
      return `<div class="expand-node-item" style="--color: ${info.color}">${extendItems}</div>`;
    }
    return `
            <div class="node-item" style="--color: ${info.color}" title="${curFieldName} ${curFieldAlias}">
                <i class="item-icon ${info.icon}"></i>
                <span class="text-overflow">${curFieldName} ${curFieldAlias}</span>
            </div>
        `;
  }

  /**
     * 获取节点配置信息
     */
  public getNodeConfig() {
    const nodeConfig: any[] = [];
    const headerHeight = 88;
    const itemHeight = 44;
    this.nodes.forEach(node => {
      let height = headerHeight + node.fields.length * itemHeight;
      if (
        this.collpse.includes(node.node_type)
                && this.activeCalculationAtom !== node.node_id
                && this.activeIndicator !== node.node_id
                && this.activeDimensionTable !== node.node_id
                && this.activeFactTable !== node.node_id
      ) {
        height = 44;
      }
      node.table_node_type = node.node_id;
      nodeConfig.push({
        node_type: node.node_type,
        table_node_type: node.node_id,
        width: 320,
        height,
        radius: '2px',
      });
    });
    return nodeConfig;
  }

  /**
     * 渲染节点树
     */
  public renderNodeTree() {
    const self = this;
    const groupData = [
      {
        id: 'factTable',
        text: '明细数据表',
        width: 0,
        background: '#DCDEE5',
        nodeDepth: this.hasDimensionNode ? 2 : 1,
        splitor: {
          show: false,
        },
      },
    ];
    this.hasCalculationAtomNode
            && groupData.push({
              id: 'calculationAtom',
              text: '指标统计口径',
              width: 0,
              background: '#DCDEE5',
              splitor: {
                show: true,
                /** 宽度或者高度 */
                weight: 4,
                background: 'rgba(255, 255, 255, 0.3)',
              },
            });
    this.hasCalculationAtomNode
            && this.hasIndicatorNode
            && groupData.push({
              id: 'indicator',
              text: '指标',
              width: 0,
              background: '#DCDEE5',
              splitor: {
                show: true,
                weight: 4,
                background: 'rgba(255, 255, 255, 0.3)',
              },
            });
    // 当只有明细表的时候对节点做偏移
    const offsetX = groupData.length === 1 ? (this.hasDimensionNode ? -400 : -100) : 0;
    treeInstance = new BkFlow('#' + this.uid, {
      mode: 'readonly',
      background: '#F0F1F5',
      lineLabel: false,
      nodeTemplateKey: 'table_node_type',
      autoBestTarget: false,
      nodeConfig: self.getNodeConfig(),
      renderVisibleArea: true,
      /** 目前只支持 horizontal mode */
      flexTree: {
        /** 初始位置偏移量 */
        offset: {
          x: offsetX,
          y: 0,
        },

        /** 同一层级叶子结点间距 */
        depthSpacing: 30,

        /** 父子节点间间距 */
        nodeSpacing: 248,

        /** 节点对齐方式: mode = horizontal（top center bottom） | mode = vertical(left center right) */
        nodeAlign: 'center',
      },
      lineConfig: {
        canvasLine: false,
        color: '#C4C6CC',
        activeColor: '#C4C6CC',
      },
      tree: {
        formatNodePosition: true,
        mode: 'horizontal', // horizontal | vertical,
        horizontalSpacing: 0,
        verticalSpacing: 408,
        chartArea: {
          left: 0,
          top: 0,
        },
      },
      groupConfig: {
        enabled: true,
        /** 分组显示位置：bottom|right|top|left, undefined|null表示不显示 */
        position: 'bottom',
        data: groupData,
      },
      zoom: {
        scaleExtent: [0.8, 1],
        controlPanel: false,
        tools: [],
      },
      onNodeRender(node) {
        // 处理节点展开/折叠
        if (
          self.collpse.includes(node.node_type)
                    && !(
                      node.node_id === self.activeCalculationAtom
                        || node.node_id === self.activeIndicator
                        || node.node_id === self.activeDimensionTable
                        || node.node_id === self.activeFactTable
                    )
        ) {
          const info = self.tableSetting[node.node_type] || {};
          return `
                        <div class="node-wrapper ${node.node_type}" id="${node.node_id}">
                            <div class="node-item collapse-item">
                                <i class="item-icon ${info.icon}"></i>
                                <span class="text-overflow">${node[info.nameKey]} (${node[info.aliasKey]})</span>
                                <i class="toggle-icon icon-angle-double-down"></i>
                            </div>
                        </div>
                    `;
        }
        const header = self.renderHeader(node);
        const fields = node.render_fields || [];
        let items = '';
        for (const field of fields) {
          items += self.renderItem(field);
        }
        return `
                    <div class="node-wrapper ${node.node_type}">
                        ${header}
                        ${items}
                    </div>
                `;
      },
    })
      .on(
        'nodeClick',
        (node, event) => {
          // 不同节点类型处理
          const type = node.node_type;
          if (type === 'calculation_atom') {
            this.activeCalculationAtom = this.activeCalculationAtom === node.node_id ? '' : node.node_id;
            this.activeCalculationAtom && this.sendUserActionData({ name: '展开【指标统计口径】' });
          }
          if (type === 'indicator') {
            this.activeIndicator = this.activeIndicator === node.node_id ? '' : node.node_id;
            this.activeIndicator && this.sendUserActionData({ name: '展开【指标】' });
          }
          if (type === 'dimension_table') {
            this.activeDimensionTable = this.activeDimensionTable === node.node_id ? '' : node.node_id;
          }
          if (type === 'fact_table') {
            this.activeFactTable = this.activeFactTable === node.node_id ? '' : node.node_id;
          }
          treeInstance.updateTree(self.nodeTreeData, 'node_id', 'node_id', {
            nodeConfig: self.getNodeConfig()
          });
          this.setNodeLinesOffset();
        },
        'node'
      )
      .on(
        'nodeMouseEnter',
        (node, event) => {
          if (
            self.collpse.includes(node.node_type)
                        && !(
                          node.node_id === self.activeCalculationAtom
                            || node.node_id === self.activeIndicator
                            || node.node_id === self.activeDimensionTable
                            || node.node_id === self.activeFactTable
                        )
          ) {
            const keys = this.tableSetting[node.node_type];
            self.tips.instance && self.tips.instance.destroy();
            self.tips.instance = self.$bkPopover(document.getElementById(node.node_id), {
              content: node[keys.nameKey] + '<br />' + node[keys.aliasKey],
              zIndex: 9999,
              trigger: 'manual',
              boundary: 'window',
              arrow: true,
              interactive: true,
              extCls: 'bk-data-model-preview',
              placement: 'top',
            });
            self.tips.show = true;
            self.$nextTick(() => {
              self.tips.instance.show();
            });
          }
        },
        'node'
      )
      .on(
        'nodeMouseLeave',
        (node, event) => {
          self.tips.instance && self.tips.instance.hide();
          self.$nextTick(() => {
            self.tips.show = false;
          });
        },
        'node'
      );
    treeInstance.renderTree(this.nodeTreeData, 'node_id', 'node_id');
    // 设置画布初始化偏移量
    this.setViewOffset();
    // 设置节点连线偏移量
    this.setNodeLinesOffset();
  }

  /**
     * 设置画布初始化偏移量
     */
  public setViewOffset() {
    const storeNodes = treeInstance._nodesManager._store.nodes || [];
    const masterNode = storeNodes.find(node => node.node_type === 'fact_table');
    const { width, height } = treeInstance._options.computedStyle;
    const offsetX = this.hasDimensionNode ? 0 : 80;
    treeInstance.translate(width / 4 - masterNode.x - offsetX, height > 0 ? height / 4 - masterNode.y : 100);
    treeInstance.updateTree(this.nodeTreeData, 'node_id', 'node_id', { nodeConfig: this.getNodeConfig() });
    treeInstance.zoomOut();
  }

  /**
     * 设置节点连线偏移量
     */
  public setNodeLinesOffset() {
    const fromHeadHeight = 60;
    const toHeadHeight = 88;
    const halfItemHeight = 22;
    const newLines = [];
    // 获取fields集合
    const nodeFieldMap = {};
    // 主表节点Id
    let mstNodeId = '';
    for (const node of this.nodes) {
      node.model_type === 'fact_table' && (mstNodeId = node.node_id);
      nodeFieldMap[node.node_id] = node.render_fields || [];
    }
    for (const line of this.lines) {
      const fromNode = line.from;
      const toNode = line.to;
      const offsetSetting = {
        from: {
          id: fromNode,
        },
        to: {
          id: toNode,
        },
      };
      if (this.activeFactTable) {
        if (Object.prototype.hasOwnProperty.call(line, 'from_field_name')) {
          const index = nodeFieldMap[fromNode].findIndex(item => item.field_name === line.from_field_name);
          const expendFields = nodeFieldMap[fromNode]
            .slice(0, index)
            .reduce((arr, item) => arr.concat(...item['extends']), []);
          const offsetY = (index + expendFields.length + 1) * 2 - 1;
          offsetSetting.from.offset = {
            x: null,
            y: offsetY * halfItemHeight + fromHeadHeight,
          };
        }
        if (Object.prototype.hasOwnProperty.call(line, 'to_field_name')) {
          const index = nodeFieldMap[toNode].findIndex(item => item.field_name === line.to_field_name);
          const expendFields = nodeFieldMap[toNode]
            .slice(0, index)
            .reduce((arr, item) => arr.concat(...item['extends']), []);
          const offsetY = (index + expendFields.length + 1) * 2 - 1;
          offsetSetting.to.offset = {
            x: null,
            y: offsetY * halfItemHeight + toHeadHeight,
          };
        }
      }
      // 设置维度表到主表连线偏移
      if (/^dimension_table/.test(fromNode) && /^fact_table/.test(toNode) && this.activeFactTable) {
        offsetSetting.from.offset = {
          x: null,
          y: halfItemHeight,
        };
      }
      // 设置主表到统计口径连线偏移
      if (/^fact_table/.test(fromNode) && /^calculation_atom/.test(toNode)) {
        offsetSetting.from.offset = {
          x: null,
          y: this.activeFactTable ? 58 : halfItemHeight,
        };
        if (this.activeCalculationAtom === toNode) {
          offsetSetting.to.offset = {
            x: null,
            y: 58,
          };
        }
      }
      // 设置统计口径到指标连线偏移
      if (/^calculation_atom/.test(fromNode) && /^indicator/.test(toNode)) {
        if (this.activeCalculationAtom === fromNode) {
          offsetSetting.from.offset = {
            x: null,
            y: 58,
          };
        }
        if (this.activeIndicator === toNode) {
          offsetSetting.to.offset = {
            x: null,
            y: 58,
          };
        }
      }
      newLines.push(offsetSetting);
    }
    treeInstance.updateLinePosition(newLines);
  }

  /**
     * 点击下一步方法
     */
  public nextStepClick() {
    confirmModelOverview(this.modelId).then(res => {
      if (res.validateResult(null, null, false)) {
        const activeTabItem = this.DataModelTabManage.getActiveItem()[0];
        activeTabItem.lastStep = res.data.step_id;
        this.DataModelTabManage.updateTabItem(activeTabItem);
        this.DataModelTabManage.dispatchEvent('updateModel', [
          {
            model_id: this.modelId,
            step_id: res.data.step_id,
          },
        ]);
      }
    });
  }

  public created() {
    this.initPreNextManage();
    this.getModelOverviewData();
  }
}
