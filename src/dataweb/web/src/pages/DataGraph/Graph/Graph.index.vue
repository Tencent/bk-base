

<!--
  - Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
  - Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
  - BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
  -
  - License for BK-BASE 蓝鲸基础平台:
  - -------------------------------------------------------------------
  -
  - Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  - documentation files (the "Software"), to deal in the Software without restriction, including without limitation
  - the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
  - and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  - The above copyright notice and this permission notice shall be included in all copies or substantial
  - portions of the Software.
  -
  - THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
  - LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
  - NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  - WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  - SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
  -->

<template>
  <div class="flow-graph-container"
    @click="handleGraphClick">
    <div id="flowgraph"
      :class="[isReadonly && 'mode-readonly']"
      @dragover="handleDragover"
      @drop="handleDrop">
      <div class="alert-legend-container">
        {{ $t('节点状态说明') }}：
        <ul>
          <li class="no-config">
            {{ $t('未配置') }}<i class="icon-datasource" />
          </li>
          <li class="abnormal">
            {{ $t('任务异常') }}<i class="icon-datasource" />
          </li>
          <li class="started">
            {{ $t('已启动') }}<i class="icon-datasource" />
          </li>
          <li class="modified">
            {{ $t('已修改') }}<i class="icon-datasource" />
          </li>
        </ul>
      </div>
    </div>
    <GraphNodeInfo
      :nodeConfig="activeNode"
      :originConfig="activeNodeConfig"
      :isShow.sync="isNodeInfoShow"
      :openType="openType"
      :graphData="graphData"
      @updateNode="handleUpdateNode"
      @saveGraphCache="saveGraphCache" />
    <template v-if="action === 'monitor'">
      <GraphMonitor ref="bkGraphMonitor"
        :nodeConfig="activeNode"
        :flowId="flowId" />
    </template>
    <NodesAutoGenerate ref="nodesGenerator"
      @confirm="generateNodes" />
  </div>
</template>

<script>
import { mapState, mapGetters } from 'vuex';
import D3Graph from '@blueking/bkflow.js';
import NodeTemplate from './Graph.Node.Template.js';
import NodeLinkRules from './NodeLinkRules.js';
import FlowNodeToolKit from './flowNodeToolkit.js';
import GraphNodeInfo from './Graph.node.vue';
import GraphMonitor from './Components/Graph.Monitor.vue';
import NodesAutoGenerate from './Components/GenerateIndicatorNodes.vue';
import Bus from '@/common/js/bus.js';
import { showMsg, confirmMsg, debounce } from '@/common/js/util.js';

let flowGraph = null;
export default {
  name: 'Graph-index',
  components: { GraphNodeInfo, GraphMonitor, NodesAutoGenerate },
  inject: ['updateGraphLoading'],
  data() {
    return {
      flowId: '',
      debounceTimer: null,
      graphData: {},
      activeNode: {},
      clickNodeStatus: {
        id: null,
        data: null,
      },
      isNodeInfoShow: false,
      nodeLinkRules: {},
      isReadonly: false,
      backupGraph: null,
      /** 当前操作项： debug, monitor, starting, stopping */
      action: 'normal',
      openType: 'config',
    };
  },
  computed: {
    ...mapState({
      flowData: state => state.ide.flowData,
    }),
    ...mapGetters({
      getNodeConfigByNodeWebType: 'ide/getNodeConfigByNodeWebType',
      getTempNodeConfig: 'ide/getTempNodeConfig',
      getFlowNodeConfig: 'ide/flowNodeConfig',
    }),
    activeNodeConfig() {
      return this.getNodeConfigByNodeWebType(this.activeNode.node_type);
    },
  },
  watch: {
    '$route.params.fid': {
      immediate: true,
      handler(val) {
        this.flowId = val;
        (val && this.initGraph()) || (flowGraph && flowGraph.clear());
      },
    },
    graphData: {
      deep: true,
      handler(val) {
        if (!this.debounceTimer) {
          this.debounceTimer = setTimeout(() => {
            this.saveGraphCache(val);
            this.debounceTimer = null;
          }, 1000);
        }
      },
    },
  },
  async mounted() {
    Bus.$on('updateALertCountGraph', () => {
      flowGraph.renderGraph(this.graphData);
    });
    flowGraph = new D3Graph('#flowgraph', {
      mode: 'edit',
      nodeTemplateKey: 'groupTemplate',
      canvasPadding: { x: 15, y: 15 },
      background: 'rgba(0,0,0,0)',
      lineConfig: {
        canvasLine: false,
        color: '#c4c6cc',
        activeColor: '#3a84ff',
      },
      nodeConfig: [
        { groupTemplate: 'graph-square', width: 62, height: 62, radius: '4px' },
        { groupTemplate: 'graph-round', width: 72, height: 72, radius: '50%' },
        { groupTemplate: 'graph-ractangle', width: 225, height: 42, radius: '4px' },
      ],
      zoom: {
        scaleExtent: [0.5, 3],
        controlPanel: true,
      },
      nodeClick: 'Refactoring',
      onNodeRender: node => new NodeTemplate(node).getTemplateById(
        node.groupTemplate,
        (this.isReadonly && 'readonly') || 'edit',
        this.action
      ),
      onLineConnectBefore: this.beforeLineConnected,
      onLineDelete: this.beforeLineDelete,
    })
      .on(
        'nodeDblClick',
        (node, event) => {
          if (!this.isReadonly) {
            this.handleNodeDblClick(node);
          }
        },
        'node'
      )
      .on(
        'nodeMouseEnter',
        (node, event) => {
          if (this.action === 'monitor') {
            this.activeNode = node;
            this.$refs.bkGraphMonitor.handleMouseenter(event);
          }
        },
        'node'
      )
      .on(
        'nodeMouseLeave',
        (node, event) => {
          if (this.action === 'monitor') {
            this.activeNode = node;
            this.$refs.bkGraphMonitor.handleMouseleave(event);
          }
        },
        'node'
      )
      .on(
        'nodeDragEnd',
        (node, event) => {
          this.updateLocations(node);
        },
        'node'
      )
      .on(
        'lineConnected',
        (line, evt) => {
          this.updateLines(line);
        },
        'line'
      )
      .on(
        'nodeClick',
        (node, event) => {
          !this.isReadonly && this.handleGraphNodeClick(event, node);
        },
        'node'
      );
    this.getNodeLinkRules();
    this.bindEvents();
  },
  beforeDestroy() {
    this.isNodeInfoShow = false;
    flowGraph && flowGraph.clear();
    flowGraph = null;
    this.$store.commit('ide/setGraphData', {});
    document.removeEventListener('keyup', this.deleteEventHandle);
  },
  methods: {
    saveGraphCache(val) {
      const tempLocations = this.graphData.locations.filter(node => !node.hasOwnProperty('node_id'));
      const backupGraph = JSON.parse(JSON.stringify(val || this.graphData));
      backupGraph.locations = tempLocations;
      this.bkRequest.httpRequest('dataFlow/setCatchData', {
        params: {
          key: 'graph_data_' + this.flowId,
          content: backupGraph,
        },
      });
    },
    saveTempNodeConfigCache() {
      this.bkRequest.httpRequest('dataFlow/setCatchData', {
        params: {
          key: 'temp_node_config_' + this.flowId,
          content: this.getTempNodeConfig,
        },
      });
    },
    getTempNodeConfigCache() {
      this.bkRequest
        .httpRequest('dataFlow/getCatchData', { query: { key: 'temp_node_config_' + this.flowId } })
        .then(res => {
          if (res.result) {
            this.$store.commit('ide/setTempNodeConfig', res.data);
          }
        });
    },
    setAutoNodesConfig(id) {
      this.$refs.nodesGenerator.initTreeData(id);
    },
    generateNodes(resultTable) {
      const indicatorNames = resultTable.map(item => item.list).flat();

      this.$refs.nodesGenerator.loading = true;
      this.bkRequest
        .httpRequest('dataFlow/autoGenerateNodes', {
          query: {
            node_id: this.$refs.nodesGenerator.nodeId,
            indicator_names: indicatorNames.length ? indicatorNames : '',
          },
        })
        .then(res => {
          if (res.result) {
            const igniteNode = res.data.find(item => item.node_type === 'ignite');
            igniteNode && showMsg(this.$t('ignite节点生成提示'));
            this.$store.commit('ide/setTempNodeConfig', res.data);
            this.saveTempNodeConfigCache();
            this.$refs.nodesGenerator.isShow = false;
            this.drawGraphByTempNodeConfig();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.$refs.nodesGenerator.loading = false;
        });
    },
    drawGraphByTempNodeConfig() {
      const currentNodeIds = this.graphData.locations.map(item => item.node_id || item.id);
      const { x, y, id } = this.graphData.locations
        .find(node => node.node_id === this.$refs.nodesGenerator.nodeId);
      const nodeWidth = 225;
      const nodePadding = 50;
      this.getTempNodeConfig
        .sort((a, b) => a - b)
        .forEach((item, index) => {
          if (!currentNodeIds.includes(item.node_key)) {
            const type = item.node_type;
            const nodeConfig = this.getNodeConfigByNodeWebType(type);

            const curX = x + (nodeWidth + nodePadding) * (item.level + 1);
            const curY = y + index * nodePadding;
            const node = {
              groupTemplate: nodeConfig.groupTemplate,
              type: type,
              component: nodeConfig.component,
              component_name: nodeConfig.component_name,
              node_type: type,
              node_name: item.config.name,
              status: 'no-start',
              instanceTypeName: nodeConfig.node_type_instance_name,
              iconName: nodeConfig.iconName,
              x: x + (nodeWidth + nodePadding) * (item.level + 1),
              y: y + index * nodePadding,
              id: item.node_key,
            };
            // this.updateLocations(node)
            this.graphData.locations.push(node);
            flowGraph.appendNode(node);

            const line = {
              source: {
                id: item.parent_node_key || id,
                x: x + (nodeWidth + nodePadding) * item.level,
                y: y + (index - 1) * nodePadding,
              },
              target: {
                id: item.node_key,
                x: curX,
                y: curY,
              },
            };

            this.graphData.lines.push(line);
          }
        });
      flowGraph.updateGraph(this.graphData);
    },
    /** 提供接口供父级组件获取Graph实例 */
    getGraphInstance() {
      return flowGraph;
    },
    changeAction(action = 'normal') {
      this.action = action;
    },
    async beforeLineDelete(line, event, el) {
      if (!this.isDeadLine(line)) {
        if (this.flowData.status === 'running') {
          showMsg('正在运行中的流程不能编辑删除连线', 'warning');
          return false;
        }
      }

      const deleteResult = await this.handleDeleteLine(line);
      return deleteResult;
    },

    /**
     * 判断连线是否可删除（连接的节点有未启动状态）
     */
    isDeadLine(line) {
      const { source, target } = this.getLinkedNode(line);
      return (source && source.status === 'no-start') || (target && target.status === 'no-start');
    },

    getLinkedNode(line) {
      const source = this.graphData.locations.find(node => String(node.id) === String(line.source.id));
      const target = this.graphData.locations.find(node => String(node.id) === String(line.target.id));
      return { source, target };
    },

    /**
     * 删除连线
     */
    async handleDeleteLine(line) {
      const { source, target } = this.getLinkedNode(line);
      /** 如果Target节点没有保存,跳过接口调用 */
      if (!target.node_id || !source.node_id) {
        return true;
      }

      let deleteLineParams = {
        action: 'delete_lines',
        data: [
          {
            from_node_id: source.node_id,
            to_node_id: target.node_id,
          },
        ],
      };
      return this.$store
        .dispatch('api/flows/updateGraph', { flowId: this.flowId, params: deleteLineParams })
        .then(res => {
          if (res.result) {
            return Promise.resolve(true);
          } else {
            this.getMethodWarning(res.message, res.code);
            // showMsg(res.message, 'error')
            return Promise.reject(false);
          }
        })
        ['catch'](err => {
          showMsg(err.message, 'error');
          return Promise.resolve(false);
        });
    },
    handleGraphNodeClick(e, data) {
      const nodeId = data.id;
      this.handleClickedNodeClass(nodeId, e, data);
      if (/__bk-node-setting/.test(e.target.className)) {
        const index = this.graphData.locations.findIndex(node => node.id === nodeId);
        if (index >= 0) {
          if (/bk-delete/.test(e.target.className)) {
            this.handleDeleteNode(index, data);
            console.log(index, data);
          }

          /** 复制节点 */
          if (/bk-copy/.test(e.target.className)) {
            const sourceNode = this.graphData.locations[index];
            this.handleCopyNode(sourceNode);
          }

          if (/icon-more/.test(e.target.className)) {
            this.handleAppendMoreAction(data);
          }
        }
      }

      /** 点击关联任务，查看 */
      if (/icon-chain/.test(e.target.className)) {
        this.handleNodeDblClick(data, 'referTask');
      }

      /** 点击生成节点 */
      if (/bk-generate-node/.test(e.target.className)) {
        console.log(nodeId, 'setAuto');
        this.setAutoNodesConfig(data.node_id);
      }
    },

    /**
     * 设置节点高亮闪烁
     */
    handleNodeActive(nodeId) {
      if (/^\d+$/.test(nodeId)) {
        nodeId = 'ch_' + nodeId;
      }
      const focusNode = document.getElementById(nodeId);
      if (!focusNode) return;
      const targetNode = focusNode.children[0];
      targetNode.className += ' focus-node';
      this.handleAutoPositionNode(focusNode);
      this.handleClickedNodeClass(nodeId);
      setTimeout(() => {
        targetNode.className = targetNode.className.replace(' focus-node', '');
      }, 1500);
    },

    /**
     * 自动定位节点到画布可视区域
     */
    handleAutoPositionNode(focusNode) {
      const nav = document.getElementById('ide_nav_left');
      const canvas = document.getElementById('flowgraph');
      const consolePanel = document.getElementById('ide_console_panel');
      const nodeComputedStyle = focusNode.getBoundingClientRect();
      const navComputedStyle = nav.getBoundingClientRect();
      const canvasComputedStyle = canvas.getBoundingClientRect();
      const consoleComputedStyle = consolePanel.getBoundingClientRect();
      const canvasTop = canvasComputedStyle.top + 15;
      const canvasRight = canvasComputedStyle.right - 15;
      const canvasBottom = consoleComputedStyle.top - 30;
      const navRight = navComputedStyle.right + 15; // 避免节点紧挨着左侧栏

      let offsetX = null;
      let offsetY = null;
      let transform = false;
      if (nodeComputedStyle.left < navRight) {
        offsetX = navRight - nodeComputedStyle.left;
        transform = true;
      }

      if (nodeComputedStyle.top < canvasTop) {
        offsetY = canvasTop - nodeComputedStyle.top;
        transform = true;
      }

      if (nodeComputedStyle.right > canvasRight) {
        offsetX = canvasRight - nodeComputedStyle.right;
        transform = true;
      }

      if (nodeComputedStyle.bottom > canvasBottom) {
        offsetY = canvasBottom - nodeComputedStyle.bottom;
        transform = true;
      }

      if (transform) {
        const instance = this.getGraphInstance();
        instance.translateOffset(offsetX, offsetY);
      }
    },

    /**
     * 点击选择某个节点，设置选种样式
     * @param nodeId ： 选中节点ID，NUll表示清除已有样式，不添加新的
     */
    handleClickedNodeClass(nodeId = null, e, data) {
      Array.prototype.forEach.call(document.getElementsByClassName('node-active'), node => {
        if (node) {
          node.className = node.className.replace(' node-active', '');
          this.clickNodeStatus = {};
        }
      });

      if (nodeId) {
        const clickNode = document.getElementById(nodeId);
        if (clickNode) {
          clickNode.className += ' node-active';
          this.$set(this, 'clickNodeStatus', {
            id: nodeId,
            data: data,
          });
        }
      }

      e && e.preventDefault();
    },

    deleteEventHandle(e) {
      if (e.keyCode === 46) {
        // delete按键
        if (this.clickNodeStatus.id) {
          const index = this.graphData.locations.findIndex(item => item.id === this.clickNodeStatus.id);
          this.handleDeleteNode(index, this.clickNodeStatus.data);
        }
      }
    },

    handleAppendMoreAction(data) {},

    handleDeleteNode(index, data) {
      if (!data.node_id) {
        this.graphData.locations.splice(index, 1);
        flowGraph.renderGraph(this.graphData);
        flowGraph.hiddenAllEndpoints();
        this.handleDeleteLineByNode(data);
        return;
      }
      console.log(index, data);
      const content = data.groupTemplate === 'graph-square' ? this.$t('删除存储将无法查询历史数据') : '';
      confirmMsg(
        this.$t('确认删除此节点'),
        content,
        () => {
          this.updateGraphLoading(true);
          this.$store
            .dispatch('api/flows/deleteNode', { nodeId: data.node_id, flowId: this.flowId })
            .then(resp => {
              if (resp.result) {
                this.graphData.locations.splice(index, 1);
                flowGraph.renderGraph(this.graphData);
                flowGraph.hiddenAllEndpoints();
                this.handleDeleteLineByNode(data);
              } else {
                showMsg(resp.message, 'error');
              }
            })
            ['finally'](_ => {
              this.updateGraphLoading(false);
            });
        },
        null,
        { type: 'warning', okText: this.$t('确定'), theme: 'danger' }
      );
    },

    handleDeleteLineByNode(data) {
      const sourceIdex = this.graphData.lines.findIndex(line => line.source.id === data.id);
      if (sourceIdex >= 0) {
        this.graphData.lines.splice(sourceIdex, 1);
      }

      const targetIdex = this.graphData.lines.findIndex(line => line.target.id === data.id);
      if (targetIdex >= 0) {
        this.graphData.lines.splice(targetIdex, 1);
      }
    },

    handleCopyNode(sourceNode) {
      const copyNode = Object.assign({}, sourceNode, {
        id: `${sourceNode.id}_copy_${new Date().getTime()}`,
        x: sourceNode.x + 225,
        y: sourceNode.y + 90,
        status: 'unconfig',
        node_name: this.$t('双击进行配置'),
        isCopy: true,
        type: sourceNode.node_type,
      });
      delete copyNode.node_id;
      this.graphData.locations.push(copyNode);
      flowGraph.appendNode(copyNode);
    },

    handleUpdateNode(data) {
      const index = this.graphData.locations.findIndex(node => node.id === data.id);
      if (index >= 0) {
        this.$set(this, 'activeNode', data);
        data.node_type === 'data_model_app' && this.setAutoNodesConfig(data.node_id); // 数据模型节点，需要自动生成节点
        Object.assign(this.graphData.locations[index], data);
        flowGraph.updateNodeStyle(this.graphData.locations);
      }
    },

    /** 调试状态备份数据 */
    backupGraphData() {
      this.backupGraph = JSON.parse(JSON.stringify(this.graphData));
      this.action = 'debug';
    },

    /** 还原画布初始状态 */
    restoreGraphdata() {
      if (this.backupGraph) {
        this.$set(this, 'graphData', JSON.parse(JSON.stringify(this.backupGraph)));
        flowGraph.updateNodeStyle(this.graphData.locations);
      }
      this.action = 'normal';
    },

    /**
     * 更新节点HTML
     * @param nodeInfo: 待更新节点数据
     * @param assignAttr：待复制节点属性（All代表整个属性Copy）
     */
    updateNodeHtml(nodeInfo, assignAttr = null) {
      this.graphData.locations.forEach(node => {
        const _node = (nodeInfo || []).find(data => String(data.node_id) === String(node.node_id));
        if (_node) {
          const assignObj =            (assignAttr
              && Object.keys(assignAttr).reduce(
                (pre, key) => Object.assign(pre, {
                  [key]: assignAttr[key] === 'all' ? _node : _node[assignAttr[key]],
                }),
                {}
              ))
            || _node;
          Object.assign(node, assignObj);
        }
      });
      flowGraph.updateNodeStyle(this.graphData.locations);
    },

    /**
     * 更新节点HTML
     * @param nodeInfo: 待更新节点数据
     * @param assignAttr：待复制节点属性（All代表整个属性Copy）
     */
    updateNodeHtmlById(nodeInfo, assignAttr = null) {
      this.graphData.locations.forEach(node => {
        const _node = (nodeInfo || []).find(data => data.id === node.id);
        if (_node) {
          const assignObj =            (assignAttr
              && Object.keys(assignAttr).reduce(
                (pre, key) => Object.assign(pre, {
                  [key]: assignAttr[key] === 'all' ? _node : _node[assignAttr[key]],
                }),
                {}
              ))
            || _node;
          Object.assign(node, assignObj);
        }
      });
      flowGraph.updateNodeStyle(this.graphData.locations);
    },

    handleModeChange(readonly) {
      this.isReadonly = readonly;
      this.isReadonly ? flowGraph.setReadonlyMode() : flowGraph.setEditMode();
    },

    triggleGraphMode() {
      this.isReadonly = !this.isReadonly;
      this.isReadonly ? flowGraph.setReadonlyMode() : flowGraph.setEditMode();
    },

    beforeLineConnected(source, target) {
      if (source.status === 'running' && target.status === 'running') {
        this.getMethodWarning('运行节点之间不允许连线', '连线校验失败');
        return false;
      }

      if (!this.nodeLinkRules.isRulesLoaded) {
        console.warn('连线规则初始化失败，请检查相关接口');
        return false;
      }
      const result = this.nodeLinkRules.validateLinkNode(
        source,
        target,
        this.graphData.lines,
        this.graphData.locations
      );
      if (!result.success) {
        this.getMethodWarning(result.msg, '连线校验失败');
      }
      return result.success;
    },
    /** 判断当前画布是否有符合要求的 hdfs节点 */
    isUnSignHdfsOnGraph(source, target) {
      let targetNode;
      /** 判断realtime和离线之间，是否已经有hdfs节点； 如果没有，寻找画布是否有没有配置的hdfs节点 */

      this.graphData.lines.forEach(line => {
        this.graphData.locations.forEach(node => {
          if (node.node_type === 'hdfs_storage') {
            if (
              (line.source.id === source.id && node.id === line.target.id)
              || (line.target.id === target.id && node.id === line.source.id)
            ) {
              targetNode = node;
            }
          }
        });
      });

      if (targetNode) {
        /** 如果当前画布有匹配的节点，需要判断该节点是否已经前后连接 */
        const nodeConnectedLines = this.graphData.lines.filter(line => {
          return line.target.id === targetNode.id || line.source.id === targetNode.id;
        });

        return nodeConnectedLines.length > 1 ? { result: false } : { result: true, node: targetNode };
      } else {
        /** 没有匹配节点，寻找是否有为配置的空节点 */
        const unSignHdfsNodes = this.graphData.locations.filter(node => {
          const nodesWithLines = this.graphData.lines.reduce((acc, cur) => {
            return acc.concat([cur.source.id, cur.target.id]);
          }, []);
          return (
            node.node_type === 'hdfs_storage'
                        && !node.hasOwnProperty('node_id')
                        && !nodesWithLines.includes(node.id)
          );
        });

        return unSignHdfsNodes.length
          ? {
            result: true,
            node: unSignHdfsNodes[0],
          }
          : {
            result: false,
          };
      }
    },
    getNodeByNodeId(id) {
      return this.graphData.locations.find(node => node.id === id);
    },

    streamHasHdfsNode(sourceNode) {
      const line = this.graphData.lines.find(line => {
        return line.source.id === sourceNode.id
                && this.getNodeByNodeId(line.target.id).node_type === 'hdfs_storage';
      });
      const hdfsID = line && line.target.id;
      if (!hdfsID) return { result: false };
      return {
        result: true,
        node: this.getNodeByNodeId(hdfsID),
      };
    },
    /** 自动在实时和离线之间补一个hdfs节点 */
    addHdfsNode(sourceNode, targetNode) {
      const unSignHdfsNodes = this.isUnSignHdfsOnGraph(sourceNode, targetNode);
      const avaliableHdfsNodes = this.streamHasHdfsNode(sourceNode);
      let makeUpLine = [];
      if (unSignHdfsNodes.result) {
        const node = unSignHdfsNodes.node;
        for (let i = 0; i < this.graphData.lines.length; i++) {
          if (this.graphData.lines[i].source.id === node.id) {
            makeUpLine = this.lineGenerator([{ source: sourceNode, target: node }]);
            break;
          } else if (this.graphData.lines[i].target.id === node.id) {
            makeUpLine = this.lineGenerator([{ source: node, target: targetNode }]);
            break;
          }
        }
        if (!makeUpLine.length) {
          makeUpLine = this.lineGenerator([
            {
              source: sourceNode,
              target: node,
            },
            {
              source: node,
              target: targetNode,
            },
          ]);
        }
      } else if (avaliableHdfsNodes.result) {
        makeUpLine = this.lineGenerator([{ source: avaliableHdfsNodes.node, target: targetNode }]);
      } else {
        const type = 'hdfs_storage';
        const group = 'graph-square';
        const icon = 'icon-hdfs_storage';
        const nodeConfig = this.getNodeConfigByNodeWebType(type);

        const node = {
          groupTemplate: group,
          type: type,
          component: nodeConfig.component,
          component_name: nodeConfig.component_name,
          node_type: type,
          node_name: this.$t('双击进行配置'),
          status: 'no-start',
          instanceTypeName: nodeConfig.node_type_instance_name,
          iconName: icon,
          x: sourceNode.x,
          y: (sourceNode.y + targetNode.y) / 2,
          id: `bk_node_${new Date().getTime()}`,
        };
        this.graphData.lines.pop();
        const lines = this.lineGenerator([
          {
            source: sourceNode,
            target: node,
          },
          {
            source: node,
            target: targetNode,
          },
        ]);

        this.graphData.locations.push(node);
        flowGraph.appendNode(node);
        this.graphData.lines.push(...lines);
        flowGraph.updateGraph(this.graphData);
        return;
      }

      this.graphData.lines.pop();
      this.graphData.lines.push(...makeUpLine);
      flowGraph.updateGraph(this.graphData);
    },
    lineGenerator(lineConfig) {
      const lines = lineConfig.map(item => {
        const line = {
          source: {
            id: item.source.id,
            x: item.source.x,
            y: item.source.y,
          },
          target: {
            id: item.target.id,
            x: item.target.x,
            y: item.target.y,
          },
        };
        return line;
      });
      return lines;
    },
    handleDrop(event) {
      /** firefox的drop事件必须调用preventDefault()和stopPropagation(), 否则会自动重定向 */
      event.preventDefault();
      event.stopPropagation();

      console.log(event.dataTransfer);
      const type = event.dataTransfer.getData('data-node-type');
      const group = event.dataTransfer.getData('data-node-group');
      const icon = event.dataTransfer.getData('data-node-icon');
      const nodeConfig = this.getNodeConfigByNodeWebType(type);
      console.log(type, group, icon, nodeConfig);
      const node = {
        groupTemplate: group,
        type: type,
        component: nodeConfig.component,
        component_name: nodeConfig.component_name,
        node_type: type,
        node_name: this.$t('双击进行配置'),
        status: 'no-start',
        instanceTypeName: nodeConfig.node_type_instance_name,
        iconName: icon,
        x: event.layerX,
        y: event.layerY,
        id: `bk_node_${new Date().getTime()}`,
      };
      // this.updateLocations(node)
      this.graphData.locations.push(node);
      flowGraph.appendNode(node);
    },
    handleDragover(event) {
      event.preventDefault();
      event.dataTransfer.dropEffect = 'move';
    },
    initGraph() {
      if (this.flowId) {
        this.loadDataFlowCanavas();
        // 更新画布时， 查询补算状态
        this.$emit('queryForComplement');
      }
    },

    /**
     * 更新打点监控
     * @param monitorData: 监控数据
     */
    updateMonitor(monitorData = []) {
      this.graphData.locations.forEach(location => {
        const monitor = monitorData.find(data => String(location.node_id) === String(data.nodeId)) || {
          formatData: {
            output_data_count: '-',
            input_data_count: '-',
            start_time: '-',
            interval: '-',
          },
        };
        this.$set(location, 'monitor', monitor);
      });
      flowGraph.updateNodeStyle(this.graphData.locations);
      // this.magicNodePosition(true, false)
    },

    /**
     * 移除监控数据
     */
    removeMonitor() {
      this.handleModeChange(false);
      // this.isReadonly = false
      this.graphData.locations.forEach(location => {
        location.monitor = null;
      });

      flowGraph.updateNodeStyle(this.graphData.locations);
      // this.magicNodePosition(false, false)
    },

    /**
     * 显示画布中的图形
     */
    loadDataFlowCanavas() {
      this.updateGraphLoading(true);
      this.$store
        .dispatch('api/flows/requestGraphInfo', { flowId: this.flowId })
        .then(resp => {
          if (resp.result) {
            this.graphData = resp.data;
            this.graphData.locations = this.mapNodeTypeToTmplType(this.graphData.locations);
            this.$store.commit('ide/setGraphData', this.graphData);
            Bus.$emit('updateWarningInfo', this.graphData); // 告警信息面板获取告警信息
            flowGraph.renderGraph(this.graphData);
            this.autoActiveNodeByNodeId();
          } else {
            this.getMethodWarning(resp.message, resp.code);
          }
        })
        ['finally'](_ => {
          this.updateGraphLoading(false);
        });
      this.getTempNodeConfigCache();
    },

    /** 自动定位并高亮Url中指定的节点 */
    autoActiveNodeByNodeId() {
      const NID = this.$route.query.NID;
      if (NID) {
        this.handleNodeActive(NID);
      }
    },

    /** 获取连线规则 */
    getNodeLinkRules() {
      this.bkRequest.httpRequest('/dataFlow/getNodeLinkRulesV2').then(response => {
        this.nodeLinkRules = new NodeLinkRules(response.data, this.getFlowNodeConfig);
      });
    },

    /**
     * 映射节点模板配置
     */
    mapNodeTypeToTmplType(nodeConfigs) {
      return Array.prototype.map.call(nodeConfigs, conf => {
        const extConf = this.getNodeConfigByNodeWebType(conf.node_type);
        return Object.assign(conf, {
          instanceTypeName: extConf.node_type_instance_name,
          groupTemplate: extConf.groupTemplate,
          iconName: extConf.iconName,
          component: extConf.component,
          width: extConf.width
        });
      });
    },

    /**
     * 自动排版
     * @param isMonitor：参数，是否是打点监控查看状态（打点监控状态节点之间的间隔需要变大，一边打点监控输入输出图标展示）
     * @param postUpdate：参数，是否更新节点位置数据到服务器端，手动点击自动排版时需要更新服务器数据
     */
    magicNodePosition(isMonitor = false, postUpdate = true) {
      this.handleModeChange(false);
      const toolkit = new FlowNodeToolKit(this.graphData, this.getNodeConfigByNodeWebType, isMonitor);
      this.graphData = toolkit.updateFlowGraph();
      flowGraph.updateGraph(this.graphData);

      if (postUpdate) {
        const updateLocations = this.graphData.locations
          .filter(location => location && location.status !== 'unconfig')
          .map(location => ({
            node_id: location.node_id,
            frontend_info: {
              x: location.x,
              y: location.y,
            },
          }));

        let params = {
          action: 'update_nodes',
          data: updateLocations,
        };
        this.$store.dispatch('api/flows/updateGraph', { flowId: this.$route.params.fid, params: params });
      }
    },

    handleGraphClick() {
      if (this.action === 'monitor') {
        this.activeNode = {};
        this.$refs.bkGraphMonitor.handleMouseleave();
      }

      this.handleClickedNodeClass();
    },

    handleNodeDblClick(node, openType = 'config') {
      this.openType = openType;
      if (!/_source$/.test(node.instanceTypeName)) {
        const lines = [];
        this.graphData.lines.forEach(line => {
          if ((line.to_node_id && line.to_node_id === node.node_id) || line.target.id === node.id) {
            lines.push(line);
          }
        });
        if (lines.length) {
          for (let i = 0; i < lines.length; i++) {
            const sourceNode = this.graphData.locations.find(location => {
              if (
                (lines[i].from_node_id && lines[i].from_node_id === location.node_id)
                || location.id === lines[i].source.id
              ) {
                return true;
              }
              return false;
            });
            if (sourceNode && !sourceNode.node_id) {
              showMsg('请先配置父级节点', 'warning');
              return;
            }
          }
          this.$set(this, 'activeNode', node);
          this.$nextTick(() => {
            this.isNodeInfoShow = true;
          });
        } else {
          showMsg('请先从已配置的节点连线到该节点', 'warning');
        }
      } else {
        this.$set(this, 'activeNode', node);
        this.$nextTick(() => {
          this.isNodeInfoShow = true;
        });
      }
    },

    updateLocations(node) {
      let locations = this.graphData.locations;
      let updateLocations = [];
      if (node.node_id) {
        updateLocations.push({
          node_id: node.node_id,
          frontend_info: {
            x: node.x,
            y: node.y,
          },
        });
        let params = {
          action: 'update_nodes',
          data: updateLocations,
        };
        this.$store.dispatch('api/flows/updateGraph', { flowId: this.flowId, params: params });
      }
    },

    /**
     * 判断是否自动添加hdfs节点：
     *  1. 上游是实时或者flink节点，下游是离线节点时，自动添加
     *  2. 上游是数据模型或实时指标节点，下游是离线指标时，自动天假
     */
    shouldAddHdfs(sourceType, targetType) {
      return (
        (['spark_structured_streaming', 'flink_streaming', 'realtime'].includes(sourceType)
          && targetType === 'offline')
        || (['data_model_app', 'data_model_stream_indicator'].includes(sourceType)
          && targetType === 'data_model_batch_indicator')
      );
    },

    /**
     * 批量新增line
     */
    updateLines(v) {
      let sourceNodeId = v.source.node_id;
      let targetNodeId = v.target.node_id;
      const sourceType = v.source.node_type;
      const targetType = v.target.node_type;
      if (this.shouldAddHdfs(sourceType, targetType)) {
        this.addHdfsNode(v.source, v.target);
      }

      if (!sourceNodeId || !targetNodeId) {
        return;
      }
      let lines = [
        {
          from_node_id: sourceNodeId,
          to_node_id: targetNodeId,
          frontend_info: {
            source: { arrow: 'LEFT' },
            target: { arrow: 'RIGHT' },
          },
        },
      ];
      let updateNodeLinesParams = {
        action: 'create_lines',
        data: lines,
      };
      this.$store
        .dispatch('api/flows/updateGraph', { flowId: this.flowId, params: updateNodeLinesParams })
        .then(res => {
          if (res.result) {
            const status = res.data.status || {};
            const updateNodes = Object.keys(res.data.status || {}).map(key => Object.assign(
              {},
              {
                node_id: key,
                status: status[key],
              }
            )
            );
            this.updateNodeHtml(updateNodes, { status: 'status' });
          } else {
            this.getMethodWarning(res.msg, res.code);
          }
        });
    },
    bindEvents() {
      Bus.$on('updateGraph', this.loadDataFlowCanavas);
      document.addEventListener('keyup', this.deleteEventHandle);
    },
  },
};
</script>
<style lang="scss">
$running-bk: rgba(133, 209, 77, 1);
$running-bd: rgba(133, 209, 77, 1);
$running-ft: rgba(255, 255, 255, 1);
$failure-bk: rgba(255, 72, 0, 1);
$failure-bd: rgba(255, 72, 0, 1);
$failure-ft: rgba(255, 255, 255, 1);
$warning-bk: rgba(246, 174, 0, 1);
$warning-bd: rgba(246, 174, 0, 1);
$warning-ft: rgba(255, 255, 255, 1);
$success-ft: #3a84ff;
#flowgraph {
  position: relative;
  background: url('../../../bkdata-ui/common/images/dataflow_back.png');
  display: flex;
  height: 100%;
  .alert-legend-container {
    position: absolute;
    right: 0;
    bottom: 52px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    z-index: 1;
    padding: 10px;
    background-color: #fafbfd;
    color: #212232;
    font-size: 13px;
    ul {
      display: flex;
      flex-wrap: nowrap;
      li {
        margin-right: 10px;
        border: 1px solid #ddd;
        position: relative;
        display: flex;
        justify-content: space-between;
        align-items: center;
        height: 26px;
        padding-left: 5px;
        i {
          width: 23px;
          height: 100%;
          display: flex;
          align-items: center;
          justify-content: center;
          color: white;
          margin-left: 5px;
        }
      }
      .modified {
        border-color: #ffb848;
        color: #ffb848;
        i {
          background: #ffb848;
        }
      }
      .started {
        border-color: #45e35f;
        color: #45e35f;
        i {
          background: #45e35f;
        }
      }
      .abnormal {
        border-color: #ff5656;
        color: #ff5656;
        i {
          background: #ff5656;
        }
      }
      .no-config {
        border-color: #c4c6cc;
        i {
          background: #c4c6cc;
        }
      }
    }
  }
}
.bk-graph-node {
  outline: none !important;
  .recal-loader,
  .recal-loader:after {
    border-radius: 50%;
    width: 14px;
    height: 14px;
  }
  .recal-loader {
    margin: 5px auto;
    font-size: 10px;
    position: relative;
    top: 0px;
    text-indent: -9999em;
    border-top: 0.11em solid rgba(73, 79, 87, 0.2);
    border-right: 0.11em solid #494f57;
    border-bottom: 0.11em solid #494f57;
    border-left: 0.11em solid #494f57;
    transform: translateZ(0);
    animation: loadAanimation 1.1s infinite linear;
  }
  @keyframes loadAanimation {
    0% {
      -webkit-transform: rotate(0deg);
      transform: rotate(0deg);
    }
    100% {
      -webkit-transform: rotate(360deg);
      transform: rotate(360deg);
    }
  }
  &:focus {
    outline: none !important;
  }
}
.node-container {
  display: inline-block;
  background: #fff;
  &.node-active {
    .node-running-layer {
      box-shadow: 1px 1px 4px 1px #3a84ff;
    }
  }

  .flow-alert-container {
    font-size: 14px;
    color: $warning-bd;
    margin-left: 5px;
    display: flex;
    align-items: center;
    .icon-alert {
      font-size: 14px;
    }
  }

  .node-running-layer {
    cursor: pointer;
    &.focus-node {
      animation: scale 1s;
    }

    .node-storage-footer {
      &.node-footer {
        background: #fff;
      }
    }
    &.running {
      border-color: $running-bd;
      .node-content {
        border: 1px solid $running-bd;
        background: $running-bk;
        color: $running-ft;
      }

      .node-storage-footer {
        &.node-footer {
          border-color: $running-bd;
        }
      }
    }
    &.failure {
      border-color: $failure-bd;
      .node-content {
        border: 1px solid $failure-bd;
        background: $failure-bk;
        color: $failure-ft;
      }

      .node-storage-footer {
        &.node-footer {
          border-color: $failure-bd;
        }
      }
    }
    &.warning {
      border-color: $warning-bd;
      .node-content {
        border: 1px solid $warning-bd;
        background: $warning-bk;
        color: $warning-ft;
      }

      .node-storage-footer {
        &.node-footer {
          border-color: $warning-bd;
        }
      }
    }
    .node-status-container {
      position: absolute;
      left: 0;
      top: 0;
      transform: translate(0%, -100%);
      display: flex;
      align-items: center;
    }

    .node-debug-status {
      background: #6cbbf7;
      border: 4px solid #ddf0ff;
      box-sizing: content-box;
      border-radius: 50%;
      width: 14px;
      height: 14px;
      display: flex;
      align-items: center;
      justify-content: center;

      i {
        transform: scale(0.6);
        font-weight: bolder;
      }
      &.failure {
        background: #fe621d;
        border: 4px solid #fbc8af;
        i {
          color: #fff;
        }
      }
      &.warning {
        background: #fe621d;
        border: 4px solid #fbc8af;
        i {
          color: #fff;
        }
      }
      &.success {
        i {
          color: #fff;
        }
      }
      &.running {
        i {
          color: #fff;
          animation: rotate 4s infinite;
        }
      }

      &.disabled {
        background: #eee;
        cursor: not-allowed;
        border-radius: 50%;
        border: 1px solid #eee;
        i {
          color: #ddd;
          transform: rotate(30deg);
        }
      }
    }

    .node-option-monitor {
      position: absolute;
      white-space: nowrap;
      border: 1px solid #85d14d;
      display: flex;
      background: #fff;

      .info-text {
        padding: 1px 10px;
      }

      .point-debug-unit {
        background: #85d14d;
        color: #fff;
        width: 22px;
        display: flex;
        justify-content: center;
        align-items: center;
      }
      &.monitor-input {
        top: 0;
        left: 0;
        transform: translateY(calc(-100% - 10px));
      }

      &.monitor-output {
        bottom: 0;
        right: 0;
        transform: translateY(calc(100% + 10px));
      }
    }

    .node-option-container {
      padding-top: 5px;
      position: absolute;
      right: 0;
      left: 0;
      bottom: 0;
      transform: translate(0, 100%);
      display: none;
      line-height: 1;
      text-align: right;
      .bkdata-node-extend {
        position: relative;
        .__bk-node-setting {
          margin-right: 5px;
          &.bk-delete {
            &:hover {
              color: #ea3636;
            }
          }
          &:last-child {
            margin-right: 0;
          }
          &:hover {
            color: #3a84ff;
          }
          &.icon-more-2:hover + .bk-node-select {
            display: block;
          }
        }
      }
      .bk-node-select {
        &:hover {
          display: block;
        }
        display: none;
        position: absolute;
        right: -130px;
        top: 15px;
        width: 150px;
        height: 44px;
        background: #ffffff;
        border: 1px solid #dcdee5;
        border-radius: 2px;
        font-size: 12px;
        text-align: center;
        padding: 6px 0;
        li {
          height: 32px;
          line-height: 32px;
          &:hover {
            background: #e1ecff;
            color: #3a84ff;
          }
        }
      }
    }

    &:hover {
      .node-option-container {
        &.edit {
          display: inline-block;
        }
      }
    }
  }

  .node-name-label {
    position: absolute;
    bottom: -2px;
    transform: translate(0, 100%);
    left: -45px;
    width: 164px;
    font-size: 12px;
    margin-top: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    text-align: center;
  }

  .node-source-layer {
    border: 1px dashed rgba(196, 198, 204, 1);
    border-radius: 50%;
    display: inline-block;
    position: relative;
    .node-source-content {
      width: 50px;
      height: 50px;
      background: rgba(240, 241, 245, 1);
      border: 1px solid rgba(196, 198, 204, 1);
      border-radius: 50%;
      margin: 10px;
      display: flex;
      justify-content: center;
      align-items: center;
      i {
        font-size: 18px;
      }
    }

    .node-option-container {
      padding-top: 25px;
    }

    .node-debug-status {
      position: absolute;
      top: 0;
      left: 0;
      transform: translate(5px, -35%);
    }
  }

  .node-calculate-layer {
    border-radius: 4px;
    display: inline-block;

    &.failure {
      .node-calculate-content {
        .node-detail {
          border-color: $failure-bd;
        }
      }
    }

    &.warning {
      .node-calculate-content {
        .node-detail {
          border-color: $warning-bd;
        }
      }
    }

    &.running {
      .node-calculate-content {
        .node-detail {
          border-color: $running-bd;
        }
      }
    }

    .node-calculate-content {
      width: 225px;
      height: 42px;
      background: transparent;
      border-radius: 4px;
      display: flex;
      .node-icon {
        width: 44px;
        height: 42px;
        background: rgba(240, 241, 245, 1);
        border-radius: 4px 0 0 4px;
        border-right: none;
        display: flex;
        justify-content: center;
        align-items: center;
        border: solid 1px rgba(196, 198, 204, 1);
        &.running {
          background: $running-bk;
          border-right: 1px solid $running-bd;
          color: $running-ft;
          border-color: $running-bd;
        }
        &.failure {
          background: $failure-bk;
          border-right: 1px solid $failure-bd;
          color: $failure-ft;
          border-color: $failure-bd;
        }
        &.warning {
          background: $warning-bk;
          border-right: 1px solid $warning-bd;
          color: $warning-ft;
          border-color: $warning-bd;
        }
        i {
          font-size: 18px;
        }
      }
      .node-detail {
        width: 182px;
        display: flex;
        align-items: center;
        border: 1px solid rgba(196, 198, 204, 1);
        border-left: none;
        border-radius: 0 4px 4px 0;

        .node-name {
          width: 100%;
          overflow: hidden;
          white-space: nowrap;
          text-overflow: ellipsis;
          text-align: center;
          padding: 0 20px 0 10px;
        }

        .action-more {
          position: absolute;
          right: 8px;
          font-size: 18px;
          display: block;
          width: 13px;
          height: 20px;
          border-radius: 2px;

          &:hover {
            background: #3a84ff;
            color: #fff;
            cursor: pointer;
          }
          .icon-more {
            position: absolute;
            left: 4px;
            top: 2px;
            font-weight: 700;
          }
        }

        .node-debug-status {
          position: absolute;
          right: 5px;
        }
      }
      .node-status {
        .icon-close-circle-shape {
          color: #ea3636;
        }
        .icon-exclamation-circle-shape {
          color: #ff9c01;
        }
        .icon-check-circle-shape {
          color: #2dcb56;
        }
      }
      .flow-upper-status {
        position: absolute;
        top: 0;
        left: 0px;
        transform: translate(0px, -100%);
        display: flex;
        align-items: center;
        .icon-action {
          cursor: pointer;
        }
        .icon-data-amend {
          font-size: 18px;
          color: #3a84ff;
        }
      }
    }
  }

  .node-storage-layer {
    border: 1px dashed rgba(196, 198, 204, 1);
    border-radius: 4px;
    display: inline-block;
    .node-storage-content {
      width: 40px;
      height: 40px;
      background: rgba(240, 241, 245, 1);
      border: 1px solid rgba(196, 198, 204, 1);
      border-radius: 4px;
      margin: 10px;
      position: relative;

      .node-storage-body {
        height: 30px;
        width: 100%;
        display: flex;
        justify-content: center;
        align-items: center;
        i {
          font-size: 18px;
        }
      }

      .node-storage-footer {
        border-top: 1px solid rgba(196, 198, 204, 1);
        height: 8px;
        width: 100%;
        display: flex;
        justify-content: center;
        align-items: center;
        &.running-footer {
          border-top: 1px solid $running-bd;
        }
        &.failure.footer {
          border-top: 1px solid $failure-bd;
        }

        .footer-line-container {
          padding: 0 7px;
          width: 100%;
          display: flex;
          justify-content: space-between;
          .line-left {
            width: 15px;
            height: 1px;
            background: rgba(196, 198, 204, 1);
          }

          .line-right {
            width: 5px;
            height: 1px;
            background: rgba(196, 198, 204, 1);
          }
          .running {
            background: $running-bd;
          }
          .failure {
            background: $failure-bd;
          }
        }
      }
    }

    .node-option-container {
      padding-top: 25px;
    }

    .node-debug-status {
      position: absolute;
      top: 0;
      left: 0;
      transform: translate(-50%, -50%);
    }
  }
}

@keyframes rotate {
  from {
    transform: scale(0.6) rotate(0);
  }
  to {
    transform: scale(0.6) rotate(360deg);
  }
}

@keyframes scale {
  15% {
    box-shadow: 0 0 10px #333;
  }

  30% {
    box-shadow: 0 0 0 transparent;
  }

  45% {
    box-shadow: 0 0 10px #333;
  }

  60% {
    box-shadow: 0 0 10px transparent;
  }

  75% {
    box-shadow: 0 0 10px #333;
  }

  to {
    box-shadow: 0 0 10px transparent;
  }
}
</style>
