

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
  <div class="jsflow">
    <div class="canvas-area">
      <div v-if="showTool"
        :class="['tool-panel-wrap', `__${toolOption.position}`, toolOption.customClass]">
        <slot name="toolPanel">
          <tool-panel
            :class="[toolOption.direction]"
            :tools="tools"
            :isFrameSelecting="isFrameSelecting"
            @onToolClick="onToolClick" />
        </slot>
      </div>
      <div v-if="showPalette"
        ref="palettePanel"
        class="palette-panel-wrap">
        <slot name="palettePanel">
          <palette-panel :selector="selector" />
        </slot>
      </div>
      <div
        ref="canvasFlowWrap"
        class="canvas-flow-wrap"
        :style="canvasWrapStyle"
        @[mousedown]="onCanvasMouseDown"
        @[mouseup]="onCanvasMouseUp">
        <div id="canvas-flow"
          ref="canvasFlow"
          class="canvas-flow"
          :style="canvasStyle">
          <div
            v-for="node in nodes"
            :id="node.id"
            :key="node.id"
            class="jsflow-node canvas-node"
            @mouseenter="toggleHighLight(node, true)"
            @mouseleave="toggleHighLight(node, false)">
            <slot name="nodeTemplate"
              :node="node">
              <node-template :node="node" />
            </slot>
          </div>
        </div>
        <div v-if="isFrameSelecting"
          class="canvas-frame-selector"
          :style="frameSelectorStyle" />
      </div>
      <div v-if="showAddingNode"
        class="jsflow-node adding-node"
        :style="setNodeInitialPos(addingNodeConfig)">
        <slot name="nodeTemplate"
          :node="addingNodeConfig">
          <node-template :node="addingNodeConfig" />
        </slot>
      </div>
    </div>
  </div>
</template>
<script>
import { jsPlumb } from 'jsplumb';
import PalettePanel from './PalettePanel.vue';
import ToolPanel from './ToolPanel.vue';
import NodeTemplate from './NodeTemplate.vue';

import { matchSelector, getPolyfillEvent } from './jsFlowLib/dom.js';
import { uuid } from './jsFlowLib/uuid.js';
import { paletteOptions } from './jsFlowLib/defaultOptions.js';
import FlowToolkit from './FlowToolkit.js';

const props = {
  /** 是否自动连线
   *  避免异步组件连线位置问题
   *  使用异步组件时，需要在外面手动执行 connectEndpoint
   */
  autoConnect: {
    type: Boolean,
    default: true,
  },
  /** 延迟渲染节点Endpoints和连线
   *  考虑异步动态组件
   */
  delayRender: {
    default: 0,
  },
  showPalette: {
    type: Boolean,
    default: true,
  },
  showTool: {
    type: Boolean,
    default: true,
  },
  toolOption: {
    type: Object,
    required: true,
    default: () => ({
      /** 工具栏位置
       * 支持：left、right、left-bottom、right-bottom
       */
      position: 'left',

      /** 工具栏排列方式
       * 支持： row、column
       */
      direction: 'row',
      customClass: '',
    }),
  },
  tools: {
    // 工具栏选项，通过传入值来选择工具项及其顺序
    type: Array,
    default() {
      return [
        {
          type: 'zoomIn',
          name: '放大',
          cls: 'tool-item',
          /** 显示类型
           * text: 文本显示
           * icon：icon显示，需要传递icon
           * full: icon + 文本
           */
          showType: 'text',
          icon: '',
        },
        {
          type: 'zoomOut',
          name: '缩小',
          cls: 'tool-item',
          showType: 'text',
          icon: '',
        },
        {
          type: 'resetPosition',
          name: '重置',
          cls: 'tool-item',
          showType: 'text',
          icon: '',
        },
        {
          type: 'frameSelect',
          name: '框选',
          cls: 'tool-item',
          showType: 'text',
          icon: '',
        },
      ];
    },
  },
  editable: {
    type: Boolean,
    default: true,
  },
  selector: {
    type: String,
    default: paletteOptions.selector,
  },
  data: {
    type: Object,
    default() {
      return {
        nodes: [],
        lines: [],
      };
    },
  },
  nodeOptions: {
    // 节点配置项
    grid: [5, 5],
  },
  connectorOptions: {
    // 连接线配置项
    type: Object,
    default() {
      return {
        paintStyle: { fill: 'transparent', stroke: '#a9adb6', strokeWidth: 2 },
        hoverPaintStyle: { fill: 'transparent', stroke: '#3a84ff' },
        reattach: true,
      };
    },
  },
  endpointOptions: {
    // 端点配置项
    type: Object,
    default() {
      return {
        endpoint: 'Dot',
        connector: ['Flowchart', { stub: [1, 6], alwaysRespectStub: true, gap: 8, cornerRadius: 2 }],
        connectorOverlays: [['PlainArrow', { width: 8, length: 6, location: 1, id: 'arrow' }]],
        anchor: ['Left', 'Right', 'Top', 'Bottom'],
        isSource: true,
        isTarget: true,
      };
    },
  },
};

const eventDict = {
  mousedown: 'ontouchstart' in document.documentElement ? 'touchstart' : 'mousedown',
  mousemove: 'ontouchmove' in document.documentElement ? 'touchmove' : 'mousemove',
  mouseup: 'ontouchend' in document.documentElement ? 'touchend' : 'mouseup',
};

export default {
  name: 'JsFlow',
  components: {
    PalettePanel,
    ToolPanel,
    NodeTemplate,
  },
  model: {
    prop: 'data',
    event: 'change',
  },
  props,
  data() {
    const { nodes, lines } = this.data;
    return {
      nodes,
      lines,
      canvasGrabbing: false,
      isFrameSelecting: false,
      mouseDownPos: {},
      canvasPos: { x: 0, y: 0 },
      canvasOffset: { x: 0, y: 0 },
      frameSelectorPos: { x: 0, y: 0 },
      frameSelectorRect: { width: 0, height: 0 },
      selectedNodes: [],
      showAddingNode: false,
      addingNodeConfig: {},
      addingNodeRect: {},
      canvasRect: {},
      paletteRect: {},
      zoom: 1,
      ...eventDict,
    };
  },
  computed: {
    canvasWrapStyle() {
      let cursor = '';
      if (this.isFrameSelecting) {
        cursor = 'crosshair';
      } else {
        cursor = this.canvasGrabbing ? '-webkit-grabbing' : '-webkit-grab';
      }
      return { cursor };
    },
    canvasStyle() {
      return {
        left: `${this.canvasOffset.x}px`,
        top: `${this.canvasOffset.y}px`,
      };
    },
    frameSelectorStyle() {
      return {
        left: `${this.frameSelectorPos.x}px`,
        top: `${this.frameSelectorPos.y}px`,
        width: `${this.frameSelectorRect.width}px`,
        height: `${this.frameSelectorRect.height}px`,
      };
    },
  },
  watch: {
    data: {
      handler(val, old) {
        const { nodes, lines } = val;
        this.nodes = nodes;
        this.lines = lines;
      },
      deep: true,
    },
    editable(val) {
      const nodes = this.$el.querySelectorAll('.canvas-node');
      this.toggleNodeDraggable(nodes, val);
    },
  },
  mounted() {
    console.log('Init', new Date());
    this.initCanvas();
    this.registerEvent();
    this.renderData();
    this.canvasRect = this.$refs.canvasFlow.getBoundingClientRect();
    if (this.$refs.palettePanel) {
      this.paletteRect = this.$refs.palettePanel.getBoundingClientRect();
      this.registerPaletteEvent();
    }
    console.log('End', new Date());
  },
  beforeDestroy() {
    if (this.$refs.palettePanel) {
      this.$refs.palettePanel.removeEventListener(this.mousedown, this.nodeCreateHandler);
    }
    this.$el.removeEventListener(this.mousemove, this.nodeMovingHandler);
    document.removeEventListener(this.mouseup, this.nodeMoveEndHandler);
  },
  methods: {
    initCanvas() {
      const defaultOptions = {};
      const options = {
        ...this.endpointOptions,
        ...this.connectorOptions,
      };
      // jsplumb 默认属性首字母要大写
      for (const key in options) {
        const firstChar = key[0].toUpperCase();
        const upperKey = `${firstChar}${key.slice(1)}`;

        defaultOptions[upperKey] = options[key];
      }

      this.instance = jsPlumb.getInstance({
        container: 'canvas-flow',
        ...defaultOptions,
      });
    },
    // 注册事件
    registerEvent() {
      // 连线拖动之前，需要返回值
      this.instance.bind('beforeDrag', connection => {
        if (!this.editable) {
          return false;
        }
        if (typeof this.$listeners.onBeforeDrag === 'function') {
          return this.$listeners.onBeforeDrag(connection);
        } else {
          return true;
        }
      });
      // 连线放下之前，需要返回值
      this.instance.bind('beforeDrop', connection => {
        if (!this.editable) {
          return false;
        }
        if (typeof this.$listeners.onBeforeDrop === 'function') {
          return this.$listeners.onBeforeDrop(connection);
        } else {
          return true;
        }
      });
      this.instance.bind('connectionDrag', connection => {
        if (typeof this.$listeners.connectionDrag === 'function') {
          this.$emit('connectionDrag', connection);
        }
      });
      // 连线吸附之后
      this.instance.bind('connection', connection => {
        if (typeof this.$listeners.onConnection === 'function') {
          return this.$listeners.onConnection(connection);
        }
      });
      // 连线删除之前
      this.instance.bind('beforeDetach', connection => {
        if (typeof this.$listeners.onBeforeDetach === 'function') {
          return this.$listeners.onBeforeDetach(connection);
        } else {
          return true;
        }
      });
      // 连线已删除
      this.instance.bind('connectionDetached', (info, originalEvent) => {
        const lines = this.lines.filter(line => {
          return line.source.id !== info.sourceId && line.target.id !== info.targetId;
        });
        this.lines = lines;
        if (typeof this.$listeners.onConnectionDetached === 'function') {
          this.$emit('onConnectionDetached', lines);
        }
      });
      // 连线端点移动到另外端点
      this.instance.bind('connectionMoved', (info, originalEvent) => {
        if (typeof this.$listeners.onConnectionMoved === 'function') {
          this.$emit('onConnectionMoved', lines);
        }
      });
      // 连线单击
      this.instance.bind('click', (connection, originalEvent) => {
        if (typeof this.$listeners.onConnectionClick === 'function') {
          this.$emit('onConnectionClick', connection, originalEvent);
        }
      });
      // 连线双击
      this.instance.bind('dblclick', (connection, originalEvent) => {
        if (typeof this.$listeners.onConnectionDbClick === 'function') {
          this.$emit('onConnectionDbClick', connection, originalEvent);
        }
      });
    },
    renderData() {
      this.$nextTick(() => this.instance.batch(() => {
        this.nodes.forEach(node => this.initNode(node));
        this.autoConnect && this.connectEndpoint();
      })
      );
    },
    updateLines() {
      this.lines.forEach(line => this.removeConnector(line));
      this.connectEndpoint();
    },

    connectEndpoint() {
      let kit = new FlowToolkit(this.instance);
      window.requestAnimationFrame(() => {
        this.lines.forEach(line => {
          const copyLine = kit.getBestArrow(line);
          line.source.arrow = copyLine.sArrow;
          line.target.arrow = copyLine.tArrow;
          setTimeout(() => this.createConnector(line, this.connectorOptions));
        });
      });
    },

    // 创建节点并初始化拖拽
    createNode(node) {
      if (typeof this.$listeners.onCreateNodeBefore === 'function') {
        if (!this.$listeners.onCreateNodeBefore(node)) {
          return;
        }
      }
      this.nodes.push(node);
      this.$nextTick(() => {
        this.initNode(node);
        this.$emit('change', { nodes: this.nodes, lines: this.lines });
      });
    },
    // 初始化节点
    initNode(node) {
      const nodeEl = document.getElementById(node.id);
      nodeEl.style.left = `${node.x}px`;
      nodeEl.style.top = `${node.y}px`;
      this.setNodeDraggable(node, this.nodeOptions);
      this.setNodeEndPoint(node, this.endpointOptions);
      if (typeof this.$listeners.onCreateNodeAfter === 'function') {
        this.$emit('onCreateNodeAfter', node);
      }
    },
    // 删除节点
    removeNode(node) {
      const index = this.nodes.findIndex(item => item.id === node.id);
      this.nodes.splice(index, 1);
      this.instance.remove(node.id);
      const deleteLines = [];
      this.lines.forEach((line, index) => {
        if (line.sourceId === node.id || line.targetId === node.id) {
          Object.assign(line, { isDelete: true });
          this.removeConnector(line);
        }
      });
      this.$emit('change', { nodes: this.nodes, lines: this.lines.filter(line => !line.isDelete) });
    },
    // 设置节点端点
    setNodeEndPoint(node, options) {
      const endpoints = node.endpoints || ['Top', 'Right', 'Bottom', 'Left'];
      endpoints.forEach(item => {
        this.instance.addEndpoint(node.id, {
          anchor: item,
          uuid: item + node.id,
          ...options,
        });
      });
    },
    /**
     * 设置节点可拖拽
     * @param node 支持节点元素、节点id或者类数组的节点元素、节点id
     */
    setNodeDraggable(node, options) {
      if (!this.editable) {
        return;
      }

      const vm = this;
      this.instance.draggable(node.id, {
        grid: [20, 20],
        stop(event) {
          const index = vm.nodes.findIndex(el => el.id === node.id);
          const nodeConfig = Object.assign({}, node);
          const [nodeX, nodeY] = event.pos;
          nodeConfig.x = nodeX;
          nodeConfig.y = nodeY;

          vm.nodes.splice(index, 1, nodeConfig);
          vm.$emit('onNodeMove', nodeConfig, event);
        },
        ...options,
      });
    },
    /**
     * 更新节点位置
     * @params {Object} node 节点对象，eg: {id: 'test', x: 23, y: 500}
     */
    setNodePosition(node) {
      const curNode = document.getElementById(node.id);
      curNode.style.left = `${node.x}px`;
      curNode.style.top = `${node.y}px`;
      this.instance.revalidate(curNode);
    },
    /**
     * 节点和端点开启或关闭拖拽
     * @param {String、Number} node 支持节点元素、节点id或者类数组的节点元素、节点id
     * @param draggable
     */
    toggleNodeDraggable(node, draggable) {
      this.instance.setDraggable(node, draggable);
    },
    // 设置节点位置
    setNodeInitialPos(node) {
      return {
        left: `${node.x}px`,
        top: `${node.y}px`,
      };
    },
    // 创建连接线
    createConnector(line, options) {
      const lineOptions = line.options || {};
      const connection = this.instance.connect(
        {
          source: line.source.id,
          target: line.target.id,
          uuids: [line.source.arrow + line.source.id, line.target.arrow + line.target.id],
        },
        {
          ...options,
          ...lineOptions,
        }
      );
      return connection;
    },
    // 通过节点id获取所有该节点上所有连接线
    getConnectorsByNodeId(id) {
      const allConnectors = this.instance.getAllConnections();
      const connectors = allConnectors.filter(item => {
        return item.sourceId === id || item.targetId === id;
      });
      return connectors;
    },
    // 删除连接线
    removeConnector(line) {
      const connections = this.instance.getConnections({ source: line.source.id, target: line.target.id });
      connections.forEach(connection => {
        this.instance.deleteConnection(connection);
      });
    },
    /**
     * 添加连线overlay
     *
     * @param {Object} line 连线数据对象
     * @param {Object} overlay 连线overlay对象
     * eg: overlay = {
     *    type: 'Label',
     *    name: 'xxx',
     *    location: '-60',
     *    cls: 'branch-conditions',
     *    editable: true
     * }
     *
     */
    addLineOverlay(line, overlay) {
      const vm = this;
      const connections = this.instance.getConnections({ source: line.source.id, target: line.target.id });
      connections.forEach(connection => {
        connection.addOverlay([
          overlay.type,
          {
            label: overlay.name,
            location: overlay.location,
            cssClass: overlay.cls,
            id: overlay.id,
            events: {
              click(labelOverlay, originalEvent) {
                vm.$emit('onOverlayClick', labelOverlay, originalEvent);
              },
            },
          },
        ]);
      });
    },
    // 删除连线ovelay
    removeLineOverlay(line, id) {
      const connections = this.instance.getConnections({ source: line.source.id, target: line.target.id });
      connections.forEach(connection => {
        connection.removeOverlay(id);
      });
    },
    onCanvasMouseDown(e) {
      e = getPolyfillEvent(e);
      if (this.isFrameSelecting) {
        this.frameSelectHandler(e);
      } else {
        this.canvasGrabbing = true;
        this.mouseDownPos = {
          x: e.pageX,
          y: e.pageY,
        };
        this.$refs.canvasFlowWrap.addEventListener(this.mousemove, this.canvasFlowMoveHandler, false);
      }
    },
    canvasFlowMoveHandler(e) {
      e = getPolyfillEvent(e);

      this.canvasOffset = {
        x: this.canvasPos.x + e.pageX - this.mouseDownPos.x,
        y: this.canvasPos.y + e.pageY - this.mouseDownPos.y,
      };
    },
    onCanvasMouseUp(e) {
      if (this.isFrameSelecting) {
        this.frameSelectEndHandler(e);
      } else {
        this.canvasGrabbing = false;
        this.$refs.canvasFlowWrap.removeEventListener(this.mousemove, this.canvasFlowMoveHandler);
        this.canvasPos = {
          x: this.canvasOffset.x,
          y: this.canvasOffset.y,
        };
      }
    },
    registerPaletteEvent() {
      this.$refs.palettePanel.addEventListener(this.mousedown, this.nodeCreateHandler, false);
    },
    nodeCreateHandler(e) {
      const paletteNode = matchSelector(e.target, this.selector);
      if (!paletteNode) {
        return false;
      }
      const nodeType = paletteNode.dataset.type ? paletteNode.dataset.type : '';
      const nodeConfig = paletteNode.dataset ? paletteNode.dataset.config : {};

      this.showAddingNode = true;
      this.addingNodeConfig.id = uuid('node');
      this.addingNodeConfig.type = nodeType;
      this.$nextTick(() => {
        const node = this.$el.querySelector('.adding-node');
        this.addingNodeRect = node.getBoundingClientRect();
        const nodePos = this.getAddingNodePos(e);
        this.addingNodeConfig = {
          id: uuid('node'),
          type: nodeType,
          x: nodePos.x,
          y: nodePos.y,
          ...nodeConfig,
        };

        this.$el.addEventListener(this.mousemove, this.nodeMovingHandler, false);
        document.addEventListener(this.mouseup, this.nodeMoveEndHandler, false);
      });
    },
    nodeMovingHandler(e) {
      const nodePos = this.getAddingNodePos(e);
      this.$set(this.addingNodeConfig, 'x', nodePos.x);
      this.$set(this.addingNodeConfig, 'y', nodePos.y);
    },
    nodeMoveEndHandler(e) {
      this.$el.removeEventListener(this.mousemove, this.nodeMovingHandler);
      document.removeEventListener(this.mouseup, this.nodeMoveEndHandler);

      this.showAddingNode = false;

      if (e.pageX > this.paletteRect.left + this.paletteRect.width) {
        const nodeX = this.addingNodeConfig.x - this.paletteRect.width - this.canvasOffset.x;
        const nodeY = this.addingNodeConfig.y - this.canvasOffset.y;
        this.$set(this.addingNodeConfig, 'x', nodeX);
        this.$set(this.addingNodeConfig, 'y', nodeY);
        this.createNode(this.addingNodeConfig);
      }

      this.addingNodeConfig = {};
      this.addingNodeRect = {};
    },
    getAddingNodePos(e) {
      return {
        x: e.pageX - this.paletteRect.left - this.addingNodeRect.width / 2,
        y: e.pageY - this.paletteRect.top - this.addingNodeRect.height / 2,
      };
    },
    toggleHighLight(node, isHighLight = true) {
      const endpoints = this.instance.getEndpoints(node.id);
      endpoints.forEach(item => {
        item.setHover(isHighLight);
      });
    },
    onToolClick(tool) {
      typeof this[tool.type] === 'function' && this[tool.type]();
      this.$emit('onToolClick', tool);
    },
    setZoom(zoom, x = 0, y = 0) {
      this.instance.setContainer('canvas-flow');
      const transformOrigin = `${x}px ${y}px`;
      this.$refs.canvasFlow.style.transform = 'matrix(' + zoom + ',0,0,' + zoom + ',0,0)';
      this.$refs.canvasFlow.style.transformOrigin = transformOrigin;
      this.$refs.canvasFlow.zoom = zoom;
      this.zoom = zoom;
      this.instance.setZoom(zoom);
    },
    zoomIn(radio = 1.1, x, y) {
      this.setZoom(this.zoom * radio, x, y);
    },
    zoomOut(radio = 0.9, x, y) {
      this.setZoom(this.zoom * radio, x, y);
    },
    resetPosition() {
      this.setZoom(1);
      this.setCanvasPosition(0, 0);
    },
    setCanvasPosition(x, y) {
      this.canvasOffset = { x, y };
      this.canvasPos = { x, y };
    },
    // 节点框选点击
    frameSelect() {
      this.isFrameSelecting = true;
    },
    frameSelectHandler(e) {
      this.mouseDownPos = {
        x: e.pageX - this.canvasRect.left,
        y: e.pageY - this.canvasRect.top,
      };
      this.$refs.canvasFlowWrap.addEventListener(this.mousemove, this.frameSelectMovingHandler, false);
    },
    // 节点框选选框大小、位置设置
    frameSelectMovingHandler(e) {
      const widthGap = e.pageX - this.mouseDownPos.x - this.canvasRect.left;
      const heightGap = e.pageY - this.mouseDownPos.y - this.canvasRect.top;
      this.frameSelectorRect = {
        width: Math.abs(widthGap),
        height: Math.abs(heightGap),
      };
      this.frameSelectorPos = {
        x: widthGap > 0 ? this.mouseDownPos.x : this.mouseDownPos.x + widthGap,
        y: heightGap > 0 ? this.mouseDownPos.y : this.mouseDownPos.y + heightGap,
      };
    },
    frameSelectEndHandler(e) {
      this.$refs.canvasFlowWrap.removeEventListener(this.mousemove, this.frameSelectMovingHandler);
      this.$refs.canvasFlowWrap.removeEventListener(this.mouseup, this.frameSelectEndHandler);
      document.addEventListener('keydown', this.nodeLineCopyhandler, false);
      document.addEventListener('keydown', this.nodeLinePastehandler, false);
      document.addEventListener('keydown', this.nodeLineDeletehandler, false);
      document.addEventListener(this.mousedown, this.cancelFrameSelectorHandler, { capture: false, once: true });

      const selectedNodes = this.getSelectedNodes();
      this.isFrameSelecting = false;
      this.frameSelectorPos = { x: 0, y: 0 };
      this.frameSelectorRect = { width: 0, height: 0 };
      this.selectedNodes = selectedNodes;
      this.$emit('frameSelectNodes', selectedNodes.slice(0));
    },
    getSelectedNodes() {
      const { x: selectorX, y: selectorY } = this.frameSelectorPos;
      const { width: selectorWidth, height: selectorHeight } = this.frameSelectorRect;
      const selectedNodes = this.nodes.filter(node => {
        const nodeEl = document.querySelector(`#${node.id}`);
        const nodeRect = nodeEl.getBoundingClientRect();
        const nodePos = {
          left: nodeRect.left - this.canvasRect.left,
          top: nodeRect.top - this.canvasRect.top,
        };
        if (
          selectorX < nodePos.left
          && selectorX + selectorWidth > nodePos.left
          && selectorY < nodePos.top
          && selectorY + selectorHeight > nodePos.top
        ) {
          nodeEl.classList.add('selected');
          return true;
        }
      });
      return selectedNodes;
    },
    cancelFrameSelectorHandler(e) {
      this.selectedNodes.forEach(node => {
        const nodeEl = document.querySelector(`#${node.id}`);
        nodeEl && nodeEl.classList.remove('selected');
      });
      this.selectedNodes = [];
    },
    nodeLineCopyhandler(e) {
      if ((e.ctrlKey || e.metaKey) && e.keyCode === 67) {
        this.$emit('onNodeCopy', this.selectedNodes);
      }
    },
    nodeLinePastehandler(e) {
      if ((e.ctrlKey || e.metaKey) && e.keyCode === 86) {
        this.$emit('onNodePaste');
      }
    },
    nodeLineDeletehandler(e) {
      if (e.keyCode === 46 || e.keyCode === 8) {
        this.selectedNodes.forEach(node => {
          this.removeNode(node);
        });
        this.cancelFrameSelectorHandler();
      }
    },
  },
};
</script>
<style lang="scss">
.jsflow {
  height: 100%;
  border: 1px solid #cccccc;
  .canvas-area {
    position: relative;
    height: 100%;
  }
  .tool-panel-wrap {
    position: absolute;
    // padding: 10px 20px;
    background: #c4c6cc;
    opacity: 0.65;
    border-radius: 4px;
    z-index: 4;

    &.__left {
      top: 20px;
      left: 70px;
    }

    &.__right {
      top: 20px;
      right: 30px;
    }

    &.__left-bottom {
      bottom: 20px;
      left: 70px;
    }

    &.__right-bottom {
      bottom: 20px;
      right: 20px;
    }

    .tool-panel {
      display: flex;
      &.row {
        flex-direction: row;
      }

      &.column {
        flex-direction: column;
        align-items: center;
      }
    }
  }
  .palette-panel-wrap {
    float: left;
    width: 60px;
    height: 100%;
    border-right: 1px solid #cccccc;
  }
  .canvas-flow-wrap {
    position: relative;
    height: 100%;
    overflow: hidden;
  }
  .canvas-flow {
    position: relative;
    min-width: 100%;
    min-height: 100%;
  }
  .canvas-frame-selector {
    position: absolute;
    border: 1px solid #3a84ff;
    background: rgba(58, 132, 255, 0.15);
  }
  .jsflow-node {
    position: absolute;
  }
  .adding-node {
    opacity: 0.8;
  }
}
</style>
