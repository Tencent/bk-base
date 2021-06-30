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

/** 定义连线类型 */
const CONNECTORTYPE = {
  /** 辅助线，点击连接节点时 */
  AUXILIARY: 0,

  /** 实体连线，连线规则 */
  RULE: 1,

  /** 其他连线类型 */
  OTHER: 100,
};

/** 自定义状态管理 */
const STOREKEY = '__BK_D3_GRAPH_STORE_KEY__';

/** 自定义事件 */
const EVENTS_KEYS = '__BK_GRAPH_EVENTS__';

/** 清理所有自定义事件 */
const EVENTS_DESTROY_KEY = '__clean_all_events__';

/** 清理所有缓存 */
const STORE_DESTROY_KEY = '__clean_all_store__';

/** 节点点击事件类型 */
const NODE_CLICK_TYPE = {
  /** 系统原生事件 */
  NATIVE: 'native',
  /** 重构CLICK事件 */
  REFACTORING: 'refactoring',
};

/** 连接点类型 */
const ENDPOINT_TYPE = {
  /** 连接点在左边 */
  LEFT: 'left',

  /** 连接点在右边 */
  RIGHT: 'right',

  /** 连接点在顶部 */
  TOP: 'top',

  /** 在底部 */
  BOTTOM: 'bottom',

  /** 拖拽开始的节点 */
  DRAGSTART: 'drag_start',

  /** 拖拽结束连接点 */
  DRAGEND: 'drag_end',
};

/** 连接点操作状态类型 */
const ENDPOINT_ACTION = {
  /** 连接点隐藏 */
  HIDDEN: 'hidden',

  /** 连接点在移动 */
  MOVE: 'move',

  /** 鼠标滑过 */
  POINTER: 'pointer',

  /** 高亮显示 */
  LIGHT: 'light',
};

/** 对外分发事件 */
const EVENTS = {
  /** 可视区域改变事件 */
  VISIBLE_AREA_CHANGED: 'visibleAreaChanged',

  /** 可视区域移动中 */
  VISIBLE_AREA_CHANGING: 'visibleAreaChanging',

  /** 更新可视区域节点之前事件 */
  BEFORE_VISIVLE_NODE_UPDATE: 'beforeVisibleNodeUpdate',

  /**  配置节点渲染时回调，可在此处返回节点模板 */
  ON_NODE_RENDER: 'onNodeRender',

  /** 结束连线之前 */
  ON_LINE_CONNECT_BEFORE: 'onLineConnectBefore',

  /** 连线完成 */
  ON_LINE_CONNECTED: 'lineConnected',

  ON_NODE_MOUSE_ENTER: 'nodeMouseEnter',

  ON_NODE_MOUSE_LEAVE: 'nodeMouseLeave',

  ON_NODE_MOUSE_DOWN: 'nodeMousedown',

  ON_NODE_MOUSE_UP: 'nodeMouseup',

  /** 画布拖拽结束 */
  ON_CANVAS_DRAG_END: 'canvasDragEnd',

  /** 画布拖拽开始 */
  ON_CANVAS_DRAG_START: 'canvasDragStart',

  /** 节点拖拽结束 */
  ON_NODE_DRAG_END: 'nodeDragEnd',

  /** 节点鼠标左键双击事件 */
  ON_NODE_DBCLICK: 'nodeDblClick',

  ON_NODE_CLICK: 'nodeClick',

  /** 连线删除点击事件 */
  ON_LINE_DELETE_CLICK: 'lineDeleteClick',

  ON_LINE_MOUSE_ENTER: 'lineMouseEnter',

  ON_LINE_MOUSE_LEAVE: 'lineMouseLeave',
};

/** 定义节点形状 */
const NODE_SHAPE = {
  /** 圆形 */
  ROUND: 'round',

  /** 正方形 */
  SQUARE: 'square',

  /** 长方形 */
  RECT: 'rect',
};

/** 可视区域改变时，节点接下来的更新状态 */
const NODE_UPDATE_HTML_STATUS = {
  /** 不做更新 */
  NONE: 'none',

  /** 插入新的显示模板 */
  INSERT: 'insert',

  /** 移除模板 */
  REMOVE: 'remove',
};

/** 画布模式 */
const MODE = {
  /** 只读模式 */
  READONLY: 'readonly',

  /** 编辑模式 */
  EDIT: 'edit',
};

/** 事件监听类型 */
const EVENT_LISTENER_TYPE = {
  /** 监听画布的事件 */
  CANVAS: 'canvas',

  /** 监听节点事件 */
  NODE: 'node',

  /** 监听连线事件 */
  LINE: 'line',

  /** 监听连接点事件 */
  ENDPOINT: 'endpoint',

  /** 监听连线结束箭头事件 */
  ARROW: 'arrow',
};

/** 画布缩放控制面板类型 */
const CONTROL_PANNEL_TYPE = {
  ZOOM_IN: 'zoomIn',
  ZOOM_OUT: 'zoomOut',
  RESET_POSITION: 'resetPosition',
};

export {
  MODE,
  CONNECTORTYPE,
  STOREKEY,
  ENDPOINT_TYPE,
  ENDPOINT_ACTION,
  EVENTS_KEYS,
  EVENTS_DESTROY_KEY,
  STORE_DESTROY_KEY,
  EVENTS,
  NODE_SHAPE,
  NODE_UPDATE_HTML_STATUS,
  EVENT_LISTENER_TYPE,
  NODE_CLICK_TYPE,
  CONTROL_PANNEL_TYPE,
};
