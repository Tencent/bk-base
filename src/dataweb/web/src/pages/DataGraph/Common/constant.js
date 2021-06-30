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

/**
 * 常量文件定义
 */
const COMPONENT = 'tools';
const OUTLINE = 'outline';
const FUNCTION = 'function';
const TASKLIST = 'taskList';
const FLOWSEARCH = 'flowSearch';

// flow-sidebar's tab的名称定义
export const SIDEBAR = {
  COMPONENT,
  OUTLINE,
  FUNCTION,
  TASKLIST,
  FLOWSEARCH,
};

const WARNING = 'curveSetting';
const RUNNING = 'exceptionDetecting';
const HISTORY = 'executiveHistory';

// flow-console's tab的名称定义
export const CONSOLE = {
  WARNING,
  RUNNING,
  HISTORY,
};

// 节点类型定义
export const NODE_TYPES = {
  rawsource: ['rawsource', 'rtsource', 'etl_source'],
  cal: ['realtime', 'offline', 'clean', 'algorithm_model'],
  storage: ['hdfs_storage', 'mysql', 'elastic_storage'],
};

// 调试状态定义
export const DEBUG_MODE = {
  NONE: '',
  NORMAL: 'normal',
  POINT: 'point',
  RENORMAL: 'RENORMAL', // 启动失败，退回初始状态
};

// 新手任务的步骤
export const GUIDE_STEP = {
  TOGGLE_LIST: 0,
  COMPONENT_ICON: 1,
  OUTLINE_ICON: 2,
  FUNCTION_ICON: 3,
  TOOLBAR: 4,
  CONSOLE: 5,
};
