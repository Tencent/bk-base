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

/** 操作项 */
const OPERATION_ACTION = {
  /** 开始调试 */
  START_DEBUG: 'start_debug',

  /** 停止调试 */
  STOP_DEBUG: 'stop_debug',

  /** 自动排版 */
  MAGIC_POSITION: 'magic_position',

  /** 启动流程 */
  START_INSTANCE: 'start_instance',

  /** 停止流程 */
  STOP_INSTANCE: 'stop_instance',

  /** 重启流程 */
  RESTART_INSTANCE: 'restart_instance',

  /** 启动补算 */
  START_COMPLEMENT: 'start_complement',

  /** 申请补算 */
  APPLY_COMPLEMENT: 'apply_complement',

  /** 撤销补算 */
  CANCEL_COMPLEMENT: 'cancel_complement',

  /** 停止补算 */
  STOP_COMPLEMENT: 'stop_complement',

  /** 开启打点监控 */
  START_MONITORING: 'start_dot_monitoring',

  /** 停止打点监控 */
  STOP_MONITORING: 'stop_dot_monitoring',

  UPDATE_COMPLEMENT_ICON: 'update_complement_icon',
};

export { OPERATION_ACTION };
