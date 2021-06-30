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

/* eslint-disable no-param-reassign */
/* eslint-disable no-underscore-dangle */
import DataflowV2 from '@/controller/Dataflow.v2.js';

const responsiveWidth = document.body.clientWidth * 0.95;
const webNodeName = new DataflowV2().getAllWebNodesName();
const webNodeWidthMap = {};
const newLayoutNodes = [
  'batchv2',
  'DataModelStreamIndicator',
  'ModelAppNode',
  'FlinkSDK',
  'OfflineNodeNew',
  'RealtimeNew',
  'SplitNew',
  'MergeNew',
  'TdwBatch',
  'TdwJarNodeNew',
  'DataModelApp',
  'AddIndicator',
];
webNodeName.forEach((node) => {
  if (newLayoutNodes.includes(node)) {
    webNodeWidthMap[node] = responsiveWidth;
  } else {
    webNodeWidthMap[node] = '80%';
  }
});

class NodeComponentsManager {
  constructor(asyncResolve = [], asyncReject) {
    this.asyncResolve = asyncResolve;
    this.asyncReject = asyncReject;
  }

  getComponentConfig(nodeConfig) {
    if (!nodeConfig.component) return;
    const getCompInstance = nodeConfig.component;
    // const path = nodeConfig.path === 'base' ? '/pages/DataGraph/Graph/Children/' : 'bizComponents/GraphNodes/';
    const self = this;
    return {
      component: () => getCompInstance
        .then((component) => {
          if (!component.default._Ctor) {
            component.default._Ctor = { attached: false };
          }

          if (self.asyncResolve && self.asyncResolve.length) {
            if (!component.default.bk_backup) {
              component.default.bk_backup = {};
            }

            if (!component.default._Ctor.attached) {
              component.default._Ctor.attached = true;
              self.asyncResolve.forEach(fn => ((function (component, fn) {
                component.default.bk_backup[fn.fnName] = component.default[fn.fnName];
                component.default[fn.fnName] = function () {
                  if (component.default.bk_backup[fn.fnName]) {
                    if (Array.isArray(component.default.bk_backup[fn.fnName])) {
                      component.default.bk_backup[fn.fnName].forEach((backupFn) => {
                        backupFn.call(this);
                      });
                    } else {
                      component.default.bk_backup[fn.fnName].call(this);
                    }
                  }
                  fn.callback(this, component);
                };
              })(component, fn, this)));
            }
          }
          return component;
        }),
      layout: {
        width: nodeConfig.width,
      },
    };
  }
}

export default NodeComponentsManager;
