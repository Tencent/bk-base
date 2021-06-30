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

import TempCommonData from '../common/commonMethods.js';
const iconType = {
  stream: 'icon-realtime',
  clean: 'icon-icon-filter',
  batch: 'icon-offline',
  batch_model: 'icon-digger',
  stream_model: 'icon-digger',
  model: 'icon-abnormal',
  transform: 'icon-transform',
  view: 'icon-view',
  storage: 'icons-save-node',
};
const dataProcessingTemp = function (nodeData) {
  const nodeName = TempCommonData.switchName(nodeData);
  const nodeIcon = TempCommonData.nodeIcon(nodeData);
  return {
    temp: `
                <div class="node-storage-container ${nodeData.direction === 'current' ? 'border-color-none' : ''} 
                ${nodeData.direction === 'current' ? 'node-current' : ''
}"
                    >
                    <div class="node-icon ${nodeData.direction === 'current' ? 'border-white' : ''}">
                    ${nodeData.node_type === 'result_table'
  ? `<i class="icons ${nodeIcon}"></i>`
  : `<i class="${iconType[nodeData.processing_type]} icons"></i>`
}
                    </div>
                    <div class="node-name text-overflow ${nodeData.direction === 'INPUT' ? 'pl8' : ''}"
                        >${nodeName}</div>
                </div>`,
    toolTipsContent: `<div id="blood-tooltip-demo"
                    class="blood-tooltips text-overflow bk-form">
                    <div class="tooltips-title text-overflow"
                        style="font-size:15px;">
                        ${nodeData.name ? `<span>${nodeData.name}</span>` : ''}
                    </div>
                    <div class="bk-form-item mark-color"
                        style="margin-top:0px;">
                        <label class="bk-label">
                        ${TempCommonData.switchLanguage('中文名称')}：
                        </label>
                        <div class="bk-form-content">
                            <div title="${TempCommonData.orValue(nodeData.description)}"
                                class="text-overflow">
                                ${TempCommonData.orValue(nodeData.description)}
                            </div>
                        </div>
                    </div>
                    <div class="bk-form-item"
                        style="margin-top:0px;">
                        <label class="bk-label">
                        ${TempCommonData.switchLanguage('数据处理类型')}：
                        </label>
                        <div class="bk-form-content">
                            <div title="${TempCommonData.orValue(nodeData.processing_type_alias)}"
                                class="text-overflow">
                                ${TempCommonData.orValue(nodeData.processing_type_alias)}
                            </div>
                        </div>
                    </div>
                    <div class="bk-form-item"
                        style="margin-top:0px;">
                        <label class="bk-label">
                        ${TempCommonData.switchLanguage('项目')}：
                        </label>
                        <div class="bk-form-content">
                            <div title="${nodeData.project_id
      ? `[${nodeData.project_id}] ${nodeData.project_name ? nodeData.project_name : ''}`
      : TempCommonData.switchLanguage('暂无')
}"
                                class="text-overflow">
                                ${nodeData.project_id
  ? `[${nodeData.project_id}] ${nodeData.project_name ? nodeData.project_name : ''}`
  : TempCommonData.switchLanguage('暂无')
}
                            </div>
                        </div>
                    </div>
                    </div>`,
  };
};
export default dataProcessingTemp;
