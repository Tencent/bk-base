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
  stream: 'icon-batch-result',
  clean: 'icon-clean-result',
  batch: 'icon-offline-calc',
  batch_model: 'icon-modelflow-table',
  stream_model: 'icon-modelflow-table',
  model: 'icon-abnormal-table',
  transform: 'icon-transform-table',
};
const resultNode = function (nodeData) {
  const nodeName = TempCommonData.switchName(nodeData);
  return {
    temp: `
                <div class="node-storage-container data-processing node-background 
                ${nodeData.direction === 'current' ? 'border-color-none node-current' : ''
}"
                    >
                        <div class="node-icon ${nodeData.direction === 'current' ? 'border-white' : ''}">
                            <i class="icons ${iconType[nodeData.processing_type]
  ? iconType[nodeData.processing_type]
  : 'icon-resulttable'
}"
                                ></i>
                        </div>
                        <div class="node-name text-overflow ${nodeData.direction === 'INPUT' ? 'pl8' : ''}"
                            >${nodeName}</div>
                </div>`,
    toolTipsContent: `<div id="blood-tooltip-demo"
            class="blood-tooltips text-overflow bk-form">
            <div onclick="bloodToolTips.linkToDictionary()"
                class="tooltips-title click-color text-overflow"
                style="font-size:15px;">
                ${nodeData.name
      ? `<span title="${TempCommonData.switchLanguage('查看详情')}"
                >${nodeData.name}</span>`
      : ''
}
                <span title="${TempCommonData.switchLanguage('查看详情')}"
                    class="icon icon-chain"></span>
            </div>
            <div class="bk-form-item mark-color"
                style="margin-top:0px;">
                <label class="bk-label">
                ${TempCommonData.switchLanguage('中文名称')}：
                </label>
                <div class="bk-form-content">
                    <div title="${TempCommonData.orValue(nodeData.alias)}"
                        class="text-overflow">
                        ${TempCommonData.orValue(nodeData.alias)}
                    </div>
                </div>
            </div>
            <div class="bk-form-item"
                style="margin-top:0px;">
                <label class="bk-label">
                ${TempCommonData.switchLanguage('结果表类型')}：
                </label>
                <div class="bk-form-content">
                    <div title="${TempCommonData.orValue(nodeData.processing_type_alias)}"
                        class="text-overflow">
                        ${TempCommonData.orValue(nodeData.processing_type_alias)}
                        ${nodeData.processing_type_alias ? `<span>${TempCommonData.switchLanguage('结果表')}</span>` : ''
}
                    </div>
                </div>
            </div>
            <div class="bk-form-item"
                style="margin-top:0px;">
                <label class="bk-label">
                ${TempCommonData.switchLanguage('统计频率')}：
                </label>
                <div class="bk-form-content">
                    <div title="${nodeData.count_freq || nodeData.count_freq === 0
  ? `${nodeData.count_freq} ${nodeData.count_freq_unit_alias}`
  : TempCommonData.switchLanguage('暂无')
}")
                        class="text-overflow">
                        ${nodeData.count_freq || nodeData.count_freq === 0
  ? `${nodeData.count_freq} ${nodeData.count_freq_unit_alias}`
  : TempCommonData.switchLanguage('暂无')
}
                    </div>
                </div>
            </div>
            <div class="bk-form-item"
                style="margin-top:0px;">
                <label class="bk-label">
                ${TempCommonData.switchLanguage('敏感度')}：
                </label>
                <div class="bk-form-content">
                    <div title="${TempCommonData.orValue(nodeData.sensitivity === 'public'
  ? TempCommonData.switchLanguage('公开')
  : TempCommonData.switchLanguage('业务私有'))}"
                        class="text-overflow">
                        ${TempCommonData.orValue(nodeData.sensitivity === 'public'
    ? TempCommonData.switchLanguage('公开')
    : TempCommonData.switchLanguage('业务私有'))}
                    </div>
                </div>
            </div>
            <div class="bk-form-item"
                style="margin-top:0px;">
                <label class="bk-label">
                ${TempCommonData.switchLanguage('业务')}：
                </label>
                <div class="bk-form-content">
                    <div title="${nodeData.bk_biz_id
      ? `[${nodeData.bk_biz_id}] ${nodeData.bk_biz_name ? nodeData.bk_biz_name : ''}`
      : TempCommonData.switchLanguage('暂无')
}"
                        class="text-overflow">
                        ${nodeData.bk_biz_id
  ? `[${nodeData.bk_biz_id}] ${nodeData.bk_biz_name ? nodeData.bk_biz_name : ''}`
  : TempCommonData.switchLanguage('暂无')
}
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
            <div class="bk-form-item"
                style="margin-top:0px;">
                <label class="bk-label">
                ${TempCommonData.switchLanguage('更新者')}：
                </label>
                <div class="bk-form-content">
                    <div title="${TempCommonData.orValue(nodeData.updated_by)}"
                        class="text-overflow">
                        ${TempCommonData.orValue(nodeData.updated_by)}
                    </div>
                </div>
            </div>
            <div class="bk-form-item"
                style="margin-top:0px;">
                <label class="bk-label">
                ${TempCommonData.switchLanguage('更新时间')}：
                </label>
                <div class="bk-form-content">
                    <div title="${TempCommonData.orValue(nodeData.updated_at)}"
                        class="text-overflow">
                        ${TempCommonData.orValue(nodeData.updated_at)}
                    </div>
                </div>
            </div>
            </div>`,
  };
};
export default resultNode;
