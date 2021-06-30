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
 * @Name NodeTemplate
 * @Method getTemplate 获取数据地图的绘制模板
 * @param {String} nodeType 节点类型，不同节点类型有不同的渲染模板
 * @param {Object} data 接口返回数据，数据用于节点内容的展示
 * @return {NodeTemplate}
 */
class NodeTemplate {
  static getTemplate(nodeType, data) {
    // 根据功能开关选择是否开启standardStyle和standarTips
    const standardStyle = window.gVue.$modules.isActive('standard') ? 'bk-click-style' : '';
    const standardTips = window.gVue.$modules.isActive('standard') ? window.$t('点击可查看对应的数据标准详情') : '';
    // 模版渲染的优化：本来会通过逻辑判断去将name和count等一系列内容拼接到html中，现在通过下面的内容，直接拿到count，name等值，然后拼接到html中
    let template = '';
    const { id, code, name, count } = data;
    /**
         * 根据不同的nodeType对应生成不同的节点模板
         * 模板总共有以下11种类型：
         *
         * 1.current
         * 2.curstandard
         * 3.curhasstandard
         * 4.root
         * 5.rootstandard
         * 6.other
         * 7.hasstandard
         * 8.zerohasstandard
         * 9.zerostandard
         * 10.standard
         * 11.zero
         */
    switch (nodeType) {
      case 'current':
        template = `
                    <div style="height: 34px; width: 131px;border-color:#a9adb5"
                        onclick="node_click('${code}')"
                        class='jtk-window jtk-node outline-none current'
                        data-toggle="popover"
                        data-placement="bottom"
                        id='${id}'
                        data-node='${id}'
                        data-type='current'>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_right"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_right"
                            onclick="plus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_left"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_left"
                            onclick="plus_click('${code}')">
                        </i>
                        <div class="node-background-current"
                            style="display: flex;align-items: center;justify-content: space-between;padding: 0 5px;"
                            onmouseover="node_hover('${code}')">
                            <span class="node-desc-map" title="${name}">
                                ${name}
                            </span>
                            <span class="node-desc-count-map" title=${count}>
                                ${count}
                            </span>
                        </div>
                    </div>
                `;
        break;
      case 'curstandard':
        template = `
                    <div style="height: 34px; width: 131px;border-color:#a9adb5"
                        onclick="node_click('${code}')"
                        class='jtk-window jtk-node outline-none curstandard bk-mark-square-map'
                        data-toggle="popover"
                        data-placement="bottom"
                        id='${id}'
                        data-node='${id}'
                        data-type='curstandard'>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_right"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_right"
                            onclick="plus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_left"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_left"
                            onclick="plus_click('${code}')">
                        </i>
                        <div class="node-background-current"
                            style="display: flex;align-items: center;justify-content: space-between;padding: 0 5px;"
                            onmouseover="node_hover('${code}')">
                            <span class="node-desc-map" title="${name}">
                                ${name}
                            </span>
                            <span class="node-desc-count-map" title=${count}>
                                ${count}
                            </span>
                        </div>
                    </div>
                `;
        break;
      case 'curhasstandard':
        template = `
                    <div style="height: 34px; width: 131px;border-color:#a9adb5"
                        onclick="node_click('${code}')"
                        class='jtk-window jtk-node outline-none curhasstandard bk-mark-triangle-map bk-success-map'
                        data-toggle="popover"
                        data-placement="bottom"
                        id='${id}'
                        data-node='${id}'
                        data-type='curhasstandard'>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_right"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_right"
                            onclick="plus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_left"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_left"
                            onclick="plus_click('${code}')">
                        </i>
                        <div class="node-background-current"
                            style="display: flex;align-items: center;justify-content: space-between;padding: 0 5px;"
                            onmouseover="node_hover('${code}')">
                            <span class="node-desc-map" title="${name}">
                                ${name}
                            </span>
                            <span class="node-desc-count-map" title=${count}>
                                ${count}
                            </span>
                        </div>
                    </div>
                `;
        break;
      case 'root':
        template = `
                    <div style="height: 52px; width: 166px;border-color:#666bb4"
                        onclick="node_click('${code}')"
                        class='jtk-window jtk-node outline-none root'
                        data-toggle="popover"
                        data-placement="bottom"
                        id='${id}'
                        data-node='${id}'
                        data-type='root'>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_right"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_right"
                            onclick="plus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_left"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_left"
                            onclick="plus_click('${code}')">
                        </i>
                        <div class="node-background-root"
                            style="align-items: center;padding: 0 5px;"
                            onmouseover="node_hover('${code}')">
                            <div class="node-desc-map-root" style="width:154px;">
                            <span class="text-overflow"  style="display: block;vertical-align:middle;" title="${name}">
                                ${name}
                            </span>
                            </div>
                            <div>
                            <span class="node-desc-count-map-root"
                                style="display: inline-block;vertical-align:middle;"
                                title=${count}>
                                ${count}
                            </span>
                            </div>
                        </div>
                    </div>
                `;
        break;
      case 'rootstandard':
        template = `
                    <div style="height: 52px; width: 166px;border-color:#666bb4"
                        onclick="node_click('${code}')"
                        class='jtk-window jtk-node outline-none rootstandard bk-mark-triangle-map-root bk-success-map'
                        data-toggle="popover"
                        data-placement="bottom"
                        id='${id}'
                        data-node='${id}'
                        data-type='rootstandard'>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_right"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_right"
                            onclick="plus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_left"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_left"
                            onclick="plus_click('${code}')">
                        </i>
                        <div class="node-background-root"
                            style="align-items: center;padding: 0 5px;"
                            onmouseover="node_hover('${code}')">
                            <div class="node-desc-map-root" style="width:153px;">
                            <span style="display: inline-block;vertical-align:middle;text-align:left" title="${name}">
                                ${name}
                            </span>
                            </div>
                            <div>
                            <span class="node-desc-count-map-root"
                                style="display: inline-block;vertical-align:middle;text-align:right"
                                title=${count}>
                                ${count}
                            </span>
                            </div>
                        </div>
                    </div>
                `;
        break;
      case 'other':
        template = `
                    <div style="height: 34px; width: 131px;border-color:#a9adb5"
                        onclick="node_click('${code}')"
                        class='jtk-window jtk-node outline-none other'
                        data-toggle="popover"
                        data-placement="bottom"
                        id='${id}'
                        data-node='${id}'
                        data-type='other'>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_right"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_right"
                            onclick="plus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_left"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_left"
                            onclick="plus_click('${code}')">
                        </i>
                        <div class="node-background-other"
                            style="display: flex;align-items: center;justify-content: space-between;padding: 0 5px;"
                            onmouseover="node_hover('${code}')">
                            <span class="node-desc-map"
                                style="display: inline-block;vertical-align:middle;text-align:left"
                                title="${name}">
                                ${name}
                            </span>
                            <span class="node-desc-count-map"
                                style="display: inline-block;vertical-align:middle;text-align:right"
                                title=${count}>
                                ${count}
                            </span>
                        </div>
                    </div>
                `;
        break;
      case 'hasstandard':
        template = `
                    <div style="height: 34px; width: 131px;border-color:#a9adb5"
                        onclick="node_click('${code}')"
                        class='jtk-window jtk-node outline-none hasstandard bk-mark-triangle-map bk-success-map'
                        data-toggle="popover"
                        data-placement="bottom"
                        id='${id}'
                        data-node='${id}'
                        data-type='hasstandard'>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_right"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_right"
                            onclick="plus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_left"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_left"
                            onclick="plus_click('${code}')">
                        </i>
                        <div class="node-background-other"
                            style="display: flex;align-items: center;justify-content: space-between;padding: 0 5px;"
                            onmouseover="node_hover('${code}')">
                            <span class="node-desc-map"
                                style="display: inline-block;vertical-align:middle;text-align:left"
                                title="${name}">
                                ${name}
                            </span>
                            <span class="node-desc-count-map"
                                style="display: inline-block;vertical-align:middle;text-align:right"
                                title=${count}>
                                ${count}
                            </span>
                        </div>
                    </div>
                `;
        break;
      case 'zerohasstandard':
        template = `
                    <div style="height: 34px; width: 131px;border-color:#a9adb5"
                        onclick="node_click('${code}')"
                        class='jtk-window jtk-node outline-none zerohasstandard bk-mark-triangle-map bk-success-map'
                        data-toggle="popover"
                        data-placement="bottom"
                        id='${id}'
                        data-node='${id}'
                        data-type='zerohasstandard'>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_right"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_right"
                            onclick="plus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_left"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_left"
                            onclick="plus_click('${code}')">
                        </i>
                        <div class="node-background-zero"
                            style="display: flex;align-items: center;justify-content: space-between;padding: 0 5px;"
                            onmouseover="node_hover('${code}')">
                            <span class="node-desc-map-zero"
                                style="display: inline-block;vertical-align:middle;text-align:left">
                                ${name}
                            </span>
                            <span class="node-desc-count-map-zero"
                                style="display: inline-block;vertical-align:middle;text-align:right"
                                title=${count}>
                                ${count}
                            </span>
                        </div>
                    </div>
                `;
        break;
      case 'zerostandard':
        template = `
                    <div style="height: 34px; width: 131px;border-color:#a9adb5"
                        onclick="node_click('${code}')"
                        class='jtk-window jtk-node outline-none standard bk-mark-square-map'
                        data-toggle="popover"
                        data-placement="bottom"
                        id='${id}'
                        data-node='${id}'
                        data-type='standard'>
                        <p class="stan-p">标准</p>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_right"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_right"
                            onclick="plus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_left"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_left"
                            onclick="plus_click('${code}')">
                        </i>
                        <div class="node-background-standard"
                            style="display: flex;
                                align-items: center;
                                justify-content: space-between;
                                padding: 0 14px 0 5px;"
                            onmouseover="node_hover(${code})"
                            onclick="dataMapEvent.linkToStandard(${data.standard_id})">
                            <span class="node-desc-map ${standardStyle}" title="${standardTips}">
                                ${name}
                            </span>
                            <span class="node-desc-count-map" title=${count}>
                                ${count}
                            </span>
                        </div>
                    </div>
                `;
        break;
      case 'standard':
        template = `
                    <div style="height: 34px; width: 131px;border-color:#a9adb5"
                        onclick="node_click('${code}')"
                        class='jtk-window jtk-node outline-none standard bk-mark-square-map'
                        data-toggle="popover"
                        data-placement="bottom"
                        id='${id}'
                        data-node='${id}'
                        data-type='standard'>
                        <p class="stan-p">标准</p>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_right"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_right"
                            onclick="plus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_left"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_left"
                            onclick="plus_click('${code}')">
                        </i>
                        <div class="node-background-standard"
                            style="display: flex;
                                align-items: center;
                                justify-content: space-between;
                                padding: 0 14px 0 5px;"
                            onmouseover="node_hover(${code})"
                            onclick="dataMapEvent.linkToStandard(${data.standard_id})">
                            <span class="node-desc-map ${standardStyle}" title="${standardTips}">
                                ${name}
                            </span>
                            <span class="node-desc-count-map" title=${count}>
                                ${count}
                            </span>
                        </div>
                    </div>
                `;
        break;
      case 'zero':
        template = `
                    <div style="height: 34px; width: 131px;border-color:#d6d6d6"
                        onclick="node_click('${code}')"
                        class='jtk-window jtk-node outline-none zero'
                        data-toggle="popover"
                        data-placement="bottom"
                        id='${id}'
                        data-node='${id}'
                        data-type='zero'>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_right"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_right"
                            onclick="plus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-minus-square-shape plus_extend_entity_left"
                            onclick="minus_click('${code}')">
                        </i>
                        <i class="bk-icon icon-plus-square-shape plus_extend_entity_left"
                            onclick="plus_click('${code}')">
                        </i>
                        <div class="node-background-zero"
                            style="display: flex;align-items: center;justify-content: space-between;padding: 0 5px;"
                            onmouseover="node_hover('${code}')">
                            <span class="node-desc-map-zero" title="${name}">
                                ${name}
                            </span>
                            <span class="node-desc-count-map-zero" title=${count}>
                                ${count}
                            </span>
                        </div>
                    </div>
                `;
        break;
    }
    return template;
  }
}

export default NodeTemplate;
