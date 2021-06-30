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
 * 高级配置点击下拉/收起 面板组件
 */

import { CreateElement } from 'vue';
import { Component, Emit, Model, Prop, PropSync, Vue, Watch } from 'vue-property-decorator';
import CollapseTransition from '@/common/js/collapse-transition.js';
import styled from '@tencent/vue-styled-components';

const panelTextArea = styled('div', {}, { clsName: 'panel-text-area' })`
  width: 60px;
  height: 20px;
  font-size: 12px;
  margin-top: 16px;
  margin-bottom: 14px;
  text-align: left;
  color: #3a84ff;
  line-height: 16px;
  cursor: pointer;
`;

@Component({
  components: {
    CollapseTransition,
    panelTextArea: panelTextArea,
  },
})
export default class AdvancedPanel extends Vue {
  public panelShow: boolean = false;

  public clickHandler() {
    this.panelShow = !this.panelShow;
  }
  render(h: CreateElement) {
    return (
      <div class="advanced-panel">
        <panelTextArea onClick={this.clickHandler.bind(this)}>
          <span>{this.$t('高级配置')}</span>
          {this.panelShow ? <i class="icon-angle-double-down"></i> : <i class="icon-angle-double-up"></i>}
        </panelTextArea>
        <CollapseTransition>
          <div className="panel" v-show={this.panelShow}>
            {this.$slots['default']}
          </div>
        </CollapseTransition>
      </div>
    );
  }
}
