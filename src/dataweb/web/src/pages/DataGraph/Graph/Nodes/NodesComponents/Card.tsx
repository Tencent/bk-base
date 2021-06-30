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
 * input卡片容器
 */

import { CreateElement } from 'vue';
import { Component, Emit, Prop, PropSync, Vue } from 'vue-property-decorator';
import styled from '@tencent/vue-styled-components';
import CollapseTransition from '@/common/js/collapse-transition.js';
import RoundTabs from '@/components/roundTabs/index.vue';

const headerProps = {
  color: {
    default: 'red',
    type: String,
  },
};

const cardWrapper = styled('div', {}, { clsName: 'node-card-wrapper' })`
  background: #fcfdff;
  border-radius: 2px;
  box-shadow: 0px 1px 2px 0px rgba(0, 0, 0, 0.1);
  overflow: hidden;
`;

const cardHeader = styled('div', headerProps)`
    height: 54px;
    padding-right: 14px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    .header-left {
        display: flex;
        align-items: center;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    span {
        display: inline-block;
    }
    .mark-color {
        background-color: ${props => props.color}
        height: 14px;
        width: 6px;
        margin-right: 14px;
    }
    .title {
        font-size: 16px;
        font-weight: 600;
        text-align: left;
        color: #63656e;
        line-height: 22px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        vertical-align: bottom;
    }
    i {
        cursor: pointer;
        font-size: 16px;
        color: #c4c6cc;
    }
    `;

@Component({
  components: {
    CollapseTransition,
    RoundTabs,
    CardWrapper: cardWrapper,
    CardHeader: cardHeader,
  },
})
export default class InputCard extends Vue {
  @PropSync('show', { default: false }) syncShow!: boolean;
  @Prop({ default: 'red' }) color: string;
  @Prop({ default: '' }) title: string;
  @Prop(Array) panels!: Array<object>;
  @PropSync('currentTab', { default: 'config' }) syncSelectedTab!: string;

  get iconName() {
    return this.syncShow ? 'icon-shouqi' : 'icon-zhankai';
  }

  public clickHandler() {
    this.syncShow = !this.syncShow;
  }

  render(h: CreateElement) {
    return (
      <cardWrapper>
        <cardHeader color={this.color} onClick={this.clickHandler.bind(this)}>
          <div class="header-left">
            <span class="mark-color"></span>
            <span class="title">{this.title}</span>
          </div>
          <div class="header-right">
            <i class="icon-dictionary mr20"></i>
            <i class={this.iconName}></i>
          </div>
        </cardHeader>
        <CollapseTransition>
          <div class="content" v-show={this.show}>
            <RoundTabs panels={this.panels}
              selected={this.syncSelectedTab}
              {...{on: { 'update:selected': (val: string) => this.syncSelectedTab = val }}}></RoundTabs>
            {this.$slots['default']}
          </div>
        </CollapseTransition>
      </cardWrapper>
    );
  }
}
