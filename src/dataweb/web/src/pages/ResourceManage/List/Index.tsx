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

import { bkTab } from 'bk-magic-vue';
import styled from '@tencent/vue-styled-components';

import { CreateElement } from 'vue';
import { Component, Vue } from 'vue-property-decorator';

const TabComponent = styled.div`
  height: 100%;
  .bk-tab-header {
    padding: 0 50px;
    background-image: none !important;
    box-shadow: 0px 2px 4px 0px rgba(0, 0, 0, 0.1);
  }
  .bk-tab-section {
    padding: 0;
  }
`.withComponent(bkTab);

@Component({})
export default class ResourceList extends Vue {
  get routeName() {
    return this.$route.name;
  }
  public tabs: any[] = [
    {
      name: 'resource-index',
      label: this.$t('我使用的资源'),
    },
    {
      name: 'resource-owns',
      label: this.$t('我管理的资源'),
    },
    {
      name: 'resource-avaliable',
      label: this.$t('可申请的资源'),
    },
  ];
  public activeView?: string = this.routeName;
  public created() {
    console.log(this.activeView);
  }
  public handleTabChange(name: string) {
    name && this.$router.replace({ name });
  }
  public render(h: CreateElement) {
    return (
      <div>
        <TabComponent active={this.activeView}
          {...{on: {'update:active': (val:string) => this.activeView = val}}}
          type="unborder-card"
          onTab-change={this.handleTabChange}>
          <template slot="setting">
            <bkdata-button class="mr10" theme="primary">
              {this.$t('创建平台托管资源')}
            </bkdata-button>
            <bkdata-button theme="default">{this.$t('接入业务托管资源')}</bkdata-button>
          </template>
          {this.tabs.map(tab => (
            <bkdata-tab-panel key={tab.name} name={tab.name} label={tab.label}></bkdata-tab-panel>
          ))}
        </TabComponent>
        <router-view key={+new Date()} />
      </div>
    );
  }
}
