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

import { CreateElement } from 'vue';
import { Component, Emit, Model, Prop, PropSync, Vue, Watch } from 'vue-property-decorator';
import Monaco from '@/components/monaco';
import styled from '@tencent/vue-styled-components';
import { bkCollapseItem, bkFormItem, bkBadge } from 'bk-magic-vue';
import ReferTasks from '@/pages/DataGraph/Graph/Children/components/ReferTask.vue';
import { ProcessFormItem, NoPaddingTabPanel } from './BaseStyleComponent';
import windowFormItem from '../NodesComponents/WindowForm.Base.tsx';

const BadgeTip = styled('div', {})`
  .bk-badge {
    right: -5px !important;
    color: #979ba5 !important;
  }
`.withComponent(bkBadge);

const ProcessCollapseItem = styled('div', {})`
  height: 100%;
  .bk-collapse-item-header {
    height: 54px;
    line-height: 54px;
    background-color: #fcfdff;
    font-size: 16px;
    font-weight: 600;
    i {
      font-size: 16px;
      color: #c4c6cc;
      float: right;
      line-height: 54px;
    }
  }
  .bk-collapse-item-content {
    border-top: 1px solid #dcdee5;
    padding: 25px 20px;
    background-color: #fff;
    min-height: calc(100% - 54px);
  }
`.withComponent(bkCollapseItem);

@Component({
  components: {
    ProcessCollapseItem,
    NoPaddingTabPanel,
    Monaco,
    ReferTasks,
    windowFormItem,
  },
})
export default class ProcessFrame extends Vue {
  @PropSync('params', { default: () => ({}) }) syncParams!: object;
  @Prop({ default: () => ({}) }) selfConfig!: object;
  @Prop({ default: true }) hasEditor!: boolean;
  @Prop({ default: () => [] }) tabConfig!: array<object>;

  activeCollapse: string = window.$t('计算配置');
  activeName: string = '';
  referTaskPanel: object = {
    name: 'referTask',
    label: window.$t('关联任务'),
    count: 0,
  };

  tabHeaderRender(h: CreateElement) {
    return (
      <BadgeTip val={this.referTaskPanel.count} theme="#f0f1f5" position="right">
        <span class="panel-name">{this.referTaskPanel.label}</span>
      </BadgeTip>
    );
  }

  get currentRtid() {
    return (this.selfConfig.result_table_ids && this.selfConfig.result_table_ids[0]) || '';
  }

  public secondHalfAdapter(h: CreateElement) {
    return (
      <NoPaddingTabPanel active={this.activeName}
        {...{on: {'update:active': (val:string) => this.activeName = val}}}
        type="unborder-card">
        {this.tabConfig.map((panel, index) => {
          return (
            <bkdata-tab-panel
              {...{
                props: panel,
              }}
              disabled={panel.disabled}
            >
              <panel.component
                params={this.syncParams.dedicated_config}
                {...{on: {'update:params': val => {this.syncParams.dedicated_config = val;}}}}
                {...{ props: panel.props }}
              ></panel.component>
            </bkdata-tab-panel>
          );
        })}
        <bkdata-tab-panel
          {...{
            props: this.referTaskPanel,
          }}
          render-label={this.tabHeaderRender}
        >
          <ReferTasks rtid={this.currentRtid}
            nodeId={this.selfConfig.node_id}
            v-model={this.referTaskPanel.count} />
        </bkdata-tab-panel>
      </NoPaddingTabPanel>
    );
  }

  public firstHalfAdapter(h: CreateElement) {
    return (
      <div class="upper-form">
        <bkdata-form form-type="vertical">
          <ProcessFormItem label={this.$t('节点名称')} required property="name" extCls="mr40">
            <bkdata-input v-model={this.syncParams.name} size="small"></bkdata-input>
          </ProcessFormItem>
        </bkdata-form>
        <div class="sql-wrapper mt20">
          <Monaco
            v-show={this.hasEditor}
            code={this.syncParams.dedicated_config.sql}
            height="278px"
            options={{ fontSize: '14px' }}
            tools={{
              guidUrl: this.$store.getters['docs/getPaths'].realtimeSqlRule,
              toolList: {
                font_size: true,
                full_screen: true,
                event_fullscreen_default: true,
                editor_fold: true
              },
              title: 'SQL',
            }}
            on-codeChange={this.changeCode.bind(this)}
          ></Monaco>
        </div>
      </div>
    );
  }

  public changeCode(sql: string) {
    this.syncParams.dedicated_config.sql = sql;
  }

  render(h: CreateElement) {
    return (
      <div class="data-process-wrapper bk-scroll-y" style="height: calc(100% - 50px); background: #f0f1f5;">
        <bkdata-collapse v-model={this.activeCollapse} style="height: 100%">
          <ProcessCollapseItem name={window.$t('计算配置')} hide-arrow={true}>
            {this.$t('计算配置')}
            {this.activeCollapse[0] ? <i class="icon-shouqi"></i> : <i class="icon-zhankai"></i>}
            <div slot="content">
              <div class="upper-form-wrapper">{this.firstHalfAdapter(h)}</div>
              <div class="second-half">{this.secondHalfAdapter(h)}</div>
            </div>
          </ProcessCollapseItem>
        </bkdata-collapse>
      </div>
    );
  }
}
