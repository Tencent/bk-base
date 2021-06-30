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
import { Component, Emit, Prop, PropSync, Vue } from 'vue-property-decorator';
import GraphLayoutWrapper from '@/components/LayoutWrapper/index.vue';
import ProcessFrame from './ProcessFrame';
import OutputCard from './Output';
import styled from '@tencent/vue-styled-components';
import { Split, SplitArea } from 'vue-split-panel';
import { Config } from '../../../interface/INodeParams';
import { bkResizeLayout } from 'bk-magic-vue';

const NoInnerBorderResizeLayout = styled('div', {})`
  .bk-resize-layout-aside {
    border: none !important;
  }
  .bk-resize-collapse {
    display: none;
  }
  .bk-resize-trigger:hover {
    background-image: none !important;
  }
  &.bk-resize-layout-collapsed > .bk-resize-layout-aside > .bk-resize-collapse {
    display: inline-block;
  }
`.withComponent(bkResizeLayout);

const ExpandButtonBase = styled('div', {})`
  width: 23px;
  height: 100px;
  background: #63656e;
  font-size: 12px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  color: #f0f1f5;
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  cursor: pointer;
  span {
    text-align: center;
  }
`;

const RightExpandButton = ExpandButtonBase.extend`
  border-top-left-radius: 8px;
  border-bottom-left-radius: 8px;
  right: 0;
`;

const LeftExpandButton = ExpandButtonBase.extend`
  border-top-right-radius: 8px;
  border-bottom-right-radius: 8px;
`;

const PanelHeader = styled('div', {}, 'panel-header')`
  height: 50px;
  font-size: 14px;
  line-height: 50px;
  width: 60px;
  color: #313238;
`;

const PanelContent = styled('div', {
  padding: {
    default: '0 10px 0 12px',
    type: String,
  },
})`
  width: 100%;
  height: 100%;
  padding: ${props => props.padding};
`;

const ChartPanel = styled('div', {})`
  width: 20%;
  height: 100%;
  background: #252529;
  box-shadow: 3px 0px 4px 0px rgb(0 0 0 / 24%);
  z-index: 1;
  padding-top: 40px;
  padding-right: 15px;
`;

const ChartTitle = styled('h3', {})`
  font-size: 12px;
  font-family: PingFangSC, PingFangSC-Regular;
  font-weight: 400;
  text-align: right;
  color: #bfbfbf;
  line-height: 35px;
  margin-top: 16px;
  display: block;
  height: 35px;
  line-height: 35px;
`;

@Component({
  components: {
    Split,
    SplitArea,
    GraphLayoutWrapper,
    LeftExpandButton,
    RightExpandButton,
    PanelHeader,
    PanelContent,
    OutputCard,
    ChartPanel,
    ProcessFrame,
    NoInnerBorderResizeLayout,
  },
})
export default class SplitTriplePanel extends Vue {
  // getInputAdapter(h: CreateElement) {
  //     return <div>inputCard</div>
  // }
  @Prop({ default: window.$t('计算输入') }) inputHeader!: string;
  @Prop({ default: window.$t('计算处理') }) processHeader!: string;
  @Prop({ default: window.$t('计算输出') }) outputHeader!: string;
  @Prop({ default: () => [] }) bizList!: Array<object>;
  @Prop({ default: () => ({}) }) params!: Config;
  @Prop({ default: () => ({}) }) selfConfig!: object;

  public inputFold: boolean = false;
  public outputFold: boolean = false;
  public upperSectionMaxHeight: number = 238;
  public upperPanelHeight: string | number = 240;

  getMainProcessAdapter(h: CreateElement) {
    return <ProcessFrame params={this.params} selfConfig={this.selfConfig}></ProcessFrame>;
  }

  getOutputAdapter(h: CreateElement) {
    return this.params.outputs?.map(output => {
      return (
        <OutputCard
          title={`${output.bk_biz_id}_${output.table_name}`}
          bizList={this.bizList}
          params={output}
        ></OutputCard>
      );
    });
  }

  public initOutput() {
    this.params.outputs?.length === 0 && this.addOutput();
  }

  public addOutput() {
    const output = {
      bk_biz_id: this.params.bk_biz_id || this.bizList[0].biz_id,
      fields: [],
      output_name: '',
      table_name: '',
      table_custom_name: '',
      validate: {
        table_name: {
          // 输出编辑框 数据输出
          status: false,
          errorMsg: '',
        },
        output_name: {
          // 输出编辑框 中文名称
          status: false,
          errorMsg: '',
        },
        field_config: {
          status: false,
          errorMsg: this.$t('必填项不可为空'),
        },
      },
    };
    this.params.outputs?.push(output);
  }

  render(h: CreateElement) {
    return (
      <GraphLayoutWrapper ref="layoutWrapper">
        <bkdata-resize-layout
          border={false}
          placement="top"
          style="height: 100%"
          max={238}
          auto-minimize={60}
          initial-divide={this.upperPanelHeight}
          immediate={true}
          disabled={this.upperPanelHeight === '0'}
          ref="top-bottom-split"
        >
          <div class="chart" style="height: 100%; display: flex;" slot="aside">
            <ChartPanel>
              {this.params.window_info?.map(input => {
                return <ChartTitle class="result-table">{input.result_table_id}</ChartTitle>;
              })}
            </ChartPanel>
            <div class="chart-main" style="flex: 1; background: #252529;"></div>
          </div>
          <NoInnerBorderResizeLayout
            slot="main"
            style="height: 100%"
            border={false}
            collapsible={true}
            min={299}
            auto-minimize={310}
            immediate={true}
            initial-divide="480px"
          >
            <PanelContent slot="aside" class="bk-scroll-y">
              <PanelHeader>{this.inputHeader}</PanelHeader>
              {this.$slots.inputs}
            </PanelContent>
            <NoInnerBorderResizeLayout
              slot="main"
              style="height: 100%"
              collapsible={true}
              border={false}
              min={299}
              auto-minimize={310}
              immediate={true}
              placement="right"
              initial-divide="480px"
            >
              <PanelContent slot="main">
                <PanelHeader>{this.processHeader}</PanelHeader>
                {this.$slots.content ? this.$slots.content : this.getMainProcessAdapter(h)}
              </PanelContent>
              <PanelContent padding="0 12px 0 12px" slot="aside">
                <PanelHeader>{this.outputHeader}</PanelHeader>
                {this.$slots.outputs ? this.$slots.outputs : this.getOutputAdapter(h)}
              </PanelContent>
            </NoInnerBorderResizeLayout>
          </NoInnerBorderResizeLayout>
        </bkdata-resize-layout>
      </GraphLayoutWrapper>
    );
  }
}
