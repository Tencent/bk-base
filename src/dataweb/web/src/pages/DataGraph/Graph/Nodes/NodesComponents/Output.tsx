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
import Card from './Card';
import OutputBaseForm from '@/pages/DataGraph/Graph/Children/components/OutputBaseForm.vue';
import styled from '@tencent/vue-styled-components';

const CardContentWrapper = styled('div', {}, { clsName: 'output-card-wrapper' })`
  padding: 20px;
  border-top: 1px solid #dcdee5;
`;

@Component({
  components: {
    Card,
    OutputBaseForm,
    CardContentWrapper,
  },
})
export default class OutputCard extends Vue {
  @Prop({ default: false }) title!: string;
  @Prop({ default: false }) hasNodeId!: boolean;
  @Prop({ default: false }) isShowDataFix: boolean;
  @Prop({ default: () => [] }) bizList: Array<object>;
  @Prop({ default: () => ({}) }) selfConfig: object;
  @Prop({ default: () => ({}) }) params: object;
  @Prop({ default: () => ({}) }) fullParams: object;

  show: boolean = true;

  render(h: CreateElement) {
    return (
      <Card show={this.show}
        {...{on: {'update:show': (val:boolean) => this.show = val}}}
        title={this.title}
        panels={[]}
        currentTab={''}
        color="white">
        <CardContentWrapper>
          <bkdata-form form-type="vertical">
            <OutputBaseForm
              bizList={this.bizList}
              output={this.params}
              isMultiple={false}
              hasNodeId={this.hasNodeId}
              self-config={this.selfConfig}
              params={this.fullParams}
              ref="baseForm"
            ></OutputBaseForm>
          </bkdata-form>
        </CardContentWrapper>
      </Card>
    );
  }
}
