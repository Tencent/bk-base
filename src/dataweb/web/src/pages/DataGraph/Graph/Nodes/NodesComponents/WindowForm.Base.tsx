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
 * 输入卡片的表单元件 Item
 */

import { CreateElement } from 'vue';
import { Component, Emit, Model, Prop, PropSync, Vue, Watch } from 'vue-property-decorator';
import styled from '@tencent/vue-styled-components';
import {
  windowComposeFormItem,
  windowFormItem,
  offsetTip,
  fullWidthSelect,
  mainInput,
  MiniDatePicker,
} from './BaseStyleComponent';
import moment from 'moment';
@Component({
  components: {
    windowComposeFormItem: windowComposeFormItem,
    mainInput: mainInput,
    windowFormItem: windowFormItem,
    fullWidthSelect: fullWidthSelect,
    offsetTip: offsetTip,
  },
})
export default class WindowFormBase extends Vue {
  @Prop({ default: '' }) label!: string;
  @Prop({ default: '' }) labelDesc: string;
  @Prop({ default: '' }) offsetTip: string;
  @Prop({ default: '72px' }) minorSelectWidth: string;
  @Prop({ default: 'select' }) mainFormType: string;
  @Prop({ default: 'single' }) layout!: string;
  @Prop({ default: () => [] }) mainSelectList: Array<object>;
  @Prop({ default: () => [] }) minorSelectList: Array<object>;
  @Prop({ default: true }) withLabel: boolean;
  @Prop({ default: () => {} }) mainInputProps: object;
  @PropSync('mainParam', { default: '' }) syncMainParam!: string;
  @PropSync('minorParam', { default: '' }) syncMinorParam!: string;

  formatTime(value) {
    this.syncMainParam = moment(value).format('YYYY-MM-DD HH:mm:ss');
  }

  minorSelectedHandle(val: number | string | Array<string>) {
    this.$emit('minor-selected', val);
  }

  mainSelectedHandle(val: number | string | Array<string>) {
    this.$emit('main-selected', val);
  }

  mainInputHandler(val: number | string) {
    this.$emit('main-input', val);
  }

  datePickSuccess() {
    this.$emit('date-pick');
  }

  getInputAdapter(h: CreateElement) {
    let inputForm;
    switch (this.layout) {
      case 'single':
        if (this.mainFormType === 'select') {
          inputForm = (
            <bkdata-select
              v-model={this.syncMainParam}
              size="small"
              style="width: 100%"
              clearable={false}
              on-selected={this.mainSelectedHandle.bind(this)}
            >
              {this.mainSelectList.map(item => {
                return <bkdata-option id={item.value} name={item.name}></bkdata-option>;
              })}
            </bkdata-select>
          );
        } else if (this.mainFormType === 'input') {
          inputForm = <bkdata-input v-model={this.syncMainParam} size="small"></bkdata-input>;
        } else if (this.mainFormType === 'date') {
          inputForm = (
            <bkdata-date-picker
              v-model={this.syncMainParam}
              {...{
                props: this.mainInputProps,
              }}
              type="datetime"
              transfer={true}
              style="width: 100%;"
              font-size="normal"
              on-change={this.formatTime.bind(this)}
              on-pick-success={this.datePickSuccess.bind(this)}
            ></bkdata-date-picker>
          );
        }
        break;
      case 'double':
        if (this.mainFormType === 'select') {
          inputForm = (
            <window-compose-form-item>
              <bkdata-select
                extCls="bk-select-main"
                v-model={this.syncMainParam}
                size="small"
                style="width: 100%"
                clearable={false}
              >
                {this.mainSelectList.map(item => {
                  return <bkdata-option id={item.value} name={item.name}></bkdata-option>;
                })}
              </bkdata-select>
              <bkdata-select
                v-model={this.syncMinorParam}
                size="small"
                clearable={false}
                on-selected={this.minorSelectedHandle.bind(this)}
                style={{ width: this.minorSelectWidth }}
              >
                {this.minorSelectList.map(item => {
                  return <bkdata-option id={item.value} name={item.name}></bkdata-option>;
                })}
              </bkdata-select>
            </window-compose-form-item>
          );
        } else {
          inputForm = (
            <window-compose-form-item>
              <bkdata-input
                v-model={this.syncMainParam}
                size="small"
                on-input={this.mainInputHandler.bind(this)}
              ></bkdata-input>
              <bkdata-select
                v-model={this.syncMinorParam}
                size="small"
                clearable={false}
                on-selected={this.minorSelectedHandle.bind(this)}
              >
                {this.minorSelectList.map(item => {
                  return <bkdata-option id={item.value} name={item.name}></bkdata-option>;
                })}
              </bkdata-select>
            </window-compose-form-item>
          );
        }
        break;
      default:
        inputForm = (
          <window-compose-form-item inputWidth="calc(100% - 136px)">
            <offsetTip>{this.offsetTip}</offsetTip>
            <bkdata-input
              v-model={this.syncMainParam}
              size="small"
              on-input={this.mainInputHandler.bind(this)}
            ></bkdata-input>
            <bkdata-select
              v-model={this.syncMinorParam}
              size="small"
              clearable={false}
              on-selected={this.minorSelectedHandle.bind(this)}
            >
              {this.minorSelectList.map(item => {
                return <bkdata-option id={item.value} name={item.name}></bkdata-option>;
              })}
            </bkdata-select>
          </window-compose-form-item>
        );
        break;
    }
    return inputForm;
  }

  render(h: CreateElement) {
    return this.withLabel ? (
      <windowFormItem label={this.label} desc={this.labelDesc}>
        {this.getInputAdapter(h)}
      </windowFormItem>
    ) : (
      this.getInputAdapter(h)
    );
  }
}
