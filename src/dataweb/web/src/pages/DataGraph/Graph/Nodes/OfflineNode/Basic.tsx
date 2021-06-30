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
import { Component, Emit, InjectReactive, Model, Prop, PropSync, Vue, Watch } from 'vue-property-decorator';
import { ProcessFormItem, MiniDatePicker, FlexForm } from '../NodesComponents/BaseStyleComponent';
import BaseForm from '../NodesComponents/WindowForm.Base';
import moment from 'moment';
import { DedicatedConfig } from '../../../interface/INodeParams';

@Component({
  comments: {
    MiniDatePicker,
    BaseForm,
    FlexForm,
  },
})
export default class OfflineBasicPanel extends Vue {
  @InjectReactive() resultTableList!: Array<object>;
  @PropSync('params', { default: () => ({}) }) syncParams!: DedicatedConfig;

  get periodList() {
    return [
      {
        value: 'hour',
        name: this.$t('小时'),
      },
      {
        value: 'day',
        name: this.$t('天'),
      },
      {
        value: 'week',
        name: this.$t('周'),
      },
      {
        value: 'month',
        name: this.$t('月'),
      },
    ];
  }

  get baselineList() {
    return [
      {
        value: 'start',
        name: this.$t('起始时间'),
      },
      {
        value: 'end',
        name: this.$t('结束时间'),
      },
    ];
  }

  get offsetText() {
    if (this.syncParams.output_config.output_baseline_type === 'schedule_time') return `${this.$t('往左')}(-)`;
    return this.syncParams.output_config.output_baseline_location === 'start'
      ? `${this.$t('往右')}(+)`
      : `${this.$t('往左')}(-)`;
  }

  /**
   * setStartTime
   * @param val
   */
  setStartTime(val) {
    switch (val) {
      case 'month':
        this.syncParams.schedule_config.start_time = moment()
          .startOf('month')
          .add(1, 'month')
          .format('YYYY-MM-DD HH:mm:ss');
        break;
      case 'week':
        this.syncParams.schedule_config.start_time = moment()
          .startOf('week')
          .add(1, 'day')
          .add(1, 'week')
          .format('YYYY-MM-DD HH:mm:ss');
        break;
      case 'day':
        this.syncParams.schedule_config.start_time = moment()
          .startOf('day')
          .add(1, 'day')
          .format('YYYY-MM-DD HH:mm:ss');
        break;
      default:
        this.syncParams.schedule_config.start_time = moment()
          .add(1, 'hour')
          .set({ minute: 0, second: 0 })
          .format('YYYY-MM-DD HH:mm:ss');
        break;
    }
  }

  /**
   * formatStartTime
   * @param val
   */
  formatStartTime(val: Date) {
    this.syncParams.schedule_config.start_time = moment(val).format('YYYY-MM-DD HH:mm:ss');
  }

  /**
   * setBaseLine
   * @param val
   */
  setBaseLine(val: string) {
    this.syncParams.output_config.output_offset = val === 'schedule_time' ? '1' : '0';
    this.syncParams.output_config.output_baseline_type = val;
  }

  /**
   * render
   * @param h
   * @returns
   */
  render(h: CreateElement) {
    return (
      <div class="base-config-wrapper">
        <FlexForm form-type="vertical" extCls="mb30">
          <ProcessFormItem label={this.$t('调度周期')} required property="name" extCls="mr40">
            <BaseForm
              withLabel={false}
              layout="double"
              mainFormType="input"
              mainParam={this.syncParams.schedule_config.count_freq}
              {...{on:{'update:mainParam':
                                val => this.syncParams.schedule_config.count_freq = val}}}
              minorParam={this.syncParams.schedule_config.schedule_period}
              {...{on:{'update:minorParam':
                                val => this.syncParams.schedule_config.schedule_period = val}}}
              minorSelectList={this.periodList}
              on-minor-selected={this.setStartTime.bind(this)}
            ></BaseForm>
          </ProcessFormItem>
          <ProcessFormItem label={this.$t('启动时间')} required property="name">
            <BaseForm
              withLabel={false}
              layout="single"
              mainFormType="date"
              mainInputProps={{
                'time-picker-options': {
                  steps: [1, 60, 60],
                },
              }}
              mainParam={this.syncParams.schedule_config.start_time}
              {...{on: {'update:mainParam': val => this.syncParams.schedule_config.start_time = val }}}
            ></BaseForm>
          </ProcessFormItem>
        </FlexForm>
        <bkdata-checkbox
          v-model={this.syncParams.output_config.enable_customize_output}
          extCls="mb10"
          style="display: block"
        >
          {this.$t('自定义结果数据时间')}
        </bkdata-checkbox>
        {this.syncParams.output_config.enable_customize_output ? (
          <div class="bk-button-group mb10">
            <bkdata-button
              class={
                this.syncParams.output_config.output_baseline_type === 'upstream_result_table'
                  ? 'is-selected' : ''
              }
              size="small"
              style="width: 180px"
              onClick={() => {
                this.setBaseLine('upstream_result_table');
              }}
            >
              {this.$t('基于上游结果数据表')}
            </bkdata-button>
            <bkdata-button
              class={
                this.syncParams.output_config.output_baseline_type === 'schedule_time'
                  ? 'is-selected' : ''}
              size="small"
              style="width: 180px"
              onClick={() => {
                this.setBaseLine('schedule_time');
              }}
            >
              {this.$t('基于调度时间')}
            </bkdata-button>
          </div>
        ) : (
          ''
        )}
        <FlexForm form-type="vertical"
          extCls="mb30" v-show={this.syncParams.output_config.enable_customize_output}>
          <ProcessFormItem
            v-show={this.syncParams.output_config.output_baseline_type === 'upstream_result_table'}
            label={this.$t('基准时间')}
            required
            property="name"
            extCls="mr40"
          >
            <BaseForm
              withLabel={false}
              layout="double"
              mainFormType="select"
              mainParam={this.syncParams.output_config.output_baseline}
              {...{on: {'update:mainParam': val => this.syncParams.output_config.output_baseline= val}}}
              minorParam={this.syncParams.output_config.output_baseline_location}
              {...{on: {'update:minorParam':
                                val => this.syncParams.output_config.output_baseline_location = val}}}
              mainSelectList={this.resultTableList}
              minorSelectList={this.baselineList}
              minorSelectWidth="100px"
            ></BaseForm>
          </ProcessFormItem>
          <ProcessFormItem label={this.$t('偏移')} required property="name">
            <BaseForm
              withLabel={false}
              layout="tri"
              offsetTip={this.offsetText}
              mainParam={this.syncParams.output_config.output_offset}
              {...{on: {'update:mainParam': val => this.syncParams.output_config.output_offset = val}}}
              minorParam={this.syncParams.output_config.output_offset_unit}
              {...{on: {'update:minorParam':
                                val => this.syncParams.output_config.output_offset_unit = val}}}
              minorSelectList={this.periodList}
            ></BaseForm>
          </ProcessFormItem>
        </FlexForm>
      </div>
    );
  }
}
