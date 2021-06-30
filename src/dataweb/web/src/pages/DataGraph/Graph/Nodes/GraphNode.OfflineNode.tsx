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
import { Component, Emit, Model, Prop, PropSync, ProvideReactive, Vue, Watch } from 'vue-property-decorator';
import { INodeParams } from '../../interface/INodeParams';
import GraphNodeBase from './GraphNode.Base';
import OutputCard from './NodesComponents/Output';
import InputCard from './OfflineNode/Input.vue';
import OfflineProcess from './OfflineNode/OfflineProcess';
import moment from 'moment';

@Component({
  components: {
    InputCard,
    OutputCard,
    OfflineProcess,
  },
})
export default class GraphNodeOfflineV2 extends GraphNodeBase {
  @ProvideReactive() public resultTableList = [];
  @ProvideReactive() public parentResultTables = this.parentResultTables;

  public colorSets: string[] = [
    '#58c5db',
    '#769afa',
    '#b777eb',
    '#eb77be',
    '#ea7079',
    '#eba077',
    '#ebcd77',
    '#abd171',
    '#71d19e',
    '#a5dbf5',
  ];

  public params: INodeParams = {
    config: {
      outputs: [],
      inputs: [],
      name: '',
      bk_biz_id: '',
      dedicated_config: {
        sql: '',
        self_dependence: {
          self_dependency: false, // 是否启用自依赖
          self_dependency_config: {
            fields: [],
            dependency_rule: 'self_finished',
          }, // 自依赖schema配置
        },
        schedule_config: {
          count_freq: '1', //调度周期
          schedule_period: 'hour', //调度时间
          start_time: moment()
            .add(1, 'hour')
            .set({ minute: 0, second: 0 })
            .format('YYYY-MM-DD HH:mm:ss'), //开始时间
        },
        recovery_config: {
          recovery_enable: false,
          recovery_times: '1',
          recovery_interval: '5m',
        },
        output_config: {
          enable_customize_output: false,
          output_baseline: '',
          output_baseline_location: 'start',
          output_offset: '0',
          output_offset_unit: 'hour',
          output_baseline_type: 'upstream_result_table',
        },
      },
      window_info: [],
    },
  };
  public inputCards: object[] = [];

  @Watch('params.config.inputs', { immediate: true, deep: true })
  public inputsChange(val: object[]) {
    if (val.length === 0) {
      return;
    }
    const resolveFun = (acc: any, data: any, idx: number) => data.from_result_table_ids
      .forEach((id: number | string, idIndex: number) => {
        const card = {
          show: idx === 0 && idIndex === 0,
          title: id,
        };
        acc.push(card);
        this.setWindowInfo(id);
      });
    this.inputCards = val.reduce((acc: object[], cur, idx) => {
      cur && resolveFun(acc, cur, idx);
      return acc;
    }, []);
  }

  @Watch('inputCards')
  public onInputCardsChange() {
    this.$set(
      this,
      'resultTableList',
      this.inputCards.map(item => {
        return {
          name: item.title,
          value: item.title,
        };
      })
    );
  }

  public setWindowInfo(id: string | number) {
    if (!this.params.config.window_info?.find(item => item.result_table_id === id)) {
      const parentNode = Object.values(this.parentConfig).find(item => item.result_table_id === id);
      const windowInfo = this.params.config.window_info;
      const config = {
        result_table_id: id, //上游rt id
        //窗口类型，滚动窗口/滑动窗口/累加窗口/全量数据
        window_type: parentNode && parentNode.node_type === 'unified_kv_source' ? 'whole' : 'scroll',
        window_offset: '0', //窗口偏移值，在滚动窗口/滑动窗口/累加窗口中使用
        window_offset_unit: 'hour', //窗口偏移值单位，在滚动窗口/滑动窗口/累加窗口中使用
        dependency_rule: 'all_finished', //调度中的依赖策略，所有窗口类型都会使用到
        window_size: '1', //窗口大小，在滑动窗口/累加窗口中使用
        window_size_unit: 'hour', //窗口大小单位，在滑动窗口/累加窗口中使用
        window_start_offset: '0', //窗口起始偏移，在累加窗口中使用
        window_start_offset_unit: 'hour', //窗口起始偏移单位，在累加窗口中使用
        window_end_offset: '1', //窗口结束偏移，在累加窗口中使用
        window_end_offset_unit: 'hour', //窗口结束偏移单位，在累加窗口中使用
        accumulate_start_time: new Date(
          moment().add(1, 'hour')
            .set({ minute: 0, second: 0 })
        ), //累加起始基准值，在累加窗口中使用
        color: '#58c5db', // 默认颜色
      };

      windowInfo?.push(config);
      this.$set(this.params.config, 'window_info', windowInfo);
    }
  }

  public initOutput() {
    this.$refs.process.initOutput();
  }

  public customValidateForm() {
    const start_time = moment(this.params.config.dedicated_config.schedule_config.start_time).format(
      'YYYY-MM-DD HH:mm:ss'
    );
    this.params.config.dedicated_config.schedule_config.start_time = start_time;

    this.params.config.window_info?.forEach(item => {
      if (item.window_type === 'scroll') {
        item.window_size = this.params.config.dedicated_config.schedule_config.count_freq;
        item.window_size_unit = this.params.config.dedicated_config.schedule_config.schedule_period;
      }
      item.accumulate_start_time = moment(item.accumulate_start_time).format('YYYY-MM-DD HH:mm:ss');
    });
    return true;
  }

  public customSetConfigBack() {
    return new Promise((resolve, reject) => {
      if (this.params.config.dedicated_config.sql === '') {
        this.params.config.dedicated_config.sql = this.getCode();
      }

      this.params.config.window_info?.forEach((item, index) => {
        item.color = this.colorSets[index];
      });
      resolve('true');
    });
  }

  /** 渲染body适配器 */
  public getBodyAdapter(h: CreateElement) {
    return (
      <OfflineProcess params={this.params.config} bizList={this.bizList}
        selfConfig={this.selfConfig} ref="process">
        <template slot="inputs">
          {this.inputCards.map((card, index) => {
            return (
              <InputCard
                ref="dataInput"
                title={card.title}
                show={card.show}
                {...{on: {'update:show': (val: boolean) => card.show = val} }}
                originParams={this.params.config}
                params={this.params.config.window_info[index]}
                {...{on: {'update:params': (val) => this.params.config.window_info[index] = val}}}
                parentNodeType={this.parentConfig[index].node_type}
                parentResultTables={this.parentResultTables}
                style="margin-bottom: 16px;"
              ></InputCard>
            );
          })}
        </template>
      </OfflineProcess>
    );
  }
}
