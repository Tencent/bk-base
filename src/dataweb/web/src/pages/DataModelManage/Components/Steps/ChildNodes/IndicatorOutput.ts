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

import { translateUnit } from '@/common/js/util';
import moment from 'moment';
import { Component, Prop, Vue, Watch } from 'vue-property-decorator';
import { ICalculationAtomsResults, ICreateIndexParams } from '../../../Interface/indexDesign';

/*** 添加指标数据输出组件 */
@Component({
  components: {},
})
export default class IndicatorOutput extends Vue {
  @Prop({ default: () => ({}) }) public params: ICreateIndexParams;
  @Prop({ default: () => [] }) public indicatorTableFields!: any[];
  @Prop({ default: 'stream' }) public nodeType: string;
  @Prop({ default: false }) public isSaveNode: boolean; // true为编辑，false为新建
  @Prop({ default: false }) public isRestrictedEdit: boolean;

  public indicatorParams = {
    indicator_name: '',
    indicator_alias: '',
    description: '',
  };

  public rules = {
    indicator_name: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
      {
        max: 50,
        message: '不能多于50个字符',
        trigger: 'blur',
      },
      {
        regex: /^[a-zA-Z][a-zA-Z0-9_]*$/,
        message: '只能是英文字母、下划线和数字组成，且字母开头',
        trigger: 'blur',
      },
    ],
    indicator_alias: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
      {
        max: 50,
        message: '不能多于50个字符',
        trigger: 'blur',
      },
    ],
  };

  public windowTypeLength = '';

  public iconDistance = 50;

  get indicatorName() {
    return `${this.params.calculation_atom_name}_${this.indicatorParams.indicator_name}${this.windowTypeLength
      ? `_${this.windowTypeLength}` : ''
            }`;
  }

  get indicatorAlias() {
    return `${this.preAliasName}_${this.indicatorParams.indicator_alias}`;
  }

  get preAliasName() {
    if (this.windowTypeLength) {
      const formatUnit = {
        min: 'm',
        m: 'M',
      };
      const typeMap = {
        s: this.$t('秒'),
        min: this.$t('分钟'),
        h: this.$t('小时'),
        d: this.$t('天'),
        w: this.$t('周'),
        m: this.$t('月'),
      };
      const type = this.params?.scheduling_content?.window_type;
      const num = parseInt(this.windowTypeLength);
      const unit = this.windowTypeLength.replace(/\d+/, '');
      if (this.nodeType === 'stream' && type === 'scroll') {
        const time = num <= 1 ? '' : num;
        return this.$t('每') + time + typeMap[unit];
      } else if (this.nodeType === 'stream' && type === 'slide') {
        return this.$t('最近') + num + typeMap[unit];
      } else if (this.nodeType === 'batch' && type === 'fixed') {
        const unitMap = {
          hour: 'h',
          day: 'd',
          week: 'w',
          month: 'm',
        };
        const countFreqUnit = unitMap[this.params?.scheduling_content?.schedule_period];
        const countFreqTime = moment
          .duration(
            Number(this.params?.scheduling_content?.count_freq) || 0,
            formatUnit[countFreqUnit] || countFreqUnit
          )
          .valueOf();
        const windowTime = moment.duration(num, formatUnit[unit] || unit).valueOf();
        const pre = countFreqTime >= windowTime ? this.$t('每') : this.$t('最近');
        const time = num <= 1 && countFreqTime >= windowTime ? '' : num;
        return pre + time + typeMap[unit];
      } else {
        return '';
      }
    }
    return '';
  }

  @Watch('params', { immediate: true, deep: true })
  public handleParamsChanged() {
    if (this.params?.scheduling_content?.window_type && !this.isRestrictedEdit) {
      this.getWindowTypeLength(this.params?.scheduling_content?.window_type);
    }
    // 编辑回填,编辑状态不允许再修改windowTypeLength
    if (this.isSaveNode && !this.indicatorParams.indicator_name) {
      this.windowTypeLength = '';
      const { indicator_name, indicator_alias, description } = this.params;
      // 累加窗口默认windowTypeLength为''
      if (!['accumulate_by_hour', 'accumulate'].includes(this.params.scheduling_content?.window_type)) {
        const index = indicator_name.lastIndexOf('_');
        const patten = /^\d+(s|min|h|d|w|m)$/;
        if (patten.test(indicator_name.slice(index + 1))) {
          this.windowTypeLength = indicator_name.slice(index + 1);
        }
      } else {
        this.windowTypeLength = '';
      }
      this.indicatorParams = {
        indicator_name: indicator_name
          .replace(`${this.params.calculation_atom_name}_`, '')
          .replace(this.windowTypeLength ? `_${this.windowTypeLength}` : '', ''),
        indicator_alias: this.preAliasName
          ? indicator_alias.replace(`${this.preAliasName}_`, '') : indicator_alias,
        description,
      };
    }
  }

  @Watch('windowTypeLength', { immediate: true })
  public handleWindowTypeLengthChanged() {
    this.$nextTick(() => {
      this.iconDistance = 8 + this.$refs.windowTypeEl.offsetWidth;
    });
  }

  public getWindowTypeLength(type: string) {
    // windowTypeLength取值，单位：s、min、h、d、w、m
    // 窗口类型：
    // 滚动(scroll) => 等于统计频率
    // 滑动(slide) => 窗口长度
    // 累加(accumulate) => ''
    // 固定窗口(fixed) => 窗口长度
    if (this.nodeType === 'stream') {
      if (type === 'scroll') {
        this.windowTypeLength = translateUnit(this.params.scheduling_content.count_freq);
      } else if (type === 'slide') {
        this.windowTypeLength = translateUnit(this.params.scheduling_content.window_time * 60);
      } else {
        this.windowTypeLength = '';
      }
    } else if (this.nodeType === 'batch') {
      const unitMap = {
        hour: 'h',
        day: 'd',
        week: 'w',
        month: 'm',
      };
      if (type === 'fixed') {
        this.windowTypeLength = this.params.scheduling_content.unified_config.window_size
                    + unitMap[this.params.scheduling_content.unified_config.window_size_period];
      } else {
        this.windowTypeLength = '';
      }
    }
  }

  public validateFormData() {
    return this.$refs.baseForm.validate().then(
      () => {
        this.$emit('changeIndicatorParams', {
          indicator_name: this.indicatorName,
          indicator_alias: this.preAliasName ? this.indicatorAlias : this.indicatorParams.indicator_alias,
          description: this.indicatorParams.description,
        });
        return true;
      },
      () => {
        return false;
      }
    );
  }
}
