

<!--
  - Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
  - Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
  - BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
  -
  - License for BK-BASE 蓝鲸基础平台:
  - -------------------------------------------------------------------
  -
  - Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  - documentation files (the "Software"), to deal in the Software without restriction, including without limitation
  - the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
  - and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  - The above copyright notice and this permission notice shall be included in all copies or substantial
  - portions of the Software.
  -
  - THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
  - LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
  - NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  - WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  - SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
  -->

<template>
  <div class="window-setting-wrapper">
    <bkdata-form ref="form"
      extCls="bk-common-form"
      :labelWidth="125"
      :model="params">
      <bkdata-form-item
        extCls="form-content-auto-width"
        property="window_type"
        :label="$t('窗口类型')"
        :required="true"
        :rules="validate.required">
        <bkdata-radio-group v-model="paramsValue.window_type">
          <template v-for="(item, index) in windowTypes">
            <bkdata-radio
              v-if="$modules.isActive('session_window')"
              :key="index"
              :value="item.value"
              :disabled="isSetDisable">
              {{ item.name }}
            </bkdata-radio>
          </template>
        </bkdata-radio-group>
      </bkdata-form-item>
      <bkdata-form-item v-if="params.window_type !== 'none'"
        extCls="form-content-auto-width"
        :label="$t('窗口示例')">
        <ScrollWindowBar :type="windowType" />
      </bkdata-form-item>
      <bkdata-form-item
        v-if="params.window_type !== 'none' && params.window_type !== 'session'"
        property="count_freq"
        :label="$t('统计频率')"
        :required="true"
        :rules="validate.required">
        <bkdata-selector
          settingKey="id"
          displayKey="name"
          :disabled="isSetDisable && !['accumulate', 'slide'].includes(params.window_type)"
          :list="countFreqList"
          :selected.sync="params.count_freq" />
      </bkdata-form-item>
      <bkdata-form-item
        v-if="params.window_type === 'slide' || params.window_type === 'accumulate'"
        property="window_size"
        :label="$t('窗口长度')"
        :required="true"
        :rules="validate.required">
        <bkdata-selector
          settingKey="id"
          displayKey="name"
          :disabled="isSetDisable && params.window_type !== 'accumulate'"
          :list="windowInterval"
          :selected.sync="params.window_time" />
      </bkdata-form-item>
      <bkdata-form-item
        v-if="params.window_type === 'session'"
        property="session_gap"
        :label="$t('间隔时间')"
        :required="true"
        :rules="validate.required">
        <bkdata-selector
          settingKey="id"
          displayKey="name"
          :list="waitTime"
          :placeholder="$t('请选择')"
          :selected.sync="params.session_gap" />
      </bkdata-form-item>
      <bkdata-form-item
        v-if="params.window_type !== 'none'"
        property="waiting_time"
        :label="$t('等待时间')"
        :required="true"
        :rules="validate.required">
        <bkdata-selector
          settingKey="id"
          displayKey="name"
          :list="waitTime"
          :placeholder="$t('请选择')"
          :selected.sync="params.waiting_time" />
      </bkdata-form-item>
      <bkdata-form-item
        v-if="params.window_type === 'session'"
        property="expired_time"
        :label="$t('过期时间')"
        :required="true"
        :rules="validate.required">
        <bkdata-selector
          settingKey="id"
          displayKey="name"
          :list="expiredTimeList"
          :placeholder="$t('请选择')"
          :selected.sync="params.expired_time" />
      </bkdata-form-item>
    </bkdata-form>
    <realtime-dealy
      v-if="params.window_type !== 'none'"
      v-model="paramsValue.window_lateness"
      :windowType="params.window_type" />
  </div>
</template>

<script>
import RealtimeDealy from '../Realtime.delay';
import ScrollWindowBar from '../../../Components/ScrollWindowBar';

const windowTypeList = [
  {
    value: 'none',
    name: $t('无窗口'),
  },
  {
    value: 'scroll',
    name: $t('滚动窗口'),
  },
  {
    value: 'slide',
    name: $t('滑动窗口'),
  },
  {
    value: 'accumulate',
    name: $t('累加窗口'),
  },
  {
    value: 'session',
    name: $t('会话窗口'),
  },
];
export default {
  components: {
    RealtimeDealy,
    ScrollWindowBar,
  },
  props: {
    params: {
      type: Object,
      default: () => ({}),
    },
    // 需要过滤的窗口类型的列表
    filteWindowTypes: {
      type: Array,
      default: () => [],
    },
    isSetDisable: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      localParams: {},
      validate: {
        required: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
        ],
      },
    };
  },
  computed: {
    paramsValue: {
      get() {
        return this.params;
      },
      set(value) {
        Object.assign(this.params, value);
      },
    },
    waitTime() {
      const times = [0, 10, 30, 60, 180, 300, 600];
      return this.timeCalculator(times, '秒');
    },
    /** 窗口长度 */
    windowInterval() {
      const windowType = this.params.window_type;
      let times = [];
      if (windowType === 'slide') {
        times = [2, 10, 30, 45, 60, 1440];
      } else {
        times = [10, 30, 45, 60, 1440];
      }
      return this.timeCalculator(times, '分钟', { id: 1440, name: '24' + this.$t('小时') });
    },
    countFreqList() {
      const times = [30, 60, 180, 300, 600];
      return this.timeCalculator(times, '秒');
    },
    expiredTimeList() {
      const times = [0, 1, 3, 5, 10, 30];
      return this.timeCalculator(times, '分钟');
    },
    windowType() {
      const type = this.params.window_type;
      if (type === 'scroll' || type === 'session') {
        return 'scroll';
      }
      return type;
    },
    windowTypes() {
      // 功能开关控制的窗口类型
      let functionSwitchTypes = ['session'];
      functionSwitchTypes = functionSwitchTypes.filter(item => this.$modules.isActive(item));

      return windowTypeList.filter(item => {
        return ![...this.filteWindowTypes, ...functionSwitchTypes].includes(item.value);
      });
    },
  },
  methods: {
    timeCalculator(times, unit, special = { id: null, name: null }) {
      const timeOptions = times.map(time => {
        const name = time === 0 ? this.$t('无') : time.toString() + this.$t(unit);
        const obj = time === special.id ? special : { id: time, name: name };
        return obj;
      });
      return timeOptions;
    },
    validateForm() {
      return this.$refs.form.validate().then(
        validator => {
          return true;
        },
        validator => {
          return false;
        }
      );
    },
  },
};
</script>

<style lang="scss" scoped></style>
