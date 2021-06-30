

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
  <div class="flink-basic-wrapper"
    style="width: 100%">
    <bkdata-form ref="form"
      :labelWidth="100"
      :model="params">
      <bkdata-form-item :label="$t('启动时间')"
        :property="'start_time'">
        <bkdata-date-picker
          v-model="paramsValue.advanced.start_time"
          :type="startTimeDateType"
          :transfer="true"
          :timePickerOptions="{
            steps: [1, 60, 60],
          }"
          :options="starttimePickerOptions"
          :format="timeForamt"
          style="width: 300px"
          @change="formatStartTime" />
        <i v-bk-tooltips="$t('离线计算任务调度的开始时间')"
          class="bk-icon icon-question-circle ml10" />
      </bkdata-form-item>
      <div class="checkbox-wrapper mb20">
        <bkdata-checkbox v-model="paramsValue.advanced.recovery_enable">
          {{ $t('调度失败重试') }}
        </bkdata-checkbox>
        <i v-bk-tooltips="$t('任务执行失败后重试')"
          class="bk-icon icon-question-circle" />
      </div>
      <bkdata-form-item :label="$t('重试次数')"
        :property="'recovery_times'"
        extCls="mb20">
        <bkdata-select
          v-model="paramsValue.advanced.recovery_times"
          :disabled="!params.advanced.recovery_enable"
          style="width: 300px">
          <bkdata-option v-for="(item, index) in numberRetryList"
            :id="item.id"
            :key="index"
            :name="item.id" />
        </bkdata-select>
      </bkdata-form-item>
      <bkdata-form-item :label="$t('重试间隔')"
        :property="'recovery_enable'">
        <bkdata-select
          v-model="paramsValue.advanced.recovery_interval"
          :disabled="!params.advanced.recovery_enable"
          style="width: 300px">
          <bkdata-option v-for="(item, index) in intervalList"
            :id="item.id"
            :key="index"
            :name="item.name" />
        </bkdata-select>
      </bkdata-form-item>
    </bkdata-form>
  </div>
</template>

<script>
import moment from 'moment';
export default {
  props: {
    params: {
      type: Object,
      default: () => ({}),
    },
    parentConfig: {
      type: [Object, Array],
      default: () => ({}),
    },
    selfConfig: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      numberRetryList: [],
      intervalList: [],
    };
  },
  computed: {
    paramsValue: {
      get() {
        return this.params;
      },
      set(val) {
        Object.assign(this.params, val);
      },
    },
    startTimeDateType() {
      switch (this.params.schedule_period) {
        case 'hour':
        case 'day':
          return 'datetime';
        case 'week':
          return 'date';
        case 'month':
          return 'month';
        default:
          return 'datetime';
      }
    },
    starttimePickerOptions() {
      if (!this.params.advanced.self_dependency) {
        const self = this;
        return {
          disabledDate(time) {
            if (self.params.schedule_period === 'week') {
              return (
                time.getTime() <
                  moment(new Date())
                    .subtract(1, 'days')
                    .valueOf() || time.getDay(time) !== 1
              );
            }
            return (
              time.getTime() <
              moment(new Date())
                .subtract(1, 'days')
                .valueOf()
            );
          },
        };
      } else {
        return {};
      }
    },
    timeForamt() {
      return this.params.schedule_period === 'hour' ? 'yyyy-MM-dd HH:mm' : 'yyyy-MM-dd';
    },
    momentTimeFormat() {
      return this.params.schedule_period === 'hour' ? 'YYYY-MM-DD HH:00' : 'YYYY-MM-DD 00:00';
    },
  },
  created() {
    /** 初始化重试次数 */
    for (let i = 0; i < 3; i++) {
      let obj = { id: i + 1 };
      this.numberRetryList.push(obj);
    }

    const interval = [5, 10, 15, 30, 60];
    for (let item of interval) {
      this.intervalList.push({
        id: `${item}m`,
        name: item + $t('分钟'),
      });
    }
  },
  methods: {
    formatStartTime(val) {
      this.paramsValue.advanced.start_time = moment(val).format(this.momentTimeFormat);
    },
    validateForm() {
      return true;
    },
  },
};
</script>

<style lang="scss" scoped>
.flink-basic-wrapper {
  min-height: 300px;
  .checkbox-wrapper {
    margin-left: 100px;
    margin-bottom: 12px;
    ::v-deep .bk-form-checkbox {
      margin-right: 5px;
    }
  }
  ::v-deep .bk-form-item {
    width: 100% !important;
    .bk-label {
      text-align: left;
    }
  }
}
</style>
