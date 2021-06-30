

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
  <div v-bkloading="{ isLoading: loading }"
    class="advanced-node-edit node"
    :model="nodeParams.advanced">
    <bkdata-form ref="form"
      :labelWidth="125">
      <div class="content-up">
        <bkdata-form-item
          :label="$t('启动时间')"
          property="start_time"
          :rules="rules.startTime"
          :errorDisplayType="'normal'">
          <bkdata-date-picker
            :value="nodeParams.advanced.start_time"
            :placeholder="$t('请选择')"
            :type="'datetime'"
            :timePickerOptions="{
              steps: [1, 60, 60],
            }"
            :transfer="true"
            :options="starttimePickerOptions"
            :format="timeForamt"
            style="width: 310px"
            @change="checkStartTime" />
          <i v-bk-tooltips="$t('离线计算任务调度的开始时间')"
            class="bk-icon icon-question-circle" />
        </bkdata-form-item>
        <bkdata-form-item>
          <bkdata-checkbox v-model="nodeParamsValue.advanced.force_execute">
            {{ $t('启动后立刻计算') }}
          </bkdata-checkbox>
          <i
            v-bk-tooltips="$t('离线计算任务启动后_立刻执行下次调度时间的计算任务')"
            class="bk-icon icon-question-circle" />
        </bkdata-form-item>
      </div>
      <div class="content-down">
        <bkdata-form-item>
          <bkdata-checkbox v-model="nodeParamsValue.advanced.recovery_enable">
            {{ $t('调度失败重试') }}
          </bkdata-checkbox>
          <i v-bk-tooltips="$t('任务执行失败后重试')"
            class="bk-icon icon-question-circle" />
        </bkdata-form-item>
        <bkdata-form-item :label="$t('重试次数')">
          <bkdata-selector
            :list="numberRetryList"
            :disabled="!nodeParams.advanced.recovery_enable"
            :selected.sync="nodeParams.advanced.recovery_times"
            :settingKey="'id'"
            :displayKey="'id'"
            :customStyle="{
              width: '312px',
            }" />
        </bkdata-form-item>
        <bkdata-form-item :label="$t('重试间隔')">
          <bkdata-selector
            :list="intervalList"
            :disabled="!nodeParams.advanced.recovery_enable"
            :selected.sync="nodeParams.advanced.recovery_interval"
            :settingKey="'id'"
            :displayKey="'name'"
            :customStyle="{
              width: '312px',
            }" />
        </bkdata-form-item>
      </div>
    </bkdata-form>
  </div>
</template>

<script>
import moment from 'moment';
export default {
  model: {
    prop: 'nodeParams',
    event: 'change',
  },
  props: {
    nodeParams: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      starttimePickerOptions: {},
      rules: {
        startTime: [
          {
            validator: () => {
              const val = this.nodeParams.advanced.start_time;
              if (new Date(val) > new Date() || val === '') {
                return true;
              }
              return false;
            },
            message: this.$t('高级配置启动时间应大于当前时间'),
            trigger: 'blur',
          },
        ],
      },
      loading: false,
    };
  },
  computed: {
    nodeParamsValue: {
      get() {
        return this.nodeParams;
      },
      set(val) {
        Object.assign(this.nodeParams, val);
      },
    },
    timeForamt() {
      return this.nodeParams.schedule_period === 'hour' ? 'yyyy-MM-dd HH:mm' : 'yyyy-MM-dd';
    },
    intervalList() {
      const list = [5, 10, 15, 30, 60];
      return list.map(item => {
        return {
          id: item + 'm',
          name: item + '分钟',
        };
      });
    },
    numberRetryList() {
      const list = [];
      for (let i = 0; i < 3; i++) {
        let obj = { id: i + 1 };
        list.push(obj);
      }
      return list;
    },
  },
  mounted() {
    // 初始化高级配置启动时间
    this.starttimePickerOptions = {
      disabledDate(time) {
        return (
          time.getTime() <
          moment(new Date())
            .subtract(1, 'days')
            .valueOf()
        );
      },
    };
  },
  methods: {
    validateForm() {
      return this.$refs.form.validate().then(
        validator => {
          return Promise.resolve(validator);
        },
        validator => {
          return Promise.reject(validator);
        }
      );
    },
    checkStartTime(date) {
      // 格式化时间
      this.nodeParamsValue.advanced.start_time = date ? moment(date).format('YYYY-MM-DD HH:mm') : '';
    },
  },
};
</script>

.
<style lang="scss" scoped>
::v-deep .bk-form {
  .bk-form-item {
    position: relative;
  }
  .bk-form-content {
    .icon-question-circle {
      position: absolute;
      right: 0;
      cursor: pointer;
    }
  }
}
.advanced-node-edit {
  min-height: 800px;
}
</style>
