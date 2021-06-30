

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
  <div class="delay-config-wrapper">
    <bkdata-form :labelWidth="125"
      extCls="bk-common-form">
      <bkdata-form-item>
        <bkdata-checkbox v-model="nodeParamsValue.allowed_lateness"
          class="mr20">
          {{ $t('是否计算延迟数据') }}
        </bkdata-checkbox>
      </bkdata-form-item>
      <template v-if="nodeParams.allowed_lateness">
        <bkdata-form-item>
          <Tips
            :text="$t('延迟数据会单独计算并输出结果')"
            :link="{
              text: $t('详见文档'),
              url: $store.getters['docs/getNodeDocs'].realtimeDelay,
            }"
            :size="'small'" />
        </bkdata-form-item>
        <bkdata-form-item>
          <div class="delay-form-wrapper">
            <bkdata-form :labelWidth="94">
              <bkdata-form-item :label="$t('延迟时间')"
                style="width: 100%">
                <bkdata-selector
                  :displayKey="'name'"
                  :list="lateTimeList"
                  :placeholder="$t('请选择')"
                  :selected.sync="nodeParams.lateness_time"
                  :settingKey="'id'" />
              </bkdata-form-item>
              <bkdata-form-item :label="$t('统计频率')"
                style="width: 100%">
                <bkdata-selector
                  :displayKey="'name'"
                  :list="lateCountFreqList"
                  :disabled="windowType === 'session'"
                  :placeholder="$t('请选择')"
                  :selected.sync="nodeParams.lateness_count_freq"
                  :settingKey="'id'" />
              </bkdata-form-item>
            </bkdata-form>
          </div>
        </bkdata-form-item>
      </template>
    </bkdata-form>
  </div>
</template>

<script>
import Tips from '@/components/TipsInfo/TipsInfo.vue';
export default {
  components: {
    Tips,
  },
  model: {
    event: 'change',
    prop: 'nodeParams',
  },
  props: {
    nodeParams: {
      type: Object,
      default: () => ({}),
    },
    windowType: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      lateCountFreqList: [],
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
    lateTimeList() {
      const times = [1, 6, 12, 24, 48];
      return this.timeCalculator(times, '小时');
    },
  },
  watch: {
    windowType: {
      immediate: true,
      handler: function (val) {
        let times;
        if (val === 'session') {
          times = [0, 60, 180, 300, 600];
          this.nodeParamsValue.lateness_count_freq = 0;
        } else {
          this.nodeParamsValue.lateness_count_freq = 60;
          times = [60, 180, 300, 600];
        }
        this.lateCountFreqList = this.timeCalculator(times, '秒');
      },
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
  },
};
</script>

<style lang="scss" scoped>
::v-deep .bk-label {
  font-weight: normal;
}
.delay-config-wrapper {
  .delay-form-wrapper {
    padding: 15px 10px 15px 0;
    width: 100%;
    height: 104px;
    background-color: #fafbfd;
    border: 1px solid #f0f1f5;
    border-radius: 2px 2px 2px 2px;
  }
}
</style>
