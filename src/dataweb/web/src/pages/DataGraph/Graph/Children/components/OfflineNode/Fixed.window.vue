

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
  <div class="window-display-wrapper">
    <bkdata-form ref="commonForm"
      :labelWidth="125"
      extCls="bk-common-form"
      :model="nodeParams.config">
      <bkdata-form-item :label="$t('窗口类型')"
        :required="true"
        :property="'window_type'"
        extCls="mb20">
        <bkdata-radio-group v-model="nodeParamsValue.config.window_type">
          <bkdata-radio value="fixed"
            :disabled="isSetDisable">
            {{ $t('固定窗口') }}
          </bkdata-radio>
          <bkdata-radio :value="'accumulate_by_hour'"
            :disabled="isSetDisable">
            {{ $t('按小时累加窗口') }}
          </bkdata-radio>
        </bkdata-radio-group>
      </bkdata-form-item>
      <bkdata-form-item extCls="form-content-auto-width"
        :label="$t('窗口示例')">
        <ScrollWindowBar :type="windowType" />
      </bkdata-form-item>
    </bkdata-form>
    <!--固定窗口显示-->
    <div v-show="nodeParams.config.window_type === 'fixed'"
      class="mt20">
      <bkdata-form ref="fixedForm"
        :labelWidth="125"
        extCls="bk-common-form"
        :model="nodeParams.config">
        <bkdata-form-item
          :label="$t('统计频率')"
          :required="true"
          :property="'count_freq'"
          :rules="validate.countFreq"
          extCls="mb20">
          <bkdata-input
            v-model="nodeParamsValue.config.count_freq"
            :min="1"
            class="number-select-width"
            type="number" />
          <bkdata-selector
            :list="countFreqList"
            class="unit-select-width"
            :selected.sync="nodeParams.config.schedule_period"
            :settingKey="'value'"
            :displayKey="'name'"
            @item-selected="handleUnit" />
        </bkdata-form-item>
        <bkdata-form-item :label="$t('统计延迟')"
          :required="true"
          :property="'fixed_delay'"
          extCls="mb20">
          <bkdata-input
            v-model="nodeParamsValue.config.fixed_delay"
            :min="0"
            class="number-select-width"
            type="number" />
          <bkdata-selector
            :list="delayUinit"
            class="unit-select-width"
            :selected.sync="nodeParams.config.delay_period"
            :settingKey="'value'"
            :displayKey="'name'" />
        </bkdata-form-item>
        <template v-if="isShowDependencyConfig && $modules.isActive('offline_customconf')">
          <bkdata-form-item
            :label="$t('依赖配置')"
            :required="true"
            :property="'dependency_config_type'"
            :rules="validate.required"
            extCls="mb20">
            <bkdata-radio-group v-model="nodeParamsValue.config.dependency_config_type"
              @change="dependencyChange">
              <bkdata-radio :value="'unified'">
                {{ $t('统一配置') }}
              </bkdata-radio>
              <bkdata-radio :value="'custom'"
                :disabled="customConfigDisabled">
                {{ $t('自定义配置') }}
              </bkdata-radio>
            </bkdata-radio-group>
          </bkdata-form-item>
        </template>
        <bkdata-form-item
          v-if="nodeParams.config.dependency_config_type === 'unified'"
          extCls="bk-common-form mb20"
          :label="$t('窗口长度')"
          :required="true"
          :property="'unified_config.window_size'"
          :rules="validate.required">
          <bkdata-input
            v-model="nodeParamsValue.config.unified_config.window_size"
            :min="1"
            class="number-select-width"
            type="number"
            :disabled="isSetDisable" />
          <bkdata-selector
            class="unit-select-width"
            :list="countFreqList"
            :disabled="isSetDisable"
            :selected.sync="nodeParams.config.unified_config.window_size_period"
            :settingKey="'value'"
            :displayKey="'name'"
            @item-selected="handleUnit" />
        </bkdata-form-item>
        <!-- 自定义配置 -->
        <template v-if="nodeParams.config.dependency_config_type === 'custom'">
          <div v-for="(item, index) in customConfigArray"
            :key="index"
            class="bk-common-form">
            <div class="line" />
            <div class="parent-table-name">
              {{ item.key }} :
            </div>
            <!-- 窗口长度 -->
            <bkdata-form-item
              :label="$t('窗口长度')"
              :required="true"
              :property="`custom_config.${item.key}.window_size`"
              :rules="validate.required"
              extCls="mb20">
              <bkdata-input v-model="item.window_size"
                :min="1"
                class="number-select-width"
                type="number" />
              <bkdata-selector
                class="unit-select-width"
                :list="countFreqList"
                :selected.sync="item.window_size_period"
                :settingKey="'value'"
                :displayKey="'name'"
                @item-selected="handleUnit" />
            </bkdata-form-item>
            <bkdata-form-item
              :label="$t('窗口延迟')"
              :required="true"
              :property="`custom_config.${item.key}.window_delay`"
              :rules="validate.required"
              extCls="mb20">
              <bkdata-input v-model="item.window_delay"
                :min="0"
                class="number-select-width"
                type="number" />
              <bkdata-selector
                class="unit-select-width"
                :list="countFreqList"
                :selected.sync="item.window_size_period"
                :settingKey="'value'"
                :displayKey="'name'"
                @item-selected="handleUnit" />
            </bkdata-form-item>
            <bkdata-form-item
              v-if="$modules.isActive('offline_feature')"
              :label="$t('依赖策略')"
              :required="true"
              :property="`custom_config.${item.key}.dependency_rule`"
              :rules="validate.required"
              extCls="mb20">
              <bkdata-selector :list="dependency_rule"
                :selected.sync="item.dependency_rule" />
            </bkdata-form-item>
          </div>
        </template>
        <bkdata-form-item
          v-if="$modules.isActive('offline_feature') && nodeParams.config.dependency_config_type === 'unified'"
          :label="$t('依赖策略')"
          :required="true"
          :property="'unified_config.dependency_rule'"
          :rules="validate.required">
          <bkdata-selector :list="dependency_rule"
            :selected.sync="nodeParams.config.unified_config.dependency_rule" />
        </bkdata-form-item>
      </bkdata-form>
    </div>
    <!--累加窗口显示-->
    <div v-show="nodeParams.config.window_type === 'accumulate_by_hour'"
      class="mt20">
      <bkdata-form ref="accumulateForm"
        :labelWidth="125"
        extCls="bk-common-form"
        :model="nodeParams.config">
        <bkdata-form-item
          :label="$t('统计频率')"
          :required="true"
          :property="'count_freq'"
          :rules="validate.countFreq"
          extCls="mb20">
          <bkdata-input
            v-model="nodeParamsValue.config.count_freq"
            disabled
            class="number-select-width"
            :min="1"
            type="number"
            value="1" />
          <bkdata-selector
            :disabled="delayDisabled"
            class="unit-select-width"
            :list="delayUinit"
            :selected.sync="nodeParams.config.schedule_period"
            :settingKey="'value'"
            :displayKey="'name'" />
        </bkdata-form-item>
        <bkdata-form-item
          v-if="!isDelyTimeHidden"
          :label="$t('延迟时间')"
          :required="true"
          :property="'delay'"
          :rules="validate.required"
          extCls="mb20">
          <bkdata-selector
            :list="delayTimeList"
            :selected.sync="nodeParams.config.delay"
            :settingKey="'id'"
            :displayKey="'name'" />
        </bkdata-form-item>
        <bkdata-form-item
          :label="$t('数据起点')"
          :required="true"
          :property="'data_start'"
          :rules="validate.required"
          extCls="mb20">
          <bkdata-selector
            :displayKey="'name'"
            :list="timeStart"
            :placeholder="$t('请选择')"
            :selected.sync="nodeParams.config.data_start"
            :settingKey="'id'" />
        </bkdata-form-item>
        <bkdata-form-item
          :label="$t('数据终点')"
          :required="true"
          :property="'data_end'"
          :rules="validate.required"
          extCls="mb20">
          <bkdata-selector
            :displayKey="'name'"
            :list="timeEnd"
            :placeholder="$t('请选择')"
            :selected.sync="nodeParams.config.data_end"
            :settingKey="'id'" />
        </bkdata-form-item>
        <bkdata-form-item
          v-if="$modules.isActive('offline_feature')"
          :label="$t('依赖策略')"
          :required="true"
          :property="'unified_config.dependency_rule'"
          :rules="validate.required"
          extCls="mb20">
          <bkdata-selector :list="dependency_rule"
            :selected.sync="nodeParams.config.unified_config.dependency_rule" />
        </bkdata-form-item>
      </bkdata-form>
    </div>
  </div>
</template>

<script>
import ScrollWindowBar from '../../../Components/ScrollWindowBar';
import utils from '@/pages/DataGraph/Common/utils.js';
import Bus from '@/common/js/bus.js';

export default {
  components: {
    ScrollWindowBar,
  },
  model: {
    prop: 'nodeParams',
    event: 'change',
  },
  props: {
    nodeParams: {
      type: Object,
      default: () => ({}),
    },
    parentConfig: {
      type: [Object, Array],
      default: () => [],
    },
    selfConfig: {
      type: Object,
      default: () => ({}),
    },
    isShowDependencyConfig: {
      type: Boolean,
      default: true,
    },
    isSetDisable: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      delayDisabled: true,
      dependency_rule: [
        {
          id: 'no_failed',
          name: this.$t('无失败'),
        },
        {
          id: 'all_finished',
          name: this.$t('全部成功'),
        },
        {
          id: 'at_least_one_finished',
          name: this.$t('一次成功'),
        },
      ],
      validate: {
        countFreq: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
        ],
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
    nodeParamsValue: {
      get() {
        return this.nodeParams;
      },
      set(val) {
        Object.assign(this.nodeParams, val);
      },
    },
    countFreqList() {
      if (this.$modules.isActive('batch_week_task')) {
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
      return [
        {
          value: 'hour',
          name: this.$t('小时'),
        },
        {
          value: 'day',
          name: this.$t('天'),
        },
      ];
    },
    delayUinit() {
      if (!this.$modules.isActive('batch_week_task')) {
        return [
          {
            value: 'hour',
            name: this.$t('小时'),
          },
        ];
      }
      const unit = this.nodeParams.config.schedule_period;
      switch (unit) {
        case 'week':
        case 'month':
          return [
            {
              value: 'hour',
              name: this.$t('小时'),
            },
            {
              value: 'day',
              name: this.$t('天'),
            },
          ];
        default:
          return [
            {
              value: 'hour',
              name: this.$t('小时'),
            },
          ];
      }
    },
    customConfigDisabled() {
      const parentConfig = this.parentConfig;
      if (Array.isArray(parentConfig)) {
        let result = true;
        parentConfig.forEach(item => {
          if (item.node_type !== 'batch_kv_source') {
            result = false;
          }
        });
        return result;
      }
      return false;
    },
    customConfigArray() {
      return Object.values(this.nodeParams.config.custom_config);
    },
    windowType() {
      if (this.nodeParams.config.window_type === 'fixed') {
        return 'scroll';
      } else {
        return 'accumulate';
      }
    },
    /** 判断父级节点是否是离线节点，如果是则隐藏延迟时间 */
    isDelyTimeHidden() {
      return (
        this.parentConfig
        && (Array.isArray(this.parentConfig)
          ? this.parentConfig.some(pa => pa.node_type === this.nodeParams.node_type)
          : this.parentConfig['node_type'] === this.nodeParams.node_type)
      );
    },
    timeStart() {
      return utils.getTimeList();
    },
    timeEnd() {
      return utils.getTimeList('59');
    },
    delayTimeList() {
      return utils.getTimeList('小时S', 0);
    },
  },
  watch: {
    'nodeParams.config.window_type'(val) {
      if (this.selfConfig.hasOwnProperty('node_id') && val === 'fixed') {
        const period = this.nodeParams.config.unified_config.window_size_period;
        const windowSize = this.nodeParams.config.unified_config.window_size;
        this.nodeParamsValue.config.unified_config.window_size_period = period || 'hour';
        this.nodeParamsValue.config.unified_config.window_size = windowSize || 1;
      }
      if (val === 'accumulate_by_hour') {
        this.nodeParamsValue.config.count_freq = 1;
        this.nodeParamsValue.config.schedule_period = 'hour';
      }
    },
  },
  mounted() {
    /** 回填结束后，检测是否有新的节点接入，如果有需要更新自定义节点状态 */
    Bus.$on('set-config-done', () => {
      if (this.nodeParams.config.dependency_config_type === 'custom') {
        const customKeys = Object.keys(this.nodeParams.config.custom_config);
        if (this.parentConfig.length > customKeys.length) {
          this.parentConfig.forEach(parent => {
            console.log(parent.result_table_id);
            if (parent.node_type !== 'batch_kv_source' && !customKeys.includes(parent.result_table_id)) {
              this.initCustomSet(parent);
            }
          });
        }
      }
    });
  },
  methods: {
    /** 自定义配置修改，重置配置 */
    dependencyChange(val) {
      if (val === 'custom') {
        console.log('dependency change', this.nodeParams.config.custom_config);
        if (
          this.parentConfig.length > Object.keys(this.nodeParams.config.custom_config).length
          && this.selfConfig.hasOwnProperty('node_id')
        ) {
          // 更改已有节点并且传入多个父节点
          this.nodeParamsValue.config.custom_config = {};
        }

        Object.keys(this.nodeParams.config.custom_config).forEach(key => {
          if (!this.parentConfig.some(conf => conf.result_table_id === key)) {
            delete this.nodeParams.config.custom_config[key];
          }
        });
        this.formatCustomConfig();
      }
    },
    formatCustomConfig() {
      this.parentConfig
        .filter(item => item.node_type !== 'batch_kv_source')
        .forEach(item => {
          this.initCustomSet(item);
        });
    },
    initCustomSet(item) {
      if (this.nodeParams.node_type === 'tdw_batch') {
        this.nodeParams.config.from_nodes.forEach(node => {
          if (node.id === item.node_id) {
            this.$set(this.nodeParams.config.custom_config, node.from_result_table_ids, {
              window_size: 1,
              window_size_period: this.nodeParams.config.schedule_period,
              window_delay: 0,
              dependency_rule: 'all_finished',
              key: node.from_result_table_ids,
            });
          }
        });
        return;
      }
      this.$set(this.nodeParams.config.custom_config, item.result_table_id, {
        window_size: 1,
        window_size_period: this.nodeParams.config.schedule_period,
        window_delay: 0,
        dependency_rule: 'all_finished',
        key: item.result_table_id,
      });
    },
    validateForm() {
      if (this.nodeParams.config.window_type === 'fixed') {
        return this.$refs.fixedForm.validate().then(
          validator => {
            return Promise.resolve(validator);
          },
          validator => {
            return Promise.reject(validator);
          }
        );
      }
      return Promise.resolve(true);
    },
    /*
               节点配置下，统计频率、窗口长度和窗口延迟单位联动
            */
    handleUnit(unit) {
      Object.keys(this.nodeParams.config.custom_config).forEach(item => {
        this.nodeParamsValue.config.custom_config[item].window_size_period = unit;
      });
      this.nodeParamsValue.config.schedule_period = unit;
      this.nodeParamsValue.config.unified_config.window_size_period = unit;

      /** 当统计频率单位变为小时、天时，统计延迟单位变为小时 */
      if (!['week', 'month'].includes(unit)) {
        this.nodeParamsValue.config.delay_period = 'hour';
      }
    },
    isFixedDelayHidden() {
      return (
        this.parentConfig
        && (Array.isArray(this.parentConfig)
          ? this.parentConfig.some(pa => this.isOfflineNodeWithFixedDelay(pa))
          : this.isOfflineNodeWithFixedDelay(this.parentConfig))
      );
    },
    isOfflineNodeWithFixedDelay(node) {
      return node.node_type === this.nodeParams.node_type && node.config.fixed_delay > 0;
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .bk-form {
  .bk-form-content {
    line-height: 1;
  }
}
.bk-common-form {
  .line {
    width: 100%;
    height: 1px;
    padding: 0;
    background-color: #d5d5d5;
    overflow: hidden;
    margin: 15px auto 0;
  }
  .parent-table-name {
    padding: 10px;
    color: #737987;
    font-size: 14px;
    font-weight: 700;
    margin: 10px 0 10px 31px;
  }
}
</style>
