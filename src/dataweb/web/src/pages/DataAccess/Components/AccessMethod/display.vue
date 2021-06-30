

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
  <Container>
    <div class="am-content">
      <div class="am-content-row">
        <Item v-if="collMethod">
          <label class="bk-label">{{ $t('采集方式') }}：</label>
          <div class="bk-form-content">
            <label class="bk-form-radio">
              <i class="bk-radio-text">{{ params.collection_type }}</i>
            </label>
          </div>
        </Item>
        <Item v-if="stockData"
          v-show="stockData.show">
          <label class="bk-label">{{ $t('接入存量数据') }}：</label>
          <div class="bk-form-content">
            {{ params.start_at === 1 ? $t('否') : $t('是') }}
          </div>
        </Item>
      </div>
      <div class="am-content-row">
        <Item v-if="collCycle"
          v-show="collCycle.show && !collCycle.disabled">
          <label class="bk-label">{{ $t('采集周期') }}：</label>
          <div class="bk-form-content acc-method-input space-between">
            {{ temp.periodType }}
            <template v-if="!collCycle.disabled">
              （{{ params.period }}{{ unitMap[params.periodUnit] }}）
            </template>
          </div>
        </Item>
        <Item v-if="dateDaley && dateDaley.show">
          <label class="bk-label">{{ $t('数据延迟时间') }}：</label>
          <div class="bk-form-content acc-method-input">
            {{ params.before_time }}{{ unitMap[params.before_time_unit] }}
          </div>
        </Item>
      </div>
      <div class="am-content-row">
        <Item v-if="incrFields"
          v-show="incrFields.show">
          <label class="bk-label">{{ $t('增量字段') }}：</label>
          <div class="bk-form-content acc-method-input">
            {{ params.increment_field_type }}（{{ params.increment_field }}）
          </div>
        </Item>
        <Item v-if="dateFormat && dateFormat.show">
          <label class="bk-label">{{ $t('时间格式') }}：</label>
          <div class="bk-form-content acc-method-input">
            {{ params.time_format }}
          </div>
        </Item>
      </div>
      <div v-if="ignoreFileEndWith"
        class="am-content-row">
        <Item>
          <label class="bk-label">{{ $t('忽略文件后缀') }}：</label>
          <div class="bk-form-content acc-method-input">
            {{ ignoreFileEndWith }}
          </div>
        </Item>
      </div>
    </div>
  </Container>
</template>
<script>
import Container from '@/pages/DataAccess/NewForm/FormItems/ItemContainer';
import Item from '@/pages/DataAccess/NewForm/FormItems/Item';
import { ACCESS_METHODS, ACCESS_OPTIONS } from '@/pages/DataAccess/NewForm/Constant/index.js';
export default {
  components: {
    Container,
    Item,
  },
  props: {
    schemaConfig: {
      type: Object,
      default: () => {
        return {};
      },
    },
    formData: {
      type: Object,
    },
  },
  data() {
    return {
      unitMap: {
        m: window.$t('分钟'),
        h: window.$t('小时'),
        d: window.$t('天'),
        s: window.$t('秒'),
      },
      /** 表单提交时，深度遍历节点，是否为结束节点 */
      isDeepTarget: true,

      isFirstValidate: true,

      /** 当前选中的采集方式 index */
      activeTypeIndex: 0,

      /** 当前选中的采集周期 */
      activeCollCycleIndex: 0,

      /** 当前选中的增量字段 Index */
      activeIncrFieldIndex: 0,

      /** 当前选中时间格式 Index */
      activeDateTimeFormatIndex: 0,

      /** http接入场景，选择时间格式后，采集周期时间单位Disabled */
      httpPeridUnitDisabled: false,

      dateFormatList: [],

      params: {
        collection_type: 'incr',
        start_at: 0,
        period: '-1',
        periodUnit: 'm',
        time_format: '',
        increment_field: '',
        before_time: 1,
        increment_field_type: '',
        before_time_unit: 'm',
      },

      /** 中间变量，用于采集周期类型、值、单位改变 */
      temp: {
        period: '',
        periodUnit: '',
        periodType: '',
      },
    };
  },
  computed: {
    /** 采集方式 */
    collMethod() {
      const methods = this.schemaConfig[this.accessMethods.COLLECTION_TYPE] || [];
      return methods;
    },

    /** 采集周期
     * 依赖采集方式选择结果
     * parent : collMethod
     */
    collCycle() {
      const options = this.activeCollectionType[this.accessMethods.COLLECTION_CYCLE];
      return options || { options: [] };
    },

    /** 当前选中采集周期关联子对象 */
    activeCollCycle() {
      if (this.collCycle && this.collCycle.child) {
        if (Array.isArray(this.collCycle.child)) {
          return (this.activeCollCycleIndex >= 0 && this.collCycle.child[this.activeCollCycleIndex]) || {};
        } else {
          return this.collCycle.child || {};
        }
      } else {
        return {};
      }
    },

    /** 增量字段
     *  依赖采集周期选择结果
     *  parent : activeCollCycle
     */
    incrFields() {
      const item = (this.activeCollCycle && this.activeCollCycle[this.accessMethods.INCREMENTAL_FIELD]) || {
        options: [],
      };
      return item;
    },

    /** 当前选中增量字段子对象 */
    activeIncrFeild() {
      if (this.incrFields && this.incrFields.child) {
        if (Array.isArray(this.incrFields.child)) {
          return (this.activeIncrFieldIndex >= 0 && this.incrFields.child[this.activeIncrFieldIndex]) || {};
        } else {
          return this.incrFields.child || {};
        }
      } else {
        return {};
      }
    },

    /** 时间格式
     * 依赖增量字段选择结果
     *  parent: activeIncrFeild
     */
    dateFormat() {
      const item = this.activeIncrFeild[this.accessMethods.DATETIME_FORMAT] || { options: [] };
      return item;
    },

    /** 当前选中时间格式 */
    activeDateFormat() {
      if (this.dateFormat && this.dateFormat.child) {
        if (Array.isArray(this.dateFormat.child)) {
          return (this.activeDateTimeFormatIndex >= 0 && this.dateFormat.child[this.activeDateTimeFormatIndex]) || {};
        } else {
          return this.dateFormat.child || {};
        }
      } else {
        return {};
      }
    },

    /** 存量数据
     *  依赖时间格式选择结果
     *  parent : activeDateFormat
     */
    stockData() {
      const item = this.activeDateFormat[this.accessMethods.STOCK_DATA] || {};
      return item;
    },

    /** 时间延时
     *  依赖时间格式选择结果
     *  parent : activeDateFormat
     */
    dateDaley() {
      const item = this.activeDateFormat[this.accessMethods.DATA_DELAY_TIME] || {};
      return item;
    },

    /** 采集方式配置文件 */
    accessMethods() {
      return ACCESS_METHODS;
    },
    /** 当前选中的采集方式对象 */
    activeCollectionType() {
      return (this.collMethod && this.collMethod[this.activeTypeIndex]) || {};
    },

    /** 当前选中采集周期单位 */
    activeCycleUnits() {
      const units = this.activeCollectionType[this.accessMethods.COLLECTION_CYCLE].units;
      const activeUnit = [];
      if (this.activeCollCycleIndex >= 0) {
        units && activeUnit.push(...(units[this.activeCollCycleIndex] || []));
        if (activeUnit.length) {
          // eslint-disable-next-line vue/no-side-effects-in-computed-properties
          this.params.periodUnit = activeUnit[0].text;
        }
      }
      return activeUnit;
    },

    /** 过滤条件 */
    filters() {
      return (this.formData['access_conf_info'] || {}).filters || {};
    },

    /** 忽略文件后缀 */
    ignoreFileEndWith() {
      return this.filters['ignore_file_end_with'];
    },
  },
  watch: {
    formData: {
      handler(val) {
        val && this.renderData(val);
      },
      deep: true,
      immediate: true,
    },
  },
  methods: {
    /** 时间格式选中事件，获取当前Index */
    handleTimeFormatSelected(item) {
      this.activeDateTimeFormatIndex = this.dateFormatList.findIndex(op => op.value === item);
    },
    /** 采集周期选中事件，获取当前选中的Index */
    handleCollCycleSelected(item) {
      this.activeCollCycleIndex = this.activeCollectionType[this.accessMethods.COLLECTION_CYCLE].options.findIndex(
        op => op.value === item
      );
    },

    /** 增量字段选中事件
     *  增量字段依赖采集周期
     */
    handleIncrFieldsSelected(item) {
      this.activeIncrFieldIndex = (
        (this.activeCollCycle && this.activeCollCycle[this.accessMethods.INCREMENTAL_FIELD]) || { options: [] }
      ).options.findIndex(op => op.value === item);
    },

    /** 采集方式选中事件 */
    handleCollectionTypeChange(item, index) {
      this.activeTypeIndex = index;
    },

    /** 渲染数据，编辑页面 */
    renderData(data) {
      data['access_conf_info']
        && Object.keys(this.params).forEach(key => {
          data['access_conf_info']['collection_model'][key] !== undefined
          && this.$set(this.params, key, data['access_conf_info']['collection_model'][key]);
        });

      if (this.params.period > 0) {
        this.temp.period = 'n';
      } else {
        this.temp.period = this.params.period;
      }

      /** DB模式下回填数据 */
      if (this.params.collection_type === 'time' || this.params.collection_type === 'pri') {
        this.params.increment_field_type = (this.params.collection_type === 'time' && this.$t('时间字段')) || 'ID';
        this.params.collection_type = 'incr';
      }

      this.reRenderDataMap();
    },

    reRenderDataMap() {
      let collTypeIndex = this.collMethod.findIndex(item => item.option.value === this.params.collection_type);
      if (collTypeIndex < 0) {
        collTypeIndex = 0;
      }
      const collType = this.collMethod[collTypeIndex];
      collType && this.$set(this.params, 'collection_type', collType.option.text);
      this.handleCollectionTypeChange(null, collTypeIndex);

      const collCycleIndex = this.collCycle.options.findIndex(item => item.value === this.temp.period + '');
      const collCycle = this.collCycle.options[collCycleIndex];
      collCycle && this.$set(this.temp, 'periodType', collCycle.text);
      this.handleCollCycleSelected(this.temp.period + '', collCycleIndex);
    },
  },
};
</script>
<style lang="scss" scoped>
.am-content {
  display: flex;
  flex-wrap: wrap;

  .am-content-row {
    display: flex;
    flex-direction: row;
    justify-content: flex-start;
    .bk-label {
      display: flex;
    }
  }
}
.bk-form-content {
  display: flex;
  align-items: center;

  .bk-form-radio {
    input[type='radio'] {
      height: 18px;
    }
  }

  &.access-obj-stock {
    justify-content: flex-end;
    width: 390px;
  }
  &.space-between {
    justify-content: space-between;
  }

  &.acc-method-input {
    .acc-incr-field {
      width: 100px;
    }
    .acc-incr-value {
      width: 194px;
      margin-left: 5px;
    }
    .bk-form-control {
      margin: 0 5px;

      &.txt-right {
        text-align: right;
      }

      &.delay {
        margin: 0;
        margin-right: 5px;
        text-align: right;
      }
    }
  }
}
</style>
