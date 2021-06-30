

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
          <div class="bk-form-content full-width">
            <bkdata-radio-group v-model="params.collection_type">
              <template v-for="(item, index) in collMethod">
                <template v-if="item.disabled || readonly">
                  <bkdata-radio
                    :key="index"
                    v-bk-tooltips="$t('功能实现中_敬请期待')"
                    :disabled="true"
                    :value="item.option.value">
                    {{ $t(item.option.text) }}
                  </bkdata-radio>
                </template>
                <template v-else>
                  <bkdata-radio
                    :key="index"
                    :value="item.option.value"
                    @change="handleCollectionTypeChange(item, index)">
                    {{ $t(item.option.text) }}
                  </bkdata-radio>
                </template>
              </template>
            </bkdata-radio-group>
            <span v-if="activeCollTypeTips"
              class="coll-type-tips">
              {{ activeCollTypeTips }}
            </span>
          </div>
        </Item>
        <Item v-if="stockData"
          v-show="stockData.show">
          <div class="bk-form-content">
            <!-- 由于swtich仅支持true/false 传值，这里无法进行v-model绑定，后续组件升级建议修改 -->
            <label class="bk-label">{{ $t('接入存量数据') }}：</label>
            <bkdata-switcher
              ref="stokeDataSwitch"
              size="small"
              :onText="$t('是')"
              :offText="$t('否')"
              :disabled="stockData.disabled || readonly"
              :showText="true"
              @change="handleStockDataChange" />
          </div>
        </Item>
      </div>
      <div class="am-content-row">
        <Item v-if="incrFields"
          v-show="incrFields.show">
          <label class="bk-label">{{ $t('增量字段') }}：</label>
          <div class="bk-form-content acc-method-input">
            <bkdata-selector
              class="acc-incr-field"
              :disabled="incrFields.disabled || readonly"
              :list="incrFields.options"
              settingKey="value"
              displayKey="text"
              :selected.sync="params.increment_field_type"
              @item-selected="handleIncrFieldsSelected" />
            <dbFieldSelect
              v-tooltip.notrigger.bottom="validate['increment_field']"
              class="acc-incr-value"
              :disabled="incrFields.disabled || readonly"
              :selected.sync="params.increment_field" />
          </div>
        </Item>
        <Item v-if="collCycle"
          v-show="collCycle.show">
          <label class="bk-label">{{ $t('采集周期') }}：</label>
          <div class="bk-form-content acc-method-input space-between">
            <bkdata-selector
              :list="collCycle.options"
              :disabled="collCycle.disabled || readonly"
              settingKey="value"
              displayKey="text"
              :selected.sync="temp.periodType"
              @item-selected="handleCollCycleSelected" />
            <bkdata-input
              v-model="temp.period"
              v-tooltip.notrigger="validate['period']"
              type="number"
              :disabled="collCycle.disabled || !activeCycleUnits.length || readonly"
              class="txt-right"
              :min="0"
              style="width: 170px" />
            <bkdata-selector
              :disabled="collCycle.disabled || !activeCycleUnits.length || readonly"
              :list="activeCycleUnits"
              :selected.sync="params.periodUnit"
              settingKey="value"
              displayKey="text" />
          </div>
        </Item>
      </div>
      <div class="am-content-row">
        <Item v-if="dateDaley && dateDaley.show">
          <label class="bk-label">{{ $t('数据延迟时间') }}：</label>
          <div class="bk-form-content acc-method-input date-delay">
            <bkdata-input
              v-model="params.before_time"
              v-tooltip.notrigger="validate['before_time']"
              type="number"
              class="delay"
              :min="0"
              :disabled="dateDaley.disabled || readonly" />
            <bkdata-selector
              :list="dateDaley.units || []"
              :disabled="dateDaley.disabled || readonly"
              :selected.sync="params.before_time_unit"
              settingKey="value"
              displayKey="text" />
            <i class="bk-icon icon-exclamation-circle-shape bk-icon-new" />
          </div>
        </Item>
        <Item v-if="dateFormat && dateFormat.show">
          <label class="bk-label">{{ $t('时间格式') }}：</label>
          <div class="bk-form-content acc-method-input">
            <bkdata-selector
              v-tooltip.notrigger="validate['time_format']"
              :list="dateFormatList"
              :disabled="dateFormat.disabled || readonly"
              :selected.sync="params.time_format"
              @item-selected="handleTimeFormatSelected" />
          </div>
        </Item>
      </div>
    </div>
  </Container>
</template>
<script>
import Container from './ItemContainer';
import Item from './Item';
import dbFieldSelect from './Components/dbFieldSelect';
import { ACCESS_METHODS, ACCESS_OPTIONS } from '../Constant/index.js';
import Bus from '@/common/js/bus';
import { getDateFormat } from './Components/fun.js';
import { validateRules } from '../SubformConfig/validate.js';
export default {
  components: {
    Container,
    Item,
    dbFieldSelect,
  },
  props: {
    schemaConfig: {
      type: Object,
      default: () => {
        return {};
      },
    },
    readonly: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
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
        start_at: 1,
        period: '-1',
        periodUnit: 'm',
        time_format: '',
        increment_field: '',
        before_time: 1,
        increment_field_type: '',
        before_time_unit: 'm',
      },
      validate: {
        period: {
          content: window.$t('不能为空'),
          visible: false,
          source: 'collCycle',
          depend: 'temp.period',
          checkUnit: true,
          unitComputed: 'activeCycleUnits', // regExp: /^[1-9]\d*$/
          regs: [
            { required: true },
            { customValidateFun: this.validatePeroid, error: window.$t('不能小于等于0') },
            { customValidateFun: this.validatePeriodMax, error: window.$t('不能超过30天') },
          ],
          class: 'error-red',
        },
        increment_field: {
          content: window.$t('不能为空'),
          visible: false,
          source: 'incrFields',
          class: 'error-red',
        },
        before_time: {
          content: window.$t('不能为空'),
          visible: false,
          source: 'dateDaley',
          regs: [{ required: true }, { regExp: /^[0-9]\d*$/, error: window.$t('不能小于0') }],
          class: 'error-red',
        },
        time_format: {
          content: window.$t('请选择时间参数格式'),
          visible: false,
          source: 'dateFormat',
          class: 'error-red',
        },
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
      const checkedIndex = methods.findIndex(m => m.selected);
      const item = (methods && checkedIndex >= 0 && methods[checkedIndex]) || methods[0];
      if (item) {
        this.$set(this.params, 'collection_type', item.option.value);
        this.$set(this, 'activeTypeIndex', checkedIndex >= 0 ? checkedIndex : 0);
      }

      return methods;
    },

    /** 采集周期
     * 依赖采集方式选择结果
     * parent : collMethod
     */
    collCycle() {
      const options = this.activeCollectionType[this.accessMethods.COLLECTION_CYCLE];
      /** 设置默认采集周期 */
      if (options && options.options[0]) {
        this.$set(this, 'activeCollCycleIndex', 0);
        this.$set(this.temp, 'periodType', options.options[0].value);
        /** 下面代码没啥用，就为了激活 this.temp.periodType */
        let activeType  = this.temp.periodType;
        activeType = undefined;
      }
      return options;
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
      const options = item.options;
      /** 设置默认值 */
      if (options && options[0]) {
        console.log(options);
        const defaultOption = options.find(item => item.value === 'id') || options[0];
        this.$set(this.params, 'increment_field_type', defaultOption.value);
        this.$set(this, 'activeIncrFieldIndex', 0);
      }
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
      const options = (item && item.options) || null;
      const show = item && item.show;
      if (show && options && options[0]) {
        this.$set(this.params, 'time_format', options[0].value);
        this.$set(this, 'activeDateTimeFormatIndex', 0);
      }
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
      // this.handleStockDataChange(item.isTrue)
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
      }
      return activeUnit;
    },

    /**
     * 当前选中采集方式提示
     */
    activeCollTypeTips() {
      return this.activeCollectionType.tips;
    },
  },
  watch: {
    'params.periodUnit': function (val) {
      Bus.$emit('access.methods.period', {
        period: this.params.period,
        timeUnit: val,
      });
    },
    'temp.period': function (val) {
      if (val !== '') {
        this.params.period = val;
        Bus.$emit('access.methods.period', {
          period: val,
          timeUnit: this.params.periodUnit,
        });
      }
    },
    /** 监控配置文件改变【接入场景发生改变】 */
    schemaConfig: {
      deep: true,
      handler(val) {
        this.httpPeridUnitDisabled = false;
        this.temp.period = '';
        this.isFirstValidate = true;
        Object.keys(this.validate).forEach(key => {
          this.validate[key].visible = false;
        });
        // !this.isFirstValidate && this.validateForm(null, false)
      },
    },
    params: {
      deep: true,
      handler(val) {
        !this.isFirstValidate && this.validateForm(null, false);
        this.$forceUpdate();
      },
    },
    dateFormat: {
      immediate: true,
      deep: true,
      handler(val) {
        if (val && val.show && !this.dateFormatList.length) {
          getDateFormat().then(res => {
            this.dateFormatList = res.data;
          });
        }
      },
    },
  },
  mounted() {
    Bus.$on('access.http.timeformat', val => {
      this.params.time_format = val;
    });
  },
  methods: {
    validatePeroid(val) {
      if (this.temp.periodType === '-1') {
        return true;
      } else {
        return val > 0;
      }
    },
    validatePeriodMax(val) {
      if (this.temp.periodType === '-1') {
        return true;
      } else {
        const fixParas = { m: 60, h: 3600, s: 1, d: 24 * 3600 };
        const maxTime = 30 * 24 * 3600;
        return fixParas[this.params.periodUnit] * Number(val) <= maxTime; // { validate: , error: '不能超过30天' }
      }
    },
    validateItem(validate, target) {
      const regs = validate.regs || { required: true };
      validate.visible = false;
      let isValidate = validateRules(regs, target, validate);
      return isValidate;
    },
    validateForm(validateFunc, isSubmit = true) {
      let isValidate = true;
      isSubmit && this.$set(this, 'isFirstValidate', false);
      Object.keys(this.validate).forEach(key => {
        const source = this[this.validate[key].source];

        if (source && source.show && !source.disabled) {
          const depend = this.validate[key].depend;
          let unitIsTrue = true;
          const checkUnit = this.validate[key].checkUnit;
          if (checkUnit) {
            const compUnit = this.validate[key].unitComputed;
            const units = (compUnit && this[compUnit]) || source.units;

            /** 根据单位判断是否需要校验依赖项 */
            unitIsTrue = units && units.length;
          }
          if (depend && unitIsTrue) {
            const deps = depend.split('.');
            let target = this;
            target = deps.reduce((pre, curr) => {
              if (target !== undefined) {
                target = target[curr];
              }
              return target;
            }, target);
            if (!this.validateItem(this.validate[key], target)) {
              isValidate = false;
            }
          } else {
            if (!this.validateItem(this.validate[key], this.params[key])) {
              isValidate = false;
            }
          }
        } else {
          this.$set(this.validate[key], 'visible', false);
        }
      });
      this.$forceUpdate();
      return isValidate;
    },
    /** 时间格式选中事件，获取当前Index */
    handleTimeFormatSelected(item) {
      this.activeDateTimeFormatIndex = this.dateFormatList.findIndex(op => op.value === item);
    },
    /** 采集周期选中事件，获取当前选中的Index, 采集周期下拉只有n 和 -1 */
    handleCollCycleSelected(item, index = -1) {
      if (index !== -1) {
        if (String(item) === '-1') {
          this.params.period = '-1';
          this.temp.period = '';
        } else {
          this.temp.period = 'n';
        }
      }
      this.activeCollCycleIndex = this.activeCollectionType[this.accessMethods.COLLECTION_CYCLE].options.findIndex(
        op => op.value === item
      );
      !this.isFirstValidate && this.validateForm(null, false);
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
      this.temp.period = '';
      this.activeTypeIndex = index;
      if (this.activeCycleUnits.length) {
        this.params.periodUnit = this.activeCycleUnits[0].value;
      }
    },

    /** 是否接入存量 */
    handleStockDataChange(item) {
      this.params.start_at = item ? 0 : 1;
    },

    /** 格式化提交表单数据 */
    formatFormData() {
      const postParam = Object.assign({}, this.params);
      return {
        group: 'access_conf_info',
        identifier: 'collection_model',
        data: postParam,
      };
    },
    /** 渲染数据，编辑页面 */
    renderData(data) {
      data['access_conf_info']
        && Object.keys(this.params).forEach(key => {
          data['access_conf_info']['collection_model'].hasOwnProperty(key)
            && this.$set(this.params, key, data['access_conf_info']['collection_model'][key]);
          // 接入存量数据的Swtich组件仅支持true、false参数，这里回填直接改变值
          if (key === 'start_at') {
            this.$refs.stokeDataSwitch.value = this.params[key] === 0;
          }
        });

      /** DB模式下回填数据 */
      if (this.params.collection_type === 'time' || this.params.collection_type === 'pri') {
        this.params.increment_field_type = this.params.collection_type === 'pri' ? 'id' : 'time';
        this.params.collection_type = 'incr';
        this.handleIncrFieldsSelected(this.params.increment_field_type); // 设置增量字段回填后的正确逻辑
      }
      if (this.params.period > 0) {
        const unit = this.schemaConfig.period || {};
        const periodResolve = this.resolvePeriod(this.params.period, unit[this.params.collection_type] || [1], 0);
        this.params.period = String(periodResolve.period);
        this.temp.period = this.params.period;
        this.params.periodUnit = ['m', 'h', 'd'][periodResolve.unit];
      } else {
        this.params.period = String(this.params.period);
      }

      this.temp.periodType = this.params.period === '-1' ? '-1' : 'n';
      this.handleCollCycleSelected(this.temp.periodType);

      this.activeTypeIndex = this.collMethod.findIndex(methd => methd.option.value === this.params.collection_type);
    },

    resolvePeriod(period, units = [60, 24], index = 0) {
      let tempPeriod = null;
      if (index < units.length) {
        const unit = units[index];

        if (!(period % unit) && unit !== 1) {
          if (index + 1 <= units.length) {
            period = period / unit;
            tempPeriod = this.resolvePeriod(period, units, index + 1);
          }
        }
      }

      if (!tempPeriod) {
        tempPeriod = { period: period, unit: index };
      }
      return tempPeriod;
    },
  },
};
</script>
<style lang="scss" scoped>
.am-content {
  display: flex;
  flex-direction: column;
  .am-content-row {
    display: flex;
    flex-direction: row;
    justify-content: flex-start;

    .coll-type-tips {
      color: red;
      position: absolute;
      right: -20px;
      transform: translateX(100%);
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

  &.full-width {
    width: 100%;
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

    &.date-delay {
      position: relative;
      i.bk-icon {
        position: absolute;
        right: -25px;
      }
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
