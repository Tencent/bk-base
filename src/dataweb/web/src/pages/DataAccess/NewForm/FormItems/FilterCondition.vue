

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
  <Container class="bk-filter-condition">
    <Item v-if="!configJson.isDbMode">
      <label class="bk-label">{{ $t('字段分隔符') }}:</label>
      <div class="bk-form-content collect-cycle">
        <bkdata-combobox
          v-tooltip.notrigger="delimiterValidate"
          :displayKey="'delimiter_alias'"
          :placeholder="$t('请选择')"
          :idKey="'delimiter'"
          :list="splitList"
          :value.sync="params.delimiter"
          @item-selected="handleItemChange" />
      </div>
      <span class="col-text-op">{{ $t('如_等') }}</span>
      <i v-tooltip="filterTips"
        class="bk-icon icon-info-circle tip mr15" />
    </Item>
    <div v-focus="{ currFocus }"
      class="bk-filter-items">
      <Item v-for="(item, index) in params.fields"
        :key="index">
        <div :class="['bk-form-content filter', (configJson.isDbMode && 'db-filter') || '']">
          <div class="column-cal-logic">
            <div v-if="index !== 0"
              class="logic-op-box">
              <bkdata-selector :list="lists"
                :selected.sync="item.logic_op" />
            </div>
            <div v-else />
          </div>
          <template v-if="configJson.isDbMode">
            <dbFieldSelect :fieldList="dbTableFields"
              :selected.sync="item.index"
              class="db-field column" />
          </template>
          <template v-else>
            <span class="col-text-op">{{ $t('第') }}</span>
            <input
              v-model="item.index"
              :placeholder="$t('填一个数字')"
              auto-focus="true"
              class="bk-form-input column"
              :min="0"
              name="validation_name"
              type="number">
            <span class="col-text-op">{{ $t('列') }}</span>
          </template>
          <div class="column-cal">
            <div v-if="opLists.length !== 1">
              <bkdata-selector :list="opLists"
                :selected.sync="item.op" />
            </div>
            <div v-else
              style="text-align: center">
              {{ opLists[0].name }}
            </div>
          </div>
          <div class="column-val">
            <input
              v-model="item.value"
              class="bk-form-input column-content"
              :maxlength="50"
              name="validation_name"
              type="text">
          </div>
          <i
            v-tooltip.notrigger="{ content: filterCountContent, visible: item.visible, class: 'error-red' }"
            class="bk-icon icon-plus-circle f20 mr5 pointer"
            @click="addCondition(index, item)" />
          <template v-if="index === 0">
            <i
              v-tooltip="{ content: canClear ? $t('不能直接清空第一行过滤条件') : $t('清空过滤条件') }"
              :class="{ 'disable-clear': canClear }"
              :title="canClear ? $t('不能直接清空第一行过滤条件') : $t('清空过滤条件')"
              class="bk-icon icon-clear f25 mr5 pointer"
              @click="clearFields" />
          </template>
          <template v-else>
            <i class="bk-icon icon-minus-circle f20 mr5 pointer"
              @click="minusCondition(index)" />
          </template>
        </div>
      </Item>
    </div>
    <div v-if="configJson.tips"
      class="condition-tips">
      <div>
        {{ $t('提示') }}：
        <span class="warning">{{ $t('复杂的过滤条件超过五个会影响机器性能') }}</span>
      </div>
      <div>
        {{ $t('结果') }}：
        <span v-for="(item, index) in calcRegs"
          :key="index">
          {{ item }}
        </span>
      </div>
    </div>
    <template v-if="configJson.ignoreFiles">
      <Item>
        <label class="bk-label">{{ $t('忽略文件后缀') }}:</label>
        <div class="bk-form-content"
          style="width: 610px">
          <bkdata-input
            v-model="params.ignore_file_end_with"
            :placeholder="$t('忽略文件placeholder')"
            :type="'textarea'" />
        </div>
      </Item>
    </template>
  </Container>
</template>
<script>
import Container from './ItemContainer';
import Item from './Item';
import Bus from '@/common/js/bus';
import dbFieldSelect from './Components/dbFieldSelect';
import { getDelimiter } from './Components/fun.js';
export default {
  components: {
    Container,
    Item,
    dbFieldSelect,
  },
  directives: {
    focus: {
      componentUpdated: function (el, binding) {
        const { appendIndex, focus } = binding.value.currFocus;
        if (focus && appendIndex) {
          const inputs = el.querySelectorAll('[auto-focus="true"]');
          const input = inputs[appendIndex];
          const inputLen = inputs.length - 1;
          const insertIndex = inputLen === appendIndex ? appendIndex : inputLen;
          const insetInput = inputs[insertIndex];
          if (input && insetInput && !insetInput.dataset.focused) {
            input.focus();
            insetInput.dataset.focused = true;
          }
        }
      },
    },
  },
  props: {
    modeType: {
      type: String,
      default: 'log',
    },
    configJson: {
      type: Object,
      default: () => {
        return {};
      },
    },
  },
  data() {
    return {
      filterTips: window.$t('数据接入FilterTip'),
      delimiterValidate: {
        content: window.$t('不能为空'),
        visible: false,
        class: 'error-red',
      },
      preModeCache: {},
      loadTb: false,
      isDeepTarget: true,
      currFocus: {
        appendIndex: 0,
        focus: false,
      },
      lists: [
        {
          id: 'and',
          name: window.$t('与'),
          symbol: '&',
        },
        {
          id: 'or',
          name: window.$t('或'),
          symbol: '||',
        },
      ],
      params: {
        delimiter: '|',
        fields: [
          {
            index: '',
            value: '',
            logic_op: '',
            op: '=',
          },
        ],
        ignore_file_end_with: '',
      },
      fieldsNum: 5,
      filterCountContent: window.$t('筛选条件不能多于5个'),
      splitList: [],
      isShowFilter: true,
      showFilterType: ['log', 'tqos'],
      dbTableFields: [],
    };
  },
  computed: {
    /** 操作符列表 */
    opLists() {
      return (
        this.configJson.operators || [
          {
            id: '=',
            name: `= ${window.$t('等于')}`,
          },
        ]
      );
    },
    canClear() {
      return this.params.fields.length > 1;
    },
    accessType() {
      return this.$store.getters.getAccessType;
    },
    calcRegs() {
      return this.params.fields
        .filter(f => f.index && f.value !== '')
        .map((field, index) => {
          const symbol = (field.logic_op && this.lists.find(l => l.id === field.logic_op).symbol) || '';
          return `${(index && symbol) || ''} ${this.$t('第N列1')}${field.index}${this.$t('第N列2')} ${field.op} ${
            field.value
          }`;
        });
    },
  },
  watch: {
    /** 当选择的接入场景改变时，缓存之前的数据，回填当前数据
     *  如果当前接入场景没有缓存，则初始化
     */
    modeType: function (val, oldValue) {
      this.preModeCache = Object.assign({}, this.preModeCache, { [oldValue]: this.params });
      if (this.preModeCache[val]) {
        this.params = this.preModeCache[val];
      } else {
        this.params = {
          delimiter: '|',
          fields: [
            {
              index: '',
              value: '',
              logic_op: '',
              op: '=',
            },
          ],
          ignore_file_end_with: '',
        };
      }
    },
  },
  mounted() {
    Bus.$on('access.obj.dbtableselected', fields => {
      this.dbTableFields = fields;
    });
  },
  created() {
    getDelimiter().then(res => {
      this.splitList = res.data.map(item => Object.assign(item, {
        delimiter_alias: `${item['delimiter_alias']}${/\S/.test(item.delimiter) ? `(${item.delimiter})` : ''}`,
      })
      );
    });
  },
  methods: {
    handleItemChange() {
      this.validateForm();
    },
    validateForm(validateFunc, isSubmit = true) {
      // const isValidate = /\n/.test(this.params.delimiter) || this.params.delimiter !== ''
      // this.$set(this.delimiterValidate, 'visible', !isValidate)
      return true;
    },
    clearFields() {
      if (this.params.fields.length > 1) return;
      this.params.fields = [
        {
          index: '',
          value: '',
          logic_op: '',
          op: '=',
        },
      ];
    },
    addCondition(index, item) {
      if (this.params.fields.length >= this.fieldsNum) {
        this.$set(item, 'visible', true);
        this.$nextTick(() => {
          setTimeout(() => {
            this.$set(item, 'visible', false);
          }, 1000);
        });
      } else {
        this.currFocus.appendIndex = index + 1;
        this.currFocus.focus = true;
        this.params.fields.splice(index + 1, 0, {
          index: '',
          value: '',
          logic_op: '',
          op: '=',
        });
      }
    },
    minusCondition(index) {
      if (this.params.fields.length > 1) {
        this.params.fields.splice(index, 1);
      }
    },
    formatFormData() {
      const fields = this.params.fields.filter(
        f => ((typeof f.index === 'number' && f.index >= 0) || (typeof f.index === 'string' && f.index !== ''))
          && f.op !== ''
      );
      return {
        group: 'access_conf_info',
        identifier: 'filters',
        data: {
          delimiter: this.params.delimiter,
          fields: fields,
          ignore_file_end_with: this.params.ignore_file_end_with,
        },
      };
    },
    renderData(data) {
      const _data = (this.configJson.mapDBDataToWeb
        && this.configJson.mapDBDataToWeb(data['access_conf_info'].filters))
        || data['access_conf_info'].filters;
      Object.keys(this.params).forEach(key => {
        _data[key] && this.$set(this.params, key, _data[key]);
      });

      if (!this.params.fields.length) {
        Object.assign(this.params, {
          delimiter: '',
          fields: [
            {
              index: '',
              value: '',
              logic_op: '',
              op: '=',
            },
          ],
        });
      } else {
        this.params.fields.forEach(item => {
          !item.op && this.$set(item, 'op', '=');
        });
      }
    },
  },
};
</script>
<style lang="scss">
.bk-filter-condition {
  flex-direction: column;
  justify-content: flex-start;
  //   padding-top: 30px;

  .col-text-op {
    padding: 0 5px;

    & + .tip {
      cursor: pointer;
    }
  }

  .bk-filter-items {
    .bk-form-content {
      &.filter {
        display: flex;
        width: 100%;
        align-items: center;
        &.db-filter {
          .column-cal-logic {
            margin-left: 0px;
            margin-right: 5px;
          }

          .db-field {
            width: 327px;
            padding: 0px 5px 0 0;
          }

          .column-val {
            width: 265px;
          }
        }
        .column-cal-logic {
          min-width: 50px;
          width: 50px;
          margin-left: 46px;
        }

        .logic-op-box {
          .bkdata-selector-input {
            padding: 0 8px !important;
            padding-right: 24px !important;
          }
        }

        .bk-form-input {
          &.column {
            width: 70px;
          }
        }

        .column-cal {
          width: 80px;
          .bkdata-selector-input {
            padding: 0 5px;
          }
        }

        .column-val {
          margin: 0 5px;
          width: 432px;
        }
      }
    }
  }

  .condition-tips {
    width: 610px;
    margin-top: 20px;
    margin-left: 120px;
    margin-top: 20px;
    padding: 15px;
    background: #f1f9ff;
    border-radius: 2px;
    .warning {
      color: red;
      font-weight: 600;
    }
  }
}
</style>
