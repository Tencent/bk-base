

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
  <div class="node-basic-wrapper"
    style="width: 100%">
    <div id="validate_form5"
      class="bk-form">
      <div class="acompute-node-edit node">
        <div v-if="isShowStartTime"
          class="content-up bk-common-form">
          <div class="bk-form-item">
            <label class="bk-label">
              {{ $t('启动时间') }}
            </label>
            <div class="bk-form-content">
              <bkdata-date-picker
                :value="params.advanced.start_time"
                :placeholder="$t('请选择')"
                :type="startTimeDateType"
                :timePickerOptions="{
                  steps: [1, 60, 60],
                }"
                :transfer="true"
                :options="starttimePickerOptions"
                :format="timeForamt"
                style="width: 310px"
                @change="checkStartTime" />
              <i
                v-bk-tooltips="$t('离线计算任务调度的开始时间')"
                class="bk-icon icon-question-circle bk-question-icon" />
            </div>
            <div
              v-show="validate.startTime.status"
              class="help-block"
              style="margin-left: 125px"
              v-text="validate.startTime.errorMsg" />
          </div>
          <div class="bk-form-item">
            <span class="recal-tip">
              {{ $t('如需验证SQL逻辑，可以使用离线计算')
              }}<a target="_blank"
                :href="$store.getters['docs/getDocsUrl'].dataRecalculate">
                {{ $t('补算功能') }}
              </a>
            </span>
          </div>
        </div>
        <div class="content-mid bk-common-form">
          <div class="bk-form-item">
            <div class="bk-form-content">
              <bkdata-checkbox v-model="paramsValue.advanced.recovery_enable">
                {{ $t('调度失败重试') }}
              </bkdata-checkbox>
              <i v-bk-tooltips="$t('任务执行失败后重试')"
                class="bk-icon icon-question-circle" />
            </div>
          </div>
          <div class="bk-form-item clearfix">
            <label class="bk-label">
              {{ $t('重试次数') }}
            </label>
            <div class="bk-form-content clearfix">
              <bkdata-selector
                :list="numberRetryList"
                :disabled="!params.advanced.recovery_enable"
                :selected.sync="params.advanced.recovery_times"
                :settingKey="'id'"
                :displayKey="'id'" />
            </div>
          </div>
          <div class="bk-form-item clearfix">
            <label class="bk-label">
              {{ $t('重试间隔') }}
            </label>
            <div class="bk-form-content clearfix">
              <bkdata-selector
                :list="intervalList"
                :disabled="!params.advanced.recovery_enable"
                :selected.sync="params.advanced.recovery_interval"
                :settingKey="'id'"
                :displayKey="'name'" />
            </div>
          </div>
        </div>
        <div v-if="isOpenSelfDependency"
          class="content-down bk-common-form">
          <div class="bk-form-item">
            <div id="tooltip-content">
              <p>
                {{ $t('计算时把当前节点上一周期输出作为输入_可在SQL语句中引用当前节点数据输出表名') }}
              </p>
              <p>{{ $t('示例SQL') }}：</p>
              <p style="padding-left: 15px">
                {{ sqlTipEg }}
              </p>
              <p style="padding-left: 15px">
                {{ rtId }}
              </p>
              <p style="padding-left: 15px">
                union
              </p>
              <p style="padding-left: 15px">
                {{ sqlTipEg }}
              </p>
              <p style="padding-left: 15px">
                {{ params.bk_biz_id }}_{{ tableName }}
              </p>
            </div>
            <div class="bk-form-content">
              <bkdata-checkbox v-model="paramsValue.advanced.self_dependency"
                @change="clearFieldValidate">
                {{ $t('启用自依赖') }}
              </bkdata-checkbox>
              <i
                v-bk-tooltips="{
                  allowHtml: true,
                  content: '#tooltip-content',
                  width: 300,
                }"
                class="bk-icon icon-question-circle" />
            </div>
          </div>
          <!-- 依赖策略 -->
          <div v-if="params.advanced.self_dependency"
            class="bk-form-item input-number">
            <label class="bk-label">
              {{ $t('依赖策略') }}
              <span class="required">*</span>
            </label>
            <div class="count-freq-unit"
              style="width: 310px">
              <bkdata-selector
                :list="selfDepRulesList"
                :selected.sync="params.advanced.self_dependency_config.dependency_rule" />
            </div>
          </div>
          <OfflineNodeField
            v-if="params.advanced.self_dependency"
            ref="filedForm"
            :fields="params.advanced.self_dependency_config.fields"
            @addFieldHandle="addSelfDepField"
            @removeFieldHandle="removeSelfDepField">
            <div
              v-bkloading="{ isLoading: sqlFieldLoading }"
              class="dotted-btn emphasize"
              @click.stop="autoGeneratedSQL">
              <i class="bk-icon left-icon icon-execute icons" />{{ $t('分析SQL') }}
            </div>
          </OfflineNodeField>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import OfflineNodeField from './FieldComp.vue';
import moment from 'moment';
export default {
  components: {
    OfflineNodeField,
  },
  props: {
    params: {
      type: Object,
      default: () => ({}),
    },
    parentResultList: {
      type: [Object, Array],
      default: () => ({}),
    },
    nodeType: {
      type: String,
      default: '',
    },
    isShowStartTime: {
      type: Boolean,
      default: true,
    },
    isShowSelfDependency: {
      type: Boolean,
      default: true,
    },
  },
  data() {
    return {
      numberRetryList: [],
      intervalList: [],
      selfDepRulesList: [
        {
          id: 'self_finished',
          name: this.$t('全部成功'),
        },
        {
          id: 'self_no_failed',
          name: this.$t('无失败'),
        },
      ],
      sqlFieldLoading: false,
      validate: {
        startTime: {
          status: false,
          errorMsg: '',
        },
        lastTime: {
          status: false,
          errorMsg: '',
        },
      },
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
    isOpenSelfDependency() {
      const showNodes = ['offline', 'batchv2'];
      return (
        this.isShowSelfDependency
        && showNodes.includes(this.nodeType)
        && this.$modules.isActive('batch-self-dependency')
      );
    },
    inputFields() {
      return this.parentResultList[0].field;
    },
    rtId() {
      return Object.keys(this.parentResultList)[0];
    },
    tableName() {
      const output = this.params.outputs[0];
      return output && output.table_name;
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
    timeStart() {
      return this.getTimeList();
    },
    timeEnd() {
      return this.getTimeList('59');
    },
  },
  created() {
    this.init();
  },
  methods: {
    validateForm() {
      if (this.params.advanced.self_dependency) {
        this.$refs.filedForm
          .validateForm()
          .then(validate => {
            return Object.values(this.validate).every(item => !item.status);
          })
          ['catch'](validate => {
            return false;
          });
      }
      return Object.values(this.validate).every(item => !item.status);
    },
    init() {
      const list = [5, 10, 15, 30, 60];
      list.forEach(item => {
        this.intervalList.push({
          id: `${item}m`,
          name: `${item}分钟`,
        });
      });

      for (let i = 0; i < 3; i++) {
        let obj = { id: i + 1 };
        this.numberRetryList.push(obj);
      }
    },
    clearFieldValidate(val) {
      !val && this.$refs.filedForm.clearStatus();
    },
    checkStartTime(date) {
      // 格式化时间
      this.paramsValue.advanced.start_time = date ? moment(date).format('YYYY-MM-DD HH:mm') : '';

      if (this.params.advanced.self_dependency) return; // 如果勾选自依赖，不限制开始时间
      if (new Date(this.params.advanced.start_time) < new Date()) {
        this.validate.startTime.status = true;
        this.validate.startTime.errorMsg = this.$t('高级配置启动时间应大于当前时间');
      } else {
        this.validate.startTime.status = false;
        this.validate.startTime.errorMsg = '';
      }
    },
    addSelfDepField(item) {
      this.paramsValue.advanced.self_dependency_config.fields.push(item);
    },
    removeSelfDepField(item, index) {
      if (item.id) {
        this.paramsValue.advanced.self_dependency_config.fields = this.params.advanced.self_dependency_config.filter(
          field => field.id !== item.id
        );
      } else {
        this.paramsValue.advanced.self_dependency_config.fields.splice(index, 1);
      }
    },
    sqlTipEg() {
      let sqlTable = this.inputFields;
      let sql = 'select ';
      for (let i = 0; i < 2; i++) {
        if (sqlTable[i] !== undefined) {
          sql += sqlTable[i].name;
        }
        i === 0 ? (sql += ',') : (sql += '...');
      }
      return sql;
    },
    autoGeneratedSQL() {
      if (this.params.sql) {
        this.sqlFieldLoading = true;
        const currentFieldsObj = this.params.advanced.self_dependency_config.fields;
        const currentFieldsKey = currentFieldsObj.map(item => item.field_name);

        this.bkRequest
          .httpRequest('dataFlow/autoGenerateSQL', {
            params: {
              sql: this.params.sql,
            },
          })
          .then(res => {
            if (res.result) {
              const fields = res.data;
              /** 删除sql中已经不存在的字段 */
              if (currentFieldsKey.length) {
                for (let i = 0; i < currentFieldsKey.length; i++) {
                  if (!fields.includes(currentFieldsKey[i])) {
                    const index = currentFieldsObj.findIndex(item => item.field_name === currentFieldsKey[i]);
                    this.removeSelfDepField(currentFieldsObj[index], index);
                  }
                }
              }
              fields.forEach(field => {
                /** 避免重复追加字段，做判断追加 */
                if (!currentFieldsKey.includes(field)) {
                  const addField = {
                    field_name: field,
                    field_type: 'string',
                    description: field,
                    validate: {
                      field_name: false,
                      field_type: false,
                      description: false,
                    },
                    edit: true,
                  };
                  this.addSelfDepField(addField);
                }
              });

              this.paramsValue.advanced.self_dependency_config.fields = _.sortBy(currentFieldsObj, obj => {
                return _.indexOf(fields, obj.field_name);
              });
            } else {
              this.getMethodWarning(res.message, res.code);
            }
          })
          ['finally'](() => {
            this.sqlFieldLoading = false;
          });
      }
    },
  },
};
</script>

<style lang="scss" scoped>
.node-basic-wrapper {
  ::v-deep .bk-form-item {
    width: 100% !important;
  }
}
</style>
