

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
        <div class="content-mid bk-common-form">
          <div class="bk-form-item">
            <div class="bk-form-content">
              <bkdata-checkbox v-model="syncParams.recovery_config.recovery_enable">
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
                :disabled="!syncParams.recovery_config.recovery_enable"
                :selected.sync="syncParams.recovery_config.recovery_times"
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
                :disabled="!syncParams.recovery_config.recovery_enable"
                :selected.sync="syncParams.recovery_config.recovery_interval"
                :settingKey="'id'"
                :displayKey="'name'" />
            </div>
          </div>
        </div>
        <div v-if="isOpenSelfDependency"
          class="content-down bk-common-form">
          <div class="bk-form-item">
            <div id="tooltip-content">
              <p>{{ $t('计算时把当前节点上一周期输出作为输入_可在SQL语句中引用当前节点数据输出表名') }}</p>
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
                {{ rtId }}
              </p>
            </div>
            <div class="bk-form-content">
              <bkdata-checkbox v-model="syncParams.self_dependence.self_dependency"
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
          <div v-if="syncParams.self_dependence.self_dependency"
            class="bk-form-item input-number">
            <label class="bk-label">
              {{ $t('依赖策略') }}
              <span class="required">*</span>
            </label>
            <div class="count-freq-unit"
              style="width: 310px">
              <bkdata-selector
                :list="selfDepRulesList"
                :selected.sync="syncParams.self_dependence.self_dependency_config.dependency_rule" />
            </div>
          </div>
          <OfflineNodeField
            v-if="syncParams.self_dependence.self_dependency"
            ref="filedForm"
            :fields="syncParams.self_dependence.self_dependency_config.fields"
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

<script lang="ts">
import OfflineNodeField from '@/pages/DataGraph/Graph/Children/components/OfflineNode/FieldComp.vue';
import { Component, InjectReactive, PropSync, Vue } from 'vue-property-decorator';

@Component({
  components: {
    OfflineNodeField,
  },
})
export default class OfflineAdvanced extends Vue {
  @InjectReactive() resultTableList: Array<object>;
  @InjectReactive() parentResultTables: Array<object>;
  @PropSync('params', { default: () => ({}) }) syncParams: object;

  numberRetryList: Array<object> = [];
  intervalList: Array<object> = [];
  selfDepRulesList: Array<object> = [
    {
      id: 'self_finished',
      name: this.$t('全部成功'),
    },
    {
      id: 'self_no_failed',
      name: this.$t('无失败'),
    },
  ];
  sqlFieldLoading: boolean = false;
  validate: object = {
    startTime: {
      status: false,
      errorMsg: '',
    },
    lastTime: {
      status: false,
      errorMsg: '',
    },
  };

  get isOpenSelfDependency() {
    return this.$modules.isActive('batch-self-dependency');
  }

  get rtId() {
    return this.resultTableList[0] && this.resultTableList[0].name;
  }

  created() {
    this.init();
  }

  init() {
    const list = [5, 10, 15, 30, 60];
    list.forEach(item => {
      this.intervalList.push({
        id: `${item}m`,
        name: `${item}分钟`,
      });
    });

    for (let i = 0; i < 3; i++) {
      let obj = { id: (i + 1).toString() };
      this.numberRetryList.push(obj);
    }
  }

  validateForm() {
    if (this.syncParams.self_dependence.self_dependency) {
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
  }

  clearFieldValidate(val) {
    !val && this.$refs.filedForm.clearStatus();
  }

  addSelfDepField(item) {
    this.syncParams.self_dependence.self_dependency_config.fields.push(item);
  }

  removeSelfDepField(item, index) {
    if (item.id) {
      const config = this.syncParams.self_dependence.self_dependency_config;
      this.syncParams.self_dependence.self_dependency_config.fields = config.filter(
        field => field.id !== item.id
      );
    } else {
      this.syncParams.self_dependence.self_dependency_config.fields.splice(index, 1);
    }
  }

  get sqlTipEg() {
    if (!this.rtId) return '';
    if (Object.keys(this.parentResultTables).length) {
      let sqlTable = this.parentResultTables[this.rtId].fields;
      let sql = 'select ';
      if (sqlTable.length) {
        for (let i = 0; i < 2; i++) {
          if (sqlTable[i] !== undefined) {
            sql += sqlTable[i].name;
          }
          i === 0 ? (sql += ',') : (sql += '...');
        }
        return sql;
      }
    }
  }

  autoGeneratedSQL() {
    if (this.syncParams.sql) {
      this.sqlFieldLoading = true;
      const currentFieldsObj = this.syncParams.self_dependence.self_dependency_config.fields;
      const currentFieldsKey = currentFieldsObj.map(item => item.field_name);

      this.bkRequest
        .httpRequest('dataFlow/autoGenerateSQL', {
          params: {
            sql: this.syncParams.sql,
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

            this.syncParams.self_dependence.self_dependency_config.fields = _.sortBy(currentFieldsObj, obj => {
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
  }
}
</script>

<style lang="scss" scoped>
.node-basic-wrapper {
  /deep/ .bk-form-item {
    width: 100% !important;
  }
}
</style>
