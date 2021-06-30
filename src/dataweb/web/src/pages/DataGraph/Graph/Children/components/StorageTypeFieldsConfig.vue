

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
  <div id="tableField"
    v-bkloading="{ isLoading: loading }">
    <bkdata-table :border="true"
      :data="fieldConfig.fields"
      :emptyText="$t('暂无数据')">
      <bkdata-table-column :label="$t('字段名称')"
        prop="physical_field" />
      <bkdata-table-column :label="$t('类型')"
        prop="physical_field_type" />
      <bkdata-table-column :label="$t('描述')">
        <div slot-scope="props"
          :title="props.row.field_alias">
          {{ props.row.field_alias }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column v-if="withConfiguration"
        :label="$t('配置项')">
        <div v-if="withConfiguration"
          slot-scope="props"
          class="bk-table-inlineblock">
          <template v-for="(conf, _index) in props.row.configs">
            <bkdata-checkbox
              :key="_index"
              :disabled="conf.isReadonly || readonly"
              :value="conf.field"
              :checked="conf.checked"
              :name="props.row.physical_field"
              @change="handleItemConfChanged($event, conf, props.row, fieldConfig.opt)">
              {{ conf.value }}
            </bkdata-checkbox>
          </template>
        </div>
      </bkdata-table-column>
    </bkdata-table>
  </div>
</template>
<script>
export default {
  model: {
    prop: 'value',
    event: 'change',
  },
  props: {
    shouldProcessFields: {
      type: Boolean,
      default: false,
    },
    openDedeplication: {
      type: Boolean,
      default: false,
    },
    readonly: {
      type: Boolean,
      default: false,
    },
    value: {
      type: Object,
      default: () => ({}),
    },
    isNewform: {
      type: Boolean,
      default: true,
    },
    rtId: {
      type: String,
      default: '',
    },
    clusterType: {
      type: String,
      default: '',
    },
    forceAll: {
      type: Boolean,
      default: false,
    },
    /** 包含字段配置项 */
    withConfiguration: {
      type: Boolean,
      default: false,
    }
  },
  data() {
    return {
      fieldConfig: {
        fields: [],
      },
      originField: {},
      loading: false,
      isRender: true,
    };
  },
  watch: {
    rtId: function (val) {
      val && this.getSchemaAndSql();
    },
  },
  mounted() {
    this.rtId && this.getSchemaAndSql();
  },
  methods: {
    processFields(val, config) {
      config && config.has_unique_key && this.$emit('uniqueKeyChange', true);

      if (val) {
        this.fieldConfig = JSON.parse(JSON.stringify(this.originField));
      } else {
        this.fieldConfig.fields.forEach(item => {
          item.configs = item.configs.filter(config => config.key !== 'storage_keys');
        });
      }
    },
    handleItemConfChanged(value, conf, item, opt = 'radio') {
      if (!this.isRender) {
        if (/radio/i.test(opt)) {
          item.configs.forEach(it => {
            if (it.checked) {
              it.checked = false;
              this.cleanConfig(it.key, item.physical_field, false);
            }
          });
        }
        const isChecked = value;
        this.cleanConfig(conf.key, item.physical_field, isChecked);
        this.$set(conf, 'checked', isChecked);
        this.$emit('change', this.fieldConfig);
      }
    },
    cleanConfig(key, fieldName, isChecked = true) {
      if (isChecked) {
        if (this.fieldConfig.config[key].includes(fieldName)) {
          return;
        }
        this.fieldConfig.config[key].push(fieldName);
      } else {
        this.$set(
          this.fieldConfig.config,
          key,
          this.fieldConfig.config[key]
            .join(',')
            .replace(fieldName, '')
            .replace(',,', ',')
            .replace(/^,|,$/, '')
            .split(',')
            .filter(item => item !== '')
        );
      }
    },
    getSchemaAndSql() {
      this.loading = true;
      this.isRender = true;
      let options = {
        params: {
          rtid: this.rtId,
          clusterType: this.clusterType,
        },
      };

      if (this.isNewform || this.forceAll) {
        Object.assign(options, { query: { flag: 'all' } });
      }
      this.bkRequest
        .httpRequest('meta/getSchemaAndSqlFilterCluster', options)
        .then(res => {
          if (res.result) {
            this.$set(this, 'fieldConfig', res.data);
            this.fieldConfig.fields.forEach(item => {
              item.configs.forEach(conf => {
                conf.checked && this.fieldConfig.config[conf.key].push(item.field_name);
              });
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading = false;
          this.$emit('onSchemaAndSqlComplete', this.fieldConfig);
          this.originField = JSON.parse(JSON.stringify(this.fieldConfig));
          this.shouldProcessFields && this.processFields(this.openDedeplication, this.fieldConfig.config);

          if (Object.prototype.hasOwnProperty.call(this.fieldConfig.config, 'has_unique_key')) {
            delete this.fieldConfig.config.has_unique_key; // 删除属性，防止在修改时对外层属性影响
          }

          this.$emit('change', this.fieldConfig);

          this.$nextTick(() => {
            this.isRender = false;
          });
        });
    },
  },
};
</script>
<style lang="scss" scoped>
.bk-form-checkbox {
  margin-right: 0;
}
</style>
