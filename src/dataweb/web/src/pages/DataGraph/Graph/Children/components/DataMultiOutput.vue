

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
  <div class="multiple-output-wrapper">
    <div class="title">
      {{ $t('数据输出') }}
    </div>
    <bkdata-collapse v-model="activeOutput"
      accordion>
      <bkdata-collapse-item
        v-for="(output, index) in params"
        :key="index"
        :name="index.toString()"
        :customTriggerArea="true">
        {{ output.bk_biz_id }}_{{ output.table_name }}
        <span slot="no-trigger"
          class="no-trigger-area">
          <bkdata-popconfirm
            placement="top"
            extPopoverCls="popover-delete"
            trigger="click"
            @confirm="deleteItem(index)">
            <i class="icon-delete ml10" />
          </bkdata-popconfirm>
        </span>
        <div slot="content"
          class="content-wrapper">
          <bkdata-form formType="vertical">
            <OutputBaseForm ref="outputFields"
              :bizList="bizList"
              :output="output" />
            <bkdata-form-item :label="$t('字段设置')"
              :required="true"
              extCls="output-field">
              <OutputField
                :fields.sync="output.fields"
                @vailidateField="checkFieldContent"
                @addField="output.validate.field_config.status = false" />
              <div
                v-show="output.validate.field_config.status"
                class="help-block"
                v-text="output.validate.field_config.errorMsg" />
            </bkdata-form-item>
          </bkdata-form>
        </div>
      </bkdata-collapse-item>
    </bkdata-collapse>
    <div v-bk-tooltips="addTooltip"
      class="add-new-output"
      :class="{ disabled: isAddDisabled }"
      @click="addOutput">
      <i class="icon-plus" />
      <span class="text">{{ '新增结果数据表' }}</span>
    </div>
  </div>
</template>
<script>
import OutputBaseForm from './OutputBaseForm';
import OutputField from './OutputField';
import mixin from '../config/child.output.mixin.js';
export default {
  components: {
    OutputField,
    OutputBaseForm,
  },
  mixins: [mixin],
  data() {
    return {
      activeOutput: '0',
      validate: {
        table_name: {
          // 输出编辑框 数据输出
          status: false,
          errorMsg: '',
        },
        output_name: {
          // 输出编辑框 中文名称
          status: false,
          errorMsg: '',
        },
      },
    };
  },
  computed: {
    isAddDisabled() {
      return this.params.length >= 1;
    },
    addTooltip() {
      return {
        content: this.isAddDisabled ? this.$t('暂不支持多输出') : '',
      };
    },
  },
  methods: {
    deleteItem(index) {
      if (this.params.length === 1) return;
      this.params.splice(index, 1);
      this.activeOutput = index === 0 ? '0' : (index - 1).toString();
    },
    // 输出中文名表单验证
    checkOutputChineseName(data) {
      if (data.output_name) {
        let val = data.output_name.trim();
        if (val.length < 51 && val.length !== 0) {
          data.validate.output_name.status = false;
          return true;
        } else {
          data.validate.output_name.status = true;
          if (val.length === 0) {
            data.validate.output_name.errorMsg = this.validator.message.required;
          } else {
            data.validate.output_name.errorMsg = this.validator.message.max50;
          }
          return false;
        }
      } else {
        data.validate.output_name.status = true;
        data.validate.output_name.errorMsg = this.validator.message.required;
        return false;
      }
    },
    /*
     *   检验计算名称
     *   验证内容由字母、数字和下划线组成，且以字母开头&内容不为空
     */
    checkDataFormat(data, key) {
      let val = data[key].trim();
      let reg = new RegExp('^[0-9a-zA-Z_]{1,}$');
      if (this.validator.validWordFormat(val)) {
        data.validate.table_name.status = false;
      } else {
        data.validate.table_name.status = true;
        if (val.length === 0) {
          data.validate.table_name.errorMsg = this.validator.message.required;
        } else {
          data.validate.table_name.errorMsg = this.validator.message.wordFormat;
        }
      }
      return !data.validate.table_name.status;
    },
    validateFieldFormat(val, reg, reg2) {
      if (reg.test(val) || reg2.test(val)) {
        return true;
      }
      return false;
    },
    checkFieldContent(data) {
      data = data || this.params[this.activeOutput];
      let result = true;

      /** 检验filed是否为空 */
      if (data.fields.length === 0) {
        data.validate.field_config.status = true;
        return;
      }
      data.validate.field_config.status = false;

      /** filed不为空，深层次校验 */
      const deduplicatedObj = {}; // 去重检测
      data.fields.forEach(item => {
        if (this.validateFieldFormat(item.field_name, /^[a-zA-Z_][a-zA-Z0-9_]*$/, /^\$f\d+$/)) {
          if (deduplicatedObj.hasOwnProperty(item.field_name)) {
            item.validate.name.errorMsg = this.$t('字段名称不可重复');
            item.validate.name.status = true;
          } else {
            deduplicatedObj[item.field_name] = true;
            item.validate.name.status = false;
          }
        } else {
          item.validate.name.status = true;
          if (item.field_name.length === 0) {
            item.validate.name.errorMsg = this.validator.message.required;
          } else {
            item.validate.name.errorMsg = this.validator.message.wordFormat;
          }
        }
        item.validate.alias.status = item.field_alias === '';
        if (item.validate.name.status || item.validate.alias.status) {
          result = false;
        }
      });
      return result;
    },
    async validateForm() {
      let validateResult = true;
      for (let i = 0; i < this.params.length; i++) {
        const baseFormValidate = await this.$refs.outputFields[i].validateForm();
        const fieldsResult = await this.checkFieldContent(this.params[i]);

        const result = [baseFormValidate, fieldsResult].every(item => item === true);
        if (!result) {
          validateResult = false;
          this.activeOutput = i.toString();
          break;
        }
      }

      return validateResult;
    },
  },
};
</script>
<style lang="scss" scoped>
.multiple-output-wrapper {
  width: 100%;
  height: 100%;
  background: #f5f6fa;
  padding: 17px 24px 24px 24px;
  overflow-y: auto;
  .title {
    width: 64px;
    height: 22px;
    font-size: 16px;
    font-family: PingFangSC, PingFangSC-Medium;
    font-weight: 500;
    text-align: left;
    color: #313238;
    line-height: 22px;
    margin-bottom: 30px;
  }
  ::v-deep .bk-collapse-item {
    margin-bottom: 10px;
    background: #fff;
    position: relative;
    .no-trigger-area {
      position: absolute;
      right: 20px;
      top: -1px;
      cursor: pointer;
      &:hover {
        color: #3a84ff;
      }
    }
    .bk-collapse-item-content {
      padding-bottom: 30px;
    }
    .content-wrapper {
      ::v-deep .bk-form-item {
        width: 100% !important;
        &.output-field .bk-form-content {
          flex-direction: column;
          align-items: flex-start;
        }
      }
      .rt-wrapper {
        width: 100%;
        display: flex;
      }
    }
  }
  .add-new-output {
    width: 100%;
    height: 48px;
    background: #fafbfd;
    border: 1px dashed #c4c6cc;
    border-radius: 2px;
    color: #979ba5;
    display: flex;
    justify-content: center;
    align-items: center;
    cursor: pointer;
    &:hover {
      color: #3a84ff;
      background: #e1ecff;
    }
    .icon-plus {
      font-size: 32px;
    }
    .text {
      font-size: 14px;
    }
  }
}
</style>
