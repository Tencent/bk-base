

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
  <div class="split-output-wrapper">
    <div class="title">
      {{ $t('数据输出') }}
    </div>
    <bkdata-form ref="splitOutputForm"
      class="split-output-form"
      formType="vertical"
      :model="localParams">
      <div v-for="(item, index) in localParams.configList"
        :key="index"
        class="item-group mb10">
        <div class="item-content mr5">
          <bkdata-form-item
            class="inline-item mr10 mb0"
            :property="`configList.${index}.logic_exp`"
            :rules="rules.required">
            <bkdata-input
              v-model.trim="item.logic_exp"
              type="text"
              :placeholder="$t('例如field')"
              :title="item.logic_exp"
              :maxlength="255" />
          </bkdata-form-item>
          <bkdata-form-item
            class="inline-item mb0"
            :property="`configList.${index}.bk_biz_id`"
            :rules="rules.mustSelect">
            <bkdata-selector
              settingKey="bk_biz_id"
              searchKey="bk_biz_name"
              displayKey="display_bk_biz_id"
              :placeholder="$t('请选择')"
              :list="displayBizList"
              :searchable="true"
              :selected.sync="item.bk_biz_id" />
          </bkdata-form-item>
          <bkdata-form-item class="mt5 mb0"
            property="output.table_name"
            :rules="index === 0 ? rules.tableName : []">
            <bkdata-input
              v-model.trim="localParams.output.table_name"
              :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
              :title="localParams.output.table_name"
              :disabled="index !== 0 || hasNodeId"
              :maxlength="50"
              type="text" />
          </bkdata-form-item>
        </div>
        <div class="btns">
          <i
            v-if="localParams.configList.length > 1"
            class="bk-icon icon-minus-circle icon-btn mr5"
            @click="handleExpOption('reduce', index)" />
          <i
            v-if="index === localParams.configList.length - 1"
            class="bk-icon icon-plus-circle icon-btn"
            @click="handleExpOption('add', index)" />
        </div>
      </div>
      <bkdata-form-item
        style="width: 100%"
        property="output.output_name"
        :label="$t('中文名称')"
        :required="true"
        :rules="rules.required">
        <bkdata-input
          v-model.trim="localParams.output.output_name"
          :placeholder="$t('输出中文名')"
          :maxlength="50"
          type="text" />
      </bkdata-form-item>
    </bkdata-form>
  </div>
</template>

<script>
import { mapGetters } from 'vuex';
import mixin from '../../config/child.output.mixin.js';

export default {
  mixins: [mixin],
  props: {
    configList: {
      type: Array,
      default: () => [],
    },
    hasNodeId: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      localParams: {
        output: {
          table_name: '',
          output_name: '',
        },
        configList: [],
      },
      rules: {
        required: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
        ],
        mustSelect: [
          {
            required: true,
            message: this.$t('请选择业务'),
            trigger: 'blur',
          },
        ],
        tableName: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
          {
            regex: /^[A-Za-z][0-9a-zA-Z_]*$/,
            message: this.$t('格式不正确_内容由字母_数字和下划线组成_且以字母开头'),
            trigger: 'blur',
          },
        ],
      },
    };
  },
  computed: {
    ...mapGetters({
      allBizList: 'global/getAllBizList',
    }),
    displayBizList() {
      return (this.allBizList || []).map(item => {
        item.display_bk_biz_id = item.bk_biz_id + '_';
        return item;
      });
    },
  },
  watch: {
    configList: {
      immediate: true,
      deep: true,
      handler(values) {
        this.localParams.configList = (this.clone(values) || []).map(item => {
          item.bk_biz_id = String(item.bk_biz_id);
          return item;
        });
      },
    },
    params: {
      immediate: true,
      deep: true,
      handler(values) {
        values[0] && (this.localParams.output = this.clone(values[0]));
      },
    },
  },
  methods: {
    clone(value) {
      return JSON.parse(JSON.stringify(value));
    },
    validateTipsConfig(key) {
      const tips = {
        content: '',
        visible: '',
        class: 'error-red',
      };
      if (!key) return tips;

      const validate = this.localParams.output.validate;
      if (validate) {
        tips.content = validate[key].errorMsg;
        tips.visible = validate[key].status;
      }
      return tips;
    },
    handleExpOption(btnType, index) {
      if (btnType === 'add') {
        this.localParams.configList.push({
          bk_biz_id: '',
          logic_exp: '',
        });
      } else if (btnType === 'reduce') {
        this.localParams.configList.length > 1 && this.localParams.configList.splice(index, 1);
      }
    },
    initOutput() {
      this.params.length === 0 && this.processOutput();
    },
    async validateForm() {
      const result = await this.$refs.splitOutputForm
        .validate()
        .then(() => true)
        ['catch'](() => false);
      if (result) {
        this.params[0] = this.clone(this.localParams.output);
        this.$emit('update:configList', this.clone(this.localParams.configList));
      }
      return result;
    },
  },
};
</script>

<style lang="scss" scoped>
.split-output-wrapper {
  width: 100%;
  height: 100%;
  background: #fff;
  padding: 17px 24px 24px 24px;
  overflow-y: auto;
  border-right: 1px solid #dcdee5;
  .title {
    width: 64px;
    height: 22px;
    font-size: 16px;
    font-family: PingFangSC, PingFangSC-Medium;
    font-weight: bold;
    text-align: left;
    color: #313238;
    line-height: 22px;
    margin-bottom: 30px;
  }
  .split-output-form {
    .item-group {
      width: 100%;
      display: flex;
      align-items: center;
      &:first-child .item-content {
        padding-top: 0;
        border-top: none;
      }
    }
    .item-content {
      width: calc(100% - 34px);
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      padding-top: 10px;
      border-top: 1px solid #dcdee5;
    }
    .inline-item {
      flex: 0 0 calc(50% - 5px);
    }
    .btns {
      flex: 1;
      display: flex;
      align-items: center;
      .icon-btn {
        font-size: 14px;
        cursor: pointer;
      }
    }
  }
}
</style>
