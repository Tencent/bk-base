

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
  <div v-bkloading="{ isLoading: isLoading }"
    class="wrapper">
    <bkdata-selector :selected.sync="secretLevel"
      :disabled="disabled"
      :optionTip="true"
      :toolTipTpl="getOptionTpl"
      :list="secretLevelList"
      :settingKey="'id'"
      :displayKey="'name'"
      :allowClear="false"
      searchKey="name"
      @change="changeSecret" />
    <div class="manager-table">
      <span class="manager-title">{{ $t('管理人员') }}：</span>
      <div class="manager-table-form">
        <div class="manager-table-item">
          <label>数据管理员</label>
          <div class="manager-content">
            <div class="manager-name-container clearfix bk-scroll-y"
              :class="{ 'no-data': !manager.length }">
              <template v-if="manager.length">
                <span v-for="(name, index) in manager"
                  :key="index"
                  class="manager-name">
                  {{ name }}
                </span>
              </template>
              <template v-else>
                {{ $t('无') }}
              </template>
            </div>
          </div>
        </div>
        <div class="manager-table-item">
          <label>{{ bizRole }}</label>
          <div class="manager-content">
            <div class="manager-name-container clearfix bk-scroll-y">
              <template v-if="bizRoleMembers.length">
                <span v-for="(name, index) in bizRoleMembers"
                  :key="index"
                  class="manager-name">
                  {{ name }}
                </span>
              </template>
              <template v-else>
                {{ $t('无') }}
              </template>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    disabled: {
      type: Boolean,
      default: false,
    },
    sensitivity: {
      type: String,
      default: '',
    },
    bkBizId: {
      type: [Number, String],
      default: '',
    },
  },
  data() {
    return {
      isLoading: false,
      secretLevel: this.sensitivity,
      secretLevelList: [],
      isSecretLoading: false,
      // bizRole: ''
    };
  },
  computed: {
    dataSetId() {
      const ids = {
        raw_data: 'data_id',
        result_table: 'result_table_id',
        tdw_table: 'tdw_table_id',
      };
      return this.$route.query[ids[this.$route.query.dataType]];
    },
    bizRole() {
      if (!this.secretLevelList.length || !this.secretLevel) return '';
      return this.secretLevelList.find(item => item.id === this.secretLevel).biz_role_name;
    },
    bizRoleMembers() {
      if (!this.secretLevelList.length || !this.secretLevel) return [];
      return this.secretLevelList.find(item => item.id === this.secretLevel).biz_role_memebers;
    },
    manager() {
      return this.$attrs.manager || [];
    },
    isShowTips() {
      // 数据管理员和运维任务列表里，如果都不包含当前用户，需要显示提示语
      return ![...this.manager, ...this.bizRoleMembers].includes(this.$store.state.common.userName);
    },
  },
  watch: {
    sensitivity: {
      immediate: true,
      handler(val) {
        if (val) {
          this.secretLevel = val;
        }
      },
    },
    isShowTips: {
      immediate: true,
      handler(val) {
        this.$emit('changeWarnTipsShow', val);
      },
    },
    bkBizId: {
      immediate: true,
      handler(val) {
        if (val) {
          this.getSecretLevelList();
        }
      },
    },
  },
  methods: {
    changeSecret(id) {
      this.$emit('changeSecretLevel', id);
    },
    getSecretLevelList() {
      this.isSecretLoading = true;
      this.bkRequest
        .httpRequest('auth/getSecretLevelList', {
          query: {
            has_biz_role: true,
            bk_biz_id: this.bkBizId,
          },
        })
        .then(res => {
          if (res.result) {
            this.secretLevelList = res.data;
            this.secretLevelList.forEach(secretLevel => {
              secretLevel.disabled = !secretLevel.active;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isSecretLoading = false;
        });
    },
    getOptionTpl(option) {
      return `<span style="white-space: pre-line;">${option.description || option.name}</span>`;
    },
  },
};
</script>

<style lang="scss" scoped>
.wrapper {
  padding: 5px;
  border: 1px dashed #979ba5;
  .manager-table {
    .manager-title {
      display: inline-block;
      width: 97px;
      font-size: 14px;
      color: #63656e;
      font-weight: 700;
      padding: 5px 0 5px 15px;
      background-color: #e1ecff;
      margin: 10px 0 10px -20px;
      height: auto;
      line-height: normal;
      border-left: 4px solid #3a84ff;
    }
    .manager-table-form {
      border: 1px solid #d9dfe5;
      .manager-table-item {
        display: flex;
        line-height: 36px;
        border-bottom: 1px solid #d9dfe5;
        &:last-child {
          border-bottom: none;
        }
        label {
          width: 105px;
          text-align: center;
          border-right: 1px solid #d9dfe5;
          padding: 0 10px;
          color: #63656e;
          font-weight: bold;
        }
        .manager-content {
          display: flex;
          align-items: center;
          flex: 1;
          padding: 10px 10px 10px 15px;
          ::v-deep .bk-tag-selector {
            width: 100%;
          }
          .manager-name-container {
            padding-bottom: 4px;
            height: 78px;
            .manager-name {
              float: left;
              padding: 0px 5px;
              border-radius: 2px;
              background: #f7f7f7;
              border: 1px solid #dbe1e7;
              color: #737987;
              line-height: 20px;
              font-size: 12px;
              margin: 4px 3px 0px 0px;
            }
          }
          .no-data {
            line-height: normal;
            padding-bottom: 0;
          }
        }
      }
    }
  }
}
</style>
