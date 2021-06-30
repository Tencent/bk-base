

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
  <div v-bkloading="{ isLoading: loading }"
    class="pop-project">
    <a v-if="showClose"
      href="javascript:;"
      class="close-pop"
      @click="closePop">
      <i class="bk-icon icon-close" />
    </a>
    <p class="title">
      {{ $t('编辑项目') }}
    </p>
    <div class="table-wrapper">
      <table class="table">
        <tbody>
          <tr>
            <td width="147">
              <div class="list-title">
                {{ $t('项目名称') }}
              </div>
            </td>
            <td width="276">
              <div class="list-detail bk-form-item">
                <input
                  v-model="data.project_name"
                  type="text"
                  placeholder="请输入"
                  maxLength="50"
                  :class="['bk-form-input', { 'is-danger': data.project_name.trim().length === 0 }]">
                <div v-if="data.project_name.trim().length === 0"
                  class="error-tip">
                  {{ validator.message.project }}
                </div>
              </div>
            </td>
            <td width="147">
              <div class="list-title">
                {{ $t('项目ID') }}
              </div>
            </td>
            <td>
              <div class="list-detail bk-form">
                {{ data.project_id }}
              </div>
            </td>
          </tr>
          <tr>
            <td>
              <div class="list-title">
                {{ $t('创建人') }}
              </div>
            </td>
            <td>
              <div class="list-detail bk-form">
                {{ data.created_by }}
              </div>
            </td>
            <td>
              <div class="list-title"
                :title="$t('创建时间')">
                {{ $t('创建时间') }}
              </div>
            </td>
            <td>
              <div class="list-detail bk-form">
                {{ data.created_at }}
              </div>
            </td>
          </tr>
          <tr>
            <td>
              <div class="list-title">
                {{ $t('项目描述') }}
              </div>
            </td>
            <td colspan="3">
              <div class="list-detail bk-form-item">
                <textarea
                  v-model="data.description"
                  v-tooltip.notrigger="{
                    content: $t('不能超过50个字'),
                    visible: isVerifyPass,
                    class: 'error-red',
                  }"
                  maxlength="50"
                  :class="['bk-form-textarea', { 'is-danger': data.description.trim().length === 0 }]" />
                <div v-if="data.description.trim().length === 0"
                  class="error-tip">
                  {{ validator.message.required }}
                </div>
              </div>
            </td>
          </tr>
        </tbody>
        <!-- TDW支持组件 -->
        <component :is="tdwFragments.componment"
          v-bind="tdwFragments.bindAttr"
          @changeCheckTdw="changeCheckTdw"
          @tipsClick="linkToUserAccount" />
      </table>
    </div>
  </div>
</template>

<script>
import Bus from '@/common/js/bus.js';
import tdwPower from '@/pages/DataGraph/Common/tdwPower.js';
import extend from '@/extends/index';

export default {
  mixins: [tdwPower],
  props: {
    editData: {
      type: Object,
      default() {
        return {};
      },
    },
    showClose: {
      type: Boolean,
      dafault: true,
    },
  },
  data() {
    return {
      data: {
        project_name: '',
        description: '',
        tdw_app_groups: [],
        tdw_app_groups_value: [],
      },
      isVerifyPass: false,
      loading: true,
    };
  },
  computed: {
    /** 获取TDW支持组件 */
    tdwFragments() {
      const tdwFragments = extend.getVueFragment('tdwFragment', this) || [];
      return tdwFragments.filter(item => item.name === 'TdwSupportTpl')[0];
    },
  },
  watch: {
    'data.description': {
      deep: true,
      immediate: true,
      handler(val) {
        if (val.length === 50) {
          this.isVerifyPass = true;
        } else {
          this.isVerifyPass = false;
        }
      },
    },
  },
  mounted() {
    Bus.$on('closeEditPop', () => {
      this.closePop();
    });
  },
  methods: {
    async changeCheckTdw(checked) {
      const enableTdw = checked;
      this.enableTdw = enableTdw;
      if (enableTdw) {
        this.tdwPowerLoading = true;
        await this.requestApplication();
      } else {
        this.data.tdw_app_groups_value = [];
        this.hasPower = '';
      }
    },
    async requestApplication() {
      // 以应用组结果作为权限 修改项目时需要对这个列表进行权限disable
      const res = await this.$store.dispatch('tdw/getApplicationGroup');
      if (res.result) {
        this.hasPower = 'hasPower';
        this.applicationGroupList.splice(0);
        let group = !this.editData?.tdw_app_groups ? [] : [...this.editData.tdw_app_groups];
        group = group.map(item => {
          return {
            app_group_name: item,
          };
        });
        this.applicationGroupList = [...res.data];
        group.map(item => {
          const repeat = this.applicationGroupList.find(app => app.app_group_name === item.app_group_name);
          if (!repeat) {
            item.disabled = true;
            this.applicationGroupList.push(item);
          }
        });
      } else {
        this.hasPower = 'notPower';
      }

      this.tdwPowerLoading = false;
    },
    linkToUserAccount() {
      this.closePop();
      this.$router.push({ path: '/user-center', query: { tab: 'account' } });
    },
    closePop() {
      this.reset();
      this.isVerifyPass = false;
      this.$emit('close-pop');
    },
    getOptions() {
      return {
        data: {
          project_name: this.data.project_name.trim(),
          description: this.data.description.trim(),
          tdw_app_groups: this.data.tdw_app_groups_value,
        },
        enableTdw: this.enableTdw,
        loading: true,
      };
    },
    async init() {
      if (this.editData.tdw_app_groups && this.editData.tdw_app_groups.length) {
        this.hasPower = 'hasPower';
        this.enableTdw = true;

        await this.changeCheckTdw(this.enableTdw);

        this.$nextTick(() => {
          this.data.tdw_app_groups_value = this.editData.tdw_app_groups;
        });
      } else {
        this.data.tdw_app_groups_value = [];
        this.hasPower = '';
        this.enableTdw = false;
      }
      Object.assign(this.data, JSON.parse(JSON.stringify(this.editData)));
      this.loading = false;
    },
    reset() {
      this.loading = true;
      this.hasPower = '';
      this.data.tdw_app_groups = [];
      this.data.tdw_app_groups_value = [];
    },
  },
};
</script>

<style media="screen" lang="scss">
.is-danger {
  border-color: #ff5656;
  background-color: #fff4f4;
  color: #ff5656;
}
// .error-red{
//     z-index: 2000!important;
// }
.bk-tooltips {
  z-index: 10000;
}
.bk-form-item {
  .bk-label-checkbox {
    line-height: 32px;
    .bk-form-checkbox {
      margin-right: 10px;
    }
    .bk-checkbox-text {
      font-style: normal;
    }
  }
}
.pop-project {
  padding: 20px;
  .table-wrapper {
    width: 100%;
    max-height: 400px;
    &::-webkit-scrollbar {
      width: 5px;
      background-color: #f5f5f5;
    }
    &::-webkit-scrollbar-track {
      -webkit-box-shadow: inset 0 0 6px #eee;
      box-shadow: inset 0 0 6px #eee;
      border-radius: 10px;
      background-color: #f5f5f5;
    }
    &::-webkit-scrollbar-thumb {
      border-radius: 10px;
      background-color: rgba(0, 0, 0, 0.3);
    }
    &::-webkit-scrollbar-thumb {
      border-radius: 10px;
      background-color: rgba(0, 0, 0, 0.3);
    }
  }
  .table {
    margin-top: 25px;
    width: 100%;
    border: 1px solid #e6e9f0;
    border-collapse: collapse;
    padding: 0;
    tr {
      border-bottom: 1px solid #e6e9f0;
      &:last-of-type {
        border: none;
        .list {
          width: 100%;
        }
      }
    }
    td {
      .bk-form-item {
        display: flex;
        .bk-form-content {
          width: 100%;
          display: block;
        }
      }
      &:nth-child(2n + 1) {
        background: #fafbfd;
        &:hover {
          background: #f0f1f5;
        }
      }
      border-bottom: 1px solid #e6e9f0;
      padding: 8px;
      .list-title {
        text-align: right;
        padding: 0 15px;
        color: #737987;
      }
      .list-detail {
        float: left;
        line-height: 44px;
        input {
          width: 260px;
          color: #212232;
        }
        textarea {
          width: 680px;
          min-height: 100px;
          line-height: 24px;
          color: #212232;
        }
      }
      .td-link-notpower {
        vertical-align: middle;
        text-decoration: underline;
        line-height: 34px;
      }
      .error-tip {
        width: 100%;
        color: red;
        line-height: 20px;
      }
    }
  }
}
</style>
