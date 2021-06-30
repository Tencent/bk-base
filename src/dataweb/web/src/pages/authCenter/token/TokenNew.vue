

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
  <Layout :showHead="false"
    :showSubNav="true"
    class="token-manage-form">
    <template slot="subNav">
      <AuthNav :activeName="'TokenManagement'" />
    </template>

    <div class="sub-nav-info">
      <router-link class="link"
        to="/auth-center/token-management">
        {{ $t('授权列表') }}
      </router-link>
      > {{ title }}
    </div>

    <FormRow class="row mb15"
      :collspan="false"
      :step="1"
      :stepName="$t('选择应用')">
      <TokenAppStep v-model="params.data_token_bk_app_code"
        :disabled="!!isEdit" />
    </FormRow>

    <FormRow class="row mb15"
      :collspan="false"
      :step="2"
      :stepName="$t('选择功能权限')">
      <TokenObjectTypesStep :selectedScopes.sync="selectedScopes" />
    </FormRow>

    <FormRow class="row mb15"
      :collspan="false"
      :step="3"
      :stepName="$t('选择数据范围')">
      <TokenObjectScopesStep
        ref="TokenObjectScopesStepComp"
        :selectedScopes="selectedScopes"
        :isCollapse="true"
        :collapsesStatus="true"
        :selectedPermissions.sync="selectedPermissions" />
    </FormRow>

    <FormRow class="row mb15"
      :collspan="false"
      :step="4"
      :stepName="$t('申请授权')">
      <TokenConfirmStep ref="confirmComp"
        @setExpire="setExpire"
        @setReason="setReason" />
    </FormRow>

    <div class="submit">
      <bkdata-button theme="primary"
        class="submit-btn"
        :loading="isLoading"
        @click="submit">
        {{ $t('提交') }}
      </bkdata-button>
    </div>
  </Layout>
</template>

<script>
import { mapGetters, mapState } from 'vuex';
import Layout from '@/components/global/layout';
import FormRow from './components/FormRow';
import { postMethodWarning, showMsg } from '@/common/js/util';
import { submitNewToken, updateToken } from '@/common/api/auth.js';

import { AuthNav } from '../parts/index.js';
import TokenAppStep from './components/TokenAppStep';
import TokenObjectTypesStep from './components/TokenObjectTypesStep';
import TokenObjectScopesStep from './components/TokenObjectScopesStep';
import TokenConfirmStep from './components/TokenConfirmStep';

export default {
  components: {
    Layout,
    FormRow,
    TokenAppStep,
    TokenObjectTypesStep,
    TokenObjectScopesStep,
    TokenConfirmStep,
    AuthNav,
  },
  data() {
    return {
      isLoading: false,
      scopes: [],
      steps: {
        controllable: false,
        steps: [
          {
            title: this.$t('选择应用'),
            icon: 1,
          },
          {
            title: this.$t('选择功能权限'),
            icon: 2,
          },
          {
            title: this.$t('选择数据范围'),
            icon: 3,
          },
          {
            title: this.$t('确认提交'),
            icon: 4,
          },
        ],
        curStep: 1,
      },
      params: {
        data_token_bk_app_code: '',
        data_scope: {
          is_all: false,
          permissions: [],
        },
        reason: '',
        expire: 365,
      },
      selectedScopes: [],
      selectedPermissions: [],
    };
  },
  computed: {
    ...mapState({
      isEdit: state => state.auth.isEdit,
    }),
    title() {
      return this.isEdit ? this.$t('授权码新增权限') : this.$t('新增授权码');
    },
  },

  mounted() {
    this.$store.dispatch('auth/actionSetEditState', this.$route.name === 'TokenEdit');
    if (this.isEdit) {
      this.$store.dispatch('auth/actionLoadToken', this.$route.params.token_id);
    }
  },
  methods: {
    nextStep() {
      if (this.steps.curStep < this.steps.steps.length && this.check(this.steps.curStep)) {
        this.steps.curStep++;
      }
    },
    checkValid() {
      // 重置报错弹窗信息
      const bkMsgIns = document.querySelector('.bk-message');
      bkMsgIns && bkMsgIns.remove();

      // 应用必选
      if (!this.params.data_token_bk_app_code) {
        showMsg(this.$t('请选择应用'), 'warning');
        return false;
      }

      // 权限对象必选
      if (!this.selectedScopes.length) {
        showMsg(this.$t('请至少选择一个权限对象'), 'warning');
        return false;
      }

      /**
       * 每个权限对象的对象范围必填
       * { hasInstanceObjCount } has_instance 为 true的对象个数
       * { settedInstanceScopeCount } 已设置对象范围的对象个数
       *      当 hasInstanceObjCount === settedInstanceScopeCount 则说明每个存在对象范围实例的对象均设置了对象范围
       *      反之，则存在未设置对象范围的对象
       */
      const selectedObjItem = [];
      this.selectedPermissions.forEach(item => {
        !selectedObjItem.includes(item.action_id) && selectedObjItem.push(item.action_id);
      });
      const hasInstanceObjCount = this.selectedScopes.filter(item => item.has_instance).length;
      const settedInstanceScopeCount = selectedObjItem.length;

      if (hasInstanceObjCount !== settedInstanceScopeCount) {
        showMsg(this.$t('请选择数据范围'), 'warning');
        return false;
      }

      // 申请理由为必填项
      this.$refs.confirmComp.checkValid();
      if (!this.params.reason.trim()) {
        showMsg(this.$t('请填写申请理由'), 'warning');
        return false;
      }
      return true;
    },
    previousStep() {
      if (this.steps.curStep > 1) {
        this.steps.curStep--;
      }
    },
    submit() {
      this.clean_permissions();
      // 先校验填写
      if (this.checkValid()) {
        this.isLoading = true;
        if (this.isEdit) {
          updateToken(this.$route.params.token_id, this.params)
            .then(res => {
              if (res.result) {
                this.$store.dispatch('auth/actionResetSelection');
                postMethodWarning(this.$t('提交成功_跳转到授权管理中'), 'success');
                this.$router.push({ name: 'TokenManagement' });
              } else {
                postMethodWarning(res.message, 'error');
              }
            })
            ['finally'](res => {
              this.isLoading = false;
            });
        } else {
          submitNewToken(this.params)
            .then(res => {
              if (res.result) {
                this.$store.dispatch('auth/actionResetSelection');
                postMethodWarning(this.$t('提交成功_跳转到授权管理中'), 'success');
                this.$router.push({ name: 'TokenManagement' });
              } else {
                postMethodWarning(res.message, 'error');
              }
            })
            ['finally'](res => {
              this.isLoading = false;
            });
        }
      }
    },
    clean_permissions() {
      this.params.data_scope.permissions = [];
      // 清洗需要提交的权限
      this.selectedScopes.forEach(scope => {
        this.selectedPermissions.forEach(permission => {
          if (scope.action_id === permission.action_id && scope.object_class === permission.object_class) {
            this.params.data_scope.permissions.push(permission);
          }
        });
      });
    },
    setReason(reason) {
      this.params.reason = reason;
    },
    setExpire(expire) {
      this.params.expire = expire;
    },
  },
};
</script>

<style lang="scss">
.token-manage-form {
  .bk-form {
    .bk-form-item {
      .bk-label {
        width: 120px;
      }
      .bk-form-content {
        margin-left: 130px;
      }
    }
  }
}
</style>

<style lang="scss" scoped>
::v-deep .bk-card {
  margin-left: 48px;
  width: 560px;
  .to-be-click {
    color: #3a84ff;
  }
}
::v-deep .notChoose {
  margin-left: 48px;
}
.new-token {
  .steps-box {
    position: relative;
    padding: 20px;
    background-color: #fafafa;

    .steps-label {
      float: left;
      margin-top: 20px;
    }
    .steps-options {
      margin-left: 80px;
    }
  }

  .steps-content {
    padding: 20px;
    border: 1px solid #ccc;

    .step {
      width: 600px;
      margin-left: auto;
      margin-right: auto;
    }
  }
  .bk-form-item {
    margin-top: 20px;
    .step-controller {
      width: 200px;
      margin-left: auto;
      margin-right: auto;
    }
  }
}

.submit {
  text-align: center;
  margin: 50px 0;

  .submit-btn {
    padding: 10px 50px;
    font-size: 18px;
    height: auto;
    line-height: 1;
  }
}

.sub-nav-info {
  padding: 5px 12px 15px;
  position: relative;

  .link {
    color: #303133;
    font-weight: bold;

    &:hover {
      color: #3a84ff;
    }
  }

  &:before {
    content: '';
    position: absolute;
    left: 0;
    top: 7px;
    width: 3px;
    height: 16px;
    background: #3a84ff;
  }
}

.row,
.sub-nav-info {
  width: 1180px;
  margin: 0 auto;
}
</style>
