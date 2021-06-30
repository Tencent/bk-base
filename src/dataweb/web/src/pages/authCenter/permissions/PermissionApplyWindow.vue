

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
  <div class="bkdata-dialog">
    <bkdata-dialog v-model="isShow"
      extCls="bkdata-dialog-1"
      :loading="loading.submitLoading"
      :draggable="false"
      :hasHeader="false"
      :hasFooter="true"
      :closeIcon="true"
      :maskClose="false"
      :okText="$t('提交')"
      :cancelText="$t('取消')"
      :width="700"
      @confirm="submitApplication"
      @cancel="closeDialog">
      <div>
        <div class="title app-title">
          {{ $t('申请权限') }}
        </div>

        <div class="bkdata-form-horizontal apply-form">
          <div v-if="showWarning"
            class="form-item">
            <bkdata-alert type="error"
              :title="`项目【${objectId}】权限不足，请申请权限`" />
          </div>
          <div class="form-item">
            <label class="label-title">{{ $t('权限对象') }}：</label>
            <div class="bk-form-content">
              <ObjectTypeSelector :selectedType="selectedType"
                :objectType.sync="selectedValue.objectClass" />
            </div>
          </div>
          <div class="item-inline-wrap">
            <div v-if="isProjectSelectorShow"
              class="form-item item-inline">
              <label class="label-title">{{ $t('业务名称') }}：</label>
              <div v-bkloading="{ isLoading: loading.bizLoading }"
                class="bk-form-content">
                <bkdata-selector :selected.sync="selectedValue.bizId"
                  :filterable="true"
                  :searchable="true"
                  :list="bizList"
                  :settingKey="'bk_biz_id'"
                  :displayKey="'bk_biz_name'"
                  :allowClear="false"
                  :searchKey="'bk_biz_name'"
                  :placeholder="placeHolder.biz"
                  :disabled="disabledSelector.biz" />
              </div>
            </div>
            <div class="form-item item-inline"
              :class="{ flex100: !isProjectSelectorShow }">
              <label class="label-title">{{ $t('对象名称') }}：</label>
              <div v-bkloading="{ isLoading: loading.objectListLoading }"
                class="bk-form-content">
                <bkdata-selector :selected.sync="selectedValue.scopeId"
                  :filterable="true"
                  :searchable="true"
                  :list="objectList"
                  :settingKey="'id'"
                  :displayKey="'name'"
                  :allowClear="false"
                  :searchKey="'name'"
                  :placeholder="placeHolder.scope"
                  :disabled="disabledSelector.scope" />
              </div>
            </div>
          </div>

          <div class="form-item">
            <label class="label-title">{{ $t('申请角色') }}：</label>
            <div class="bk-form-content">
              <div class="apply-rule-select">
                <bkdata-selector :selected.sync="selectedValue.roleId"
                  :filterable="true"
                  :list="roleList"
                  :settingKey="'role_id'"
                  :displayKey="'role_name'"
                  :allowClear="false"
                  :placeholder="placeHolder.role"
                  :defineEmptyText="placeHolder.role"
                  :disabled="disabledSelector.role" />

                <template v-if="isShow">
                  <RoleWrapContent :showRoleName="false"
                    :roleId="selectedValue.roleId"
                    :isShow="isShow"
                    :accessObjects="accessObjects" />
                </template>
              </div>
            </div>
          </div>
          <div class="form-item">
            <label class="label-title">{{ $t('申请理由') }}：</label>
            <div class="bk-form-content">
              <textarea v-model="reason"
                class="bk-form-textarea" />
            </div>
          </div>
        </div>
      </div>
    </bkdata-dialog>
  </div>
</template>

<script>
import { postMethodWarning, showMsg } from '@/common/js/util.js';
import { queryAuthScope, submitApplication } from '@/common/api/auth.js';
import { ObjectTypeSelector, RoleWrapContent } from '../parts/index.js';
import { mapGetters } from 'vuex';

export default {
  components: {
    ObjectTypeSelector,
    RoleWrapContent,
  },
  props: {
    showWarning: {
      type: Boolean,
      default: false,
    },
    isOpen: {
      type: Boolean,
      default: false,
    },
    objectId: {
      type: [String, Number],
      default: '',
    },
    defaultSelectValue: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      loading: {
        submitLoading: false,
        objectListLoading: false,
      },
      isProjectSelectorShow: false,
      placeHolder: {
        role: '',
        scope: '',
        biz: this.$t('请选择'),
      },
      disabledSelector: {
        role: false,
        scope: false,
        biz: false,
      },

      selectedValue: {
        objectClass: '',
        scopeId: '',
        roleId: '',
        bizId: '',
      },
      reason: '',

      objectList: [],
      roleList: [],
      allRoleList: [],
      isShow: this.isOpen,

      // 需要支持业务过滤实例列表的对象类型
      withBizObjectClass: ['raw_data', 'result_table'],
      selectedType: '',
      accessObjects: '',
    };
  },
  computed: {
    ...mapGetters({
      allBizList: 'global/getAllBizList',
    }),
    bizList() {
      return this.allBizList;
    },
  },
  watch: {
    defaultSelectValue: {
      immediate: true,
      handler(val) {
        Object.assign(this.selectedValue, val);
      },
    },
    objectId: {
      immediate: true,
      handler(val) {
        val && this.$set(this.selectedValue, 'scopeId', Number(val));
      },
    },
    isOpen(newVal) {
      this.isShow = newVal;
    },
    isShow(newVal) {
      this.$emit('update:isOpen', newVal);
    },
    'selectedValue.objectClass': {
      immediate: true,
      handler(newVal) {
        this.accessObjects = newVal;
        if (!this.selectedType) {
          // 有从props传入的类型时，避免重置id
          this.selectedValue.bizId = '';
        }
        if (this.withBizObjectClass.includes(newVal)) {
          this.objectList = [];
          this.isProjectSelectorShow = true;
          this.placeHolder.biz = this.$t('请选择');
          this.disabledSelector.biz = false;
        } else {
          this.isProjectSelectorShow = false;
          this.placeHolder.biz = '- - -';
          this.disabledSelector.biz = true;
          if (newVal !== '') {
            this.loadObjects();
          }
        }
        // this.loadObjects()
        this.loadRoles();
      },
    },
    'selectedValue.bizId'(val) {
      (this.selectedValue.objectClass === 'project' || val) && this.loadObjects();
    },
  },
  methods: {
    openDialog(item) {
      this.isShow = true;
      if (item) {
        this.selectedType = item.data_set_type;
        this.selectedValue.bizId = item.bk_biz_id + '';
        if (item.data_set_type === 'raw_data') {
          this.selectedValue.scopeId = Number(item.data_set_id);
        } else {
          this.selectedValue.scopeId = item.data_set_id;
        }
        this.selectedValue.roleId = item.roleId;
        this.selectedValue.objectClass = item.data_set_type;
        this.loadObjects();
      }
    },
    closeDialog() {
      this.isShow = false;
      this.$emit('closeDialog');
    },
    clearSelected() {
      this.selectedValue.roleId = '';
      this.selectedValue.scopeId = '';
    },
    checkSelected() {
      if (this.selectedValue.scopeId === '') {
        showMsg(window.$t('请选择对象'), 'warning');
        return false;
      }
      if (this.selectedValue.roleId === '') {
        showMsg(window.$t('请选择角色'), 'warning');
        return false;
      }
      return true;
    },
    submitApplication() {
      this.loading.submitLoading = true;
      if (!this.checkSelected()) {
        this.$nextTick(() => {
          this.loading.submitLoading = false;
        });
        return;
      }

      let params = {
        reason: this.reason,
        ticket_type: 'apply_role',
        permissions: [
          {
            scope_id: this.selectedValue.scopeId,
            role_id: this.selectedValue.roleId,
            action: 'add_role',
          },
        ],
      };

      submitApplication(params)
        .then(res => {
          if (res.result) {
            showMsg(window.$t('提交成功'), 'success');
            this.$emit('applySuccess');
            this.closeDialog();
            this.clearSelected();
            this.$router.push({ name: 'Records' });
          } else {
            showMsg(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.loading.submitLoading = false;
        });
    },
    loadObjects() {
      this.placeHolder.scope = this.$t('数据加载中');
      this.loading.objectListLoading = true;
      if (!this.selectedType) {
        if (!this.objectId) {
          this.selectedValue.scopeId = '';
        }
      }

      queryAuthScope({
        object_class: this.selectedValue.objectClass,
        bk_biz_id: this.selectedValue.bizId,
      }).then(res => {
        this.objectList = [];
        if (res.result) {
          this.objectList = res.data;
        } else {
          showMsg(res.message, 'error');
        }
        this.placeHolder.scope = this.$t('请选择');
      });
      this.loading.objectListLoading = false;
    },
    loadRoles() {
      // 清空已选择的角色
      if (!this.selectedType) {
        this.selectedValue.roleId = '';
      }

      this.bkRequest.httpRequest('authV1/getRoles').then(res => {
        this.placeHolder.role = this.$t('请选择');
        if (res.result) {
          // 过滤出对象匹配的角色，在配置中无授权者的角色（authorizer为None）不支持申请
          this.roleList = res.data
            .filter(role => role.object_class === this.selectedValue.objectClass && role.authorizer !== null);

          if (this.roleList.length === 0) {
            this.objectList = [];
            this.placeHolder.scope = this.$t('请选择');
            this.placeHolder.role = this.$t('请选择');
            this.disabledSelector.scope = true;
            this.disabledSelector.role = true;
          } else {
            // (this.selectedValue.objectClass === 'project' || this.selectedValue.bizId) && this.loadObjects()
            this.placeHolder.scope = this.$t('请选择');
            this.placeHolder.role = this.$t('请选择');
            this.disabledSelector.scope = false;
            this.disabledSelector.role = false;
          }
        } else {
          showMsg(res.message, 'error');
        }
      });
    },
  },
};
</script>
<style lang="scss" scoped>
::v-deep .bk-dialog-wrapper .bk-dialog-body {
  padding: 3px 24px 26px !important;
}
</style>
<style lang="scss">
@import '../scss/base.scss';
.app-title {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  height: 30px;
  line-height: 30px;
}

.bkdata-form-horizontal {
  .form-item {
    margin-bottom: 10px;

    label.label-title {
      float: left;
      margin-top: 5px;
      width: 80px;
    }

    .bk-form-content {
      margin-left: 80px;
      .apply-rule-select {
        width: 100%;
        .auth-role-wrap-content {
          padding: 0 !important;
        }
      }
    }
  }
  .form-item:nth-of-type(1) {
    padding-bottom: 10px;
    font-size: 14px;
  }
  .item-inline-wrap {
    display: flex;
    justify-content: space-between;
    width: 100%;
    .form-item {
      margin-bottom: 10px;

      label.label-title {
        float: left;
        margin-top: 5px;
        width: 80px;
      }

      .bk-form-content {
        margin-left: 80px;
      }
    }
    .item-inline {
      display: inline-block;
      flex: 0.48;
    }
    .flex100 {
      flex: 1;
    }
  }
}

.apply-form {
  padding-left: 20px;
  padding-right: 20px;
  padding-top: 20px;
}
</style>
