

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
    <bkdata-dialog
      v-model="isShow"
      :hasHeader="false"
      :hasFooter="true"
      :closeIcon="true"
      :maskClose="false"
      :okText="$t('保存')"
      :cancelText="$t('取消')"
      :width="800"
      :loading="isLoading"
      @confirm="saveMember"
      @cancel="closeMemberManager">
      <div v-bkloading="{ isLoading: isLoading }"
        class="member-manager">
        <p class="title">
          {{ $t('更新成员列表') }}
        </p>

        <div class="info">
          {{ $t('您正在更新') }}
          【{{ objectClassName }}】 <span class="bold">{{ objectName }}</span> {{ $t('的成员列表') }}
        </div>
        <table class="bkdata-table-bordered member-manager-table"
          style="border-collapse: collapse">
          <colgroup>
            <col style="width: 130px">
            <col>
          </colgroup>
          <tbody>
            <tr v-for="(role, index) in targetRoles"
              :key="index">
              <td class="role-name">
                {{ role.role_name }}
              </td>
              <td>
                <bkdata-tag-input
                  :ref="role.role_id"
                  v-model="role.user_ids"
                  :placeholder="$t('请输入并按Enter结束')"
                  :disabled="!role.can_modify"
                  :hasDeleteIcon="true"
                  :list="allUsersForTag"
                  :tpl="tpl" />
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </bkdata-dialog>
  </div>
</template>

<script>
import { showMsg } from '@/common/js/util.js';
import { getAllUsers } from '@/common/api/base';
import { updateRoleUsers } from '@/common/api/auth';

export default {
  props: {
    isOpen: {
      type: Boolean,
      default: false,
    },
    scopeId: {
      type: [String, Number],
      required: true,
      default: '',
    },
    objectClass: {
      type: String,
      required: true,
    },
    roleIds: {
      type: Array,
      required: false,
      default: () => [],
    },
  },
  data() {
    return {
      isShow: this.isOpen,
      isLoading: false,
      isSaveLoading: false,
      user_roles: [],
      allUsers: [],
    };
  },
  computed: {
    allUsersForTag() {
      return (this.allUsers || []).map(user => {
        return {
          id: user,
          name: user,
        };
      });
    },
    objectUniqueFlag() {
      return `${this.objectClass}_${this.scopeId}`;
    },
    objectClassName() {
      if (this.targetRoles.length > 0) {
        return this.targetRoles[0].object_class_name;
      }
      return '';
    },
    objectName() {
      if (this.targetRoles.length > 0) {
        return this.targetRoles[0].object_name;
      }
      return '';
    },
    targetRoles() {
      let targetRoles = [];
      if (this.roleIds.length > 0) {
        for (let userRole of this.user_roles) {
          for (let _role of userRole.roles) {
            if (this.roleIds.indexOf(_role.role_id) !== -1) {
              targetRoles.push({
                role_id: _role.role_id,
                role_name: _role.role_name,
                object_class: userRole.object_class,
                object_class_name: userRole.object_class_name,
                object_name: userRole.object_name,
                object_id: userRole.object_id,
                scope_id: userRole.scope_id,
                user_ids: _role.users,
                can_modify: _role.can_modify,
              });
            }
          }
        }
      }

      // 修改 roles 有关 user_ids 的列表值，并未能触发 bk-tag-input 更新
      // 目前需要在主动设置
      this.$nextTick(() => {
        for (let r of targetRoles) {
          if (this.$refs[r.role_id] && this.$refs[r.role_id].length > 0) {
            console.log(r.role_id);
            this.$refs[r.role_id][0].updateData && this.$refs[r.role_id][0].updateData(r.user_ids);
          }
        }
      });
      return targetRoles;
    },
  },
  watch: {
    isOpen(newVal) {
      this.isShow = newVal;
      if (newVal === true) {
        this.loadMembers();
      }
    },
    isShow(newVal) {
      this.$emit('update:isOpen', newVal);
    },
  },
  mounted() {
    this.initDataMember();
  },
  methods: {
    tpl(node, ctx) {
      let parentClass = 'bkdata-selector-node';
      let textClass = 'text';
      let imgClass = 'avatar';
      return (
        <div class={parentClass}>
          <span class={textClass}>{node.name}</span>
        </div>
      );
    },
    initDataMember() {
      getAllUsers().then(res => {
        if (res.result) {
          this.allUsers = res.data;
        } else {
          showMsg(res.message, 'error');
        }
      });
    },
    loadMembers(scopeId, roleIds) {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('authV1/queryUserRoles', {
          query: {
            scope_id: this.scopeId,
            object_class: this.objectClass,
            show_display: true,
          },
        })
        .then(res => {
          if (res.result) {
            this.user_roles = res.data;
            /** 如果没传roleIds， 则将返回的所有roleID展示 */
            if (this.roleIds.length === 0) {
              // eslint-disable-next-line vue/no-mutating-props
              this.roleIds = res.data.roles.map(item => {
                return item.role_id;
              });
            }
          } else {
            showMsg(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.isLoading = false;
        });
    },
    saveMember() {
      this.isLoading = true;
      updateRoleUsers({
        role_users: this.targetRoles.filter(role => role.can_modify === true),
      })
        .then(res => {
          if (res.result) {
            this.$emit('members', this.targetRoles);
            showMsg(window.$t('保存成功'), 'success', { delay: 1000 });
          } else {
            showMsg(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.isLoading = false;
          this.isShow = false;
        });
    },
    closeMemberManager() {
      this.isShow = false;
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
.member-manager {
  .title {
    height: 20px;
    line-height: 20px;
    border-left: 4px solid #3a84ff;
    padding-left: 15px;
    color: #212232;
    font-size: 16px;
  }
  .info {
    margin-top: 20px;
    margin-bottom: 10px;
  }
  span.bold {
    text-decoration: underline;
  }
  .bk-tag-input {
    min-height: 80px !important;
  }
  .role-name {
    font-weight: bold;
  }
  .member-manager-table {
    .bkdata-selector-node {
      text-align: left;
    }
  }
}
</style>
