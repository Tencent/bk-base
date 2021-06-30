

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
    :showSubNav="true">
    <template slot="subNav">
      <AuthNav :activeName="'MyPermissions'" />
    </template>

    <div class="my-permission">
      <SearchLayout>
        <template slot="search-alert">
          <BKDataAlert>
            {{
              $t('角色是权限的集合_申请指定对象的角色_可以获得相应的权限_比如项目管理员可以进行数据开发')
            }}
          </BKDataAlert>
        </template>

        <template slot="searchTitle">
          <div class="search-box">
            <div class="search-raw">
              <div class="search-item">
                <label class="search-label">{{ $t('权限对象') }}：</label>
                <div class="search-content">
                  <ObjectTypeSelector :objectType.sync="search.objectType.value" />
                </div>
              </div>
              <div class="search-item">
                <label class="search-label">{{ $t('对象名称') }}：</label>
                <div class="search-content">
                  <bkdata-input v-model="search.objectName.value"
                    :placeholder="$t('请使用关键字搜索')" />
                </div>
              </div>
            </div>
            <div class="new-btn">
              <bkdata-button theme="primary"
                class="mr10"
                @click="applyPermission">
                {{ $t('申请权限') }}
              </bkdata-button>
              <bkdata-button theme="primary"
                @click="showTransfer">
                {{ $t('交接权限') }}
              </bkdata-button>
            </div>
          </div>
        </template>

        <template slot="searchMain">
          <div v-bkloading="{ isLoading: isLoading }"
            class="table-box">
            <bkdata-table
              :data="permissionList"
              :stripe="true"
              :emptyText="$t('暂无数据')"
              :pagination="calcPagination"
              @page-change="handlePageChange"
              @page-limit-change="handlePageLimitChange">
              <bkdata-table-column width="200"
                :label="$t('对象名称')"
                prop="object_name" />
              <bkdata-table-column width="250"
                :label="$t('对象描述')"
                prop="object_description" />
              <bkdata-table-column :label="$t('角色与成员')">
                <div slot-scope="item"
                  class="bk-table-inlineblock">
                  <template v-for="(role, index) of item.row.roles">
                    <div :key="index"
                      class="role-users-wrap">
                      <RoleWrap :name="role.role_name"
                        :roleId="role.role_id" />
                      <div class="members"
                        @click="openMemberManager(item.row, role)">
                        <i
                          v-if="role.can_modify"
                          :class="{ 'to-be-click': role.can_modify }"
                          :title="$t('编辑')"
                          class="bk-icon icon-edit" />
                        <div v-tooltip="role.users ? role.users.join(', ') : ''"
                          class="tooltip">
                          {{ role.users ? role.users.join(', ') : '' }}
                        </div>
                      </div>
                    </div>
                    <div v-if="index < item.row.roles.length - 1"
                      :key="index + 0.5"
                      class="role-splitter" />
                  </template>
                </div>
              </bkdata-table-column>
            </bkdata-table>
          </div>
        </template>
      </SearchLayout>
      <MemberManagerWindow
        ref="member"
        :isOpen.sync="memberManager.isOpen"
        :objectClass="memberManager.objectClass"
        :scopeId="memberManager.scopeId"
        :roleIds="memberManager.roleIds"
        @members="updateMembers" />
      <PermissionApplyWindow ref="apply"
        :isOpen.sync="applyPermission.isOpen" />
      <AuthTransfer ref="authTransfer" />
      <span class="icon-dimens" />
    </div>
  </Layout>
</template>

<script>
import { showMsg } from '@/common/js/util.js';
import Layout from '@/components/global/layout';
import {
  AuthNav,
  BKDataAlert,
  MemberManagerWindow,
  ObjectTypeSelector,
  RoleWrap,
  SearchLayout,
} from '../parts/index.js';
import PermissionApplyWindow from './PermissionApplyWindow';
import AuthTransfer from './AuthTransfer';

export default {
  components: {
    Layout,
    BKDataAlert,
    AuthNav,
    RoleWrap,
    SearchLayout,
    ObjectTypeSelector,
    MemberManagerWindow,
    PermissionApplyWindow,
    AuthTransfer,
  },
  data() {
    return {
      isLoading: false,
      searchTimeoutId: 0,
      pageSize: 10,
      pageCount: 0,
      pageCurrent: 1,
      search: {
        // 类型后端分页
        objectType: {
          value: this.$route.query.objType || '',
        },
        // 名称前端分页
        objectName: {
          value: this.$route.query.objName || '',
        },
      },
      searchStatus: {
        isSearching: false,
        toBeSearch: {},
      },
      permissionList: [],

      memberManager: {
        isOpen: false,
        objectClass: '',
        scopeId: '',
        roleIds: [],
      },
      paging: {
        totalPage: 1,
      },
      pageChangeFlag: false,
    };
  },
  computed: {
    calcPagination() {
      return {
        current: Number(this.currentPage),
        count: this.pageCount,
        limit: this.calcPageSize,
      };
    },
    currentPage() {
      return this.$route.query.page || 1;
    },
    calcPageSize() {
      return this.$route.query.pageSize || this.pageSize;
    },
  },
  watch: {
    'search.objectType.value'(val) {
      this.pageChangeFlag = true;
      // this.$router.push({
      //     query: {page: 1}
      // })
      // 页码重置
      // this.$refs.bkDataTable.pageRevert()
      this.$router.push({
        query: Object.assign({}, this.$route.query, { objType: val, page: 1 }),
      });
      this.loadUserRoles();
    },
    'search.objectName.value'(val) {
      this.pageChangeFlag = true;
      // this.$router.push({
      //     query: {page: 1}
      // })
      // 页码重置
      // this.$refs.bkDataTable.pageRevert()
      this.searchTimeoutId && clearTimeout(this.searchTimeoutId);
      this.searchTimeoutId = setTimeout(() => {
        this.$router.push({
          query: Object.assign({}, this.$route.query, { objName: val, page: 1 }),
        });
        this.loadUserRoles();
      }, 300);
    },
    // '$route.query.page'() {
    //     if (!this.pageChangeFlag) {
    //         this.loadUserRoles()
    //     }
    // }
  },
  mounted() {
    this.loadUserRoles();
  },
  methods: {
    showTransfer() {
      this.$refs.authTransfer.isShow = true;
    },
    handlePageChange(page) {
      this.$router.push({
        query: Object.assign({}, this.$route.query, { page: page }),
      });
      this.pageCurrent = page;
      this.loadUserRoles();
    },
    handlePageLimitChange(pageSize, preLimit) {
      this.pageSize = pageSize;
      this.$router.push({
        query: Object.assign({}, this.$route.query, { pageSize: pageSize, page: 1 }),
      });
      this.loadUserRoles();
    },
    applyPermission() {
      this.$refs.apply.openDialog();
    },
    loadUserRoles(ignoreLoading = false) {
      if (!ignoreLoading) {
        this.isLoading = true;
      }

      // @todo 这里如果接口返回太慢，上一个接口的返回会覆盖下一个接口，导致数据不符合预期，需要在发请求前清空前面的请求
      this.search.objectType.value
        && this.bkRequest
          .httpRequest('authV1/queryUserRoles', {
            query: {
              page: this.currentPage,
              page_size: this.pageSize,
              show_display: true,
              object_class: this.search.objectType.value,
              object_name__contains: this.search.objectName.value,
            },
          })
          .then(res => {
            if (res.result) {
              this.permissionList = res.data.data;
              this.paging.totalPage = Math.ceil(res.data.count / this.pageSize);
              this.pageCount = res.data.count;
            } else {
              showMsg(res.message, 'error');
            }
            this.pageChangeFlag = false;
          })
          ['finally'](() => {
            if (!ignoreLoading) {
              this.isLoading = false;
            }
          });
    },
    updateMembers(datas) {
      // 保存成功后更新列表成员
      for (let permission of this.permissionList) {
        for (let role of permission.roles) {
          for (let data of datas) {
            if (role.role_id === data.role_id && permission.scope_id === data.scope_id) {
              role.users = data.user_ids;
            }
          }
        }
      }
      this.loadUserRoles();
    },
    openMemberManager(item, role) {
      if (!role.can_modify) {
        return;
      }
      this.memberManager.isOpen = true;
      this.memberManager.objectClass = item.object_class;
      this.memberManager.scopeId = item.scope_id;
      this.memberManager.roleIds = [role.role_id];
    },
  },
};
</script>

<style lang="scss">
@import '../scss/base.scss';
.my-permission {
  .bk-table-inlineblock {
    width: 100%;
  }
  .search-box {
    padding: 5px 0px 0px 0;
    display: flex;
    .search-raw {
      display: flex;
      align-items: center;
    }
    .search-item {
      margin-bottom: 0;
      line-height: 32px;
      height: 32px;
      .search-label {
        height: 32px;
      }
    }
    .role-item {
      width: 100%;
      .search-content {
        width: 80%;
      }
    }
  }

  .bk-table td {
    font-size: 12px !important;
  }

  .bk-table tr > td:nth-child(3),
  .bk-table tr > th:nth-child(3) {
    border-right: 1px solid rgb(230, 230, 230);
  }

  .role-users-wrap {
    display: flex;
    padding: 8px 0px;
    .to-be-click {
      cursor: pointer;
      color: #3a84ff;
    }

    .usernames {
      display: inline-block;
      overflow: hidden;
      vertical-align: middle;
      text-overflow: ellipsis;
      white-space: nowrap;
      line-height: 32px;
      width: calc(100% - 250px);
    }

    .members {
      flex: 1;
      position: relative;
      padding-left: 22px;
      height: 17px;
      overflow: hidden;
      .icon-edit {
        position: absolute;
        top: 3px;
        left: 0;
      }
      .tooltip {
        width: 100%;
        white-space: nowrap;
        text-overflow: ellipsis;
        overflow: hidden;
      }
    }
  }
  .role-splitter {
    content: '';
    border-bottom: 1px dashed #ccc !important;
  }
}

.new-btn {
  display: flex;
  .disabled[disabled] {
    cursor: not-allowed !important;
  }
}

.bkdata-en {
  .my-permission {
    .search-item {
      width: auto;
      .search-label {
        width: auto;
      }
    }
  }
}
</style>
