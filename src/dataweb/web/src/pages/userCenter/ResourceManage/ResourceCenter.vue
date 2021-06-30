

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
  <div class="resourceWrapper">
    <div class="header">
      <div class="header-left">
        <bkdata-input
          v-model="searchKey"
          :placeholder="$t('请输入资源组名称')"
          :rightIcon="'bk-icon icon-search'"
          style="width: 455px"
          :inputStyle="{
            width: '455px',
          }"
          extCls="search-input" />
        <bkdata-radio-group v-model="resourceGroupOption">
          <bkdata-radio :value="'my_list'">
            {{ $t('我的资源组') }}
          </bkdata-radio>
          <bkdata-radio :value="'all_list'">
            {{ $t('全部资源组') }}
          </bkdata-radio>
        </bkdata-radio-group>
      </div>
      <div class="header-right">
        <span class="sort-text">{{ $t('资源组排序') }}：</span>
        <div class="sort-select">
          <bkdata-selector :list="sortList"
            :selected.sync="sortKey"
            :settingKey="'id'"
            :displayKey="'label'"
            @item-selected="sortResorceGroup" />
        </div>
        <div class="create-group">
          <bkdata-button theme="primary"
            @click="newResource">
            {{ $t('创建资源组') }}
          </bkdata-button>
        </div>
      </div>
    </div>
    <div v-bkloading="{ isLoading: loading }"
      class="content">
      <bkdataVirtueList v-slot="slotProps"
        :list="displayGroupList"
        :lineHeight="228"
        :pageSize="30"
        :groupItemCount="groupItemCount"
        class="contents-list-container">
        <div v-for="item in slotProps.data"
          :key="item.$index"
          class="card">
          <Resource-card :card="item"
            @deleteStart="loading = true"
            @deleteEnd="loading = false"
            @deleteSuccess="getGroupList"
            @editCard="editCard"
            @openMemberManage="openMemberManager"
            @applyResource="openAppliedResource"
            @authListShow="openAuthList" />
        </div>
      </bkdataVirtueList>
    </div>
    <new-resource ref="newResource"
      @updateList="getGroupList" />
    <MemberManagerWindow ref="member"
      :isOpen.sync="memberManager.isOpen"
      :objectClass="memberManager.objectClass"
      :scopeId="memberManager.scopeId"
      :roleIds="memberManager.roleIds" />
    <AppliedResource ref="AppliedResource"
      :resourceCard="activeResourceGroup" />
    <!-- 授权列表 -->
    <bkdata-dialog v-model="authList.show"
      width="800"
      :position="{ top: 100 }"
      :title="$t('授权列表')">
      <div v-bkloading="{ isLoading: authList.loading }"
        class="list-wrapper">
        <bkdata-table :data="authList.list">
          <bkdata-table-column :label="$t('资源组ID')"
            prop="resource_group_id" />
          <bkdata-table-column :label="$t('授权类型')"
            prop="subject_type_name" />
          <bkdata-table-column :label="$t('申请人')"
            prop="created_by" />
          <bkdata-table-column :label="$t('授权主体名称')"
            prop="subject_name" />
          <bkdata-table-column :label="$t('授权时间')"
            prop="created_at" />
        </bkdata-table>
      </div>
    </bkdata-dialog>
  </div>
</template>

<script>
import ResourceCard from './components/Card';
import NewResource from './NewResourceGroup';
import AppliedResource from './components/AppliedResource';
import { MemberManagerWindow } from '@/pages/authCenter/parts/index';
import bkdataVirtueList from '@/components/VirtueScrollList/bkdata-virtual-list.vue';
import { Numeral } from '@/bizComponents/Numeral';

export default {
  components: {
    ResourceCard,
    NewResource,
    MemberManagerWindow,
    AppliedResource,
    bkdataVirtueList,
  },
  props: {
    groupItemCount: {
      type: Number,
      default: 5,
    },
  },
  data() {
    return {
      authList: {
        loading: false,
        show: false,
        list: [],
      },
      memberManager: {
        isOpen: false,
        objectClass: '',
        scopeId: '',
        roleIds: [],
      },
      searchKey: '',
      resourceGroupOption: 'my_list',
      resourceGroup: [],
      loading: false,
      sortList: [
        {
          label: this.$t('创建时间'),
          id: 'time',
        },
        {
          label: this.$t('资源组名称'),
          id: 'name',
        },
      ],
      sortKey: 'name',
      activeResourceGroup: {},
    };
  },
  computed: {
    displayGroupList() {
      return this.resourceGroup.filter(item => {
        return item.group_name.includes(this.searchKey) || item.resource_group_id.includes(this.searchKey);
      });
    },
    sortFunc() {
      if (this.sortKey === 'name') {
        return (a, b) => {
          const nameA = a.group_name.toUpperCase();
          const nameB = b.group_name.toUpperCase();
          if (nameA < nameB) {
            return -1;
          }
          if (nameA > nameB) {
            return 1;
          }
          return 0;
        };
      } else {
        return (a, b) => {
          const valueA = new Date(a.created_at).getTime();
          const valueB = new Date(b.created_at).getTime();
          return valueB - valueA;
        };
      }
    },
  },
  watch: {
    resourceGroupOption(newValue, oldValue) {
      if (newValue !== oldValue) {
        this.getGroupList();
      }
    },
  },
  created() {
    this.getGroupList();
    this.$store.dispatch('auth/getAdminList');
  },
  methods: {
    openAuthList(card) {
      const groupId = card.resource_group_id;
      this.authList.show = true;
      this.authList.loading = true;
      this.bkRequest
        .httpRequest('resourceManage/getAuthList', {
          params: {
            resource_group_id: groupId,
          },
        })
        .then(res => {
          if (res.result) {
            this.authList.list = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.authList.loading = false;
        });
    },
    openAppliedResource(card) {
      this.activeResourceGroup = card;
      this.$refs.AppliedResource.dialogConfig.isShow = true;
    },
    openMemberManager(item) {
      this.memberManager.isOpen = true;
      this.memberManager.objectClass = 'resource_group';
      this.memberManager.scopeId = item.resource_group_id;
      this.memberManager.roleIds = ['resource_group.manager'];
    },
    editCard(cardId) {
      this.$refs.newResource.editResourceGroup(cardId);
    },
    newResource() {
      this.$refs.newResource.newResourceGroup();
    },
    sortResorceGroup(key) {
      this.resourceGroup = this.resourceGroup.sort(this.sortFunc);
    },
    getGroupList() {
      this.resourceGroup = [];
      this.loading = true;
      this.bkRequest
        .httpRequest('resourceManage/getGourpList', {
          params: {
            listId: this.resourceGroupOption,
          },
          query: {
            bk_username: this.$store.getters.getUserName,
          },
        })
        .then(res => {
          if (res.result) {
            // const copys = [...res.data]
            // for (let i = 0; i < 30; i++) {
            //     this.resourceGroup.push(copys[0])
            // }
            this.resourceGroup = res.data.sort(this.sortFunc);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .tabs-project-opbar {
  padding: 0 !important;
}
.resourceWrapper {
  width: 100%;
  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
    padding: 0 50px;
    .header-left {
      display: flex;
      align-items: center;
      .search-input {
        margin-right: 40px;
      }
    }
    .header-right {
      display: flex;
      align-items: center;
      .sort-text {
        min-width: 100px;
        display: inline-block;
        padding-right: 15px;
      }
      .sort-select {
        width: 126px;
        padding-right: 15px;
      }
    }
  }
  .content {
    height: calc(100vh - 180px);

    .contents-list-container {
      margin-right: 1px;
      ::v-deep .bkdata-virtue-content {
        display: flex;
        flex-wrap: wrap;
        align-content: flex-start;
      }

      &::-webkit-scrollbar {
        width: 6px;
      }

      &::-webkit-scrollbar-thumb {
        background-color: #c4c6cc;
      }
    }
    .card {
      display: inline-flex;
      justify-content: center;
      height: 228px;
      // margin-right: 20px;
      .card-wrapper {
        width: calc(100% - 20px);
      }
    }
  }
  @media screen and (max-width: 1209px) {
    .content {
      .card {
        width: calc(50% - 10px);
        &:nth-child(2n + 1) {
          margin-right: 0;
        }
      }
    }
  }
  @media screen and (min-width: 1210px) and (max-width: 1679px) {
    .content {
      .card {
        width: calc(33.333% - 14px);
        &:nth-child(3n + 1) {
          margin-right: 0;
        }
      }
    }
  }
  @media screen and (min-width: 1680px) and (max-width: 2200px) {
    .content {
      .card {
        width: calc(25% - 15px);
        &:nth-child(4n + 1) {
          margin-right: 0;
        }
      }
    }
  }
  @media screen and (min-width: 2200px) {
    .content .card {
      width: calc(20% - 16px);
      &:nth-child(5n + 1) {
        margin-right: 0;
      }
    }
  }
}
</style>
