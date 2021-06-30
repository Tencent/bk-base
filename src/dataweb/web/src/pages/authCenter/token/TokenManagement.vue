

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
      <AuthNav :activeName="'TokenManagement'" />
    </template>

    <div class="token-management">
      <SearchLayout>
        <template slot="search-alert">
          <BKDataAlert>
            {{ $t('授权码是构建数据平台_API_请求的重要凭证_使用数据平台_API_可以操作您有权限的对象_比如查询数据') }}
            {{ $t('为了对象及数据的安全性_请妥善保存和定期更换授权码') }}
          </BKDataAlert>
        </template>

        <template slot="searchTitle">
          <div class="search-box">
            <div style="display: flex">
              <div class="search-raw">
                <div class="search-item">
                  <label class="search-label">{{ $t('授权码') }}：</label>
                  <div class="search-content">
                    <bkdata-input v-model="search.dataToken.value"
                      :placeholder="$t('请使用部分关键字搜索')" />
                  </div>
                </div>
                <div class="search-item">
                  <label class="search-label">{{ $t('蓝鲸应用') }}：</label>
                  <div class="search-content">
                    <bkdata-selector
                      :selected.sync="search.appCode.value"
                      :filterable="true"
                      :placeholder="$t('请选择')"
                      :searchable="true"
                      :list="appList"
                      :settingKey="'bk_app_code'"
                      :displayKey="'bk_app_name'"
                      :allowClear="true"
                      searchKey="bk_app_name" />
                  </div>
                </div>
              </div>

              <div class="new-btn">
                <bkdata-button theme="primary"
                  @click="newToken">
                  {{ $t('新增授权码') }}
                </bkdata-button>
              </div>
            </div>
            <div class="search-raw">
              <div class="search-item status-item">
                <label class="search-label">{{ $t('授权状态') }}：</label>
                <div class="search-content">
                  <bkdata-checkbox-group v-model="search.status.value">
                    <bkdata-checkbox :value="'enabled'">
                      {{ $t('已启用') }}
                    </bkdata-checkbox>
                    <bkdata-checkbox :value="'expired'">
                      {{ $t('已过期') }}
                    </bkdata-checkbox>
                  </bkdata-checkbox-group>
                </div>
              </div>
            </div>
            <div class="search-raw">
              <div class="search-item expire-item">
                <label class="search-label">{{ $t('到期时间_多少天内') }}：</label>
                <div class="search-content">
                  <bkdata-radio-group v-model="search.expire.value">
                    <bkdata-radio :value="''">
                      {{ $t('不限') }}
                    </bkdata-radio>
                    <bkdata-radio :value="'7'">
                      7{{ $t('天s') }}
                    </bkdata-radio>
                    <bkdata-radio :value="'30'">
                      30{{ $t('天s') }}
                    </bkdata-radio>
                  </bkdata-radio-group>
                </div>
              </div>
            </div>
          </div>
        </template>

        <template slot="searchMain">
          <div v-bkloading="{ isLoading: isLoading }"
            class="table-box">
            <bkdata-table
              :data="tableList"
              :stripe="true"
              :emptyText="$t('暂无数据')"
              :pagination="calcPagination"
              @page-change="handlePageChange"
              @page-limit-change="handlePageLimitChange">
              <bkdata-table-column :label="$t('ID')"
                prop="id"
                width="90" />
              <bkdata-table-column :label="$t('授权码')"
                prop="data_token"
                width="200" />
              <bkdata-table-column :label="$t('应用名称')"
                prop="bk_app_name"
                width="250" />
              <bkdata-table-column :label="$t('创建时间')"
                prop="created_at"
                width="180" />
              <bkdata-table-column :label="$t('授权状态')"
                prop="status_display"
                width="120" />
              <bkdata-table-column :label="$t('到期时间')"
                prop="expired_at"
                width="180" />
              <bkdata-table-column :label="$t('操作')">
                <div slot-scope="props"
                  class="bk-table-inlineblock">
                  <!-- <span class="bk-text-button bk-primary" v-if="canRenew(item)" @click="renew(item)">
                                        {{ $t('续期') }}
                                    </span> -->
                  <span class="bk-text-button bk-primary"
                    @click="detail(props.row)">
                    {{ $t('查看') }}
                  </span>
                  <span class="bk-text-button bk-primary"
                    @click="addPermission(props.row)">
                    {{ $t('添加权限') }}
                  </span>
                  <span class="bk-text-button bk-primary"
                    @click="memberManage(props.row)">
                    {{ $t('成员管理') }}
                  </span>
                  <span
                    v-if="props.row.expired_nearly"
                    class="bk-text-button bk-primary"
                    @click="setPermissionReneval(props.row)">
                    {{ $t('续期') }}
                  </span>
                  <span v-if="canActive(props.row)"
                    class="bk-text-button bk-primary"
                    @click="active(props.row)">
                    {{ $t('启用') }}
                  </span>
                  <span v-if="canInactive(props.row)"
                    class="bk-text-button bk-primary"
                    @click="inactive(props.row)">
                    {{ $t('禁用') }}
                  </span>
                  <span v-if="canDelete(props.row)"
                    class="bk-text-button bk-primary"
                    @click="deleteToken(props.row)">
                    {{ $t('删除') }}
                  </span>
                </div>
              </bkdata-table-column>
            </bkdata-table>
          </div>
        </template>
      </SearchLayout>

      <bkdata-sideslider :quickClose="true"
        :isShow.sync="sideslider.isShow"
        :width="560"
        :title="$t('授权详情')">
        <template v-if="sideslider.isShow"
          slot="content">
          <TokenDetail :tokenId="sideslider.tokenId" />
        </template>
      </bkdata-sideslider>
      <PermissionRenewal :isOpen="isOpen"
        :authId="authId"
        @closeDialog="closeRenewalWindow" />
      <MemberManagerWindow
        :isOpen.sync="memberManager.isOpen"
        :objectClass="memberManager.objectClass"
        :scopeId="memberManager.scopeId"
        :roleIds="memberManager.roleIds" />
    </div>
  </Layout>
</template>

<script>
import moment from 'moment';
import Layout from '@/components/global/layout';
import { showMsg } from '@/common/js/util.js';
// import { BKDataTable } from '@/components/index.js'
import { queryTokens, retrieveTokenDetail } from '@/common/api/auth.js';

import { AuthNav, BKDataAlert, SearchLayout } from '../parts/index.js';
import TokenDetail from './TokenDetail';
import { MemberManagerWindow } from '@/pages/authCenter/parts/index';

export default {
  components: {
    Layout,
    // BKDataTable,
    BKDataAlert,
    AuthNav,
    SearchLayout,
    TokenDetail,
    PermissionRenewal: () => import('@/pages/authCenter/token/components/PermissionRenewal'),
    MemberManagerWindow,
  },
  data() {
    return {
      // 成员管理
      memberManager: {
        isOpen: false,
        objectClass: '',
        scopeId: '',
        roleIds: [],
      },
      tableList: [],
      pageCount: 10,
      curPage: 1,
      tableDataTotal: 1,
      sideslider: {
        isShow: false,
        tokenId: 0,
      },
      isLoading: true,
      search: {
        dataToken: {
          value: '',
        },
        appCode: {
          value: '',
        },
        expire: {
          value: '',
        },
        status: {
          value: [],
        },
        page: 1,
        page_size: 10,
      },
      appList: [],
      tokenList: [],
      isOpen: false,
      authId: null,
    };
  },
  computed: {
    calcPagination() {
      return {
        current: Number(this.curPage),
        count: this.tableDataTotal,
        limit: this.pageCount,
      };
    },
  },
  watch: {
    search: {
      handler: function (newVal) {
        this.requestTokenList();
      },
      deep: true,
    },
  },
  async mounted() {
    this.appList = await this.$store.dispatch('updateBlueKingAppList');
    this.init();
  },
  methods: {
    memberManage(item) {
      this.memberManager.isOpen = true;
      this.memberManager.objectClass = 'data_token';
      this.memberManager.scopeId = item.id;
      this.memberManager.roleIds = ['data_token.manager'];
    },
    closeRenewalWindow(isFresh) {
      this.isOpen = false;
      if (isFresh) {
        this.requestTokenList();
      }
    },
    setPermissionReneval(data) {
      this.isOpen = true;
      this.authId = data.id;
    },
    handlePageLimitChange(limit) {
      this.curPage = 1;
      this.pageCount = limit;
      this.requestTokenList(); // 目前前端分页，重新请求没有实际意义，之后后端支持后，注意修改请求参数
    },
    handlePageChange(val) {
      this.curPage = val;
      this.requestTokenList();
    },
    newToken() {
      this.$router.push({ name: 'TokenNew' });
    },
    addPermission(item) {
      this.$router.push({ name: 'TokenEdit', params: { token_id: item.id } });
    },
    canActive(item) {
      return item.status === 'inactive';
    },
    canInactive(item) {
      return item.status === 'active';
    },
    canRenew(item) {
      return item.status === 'expired';
    },
    canDelete(item) {
      return item.status === 'inactive' || item.status === 'expire';
    },
    detail(item) {
      this.sideslider.isShow = true;
      this.sideslider.tokenId = item.id;
    },
    getAppName(appCode) {
      for (let bkApp of this.appList) {
        if (appCode === bkApp.bk_app_code) {
          return bkApp.bk_app_name;
        }
      }
      return appCode;
    },
    active() {
      alert('启用');
    },
    inactive() {
      alert('禁用');
    },
    renew() {
      alert('续期');
    },
    deleteToken() {
      alert('删除');
    },
    /** 查询授权码列表 */
    requestTokenList() {
      this.isLoading = true;
      let expiredAtLte = '';
      if (this.search.expire.value !== '') {
        let expiredDay = new Date();
        expiredDay.setDate(expiredDay.getDate() + parseInt(this.search.expire.value));
        expiredAtLte = moment(expiredDay).format('YYYY-MM-DD HH:mm:ss');
      }
      let searchParams = {
        // @todo 临时前端分页，待BKDataTable改造支持后台分页后修改
        // page: this.search.page,
        // page_size: this.search.page_size,
        data_token_bk_app_code: this.search.appCode.value,
        data_token__contains: this.search.dataToken.value,
        status__in: this.search.status.value,
        expired_at__lte: expiredAtLte,
      };
      // @todo 这里bkRequest的query参数传输有问题
      // @todo 需要处理好 GET 请求参数有关数组的组装，this.qs.stringify(searchParams, {arrayFormat: 'repeat'})
      queryTokens(searchParams)
        .then(res => {
          this.tokenList = [];
          if (res.result) {
            // 组装应用名称
            for (let token of res.data) {
              token['bk_app_name'] = this.getAppName(token.data_token_bk_app_code);
              this.tokenList.push(token);
            }
            this.tokenList = res.data;
          } else {
            showMsg(res.message, 'error');
          }
        })
        ['finally'](res => {
          this.isLoading = false;
          this.tableDataTotal = this.tokenList.length;
          this.tableList = this.tokenList
            .slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage) || []; // 前端分页
        });
    },
    init() {
      this.requestTokenList();
    },
  },
};
</script>

<style lang="scss" scoped>
@import '../scss/base.scss';

.token-management {
  .search-item.expire-item,
  .search-item.status-item {
    width: 500px;

    .search-content {
      width: 400px;
    }
  }

  .bk-sideslider {
    top: 60px;
  }
}

.search-box {
  padding: 5px 0 0 0;

  .search-raw {
    margin-bottom: 5px;
    display: flex;
  }

  .search-item {
    margin-bottom: 0;
    line-height: 32px;
    height: 32px;
    .search-label {
      height: 32px;
    }
  }
}

.bkdata-en {
  .token-management {
    .search-item {
      width: auto;
      .search-label {
        width: auto;
      }
    }
  }
}

.new-btn {
  height: 32px;
  line-height: 1;
}
</style>
