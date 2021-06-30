/** 数据探索 - 右栏 - 模式公共仓库 */


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
  <div class="right-menu-wrapper">
    <div v-show="tabActived === 'RT'"
      style="width: 100%; height: 100%; display: flex; flex-direction: column">
      <!-- 模块公共仓库 -->
      <div v-show="isDepot"
        id="depot_elm"
        class="common-depot bk-scroll-y"
        :style="topWindowStyle">
        <div v-bkloading="{ isLoading: depotLoading }"
          class="main-top">
          <template v-for="item in depotList">
            <div
              :key="item.id"
              v-bk-tooltips="getTableDetail(item)"
              :class="['list-main-item', { 'is-active': item.id === tableId }]"
              @click="chooseTable(item, false)">
              <i class="icon-line-table item-icon" />
              <span class="item-name text-overflow">{{ item.result_table_id }}</span>
              <i
                v-bk-tooltips.top="getDetaultConfigTips({ content: $t('取消置顶') })"
                class="icon-urge-fill item-icon icon-right"
                @click.stop="topTask(item)" />
              <bkdata-button
                v-if="isQueryPanel"
                v-bk-tooltips.bottom="getDetaultConfigTips({ content: $t('生成默认SQL') })"
                class="run-btn ml10"
                theme="primary"
                size="small"
                @click="chooseTable(item)">
                <i class="bk-icon icon-right-shape" />
              </bkdata-button>
            </div>
          </template>
        </div>
      </div>
      <span v-show="isDepot"
        class="resize-line"
        @mousedown="handlMouseDown" />
      <div ref="bizTableList"
        class="common-depot-main common-depot-main-top">
        <div class="main-bottom-search">
          <bkdata-selector
            :selected.sync="bk_biz_id"
            :list="businessList"
            :settingKey="'bk_biz_id'"
            :displayKey="'bk_biz_name'"
            :searchable="true"
            :clearable="false"
            :isLoading="businessLoading"
            :placeholder="$t('请选择业务')"
            @item-selected="changeBusiness">
            <template slot="bottom-option">
              <div slot="extension"
                class="extension-btns">
                <span class="icon-btn"
                  @click="handleShowApply">
                  <i class="icon-apply-2 icon-item" />
                  {{ $t('申请业务数据') }}
                </span>
                <span class="icon-btn"
                  @click="handleDddDataId">
                  <i class="icon-plus-circle icon-item" />
                  {{ $t('新接入数据源') }}
                </span>
              </div>
            </template>
          </bkdata-selector>
          <bkdata-input
            v-model="value"
            class="search-inp"
            :placeholder="'请输入'"
            :clearable="true"
            :rightIcon="'bk-icon icon-search'"
            @change="debounceSearch" />
        </div>
        <div v-bkloading="{ isLoading: resultLoading }"
          class="main-bottom-list">
          <scrollList v-slot="slotProps"
            class="main-bottom-list-main bk-scroll-y"
            :pageSize="100"
            :list="searchList">
            <template v-for="(item, index) in slotProps.data">
              <div
                :key="index"
                v-bk-tooltips="getTableDetail(item)"
                class="list-main-item"
                @click="doubleClick(item, false)">
                <i class="icon-line-table item-icon" />
                <span class="item-name text-overflow">{{ item.result_table_id }}</span>
                <i
                  v-bk-tooltips.top="getDetaultConfigTips({ content: item.isTop
                    ? $t('取消置顶') : $t('置顶') })"
                  :class="[
                    'icon-right',
                    'item-icon',
                    !item.isTop ? 'icon-urge icon-trans' : 'icon-urge-fill'
                  ]"
                  @click.stop="topTask(item)" />
                <bkdata-button
                  v-if="isQueryPanel"
                  v-bk-tooltips.bottom="getDetaultConfigTips({ content: $t('生成默认SQL') })"
                  class="run-btn ml10"
                  theme="primary"
                  size="small"
                  @click="chooseTable(item)">
                  <i class="bk-icon icon-right-shape" />
                </bkdata-button>
              </div>
            </template>
            <div v-if="slotProps.data.length < 1"
              class="no-result">
              <p>{{ $t('暂无数据') }}</p>
              {{ $t('请查看其它业务') }}
            </div>
          </scrollList>
        </div>
      </div>
      <!-- 字段表 -->
      <div class="table-list"
        :style="`height: ${tableHeight}`">
        <div class="table-list-head">
          <span v-bk-overflow-tips
            class="table-name text-overflow">
            {{ tableName }}
          </span>
          <i
            v-bk-tooltips.top="getDetaultConfigTips({ content: $t('查看数据来源') })"
            class="bk-icon icon-dataflow head-icon"
            @click.stop="handleGoToTask" />
          <i
            v-bk-tooltips.top="getDetaultConfigTips({ content: curTableInfo.isTop
              ? $t('取消置顶') : $t('置顶') })"
            :class="['head-icon', !curTableInfo.isTop ? 'icon-urge icon-trans' : 'icon-urge-fill']"
            @click.stop="topTask(curTableInfo)" />
          <i
            v-if="isQueryPanel"
            v-bk-tooltips.top="getDetaultConfigTips({ content: $t('生成默认SQL') })"
            class="bk-icon icon-right-shape head-icon"
            @click.stop="initSQL" />
          <i
            v-bk-tooltips.top="getDetaultConfigTips({ content: $t('关闭') })"
            class="icon-close head-icon"
            @click.stop="isTable = false" />
        </div>
        <div v-bkloading="{ isLoading: schemaLoading }"
          class="table-list-main bk-scroll-y">
          <div class="sql-icon-list">
            <ul class="sql-button">
              <template v-for="item in activeOrders">
                <li
                  :key="item"
                  v-bk-tooltips="getIconTipConfig(item)"
                  :class="[
                    `icon-${iconNameMap[item]}_storage`,
                    (activeOrderName === item && 'selected') || '',
                    { disabled: !taskStorageList.includes(item) },
                  ]"
                  class="bk-icon fl"
                  @click="handleActiveTypeClick(item)" />
              </template>
            </ul>
          </div>
          <template v-for="(item, index) in activeSchemaList">
            <div :key="index"
              v-bk-tooltips.top="getColumnsDetail(item)"
              class="table-list-main-item">
              <i :class="['item-icon', item.is_time ? 'icon-time-small' : 'icon-dimens']" />
              <span class="item-name text-overflow">{{ item.physical_field }}</span>
              <span class="item-type">{{ item.physical_field_type }}</span>
            </div>
          </template>
          <div v-if="activeSchemaList.length < 0"
            class="no-result">
            <p>暂无数据</p>
            请查看其它数据表
          </div>
        </div>
      </div>
      <!-- 全文检索弹窗 -->
      <dialogWrapper
        :title="$t('全文检索')"
        icon="icon-search"
        :dialog="dialogConfig"
        extCls="up100px explore-es-search">
        <template #content>
          <EsQuery :resultTable="tableName" />
        </template>
      </dialogWrapper>

      <apply-biz v-if="isShowApply"
        ref="applyBiz"
        :bkBizId="bk_biz_id"
        @closeApplyBiz="handleCloseApply" />

      <PermissionApplyWindow ref="apply"
        :defaultSelectValue="{ objectClass: 'result_table' }" />
    </div>
    <div v-show="tabActived === 'ST'"
      style="width: 100%; height: 100%; display: flex; flex-direction: column">
      <div ref="bizTableList"
        class="common-depot-main common-depot-main-top">
        <div class="main-bottom-search">
          <bkdata-selector
            :selected.sync="bk_biz_id"
            :list="businessList"
            :settingKey="'bk_biz_id'"
            :displayKey="'bk_biz_name'"
            :searchable="true"
            :clearable="false"
            :isLoading="businessLoading"
            :placeholder="$t('请选择业务')"
            @item-selected="getOriginSourceList">
            <template v-if="dataExplore.isCommon"
              slot="bottom-option">
              <div slot="extension"
                class="extension-btns">
                <span class="icon-btn"
                  @click="handleShowApply">
                  <i class="icon-plus-circle icon-item" />
                  {{ $t('申请业务数据') }}
                </span>
                <span class="icon-btn"
                  @click="handleShowUpload">
                  <i class="icon-apply-2 icon-item" />
                  {{ $t('上传文件') }}
                </span>
              </div>
            </template>
          </bkdata-selector>
          <bkdata-input
            v-model="originSourceValue"
            class="search-inp"
            :placeholder="'请输入'"
            :clearable="true"
            :rightIcon="'bk-icon icon-search'" />
        </div>
        <!-- 虚拟滚动结束 -->
        <div v-bkloading="{ isLoading: originResultLoading }"
          class="main-bottom-list">
          <scrollList
            v-slot="slotProps"
            class="main-bottom-list-main bk-scroll-y"
            :pageSize="100"
            :list="displayFilesList">
            <template v-for="(item, index) in slotProps.data">
              <div :key="index"
                v-bk-tooltips="getTableDetail(item)"
                class="list-main-item">
                <i :class="['item-icon', getFileIcon(item)]" />
                <span class="item-name text-overflow">{{ item.displayName }}</span>
              </div>
            </template>
            <div v-if="slotProps.data.length < 1"
              class="no-result">
              <p>{{ $t('暂无数据') }}</p>
              {{ $t('请查看其它业务') }}
            </div>
          </scrollList>
        </div>
      </div>
    </div>
    <slot />
  </div>
</template>
<script>
import { once, on, off } from '@/common/js/events.js';
import { bkOverflowTips } from 'bk-magic-vue';
import dialogWrapper from '@/components/dialogWrapper';
import EsQuery from '@/pages/DataQuery/Components/EsQuery.vue';
import applyBiz from '@/pages/userCenter/components/applyBizData';
import PermissionApplyWindow from '@/pages/authCenter/permissions/PermissionApplyWindow.vue';
import bkStorage from '@/common/js/bkStorage.js';
import mixin from './mixin/originSourcePanel.mixin';

import { debounce } from '@/common/js/util.js';
import _ from 'underscore';
import scrollList from '@/components/scrollList/index.vue';

export default {
  directives: {
    bkOverflowTips,
  },
  components: {
    dialogWrapper,
    EsQuery,
    applyBiz,
    scrollList,
    PermissionApplyWindow,
  },
  mixins: [mixin],
  inject: ['dataExplore', 'setSQL'],
  props: {
    tabActived: String,
    isQueryPanel: {
      type: Boolean,
      default: true,
    },
    isNotebook: {
      type: Boolean,
      default: true,
    },
  },
  data() {
    return {
      isInit: false,
      rtProperty: {
        isTDWResultTable: false,
        hasNext: false,
        page: 1,
        pageSize: 100,
      },
      dialogConfig: {
        width: '1440px',
        isShow: false,
        quickClose: false,
        loading: false,
      },
      iconNameMap: {
        tspider: 'tspider',
        hdfs: 'hdfs',
        druid: 'druid',
        hermes: 'hermes',
        postgresql: 'postgresql',
        tpg: 'tpg',
        ignite: 'ignite',
        es: 'elastic',
        tsdb: 'tsdb',
        tdw: 'tdw',
        mysql: 'mysql',
        tcaplus: 'tcaplus',
        clickhouse: 'clickhouse',
      },
      movedHeight: 0,
      isDepot: false,
      isTable: false,
      tableName: '',
      depotLoading: false,
      businessLoading: false,
      resultLoading: false,
      schemaLoading: false,
      bk_biz_id: '',
      businessList: [],
      schemaList: [],
      value: '',
      depotList: [],
      resultList: [],
      tableId: 1,
      defaultSql: '',
      searchList: [],
      activeOrders: [],
      activeOrderName: '',
      activeStorage: {},
      taskStorageList: [],
      bkStoragePersonalKey: 'dataExplorePersonalBusinessId',
      bkStorageCommonKey: 'dataExploreCommonBusinessId',
      isShowApply: false,
      curTableInfo: {},
      debounceSearch: null,
    };
  },
  computed: {
    tableHeight() {
      return this.isTable ? '414px' : 0;
    },
    topWindowStyle() {
      return {
        minHeight: '122px',
        height: 122 + this.movedHeight + 'px',
      };
    },
    isShowDepot() {
      return this.depotList && this.depotList.length > 0;
    },
    // eslint-disable-next-line vue/return-in-computed-property
    commonStyle() {
      if (this.isDepot && this.isTable) return 'common-depot-main-all';
      if (this.isDepot && !this.isTable) return 'common-depot-main-top';
      if (!this.isDepot && this.isTable) return 'common-depot-main-bot';
    },
    activeStorageInstance() {
      return this.activeStorage[this.activeOrderName];
    },

    activeStorageSql() {
      return (this.activeStorageInstance || {}).sql;
    },

    activeSchemaList() {
      return (this.activeStorageInstance || {}).fields || [];
    },
    bkStorageKey() {
      return this.dataExplore.isCommon ? this.bkStorageCommonKey : this.bkStoragePersonalKey;
    },
    projectType() {
      return this.dataExplore.isCommon ? 'common' : 'personal';
    },
  },
  watch: {
    'dataExplore.projectId': {
      immediate: true,
      handler(val) {
        if (val) {
          this.isTable = false;
          this.getDepotList();
          this.getBusiness();
          this.resetRTPageSize();
        }
      },
    },
    isTable(val) {
      if (val) {
        /** table-list弹出时，添加动画，并在动画结束后去除transition 防止对拖拽的影响 */
        const el = document.querySelector('#depot_elm');
        el.style.transition = 'height 0.5s ease-in-out';
        this.movedHeight = 0;
        setTimeout(() => {
          el.style.transition = null;
        }, 500);
      }
    },
  },
  created() {
    this.debounceSearch = debounce(this.selectBusiness, 500);
  },
  mounted() {
    this.getTaskStorage();
    this.calculateDialogWidth();
    this.eventsBind();
  },
  methods: {
    eventsBind() {
      const oDiv = document.getElementsByClassName('main-bottom-list-main')[0];
      oDiv.addEventListener('scroll', _.throttle(this.onScrollHandle, 200));
    },
    onScrollHandle(event) {
      const oDiv = document.getElementsByClassName('main-bottom-list-main')[0];
      const curScrollPos = oDiv.scrollTop;
      if (curScrollPos === 0) return;
      const oldScroll = oDiv.scrollHeight - oDiv.clientHeight;
      if (curScrollPos === oldScroll) {
        this.rtProperty.hasNext && this.getResultList(true);
      }
    },
    resetRTPageSize() {
      this.rtProperty.page = 1;
    },
    /** 如果路由传了rt,则自动打开 */
    openGivenRt() {
      let tableName = '';
      let isTable = false;
      if (this.$route.params.rt_name) {
        tableName = this.$route.params.rt_name;
        isTable = true;
      } else {
        tableName = this.resultList[0].result_table_id;
      }
      this.setActiveTableInfo(tableName, isTable);
      this.isTable = isTable;
    },
    setActiveTableInfo(tableName, doubleClick) {
      this.tableName = tableName;
      this.getSchema(tableName, doubleClick);
    },
    calculateDialogWidth() {
      const width = document.body.clientWidth;
      const height = document.body.clientHeight;
      const el = document.querySelector('.explore-es-search .content');
      this.dialogConfig.width = width * 0.9 + 'px';
      el.style.height = height * 0.65 + 'px';
    },
    getDetaultConfigTips(config = {}) {
      return Object.assign(
        {
          interactive: false,
        },
        config
      );
    },
    getIconTipConfig(item) {
      return {
        maxWidth: 500,
        content: item,
        placement: 'top',
        boundary: 'window',
      };
    },
    handleActiveTypeClick(item) {
      if (this.taskStorageList.includes(item)) {
        this.activeOrderName = item;
      }
      // this.$emit('setSQL', this.activeStorageSql)
    },
    // 点击选择数据表
    chooseTable(item, isDouble = true) {
      this.tableId = item.id;
      this.doubleClick(item, isDouble);
    },
    // 获取置顶表
    getDepotList() {
      this.depotLoading = true;
      this.dataExplore.projectId
        && this.bkRequest
          .httpRequest('dataExplore/getDepotList', {
            params: {
              project_id: this.dataExplore.projectId,
            },
            query: {
              project_type: this.projectType,
            },
          })
          .then(res => {
            if (res.data) {
              this.depotList = res.data.map(item => Object.assign(item, { isTop: true }));
              this.isDepot = this.depotList.length > 0;
            }
          })
          ['finally'](() => {
            this.depotLoading = false;
          });
    },
    getDefaultBizId() {
      if (this.$route.query.bizId) {
        return this.$route.query.bizId;
      }
      const storageId = bkStorage.get(this.bkStorageKey);
      const existence = storageId
        ? this.businessList.find(item => Number(item.bk_biz_id) === Number(storageId))
        : false;
      return existence ? storageId : this.businessList[0].bk_biz_id;
    },
    // 获取业务列表
    getBusiness() {
      const actionId = 'result_table.query_data';
      const dimension = 'bk_biz_id';
      this.businessLoading = true;
      const params = this.dataExplore.isCommon
        ? { params: { pid: this.dataExplore.projectId } }
        : { params: { action_id: actionId, dimension: dimension } };
      const api = this.dataExplore.isCommon ? 'dataFlow/getBizListByProjectId' : 'meta/getMineBizs';
      this.bkRequest
        .httpRequest(api, params)
        .then(res => {
          if (res.data) {
            this.businessList = res.data;
            if (this.businessList.length) {
              this.bk_biz_id = this.getDefaultBizId();
              if (!this.resultLoading && this.bk_biz_id) {
                this.$nextTick(this.getResultList);
                if (!this.isInit) {
                  this.isInit = true;
                }
              }
            } else {
              this.bk_biz_id = '';
              this.searchList = [];
            }
          }
        })
        ['finally'](() => {
          this.businessLoading = false;
        });
    },
    // 切换业务
    changeBusiness(val) {
      this.resetRTPageSize(); // 切换业务时，page恢复
      this.getResultList();
      this.value = ''; // 清空搜索条件
      bkStorage.set(this.bkStorageKey, val);
    },
    resultListApi(loadMore, filterValue) {
      this.resultLoading = true;
      if (loadMore) {
        this.rtProperty.page += 1;
      } else {
        this.rtProperty.page = 1;
      }

      const relatedFilter = JSON.stringify({
        type: 'processing_type',
        attr_name: 'show',
        attr_value: 'queryset',
      });

      const api = this.dataExplore.isCommon ? 'dataExplore/getDataByProject' : 'meta/getMineResultTablesNew';
      const params = this.dataExplore.isCommon
        ? {
          params: { project_id: this.dataExplore.projectId },
          query: { page: this.rtProperty.page, with_queryset: this.isNotebook, extra_fields: 1 },
        }
        : {
          query: Object.assign(
            {
              bk_biz_id: this.bk_biz_id,
              page: this.rtProperty.page,
              page_size: this.rtProperty.pageSize,
              tdw_filter: filterValue,
              is_query: 1,
            },
            this.isNotebook ? { related_filter: relatedFilter } : {}
          ),
        };

      /**
       *  预计2021年春节后再替换回以下注释代码，注意同步修改dataExplore/getDataByProject接口
       */

      // const params = this.dataExplore.isCommon
      //     ? this.isNotebook
      //         ? {
      //             query: {
      //                 project_id: this.dataExplore.projectId,
      //                 related_filter: JSON.stringify({
      //                     type: 'processing_type',
      //                     attr_name: 'show',
      //                     attr_value: 'queryset'
      //                 }),
      //                 with_detail: 1 // 数据在data.data
      //             }
      //         }
      //         : {
      //             query: {
      //                 project_id: this.dataExplore.projectId,
      //                 with_detail: 1
      //             }
      //         }
      //     : {
      //         query: Object.assign({
      //             bk_biz_id: this.bk_biz_id,
      //             page: this.rtProperty.page,
      //             page_size: this.rtProperty.pageSize,
      //             tdw_filter: filterValue,
      //             is_query: 1,
      //         }, this.isNotebook && {
      //             related_filter: JSON.stringify({
      //                 type: 'processing_type',
      //                 attr_name: 'show',
      //                 attr_value: 'queryset'
      //             })
      //         })
      //     }
      this.bkRequest
        .httpRequest(api, params)
        .then(res => {
          if (res.data) {
            const list = res.data.data.filter(item => {
              return (
                item.generate_type !== 'system'
                && (this.dataExplore.isCommon ? item.bk_biz_id === this.bk_biz_id : true)
              );
            });
            if (loadMore) {
              this.resultList = this.resultList
                .concat(list.map(item => Object.assign(item, { isTop: false })));
            } else {
              this.resultList = list.map(item => Object.assign(item, { isTop: false }));
            }
            this.rtProperty.isTDWResultTable = res.data.acc_tdw;
            this.rtProperty.hasNext = res.data.has_next;
            // this.searchList = JSON.parse(JSON.stringify(this.resultList))
            if (res.data.data.length > 0) {
              // 数据表中标记已经置顶的数据表
              this.depotList.forEach(item => {
                const result = this.resultList
                  .filter(opt => opt.result_table_id === item.result_table_id)[0];
                result && this.$set(result, 'isTop', true);
              });
              this.searchList = JSON.parse(JSON.stringify(this.resultList));
              if (!this.tableName) {
                this.openGivenRt();
              }
            }
          }
        })
        ['finally'](() => {
          if (this.$route.query.rt_name) {
            this.value = this.$route.query.rt_name;
            this.selectBusiness(this.value);
          }
          this.resultLoading = false;
        });
    },
    // 根据业务获取结果表
    getResultList(loadMore = false) {
      this.searchList = loadMore ? this.searchList : [];
      this.resultListApi(loadMore);
    },
    // 获取数据表的schema
    getSchema(rtid, dbClick) {
      this.bkRequest
        .httpRequest('meta/getSchemaAndSqlBase', {
          params: { rtid: rtid },
          query: { storage_hint: true },
        })
        .then(res => {
          if (res.result && res.data.order[0]) {
            const key = res.data.order[0];
            this.$set(this, 'activeOrders', res.data.order);
            const activeOrderIndex = res.data.order.findIndex(item => this.taskStorageList.includes(item));
            this.$set(this, 'activeOrderName', res.data.order[activeOrderIndex]);
            this.$set(this, 'activeStorage', res.data.storage);
            const { fields, sql } = res.data.storage[key] || {};

            // this.defaultSql = sql

            if (dbClick) {
              /** 如果是es查询，则打开全文检索并退出当前执行 */
              if (this.activeOrderName === 'es') {
                this.dialogConfig.isShow = true;
                return;
              }
              // this.$emit('setSQL', sql)
              this.setSQL(sql);
            }
          }
        })
        ['finally'](() => {
          this.schemaLoading = false;
        });
    },
    // 双击数据表
    doubleClick(item, dbClick = true) {
      if (!dbClick) this.schemaLoading = true;
      this.tableName = item.result_table_id;
      this.curTableInfo = item;
      this.isTable = true;
      this.getSchema(item.result_table_id, dbClick);
    },
    initSQL() {
      if (this.activeOrderName === 'es') {
        this.dialogConfig.isShow = true;
        return;
      }
      this.setSQL(this.activeStorageSql);
      // this.$emit('setSQL', this.activeStorageSql)
    },
    handleGoToTask() {
      this.$router.push('/routehub/?rtid=' + this.tableName);
    },
    // 置顶表&取消置顶
    topTask(obj) {
      const typeUrl = obj.isTop ? 'cancelTop' : 'topDepot';
      this.bkRequest
        .httpRequest(`dataExplore/${typeUrl}`, {
          params: {
            project_id: this.dataExplore.projectId,
            project_type: this.projectType,
            result_table_id: obj['result_table_id'],
          },
        })
        .then(res => {
          if (res.code === '00') {
            const resultInTop = this.resultList
              .filter(item => item.result_table_id === obj.result_table_id);
            if (this.resultList.length > 0 && resultInTop.length) {
              resultInTop[0].isTop = !obj.isTop;
              this.searchList = JSON.parse(JSON.stringify(this.resultList));
            }
            this.curTableInfo.isTop = this.curTableInfo.result_table_id === obj.result_table_id
              ? !obj.isTop : this.curTableInfo.isTop;
            this.getDepotList();
          }
        });
    },
    // 输入搜索数据表
    selectBusiness(val) {
      if (this.rtProperty.isTDWResultTable) {
        this.searchList = [];
        this.resultListApi(false, val);
        return;
      }

      this.searchList = JSON.parse(JSON.stringify(this.resultList));
      const data = [];
      this.searchList.filter(item => {
        if (item.result_table_id.toLowerCase().split(val.toLowerCase()).length > 1) {
          data.push(item);
        }
      });
      this.searchList = data;
    },
    // 表格字段详情
    getColumnsDetail(item) {
      // const content = `<p style="color: #979BA5;">${this.$t('点击录入到编辑器中')}</p>
      const content = `
                        <div>${this.$t('字段名称')}: ${item.physical_field}</div>
                        <div>${this.$t('字段类型')}: ${item.physical_field_type}</div>
                        <div>${this.$t('字段描述')}: ${item.description}</div>`;
      return this.setDetailTooltips(content);
    },
    // 表格详情
    getTableDetail(item) {
      const dicUrl = this.$router.resolve({
        name: 'DataDetail',
        query: {
          dataType: 'result_table',
          result_table_id: item.result_table_id,
          bk_biz_id: item.bk_biz_id,
        },
      });

      const dataSourceUrl = this.$router.resolve({
        name: 'data_detail',
        params: {
          did: item.id,
          tabid: '2',
        },
      });

      const info = item.result_table_id
        ? {
          alias: item.description,
          url: dicUrl.href,
          text: this.$t('数据字典详情'),
        }
        : {
          alias: item.raw_data_alias,
          url: dataSourceUrl.href,
          text: this.$t('数据源详情'),
        };

      const alias = item.result_table_id ? item.description : item.raw_data_alias;
      const content = `<div>${item.result_table_id ? this.$t('结果表名') : this.$t('文件名')}: ${item.result_table_id
        || item.file_name}</div>
                        <div>${this.$t('中文描述')}: ${info.alias || '-'}</div>
                        <div>${this.$t('创建时间')}: ${item.created_at}</div>
                        <div>${this.$t('更新时间')}: ${item.updated_at}</div>
                        <a href="${info.url}" target="_blank" >
                            ${info.text}
                            <i class="icon-share"></i>
                        </a>`;
      return this.setDetailTooltips(content);
    },
    // 设置详情
    setDetailTooltips(content) {
      return {
        boundary: document.body,
        appendTo: document.body,
        placement: 'left-start',
        theme: 'light dataExplore-rt-tips',
        content: `${content}`,
        zIndex: '1001',
      };
    },
    // 获取任务支持的存储
    getTaskStorage() {
      this.bkRequest.httpRequest('dataExplore/getAvailableStorage').then(res => {
        if (res.code === '00') {
          this.taskStorageList = res.data;
        }
      });
    },
    handleShowApply() {
      if (this.dataExplore.isCommon) {
        this.isShowApply = true;
        this.$nextTick(() => {
          const projectId = this.dataExplore.projectId && this.dataExplore.projectId + '';
          this.$refs.applyBiz.show(
            {
              project_id: projectId,
              project_name: this.dataExplore.projectName,
            },
            '',
            false
          ); // 暂时修改false，默认不选择原始数据源
        });
      } else {
        this.$refs.apply.openDialog();
      }
    },
    handleCloseApply() {
      this.isShowApply = false;
    },
    handleDddDataId() {
      const url = this.$router.resolve({ name: 'createDataid' });
      window.open(url.href, '_blank');
    },
    // 鼠标拖拽
    handlMouseDown({ y: startY }) {
      const moveEle = document.querySelector('#depot_elm');
      const downEle = document.getElementsByClassName('common-depot-main-top')[0];

      const startHeight = this.movedHeight;

      const mouseMoveHandle = ({ y: mouseY }) => {
        setTimeout(() => {
          const maxUpHeight = downEle.offsetHeight;
          const moveDistance = mouseY - startY;
          const newHeight = moveDistance + startHeight;

          if ((moveDistance > 0 && maxUpHeight > 220) || (moveDistance < 0 && newHeight >= 0)) {
            this.movedHeight = newHeight;
          }
        }, 60);
      };
      on(document, 'mousemove', mouseMoveHandle);
      once(document, 'mouseup', () => {
        this.isDrop = false;
        off(document, 'mousemove', mouseMoveHandle);
      });
    },
  },
};
</script>
<style lang="scss" scoped>
$fieldContainerHeight: 414px;
$fieldListHeight: 320px;
.extension-btns {
  display: flex;
  align-items: center;
  .icon-btn {
    flex: 1;
    height: 32px;
    line-height: 32px;
    color: #63656e;
    cursor: pointer;
    &:hover {
      color: #3a84ff;
    }
    .icon-item {
      font-size: 14px;
    }
  }
}
.explore-es-search {
  .content {
    height: 600px;
  }
}
.right-menu-wrapper {
  width: 100%;
  height: calc(100% - 48px);
  user-select: none;
}
.common-depot {
  width: 100%;
  height: 122px;
  .main-top {
    min-height: 130px;
    padding: 10px 0;
    overflow: auto;
    .is-active {
      background: #eaf3ff;
      .item-name {
        color: #3a84ff;
      }
      .run-btn,
      .icon-right {
        display: block;
      }
    }
  }
}
.resize-line {
  display: block;
  width: 100%;
  height: 2px;
  border-bottom: 1px solid #dcdee5;
  cursor: row-resize;
}
.common-depot-main {
  width: 100%;
  height: 100%;
  .bk-select .bk-select-loading {
    top: 4px;
  }
  .main-bottom-search {
    padding: 16px 16px 10px;
    .search-inp {
      margin-top: 8px;
    }
    .bk-form-input {
      height: 27px;
    }
    .bk-select {
      margin-bottom: 16px;
      .bk-select-name {
        height: 26px;
      }
      .icon-angle-down {
        top: 3px;
      }
    }
  }
  .main-bottom-list {
    height: calc(100% - 106px);
    overflow: hidden;
    .head-icon {
      font-size: 12px;
      font-weight: 900;
      margin-right: 5px;
      cursor: pointer;
    }
    .main-bottom-list-main {
      height: 100%;
      // overflow: auto;
    }
  }
}
.common-depot-main-top {
  flex: 1 1;
  min-height: 220px;
}
.common-depot-main-bot {
  height: calc(100% - #{$fieldContainerHeight});
}
.common-depot-main-all {
  height: calc(100% - 520px);
  min-height: 226px;
}
.list-main-item {
  display: flex;
  align-items: center;
  height: 28px;
  line-height: 28px;
  padding: 0 16px 0 20px;
  font-size: 12px;
  color: #63656e;
  cursor: pointer;
  content-visibility: auto;
  contain-intrinsic-size: 0 28px;
  .item-icon {
    font-size: 14px;
    color: #c4c6cc;
  }
  .item-name {
    flex: 1;
    margin: 0 4px 0 8px;
  }
  .run-btn {
    width: 42px;
    min-width: 42px;
    height: 20px;
    padding: 0;
    font-size: 0;
    display: none;
    .icon-right-shape {
      font-size: 14px;
    }
  }
  .icon-right {
    color: #979ba5;
    cursor: pointer;
    display: none;
  }

  &:hover {
    background-color: #eaf3ff;
    .run-btn,
    .icon-right {
      display: block;
    }
  }
  .icon-trans {
    transform: rotate(45deg);
  }
}
.table-list {
  width: 100%;
  transition: all 0.5s ease-in-out;
  box-shadow: 0px -3px 4px -1px #dcdee5;
  .table-list-head {
    display: flex;
    align-items: center;
    height: 44px;
    line-height: 44px;
    border-bottom: 1px solid #dcdee5;
    .table-name {
      flex: 1;
      color: #313238;
      padding-left: 16px;
      padding-right: 10px;
    }
    .head-icon {
      font-size: 14px;
      color: #979ba5;
      margin-right: 10px;
      cursor: pointer;
      &.icon-dataflow {
        font-size: 12px;
      }
      &.icon-close {
        display: flex;
        align-items: center;
        width: 16px;
        height: 16px;
        font-weight: bold;
        font-size: 20px;
        margin-left: -4px;
      }
      &.icon-trans {
        margin-top: 2px;
        transform: rotate(45deg);
      }
    }
  }
  .table-list-main {
    padding-top: 10px;
    height: calc(100% - 38px);
    padding-bottom: 10px;
    // overflow: auto;
    .sql-icon-list {
      margin: 6px 16px 10px;
      .sql-button {
        display: inline-block;
        padding: 2px;
        background-color: #f0f1f5;
        li {
          display: flex;
          align-items: center;
          justify-content: center;
          width: 28px;
          height: 28px;
          font-size: 18px;
          color: #c4c6cc;
          cursor: pointer;
          &:hover {
            background-color: #fafbfd;
          }
          &.selected {
            background-color: #ffffff;
            color: #3a84ff;
          }
        }
      }
    }
    .table-list-main-item {
      display: flex;
      align-items: center;
      height: 28px;
      line-height: 28px;
      font-size: 12px;
      color: #c4c6cc;
      padding: 0 8px;
      margin: 0 16px;
      cursor: pointer;
      &:hover {
        background-color: #f5f6fa;
      }
      .item-icon {
        font-size: 14px;
      }
      .item-name {
        flex: 1;
        color: #63656e;
        padding: 0 10px;
      }
    }
  }
}
</style>

<style lang="scss">
.dataExplore-rt-tips-theme {
  .tippy-content {
    line-height: 20px;
    padding: 3px 0;
    color: #63656e;
    div {
      padding-bottom: 2px;
    }
    a {
      line-height: 14px;
      color: #3a84ff;
    }
    .icon-share {
      padding-left: 4px;
    }
  }
}
</style>
