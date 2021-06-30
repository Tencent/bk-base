

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

<template class="dataManage">
  <Layout :crumbName="$t('数据源列表')">
    <AccessDataSource @dataSourceLoaded="handleDataSourceLoaded" />
    <div class="dataManage">
      <div class="inquire-content clearfix">
        <div class="new-button"
          @click="showCreatePop">
          <bkdata-button theme="primary"
            @click="$router.push({ name: 'createDataid', query: { from: $route.path } })">
            {{ $t('新接入数据源') }}
          </bkdata-button>
        </div>
        <div class="inquire">
          <label class="title">{{ $t('所属业务') }}：</label>
          <div class="business-select">
            <bkdata-selector
              :selected.sync="params.bk_biz_id"
              :filterable="select.filterable"
              :placeholder="select.projectHoder"
              :searchable="true"
              :list="selectList.project"
              :settingKey="'bk_biz_id'"
              :displayKey="'bk_biz_name'"
              :allowClear="true"
              searchKey="bk_biz_name"
              @item-selected="handleChangeBiz($event)"
              @clear="handleChangeBiz($event, true)" />
          </div>
        </div>
        <div class="inquire">
          <label class="title">{{ $t('数据源名称') }}：</label>
          <div class="identification-input">
            <bkdata-input
              v-model="params['raw_data_name__icontains']"
              type="text"
              clearable
              :placeholder="$t('数据源名称或DataID')"
              :rightIcon="'bk-icon icon-search'"
              @clear="onParamsChange"
              @enter="onParamsChange" />
          </div>
        </div>
        <div class="inquire advance-search">
          <bkdata-button :iconRight="searchIcon"
            :title="$t('高级搜索')"
            @click="showSearchMenu">
            {{ $t('高级搜索') }}
          </bkdata-button>
        </div>
      </div>
      <transition name="fade">
        <div v-show="isShowAdvancedSearch"
          class="advanced-search-container inquire-content">
          <div class="inquire">
            <label class="title">{{ $t('接入类型') }}：</label>
            <div class="business-select">
              <bkdata-selector
                :selected.sync="params.data_scenario"
                :filterable="select.filterable"
                :placeholder="select.typeHoder"
                :searchable="true"
                :list="selectList.type"
                :settingKey="'data_scenario_name'"
                :displayKey="'data_scenario_alias'"
                :allowClear="true"
                searchKey="data_scenario_alias"
                @item-selected="onParamsChange"
                @clear="onParamsChange" />
            </div>
          </div>
          <div class="inquire">
            <label class="title">{{ $t('创建人') }}：</label>
            <div class="identification-input">
              <bkdata-input
                v-model="params['created_by']"
                type="text"
                clearable
                :placeholder="$t('创建人')"
                :rightIcon="'bk-icon icon-search'"
                @clear="onParamsChange"
                @enter="onParamsChange" />
            </div>
          </div>
          <div class="inquire">
            <label class="title">{{ $t('创建时间') }}：</label>
            <div class="identification-input">
              <bkdata-date-picker
                v-model="created_at"
                clearable
                :format="'yyyy-MM-dd HH:mm:ss'"
                :placeholder="$t('选择日期时间范围')"
                :type="'datetimerange'"
                @change="changeTimeFormate" />
            </div>
          </div>
        </div>
      </transition>
      <div class="table">
        <div v-bkloading="{ isLoading: tableLoading }"
          class="dataid-table">
          <bkdata-table
            ref="dataidTable"
            :emptyText="$t('暂无数据')"
            :data="tableSearchList"
            :pagination="pagination"
            @page-change="handlePageChange"
            @page-limit-change="handlePageLimitChange">
            <bkdata-table-column
              :sortable="true"
              :label="$t('数据源名称')"
              :width="tagTableWidth"
              sortBy="bk_source_name">
              <template slot-scope="props">
                <span class="source-name">
                  <a :href="`#/data-access/data-detail/${props.row.id}?page=1#list`">
                    {{ props.row.bk_source_name }}
                  </a>
                </span>
                <div class="tag-wrapper">
                  <span
                    v-for="(tag, index) in props.row.tags"
                    :key="index"
                    :class="[tag.isMultiple
                               ? 'multiple-tag' : 'multiple-tag',
                             { 'tag-more': tag.alias === '...' }]">
                    {{ tag.alias }}
                  </span>
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column
              :width="dataDespWidth"
              :sortable="true"
              :label="$t('数据源描述')"
              sortBy="raw_data_alias">
              <template slot-scope="props">
                <span v-bk-tooltips="{ content: props.row.raw_data_alias }">
                  {{ props.row.raw_data_alias }}
                </span>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :width="businessWidth"
              :sortable="true"
              :label="$t('业务')"
              sortBy="bk_dis_name">
              <template slot-scope="props">
                <span v-bk-tooltips="{ content: props.row.bk_dis_name }">
                  {{ props.row.bk_dis_name }}
                </span>
              </template>
            </bkdata-table-column>
            <bkdata-table-column
              :width="accessMethodWidth"
              :sortable="true"
              :label="$t('接入渠道')"
              sortBy="bk_app_code_alias">
              <div slot-scope="props"
                :title="props.row.bk_app_code_alias"
                class="text-overflow">
                {{ props.row.bk_app_code_alias }}
              </div>
            </bkdata-table-column>
            <bkdata-table-column
              :width="accessTypeWidth"
              :sortable="true"
              :label="$t('接入类型')"
              prop="data_scenario_alias"
              sortBy="data_scenario_alias" />
            <bkdata-table-column
              :width="tagSourceWidth"
              :sortable="true"
              :label="$t('数据来源')"
              sortBy="bk_dis_source_from">
              <template slot-scope="props">
                <span v-bk-tooltips="{ content: props.row.bk_dis_source_from }">
                  {{
                    props.row.bk_dis_source_from
                  }}
                </span>
              </template>
            </bkdata-table-column>
            <bkdata-table-column
              width="115"
              :sortable="true"
              :label="$t('创建人')"
              prop="created_by"
              sortBy="created_by" />
            <bkdata-table-column :sortable="true"
              :label="$t('创建时间')"
              prop="created_at"
              sortBy="created_at" />
            <bkdata-table-column prop="detail"
              fixed="right"
              :label="$t('操作')"
              :width="160"
              :resizable="false">
              <div slot-scope="item"
                class="bk-table-inlineblock">
                <operation-group>
                  <a
                    :class="['detail',
                             { 'is-disabled': !dataChannel.includes(item.row.bk_app_code) }
                    ]"
                    :href="
                      (dataChannel.includes(item.row.bk_app_code) &&
                        `#/data-access/updatedataid/${item.row.id}?from=${$route.path}`) ||
                        'javascript:void(0);'
                    ">
                    {{ $t('编辑') }}
                  </a>
                  <span class="detail"
                    @click="showDelete(item.row)">
                    {{ $t('删除') }}
                  </span>
                </operation-group>
              </div>
            </bkdata-table-column>
          </bkdata-table>
        </div>
      </div>
    </div>
    <bkdata-dialog
      v-model="isDeleteShow"
      :extCls="'bkdata-dialog data-manage-dialog'"
      :title="$t('请确认是否删除数据源')"
      :theme="'danger'"
      :loading="isDeleteLoading"
      :hasHeader="false"
      :closeIcon="true"
      :hasFooter="false"
      :maskClose="false"
      @confirm="confirmDeleteRawData" />
  </Layout>
</template>

<script type="text/javascript">
import tippy from 'tippy.js';
import mixin from '@/components/dataTag/tagMixin.js';
import { mapGetters } from 'vuex';
import Layout from '../../components/global/layout';
import operationGroup from '@/bkdata-ui/components/operationGroup/operationGroup';
import AccessDataSource from '../DataAccess/NewForm/AccessDataSource';
import bkStorage from '@/common/js/bkStorage.js';

export default {
  components: {
    operationGroup,
    Layout,
    AccessDataSource,
  },
  mixins: [mixin],
  props: {
    did: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      accessTypeWidth: 110,
      accessMethodWidth: 110, // 接入渠道表格宽度
      businessWidth: 200, // 业务表格宽度
      dataDespWidth: 200, // 数据源描述表格宽度
      tagSourceWidth: 115, // 数据来源表格宽度
      tagTableWidth: 380, // 标签列的宽度，以适应不同宽度的屏幕
      tagDisplayNum: 5, // 每行展示最多的tag标签数，多于此数展示 "..."
      tippyInstance: [],
      tagTippyContent: [],
      treeProps: {
        label: 'category_alias',
        children: 'sub_list',
        value: 'category_name',
      },
      validDataScenario: ['log', 'custom'],
      loaded: false,
      pop: {
        defaultDataType: {
          // 弹窗选择数据类型
          defaultModel: 'climbing',
          disabled: true,
          list: [],
        },
        business: {},
      },
      tableLoading: true, // 表格加载
      selectList: {
        // 三个搜素下拉列表
        project: [],
        type: [],
        source: [],
        category: [],
        originCategory: [],
      },
      created_at: [],
      params: {
        bk_biz_id: '', // 所属业务
        data_scenario: '', // 接入类型
        data_source: '', // 数据来源
        data_category: [], // 数据源分类
        raw_data_name__icontains: '',
        created_begin: '',
        created_end: '',
        created_by: '',
      },
      select: {
        projectHoder: '',
        typeHoder: '',
        sourceHoder: '',
        categoryHoder: '',
        filterable: true,
      },
      tableList: [],
      tableDataTotal: 1,
      pageCount: 1,
      pageSize: 20,
      inputSearchTimeoutId: 0,
      tableFields: [
        {
          name: 'id',
          text: this.$t('数据ID'),
          width: '100px',
          sortable: false,
          sort: -1,
        },
        {
          name: 'bk_dis_name',
          text: this.$t('业务'),
          // width: `150px`,
          sortable: false,
        },
        {
          name: 'raw_data_name',
          text: this.$t('数据源名称_中文名_'),
          sortable: false,
        },
        {
          name: 'created_by',
          text: this.$t('创建人'),
          width: '115px',
          sortable: false,
        },
        {
          name: 'created_at',
          text: this.$t('创建时间'),
          width: '180px',
          sortable: false,
        },
        {
          name: 'data_source_alias',
          text: this.$t('数据来源'),
          width: '140px',
          sortable: false,
        },
        {
          name: 'data_category_alias',
          text: this.$t('数据源分类'),
          width: '135px',
          sortable: false,
        },
        {
          name: 'data_scenario_alias',
          text: this.$t('接入类型'),
          width: '115px',
          sortable: false,
        },
        {
          name: 'bk_app_code_alias',
          text: this.$t('接入渠道'),
          width: '150px',
          sortable: false,
        },
        {
          name: 'detail',
          width: '150px',
          text: this.$t('操作'),
        },
      ],
      deleteData: null,
      isDeleteShow: false,
      isDeleteLoading: false,
      dataChannel: ['dataweb', 'data'], // 允许编辑的接入渠道
      isShowAdvancedSearch: false,
      searchIcon: 'icon-angle-double-down',
      bkStorageKey: 'dataManageBizId',
    };
  },
  computed: {
    ...mapGetters({
      getDataTagList: 'dataTag/getTagList',
      getMultipleTagsGroup: 'dataTag/getMultipleTagsGroup',
      getSingleTagsGroup: 'dataTag/getSingleTagsGroup',
    }),
    tableSearchList() {
      return this.tableList;
    },
    pagination() {
      return {
        count: this.tableDataTotal,
        limit: this.pageSize,
        current: this.currentPage || 1,
      };
    },
    currentPage() {
      return this.$route.query.page || 1;
    },
  },
  watch: {
    '$route.query': {
      deep: true,
      handler: function (val) {
        this.getDataIdList();
      },
    },
  },
  mounted() {
    this.init();
    this.getProjectList();
    this.getDataTagListFromServer();
    this.getTypeList();
    this.getSourceList();
    this.getCategoryList();
    this.getEditAbleAppCode();
  },
  created() {
    if (Object.keys(this.$route.query).length) {
      this.params = {
        bk_biz_id: this.$route.query.bk_biz_id,
        data_scenario: this.$route.query.data_scenario,
        data_source: this.$route.query.data_source,
        data_category: this.$route.query.data_category ? [this.$route.query.data_category] : [],
        name: this.$route.query.name,
      };
    }
  },
  methods: {
    showSearchMenu() {
      this.isShowAdvancedSearch = !this.isShowAdvancedSearch;
      this.searchIcon = this.isShowAdvancedSearch ? 'icon-angle-double-up' : 'icon-angle-double-down';
    },
    changeTimeFormate() {
      [this.params.created_begin, this.params.created_end] = this.created_at.map(item => !item
        ? ''
        : this.forMateDate(new Date(item).getTime())
      );
      this.onParamsChange();
    },
    forMateDate(time) {
      let preArr = Array.apply(null, Array(10)).map((elem, index) => {
        return '0' + index;
      });
      let date = new Date(time);
      let year = date.getFullYear();
      let month = date.getMonth() + 1; // 月份是从0开始的
      let day = date.getDate();
      let hour = date.getHours();
      let min = date.getMinutes();
      let sec = date.getSeconds();

      let newTime =        year
        + '-'
        + (preArr[month] || month)
        + '-'
        + (preArr[day] || day)
        + ' '
        + (preArr[hour] || hour)
        + ':'
        + (preArr[min] || min)
        + ':'
        + (preArr[sec] || sec);
      return newTime;
    },
    showDelete(data) {
      this.deleteData = data;
      this.isDeleteShow = true;
    },
    confirmDeleteRawData() {
      this.isDeleteLoading = true;
      this.bkRequest
        .httpRequest('dataAccess/deleteRawData', {
          params: {
            raw_data_id: this.deleteData.id,
            bk_biz_id: this.deleteData.bk_biz_id,
            bk_app_code: this.deleteData.bk_app_code,
            appenv: window.BKBASE_Global.runVersion,
          },
        })
        .then(res => {
          if (res.result && res.data === 'ok') {
            this.getDataIdList();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.isDeleteLoading = false;
          this.isDeleteShow = false;
        });
    },
    handlePageChange(page) {
      this.paramsChange(page);
    },
    handlePageLimitChange(pageSize) {
      this.pageSize = pageSize;
      this.getDataIdList();
    },
    handleDataSourceLoaded(data) {
      this.$set(this.selectList, data.key, data.data);
    },
    onParamsChange() {
      this.paramsChange();
    },

    // 保留选中业务状态
    handleChangeBiz(id, isClear) {
      const bizId = isClear ? '' : id;
      bkStorage.set(this.bkStorageKey, bizId);
      this.paramsChange();
    },

    paramsChange(page = 1) {
      this.$nextTick(() => {
        // this.$refs.dataidTable.pageRevert()
        let params;
        if (JSON.stringify(this.$route.query) !== '{}') {
          params = Object.assign({}, this.$route.query, this.params, { page: page });
        } else {
          params = this.params;
        }
        let _params = JSON.parse(JSON.stringify(params));
        _params.data_category = _params.data_category.slice(-1)[0];
        this.$router.push({
          query: _params,
        });
        this.params.bk_biz_id = parseFloat(this.params.bk_biz_id) ? parseFloat(this.params.bk_biz_id) : '';
      });
    },

    /*
                    搜索
                */
    search(e) {
      this.inputSearchTimeoutId && clearTimeout(this.inputSearchTimeoutId);
      this.inputSearchTimeoutId = setTimeout(() => {
        this.$set(this.params, 'raw_data_name__icontains', e.target.value);
      }, 500);
    },
    defaultDialog() {
      this.$router.push({ path: '/data-access/new-dataid/' });
    },
    dataDetail(data) {
      this.$router.push({ name: 'data_detail', params: { did: data.row.id }, hash: '#list', query: { page: 1 } });
    },
    reEdit(data) {
      if (!this.dataChannel.includes(data.row.bk_app_code)) return;
      this.$router.push({
        name: 'updateDataid',
        params: { did: data.row.id },
        query: { from: this.$route.path },
      });
    },
    getSourceTag(data) {
      return data.map(item => item.alias).join();
    },
    /*
                获取dataid列表
            */
    getDataIdList() {
      let self = this;
      this.tableLoading = true;
      /**
       * V3 Access Api
       * Merge Ieod
       *   */
      let params = JSON.parse(JSON.stringify(this.params));
      params.data_category = params.data_category.slice(-1)[0];
      params = Object.assign(params, {
        show_display: 1,
        page: this.currentPage,
        page_size: this.pageSize,
      });
      const query = params;
      this.bkRequest
        .httpRequest('dataAccess/getAccessList', { query })
        .then(res => {
          if (res.result) {
            this.tableDataTotal = res.data.count;
            this.pageCount = Math.ceil(res.data.count / this.pageSize);
            this.tableList = res.data.results.map(d => {
              this.$set(d, 'bk_source_name', `[${d.id}]${d.raw_data_name}`);
              this.$set(d, 'bk_dis_name', `[${d.bk_biz_id}]${d.bk_biz_name}`);
              this.$set(
                d,
                'bk_dis_source_from',
                (d.data_source_tags && d.data_source_tags.length
                                && this.getSourceTag(d.data_source_tags)) || this.$t('未知')
              );

              const { business = [], desc = [] } = JSON.parse(JSON.stringify(d.tags || '{}'));
              const tags = business.concat(desc);
              this.handleTagData(tags, this.getSingleTagsGroup, false);
              this.handleTagData(tags, this.getMultipleTagsGroup);
              if (tags.length > this.tagDisplayNum) {
                const tippyContent = tags.splice(this.tagDisplayNum, tags.length - this.tagDisplayNum, {
                  isMultiple: true,
                  alias: '...',
                });
                this.tagTippyContent.push(tippyContent);
              }
              this.$set(d, 'tags', tags);
              return d;
            });
            this.$nextTick(() => {
              this.addContentTippy();
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.loaded = true;
          this.tableLoading = false;
        })
        ['finally'](() => {
          this.tableLoading = false;
        });
    },
    /*
                获取业务列表
                v3 new api
            */
    getProjectList() {
      this.select.projectHoder = this.$t('数据加载中');
      let actionId = 'raw_data.update';
      let dimension = 'bk_biz_id';
      this.bkRequest
        .httpRequest('meta/getMineBizs', { params: { action_id: actionId, dimension: dimension } })
        .then(res => {
          if (res.data && res.result) {
            const result = (res.data || []).map(biz => {
              biz.bk_biz_id = Number(biz.bk_biz_id);
              return biz;
            });
            this.$set(this.selectList, 'project', result);
          } else {
            console.log('数据集成-我的业务列表-Error:', res.message);
            this.getMethodWarning(res.message, res.code);
            this.tableLoading = false;
          }
        })
        ['catch'](e => {
          console.log('数据集成-我的业务列表-Error:', e);
        })
        ['finally'](() => {
          this.select.projectHoder = this.$t('请选择业务');
        });
    },
    /*
                    获取接入类型
                */
    getTypeList() {
      this.select.typeHoder = this.$t('数据加载中');
      // this.selectList.type = this.filterList('data_scenario', 'data_scenario_display')
      this.axios
        .get('v3/access/scenario/')
        .then(res => {
          if (res.result) {
            this.selectList.type = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.select.typeHoder = this.$t('请选择');
        });
    },
    /*
                    数据来源
                */
    getSourceList() {
      this.select.sourceHoder = this.$t('数据加载中');
      // this.selectList.source = this.filterList('data_source', 'data_source_display')
      this.axios
        .get('v3/access/source/')
        .then(res => {
          if (res.result) {
            this.selectList.source = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.select.sourceHoder = this.$t('请选择数据来源');
        });
    },
    /*
                    数据源分类
                */
    getCategoryList() {
      this.select.categoryHoder = this.$t('数据加载中');
      this.$store
        .dispatch('getDataSrcCtg')
        .then(res => {
          if (res.result) {
            this.selectList.category = res.data;
            // this.selectList.category.unshift({
            //     data_category_name: '',
            //     data_category_alias: '全部'
            // })
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.select.categoryHoder = this.$t('请选择数据源分类');
        });
    },
    /*
                    过滤列表
                */
    filterList(id, name) {
      let list = [];
      // [{
      //     id: '',
      //     name: this.$t('全部')
      // }]
      this.tableList.forEach(element => {
        let condition = true;
        list.forEach(item => {
          condition = element[id] === item.id ? false : condition;
        });
        if (condition) {
          list.push({
            id: element[id],
            name: element[name],
          });
        }
      });
      return list;
    },
    showCreatePop() {
      this.isShow = true;
    },
    // 初始化
    init() {
      this.setTagColumnStyle();
      this.$set(this.params, 'raw_data_name__icontains', this.$route.query.raw_data_name__icontains);
      const bizId = this.params.bk_biz_id;
      if (!bizId) {
        const storageBizId = bkStorage.get(this.bkStorageKey);
        this.params.bk_biz_id = /^\d+$/.test(storageBizId) ? Number(storageBizId) : '';
      }
      this.paramsChange();
    },
    /**
     * 根据屏幕宽度调整表格的宽度
     *      通过多项式拟合，计算在多种分辨率情况下的标签宽度
     *      在超宽屏幕下（大于2000）时，需要对表格的剩余空间进行调整
     */
    setTagColumnStyle() {
      const tableMargin = 55;
      const tablePadding = 15;
      const remainTableSpace = 500;
      const screenWidth = window.screen.width;
      const displayNum = 5 + parseInt((screenWidth - 1680) / 120);
      this.tagTableWidth = 0.00008217689935520059 * screenWidth * screenWidth
            + 0.18332982898794453 * screenWidth - 181.93019343986498;
      if (screenWidth < 1600) {
        this.tagSourceWidth = 100;
      } else {
        this.tagSourceWidth = -0.000320647603027754 * screenWidth * screenWidth
                + 1.5293313708999143 * screenWidth - 1549.2809083263235;
      }
      if (screenWidth > 2000) {
        this.accessTypeWidth = this.accessMethodWidth = 140;
        this.businessWidth = this.dataDespWidth = 0.000050000000000000185 * screenWidth * screenWidth -
                0.015000000000000908 * screenWidth - 8.99999999999891;
      } else if (screenWidth < 1600) {
        this.accessTypeWidth = this.accessMethodWidth = this.businessWidth = this.dataDespWidth = 115;
      }
      this.tagDisplayNum = displayNum > 9 ? 9 : displayNum;
    },
    addContentTippy() {
      const tags = document.querySelectorAll('.tag-more');
      this.tippyInstance = [];
      tags.forEach((tag, index) => {
        this.tippyInstance.push(
          tippy(tag, {
            content: this.toolTipContent(this.tagTippyContent[index]),
            arrow: true,
            theme: 'light',
            maxWidth: 'none',
            interactive: true,
            appendTo: document.getElementsByClassName('dataid-table')[0],
          })
        );
      });
    },
    toolTipContent(data) {
      let tippyContent = '<div class="tag-wrapper">';
      (data || []).forEach(tag => {
        tippyContent += `<span class="multiple-tag">${tag.alias}</span>`;
        // 暂时去掉颜色的区分
        // if (tag.isMultiple) {
        //     tippyContent += `<span class="multiple-tag">${tag.alias}</span>`
        // } else {
        //     tippyContent += `<span class="single-tag">${tag.alias}</span>`
        // }
      });
      tippyContent += '</div>';
      return tippyContent;
    },
    /** 获取接入渠道可编辑白名单 */
    getEditAbleAppCode() {
      this.bkRequest.httpRequest('dataAccess/getEditAbleAppCode').then(res => {
        if (res.result) {
          this.dataChannel = res.data;
        }
      });
    },
  },
};
</script>
<style lang="scss" scoped>
::v-deep .data-manage-dialog {
  .bk-dialog-body {
    height: 0 !important;
  }
}
.fade-enter-active,
.fade-leave-active {
  z-index: 900;
  transition: height 5s;
}
.fade-enter, .fade-leave-to /* .fade-leave-active below version 2.1.8 */ {
  height: 0;
  z-index: -1;
  padding: 0 !important;
}
</style>
<style media="screen" lang="scss">
.dataManage {
  .detail {
    color: #3a84ff;
    cursor: pointer;
  }
  .is-disabled {
    color: #666;
    opacity: 0.6;
    cursor: not-allowed;
  }
  /*样式覆盖*/
  .bk-dialog {
    .bk-dialog-style {
      width: 100%;
    }
    .content-from {
      margin: 0 auto;
      width: 590px;
    }
  }
  .bk-dialog-footer {
    height: 126px;
  }
  .bk-dialog-footer .bk-dialog-outer {
    text-align: center;
    background: #fff;
    padding: 18px 0 47px;
  }
  .bk-dialog-body {
    padding: 20px 20px 60px;
    overflow-y: auto;
    &::-webkit-scrollbar {
      width: 6px;
      height: 5px;
    }

    &::-webkit-scrollbar-thumb {
      border-radius: 20px;
      background-color: #a5a5a5;
    }
  }
  // min-width: 1200px;
  .header {
    background: #f2f4f9;
    padding: 19px 75px;
    color: #1a1b2d;
    font-size: 16px;
    p {
      line-height: 36px;
      padding-left: 22px;
      position: relative;
      &:before {
        content: '';
        width: 4px;
        height: 20px;
        position: absolute;
        left: 0px;
        top: 8px;
        background: #3a84ff;
      }
    }
  }
  .inquire-content {
    position: relative;
    line-height: 36px;
    padding: 5px 130px 5px 0;
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    // box-shadow: 0 0 10px 0 rgba(31,32,36,0.04);
    .new-button {
      position: absolute;
      right: 0;
      top: 4px;
      margin: 0;
      height: 36px;
    }
    .inquire {
      display: flex;
      align-content: center;
      margin: 0 10px 15px 0;
      &:first-of-type {
        margin-left: 0;
      }
      .bk-select-list {
        z-index: 1005;
      }
      .bk-select-list .bk-select-list-item {
        height: auto;
      }

      .bkdata-selector-input {
        &.placeholder {
          color: inherit;
        }
      }
    }
    .inquire-button {
      float: left;
      margin-left: 22px;
    }
    .title {
      float: left;
      margin-right: 5px;
      width: 90px;
      text-align: right;
      line-height: 32px;
    }
    .business-select {
      float: left;
      width: 185px;

      .el-cascader {
        line-height: 36px;
      }

      .is-opened {
        border-color: #4f5399;
      }

      .el-input__inner {
        border: none;
        height: 36px;
        &::-webkit-input-placeholder {
          color: #666;
          line-height: 32px;
        }
      }

      .el-cascader__label {
        width: 100%;
        height: 32px;
        line-height: 32px;
        padding: 0 10px;
        padding-right: 33px;
        border: 1px solid #c3cdd7;
        border-radius: 2px;
        font-size: 14px;
        color: #666;
        outline: none;
        box-shadow: none;
        cursor: pointer;
        transition: border linear 0.2s;
      }
    }
    .identification-input {
      line-height: normal;
      width: 185px;
      .bk-form-input {
        display: block;
      }
      .bk-date-picker {
        width: 300px;
      }
    }

    // @media (max-width: 1520px) {
    //     .inquire {
    //         width: 18.4%;
    //         margin-left: 2%;
    //     }
    //     .business-select {
    //         width: calc(100% - 75px);
    //     }
    //     .source{
    //         width: calc(100% - 89px);
    //     }
    //     .identification-input {
    //         width: calc(100% - 103px);
    //     }
    // }
  }
  .bk-loading {
    /* height: calc(100% - 50px); */
    margin-top: 0px;
  }
  .sideslider-wrapper {
    box-shadow: none !important;
  }
  .data-ip {
    .content-down {
      padding-top: 0px !important;
    }
    position: relative;
  }
  .dataid-table {
    ::v-deep .bk-table-fit {
      border-right: 1px solid #dfe0e5;
    }
    .source-name {
      display: inline-block;
      margin-top: 4px;
    }
    .tag-wrapper {
      padding: 2px 0;
      display: flex;
      align-items: center;
      .single-tag,
      .multiple-tag {
        display: inline-block;
        height: 25px;
        line-height: 24px;
        border: 1px solid #ffeacc;
        background: #fff;
        color: #faad14;
        font-size: 12px;
        padding: 0 5px;
        border-radius: 3px;
        margin-right: 5px;
        margin-bottom: 3px;
        margin-top: 3px;
      }
      .multiple-tag {
        border: 1px solid #e1ecff;
        background: #fff;
        color: #3a84ff;
      }
      .tag-more {
        cursor: pointer;
      }
    }
    .raw-data {
      width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
  }
}
</style>
