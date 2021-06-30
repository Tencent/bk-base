

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
  <div class="data-dict">
    <div class="sub-head">
      <div class="bkdata-button-group">
        <bkdata-button :disabled="isTableLoading"
          :class="['button-group', dataType === 'all' ? 'is-selected' : '']"
          @click="changeDataType('all')">
          {{ $t('全部') }}
        </bkdata-button>
        <bkdata-button :class="['button-group', dataType === 'raw_data' ? 'is-selected' : '']"
          :disabled="isTableLoading || platform === 'tdw'"
          @click="changeDataType('raw_data')">
          {{ $t('数据源') }}
        </bkdata-button>
        <bkdata-button :class="['button-group', dataType === 'result_table' ? 'is-selected' : '']"
          :disabled="isTableLoading"
          @click="changeDataType('result_table')">
          {{ $t('结果数据') }}
        </bkdata-button>
        <!-- <div class="search-input">
                    <ul class="data-type-wrap">
                        <li :class="{'disabled': isTableLoading || !isSwitchStandard}"
                            v-bk-tooltips.top="$t('点击可查看标准化数据')"
                            @click="addDataType('only_standard')">{{$t('标准化数据')}}
                            <span class="corner-mark"
                                :class="{'border-clicked': searchType.includes('only_standard')}">
                                <span class="icon-check-1"></span></span> </li>
                        <li :class="{'disabled': isTableLoading}"
                            v-bk-tooltips.top="$t('点击可查看我创建的数据')"
                            @click="addDataType('mine')">{{$t('我创建的数据')}}
                            <span class="corner-mark"
                                :class="{'border-clicked': searchType.includes('mine')}">
                                <span class="icon-check-1"></span>
                            </span></li> -->
        <!-- <li class="disabled"
                            v-bk-tooltips.top="$t('该功能正在开发中')">{{$t('我的收藏')}}</li> -->
        <!-- </ul>
                </div> -->
      </div>
      <ul v-if="nodeTreeData.length"
        class="node-tree">
        <li v-for="(item, index) in nodeTreeData"
          :key="index">
          <p @click="switchType(item.category_name, index)">
            {{ item.category_alias }}
          </p>
          <span v-if="index !== nodeTreeData.length - 1">></span>
        </li>
      </ul>
    </div>
    <div class="data-table"
      :class="{ 'data-table-border': isTableLoading }">
      <div v-if="!isCountLoading"
        v-bkloading="{ isLoading: isCountLoading }"
        :class="{ 'data-count-bottom': isTableLoading }"
        class="data-count">
        <!-- eslint-disable-next-line max-len -->
        {{ $t('共') }}<span> {{ cleanDataCount }} </span>{{ $t('个') }}{{ $t('数据源') }},<span>{{ resultTableCount }} </span>{{ $t('个') }}{{ $t('结果表') }}，{{ $t('分布于') }}<span> {{ bkBizCount }} </span>{{ $t('个') }}{{ $t('业务') }}，<span> {{ projectCount }} </span>{{ $t('个')
        }}{{ $t('项目') }}
      </div>
      <div v-bkloading="{ isLoading: isTableLoading }">
        <template v-if="isShowDataSys">
          <bkdata-table ref="table"
            :data="tableData"
            :emptyText="$t('暂无数据')"
            :pagination="pagination"
            @page-limit-change="handlePageLimitChange"
            @page-change="handlePageChange"
            @row-mouse-enter="changeProgressBack">
            <bkdata-table-column minWidth="310"
              :label="$t('数据名称')">
              <div slot-scope="props"
                class="table-data-name"
                :class="{ 'table-data-name-safari': explorer === 'Safari' }">
                <span v-if="props.row.is_standard === 1"
                  :title="$t('标准化数据')"
                  class="data-dict-badge" />
                <span v-if="props.row.data_set_type === 'raw_data'"
                  :title="returnTitle(props)"
                  class="icon-datasource table-data-left" />
                <span v-else-if="props.row.data_set_type === 'tdw_table'"
                  :title="returnTitle(props)"
                  class="icon-tdw-table table-data-left" />
                <span v-else-if="props.row.data_set_type === 'result_table'"
                  :title="returnTitle(props)"
                  :class="[resultTableIcons[props.row.processing_type] || 'icon-resulttable', 'table-data-left']" />
                <span v-else
                  :title="returnTitle(props)"
                  class="icon-resulttable table-data-left" />
                <div class="table-data-right text-overflow">
                  <div class="biz-name">
                    <template v-if="!props.row.is_not_exist">
                      <div v-if="props.row.data_set_type !== 'raw_data'"
                        class="text-overflow data-name"
                        :title="`${props.row.data_set_id}${getDes(props.row.description)}`"
                        @click="linkTo(props.row)">
                        {{ props.row.platform === 'tdw'
                          ? props.row.table_name
                          : props.row.data_set_id }}{{ getDes(props.row.description) }}
                      </div>
                      <div v-else
                        class="text-overflow data-name"
                        :title="`[${props.row.data_set_id}]${getSafeValue(props.row.raw_data_name)}
                          ${props.row.raw_data_name !== props.row.raw_data_alias
                          ? `（${getSafeValue(props.row.raw_data_alias)}）`
                        : ''}`"
                        @click="linkTo(props.row)">
                        <!-- eslint-disable-next-line max-len -->
                        <span class="font-weight7">[{{ props.row.data_set_id }}]</span>{{ getSafeValue(props.row.raw_data_name) }}<span v-if="props.row.raw_data_name !== props.row.raw_data_alias">（{{ getSafeValue(props.row.raw_data_alias) }}）</span>
                      </div>
                    </template>
                    <template v-else>
                      <p v-bk-tooltips.top="$t('该数据无法查看详情')"
                        class="text-overflow disabled"
                        :title="`${props.row.data_set_id}${getDes(props.row.description)}`">
                        {{ props.row.data_set_id }}{{ getDes(props.row.description) }}
                      </p>
                    </template>
                  </div>
                  <div class="platform">
                    <template v-if="props.row.hasOwnProperty('tag_list') && props.row.tag_list">
                      <span v-for="(item, index) in props.row.tag_list.slice(0, 3)"
                        :key="index"
                        :title="getEnName(item)"
                        class="platform-item text-overflow">
                        {{ getEnName(item) }}
                      </span>
                      <bkdata-popover placement="top">
                        <span v-if="props.row.tag_list.length > 3"
                          class="icon-ellipsis platform-item-ellipsis" />
                        <div slot="content"
                          class="clearfix"
                          style="white-space: normal; max-width: 500px; max-height: 400px">
                          <span v-for="(item, index) in props.row.tag_list.slice(3)"
                            :key="index"
                            class="data-platform-item">
                            {{ getEnName(item) }}
                          </span>
                        </div>
                      </bkdata-popover>
                    </template>
                  </div>
                </div>
              </div>
            </bkdata-table-column>
            <bkdata-table-column minWidth="150"
              :label="$t('业务名称')">
              <span slot-scope="props"
                class="proj-name table-item text-overflow"
                :class="{ 'proj-name-safari': explorer === 'Safari' }"
                :title="`[${props.row.bk_biz_id}] ${getSafeValue(props.row.bk_biz_name)}`">
                <span v-if="props.row.bk_biz_id === 0 || props.row.bk_biz_id"
                  class="font-weight7">
                  {{ `[${props.row.bk_biz_id}] ` }}
                </span>{{ getSafeValue(props.row.bk_biz_name) }}
              </span>
            </bkdata-table-column>
            <bkdata-table-column minWidth="140"
              :label="$t('项目名称')">
              <span
                v-if="props.row.platform !== 'tdw' && props.row.data_set_type !== 'raw_data'"
                slot-scope="props"
                class="proj-name table-item text-overflow"
                :class="{ 'proj-name-safari': explorer === 'Safari' }"
                :title="props.row.project_id
                  ? `[${props.row.project_id}] ${props.row.project_name}`
                  : `${props.row.project_name}`">
                <span v-if="props.row.project_id === 0 || props.row.project_id"
                  class="font-weight7">
                  {{ `[${props.row.project_id}] ` }}
                </span>{{ getSafeValue(props.row.project_name) }}
              </span>
              <span v-else>{{ '—' }}</span>
            </bkdata-table-column>
            <bkdata-table-column v-if="['result_table', 'all'].includes(dataType) && platform !== 'tdw'"
              width="100"
              :renderHeader="renderStandard"
              prop="is_standard"
              :label="$t('是否标准')">
              <template slot-scope="props">
                <span v-if="props.row.data_set_type === 'result_table' && !isSwitchStandard">{{ $t('否') }}</span>
                <span v-else>
                  <span :class="{ 'click-style': props.row.is_standard === 1 }"
                    :title="props.row.data_set_type === 'result_table'
                      ? (props.row.is_standard === 1
                        ? $t('是')
                        : $t('否'))
                      : '—'"
                    @click="linkToStandard(props.row)">
                    {{ props.row.data_set_type === 'result_table'
                      ? (props.row.is_standard === 1
                        ? $t('是')
                        : $t('否'))
                      : '—' }}
                  </span>
                </span>
              </template>
            </bkdata-table-column>
            <template v-if="judgeIsShowField('asset_value_metric.asset_value_score')">
              <bkdata-table-column width="120"
                :label="$t('价值评分')"
                :renderHeader="renderHeader"
                prop="asset_value_metric.asset_value_score">
                <div v-if="props.row.data_set_type === 'tdw_table'"
                  slot-scope="props">
                  {{ '—' }}
                </div>
                <div v-else
                  slot-scope="props"
                  v-bk-tooltips="getRangeTooltips()"
                  class="rate-progress">
                  <bkdata-rate :extCls="'range-rate-star-wrap'"
                    :rate="(props.row.asset_value_metric.asset_value_score || 0) / 20"
                    :width="12"
                    :edit="false" />
                  <p class="rate-score">
                    {{ props.row.asset_value_metric.asset_value_score || 0 }}
                  </p>
                </div>
              </bkdata-table-column>
            </template>
            <template v-if="judgeIsShowField('heat_metric.heat_score')">
              <bkdata-table-column width="90"
                headerAlign="right"
                :label="$t('热度')"
                :renderHeader="renderHeader"
                prop="heat_metric.heat_score">
                <div slot-scope="{ row }"
                  class="text-right">
                  {{ getSafeValue(row.heat_metric.heat_score, '—') }}
                </div>
              </bkdata-table-column>
              <bkdata-table-column width="10" />
            </template>
            <template v-if="judgeIsShowField('range_metric.normalized_range_score')">
              <bkdata-table-column width="90"
                headerAlign="right"
                :label="$t('广度')"
                :renderHeader="renderHeader"
                prop="range_metric.normalized_range_score">
                <div slot-scope="{ row }"
                  class="text-right">
                  {{ getSafeValue(row.range_metric.normalized_range_score, '—') }}
                </div>
              </bkdata-table-column>
              <bkdata-table-column width="10" />
            </template>
            <template v-if="judgeIsShowField('importance_metric.importance_score')">
              <bkdata-table-column width="100"
                :label="$t('重要度')"
                :renderHeader="renderHeader"
                prop="importance_metric.importance_score">
                <div slot-scope="{ row }"
                  class="text-right">
                  {{ row.importance_metric ? row.importance_metric.importance_score : '—' }}
                </div>
              </bkdata-table-column>
              <bkdata-table-column width="10" />
            </template>
            <bkdata-table-column v-if="judgeIsShowField('cost_metric.format_capacity')"
              width="130"
              :renderHeader="renderHeader"
              :label="$t('存储成本')">
              <div slot-scope="{ row }"
                class="text-right pr-storage">
                <!-- 0.000 || 0.000B 时，展示- -->
                {{ !row.cost_metric
                  || parseInt(row.cost_metric.format_capacity) === 0 ? '—' : row.cost_metric.format_capacity }}
              </div>
            </bkdata-table-column>
            <template v-if="judgeIsShowField('assetvalue_to_cost')">
              <bkdata-table-column width="100"
                :label="$t('收益比')"
                :renderHeader="renderHeader"
                prop="assetvalue_to_cost">
                <div slot-scope="{ row }"
                  class="text-right">
                  {{ row.assetvalue_to_cost > 0 ? row.assetvalue_to_cost : '—' }}
                </div>
              </bkdata-table-column>
              <bkdata-table-column width="10" />
            </template>
            <bkdata-table-column v-if="judgeIsShowField('sensitivity')"
              width="120"
              :label="$t('敏感度')">
              <div slot-scope="props">
                <sensitive-block :warningClass="props.row.sensitivity"
                  :text="sensitiveObj[props.row.sensitivity]" />
              </div>
            </bkdata-table-column>
            <!-- 数据所在系统分为数据平台和TDW平台 -->
            <bkdata-table-column minWidth="120"
              :label="$t('数据所在系统')">
              <div slot-scope="props"
                :title="props.row.platform"
                class="text-overflow">
                {{ platFormNameList[props.row.platform] }}
              </div>
            </bkdata-table-column>
            <bkdata-table-column v-if="judgeIsShowField('created_by')"
              minWidth="100"
              prop="created_by"
              :label="$t('创建者')">
              <div slot-scope="props"
                :title="props.row.created_by"
                class="created text-overflow">
                {{ props.row.created_by }}
              </div>
            </bkdata-table-column>
            <bkdata-table-column minWidth="160"
              :renderHeader="renderHeader"
              :label="$t('最近修改时间')"
              prop="updated_at" />
            <bkdata-table-column minWidth="90"
              :label="$t('权限')">
              <div slot-scope="props">
                <span v-if="props.row.has_permission"
                  class="icon-check-circle-fill" />
                <span v-else-if="props.row.ticket_status === 'processing'"
                  v-bk-tooltips="$t('权限正在申请中')"
                  class="icon-ellipsis" />
                <a v-else
                  href="javascript:;"
                  :class="{ disabled: props.row.is_not_exist }"
                  @click="applyPermission(props.row)">
                  {{ $t('申请') }}
                </a>
              </div>
            </bkdata-table-column>
            <!-- <bkdata-table-column min-width="120"
                            :label="$t('我的收藏')">
                            @click="props.row.isCollect = !props.row.isCollect"
                            <template slot-scope="props">
                                <span class="disabled"
                                    v-bk-tooltips.top="$t('该功能正在开发中')"
                                    :class="props.row.isCollect ? 'icon-star-shape' : 'icon-star'"></span>
                            </template>
                        </bkdata-table-column> -->
            <bkdata-table-column width="50"
              fixed="right"
              :renderHeader="renderSetting" />
          </bkdata-table>
        </template>
      </div>
    </div>
    <PermissionApplyWindow v-if="isApplyPermissionShow"
      ref="apply" />
  </div>
</template>
<script>
import PermissionApplyWindow from '@/pages/authCenter/permissions/PermissionApplyWindow';
import SensitiveBlock from './components/children/SensitiveBlock';
import { ajax } from '@/common/js/ajax';
import { getExplorer, isNumber } from '@/pages/datamart/common/utils.js';
import { mapState } from 'vuex';
import Bus from '@/common/js/bus';
import { postMethodWarning } from '@/common/js/util';

const sortFieldMap = {
  order_heat: $t('热度'),
  order_range: $t('广度'),
  order_time: $t('最近修改时间'),
  order_asset_value: $t('价值评分'),
  order_importance: $t('重要度'),
  order_storage_capacity: $t('存储成本'),
  order_assetvalue_to_cost: $t('收益比'),
};

// 条件排序
const sortParams = {
  0: null,
  1: 'desc', // 降序
  2: 'asc', // 升序
};

const CancelToken = ajax.CancelToken;
let cancel1, cancel2, cancel3;
export default {
  components: {
    PermissionApplyWindow,
    SensitiveBlock,
  },
  props: {
    bizId: {
      type: [String, Number],
    },
    projectId: {
      type: [String, Number],
    },
    keyword: {
      type: String,
      dafault: '',
    },
    labelList: {
      type: Array,
      dafault: () => [],
    },
    platform: {
      type: String,
      default: '',
    },
    nodeTreeData: {
      type: Array,
      default: () => [],
    },
    isShowStandard: {
      type: Boolean,
      default: false,
    },
    advanceStandardParams: {
      // 数据标准高级搜索参数
      type: Object,
      default: () => ({}),
    },
    historySearchParams: {
      // 历史搜索参数
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      searchType: ['standard'],
      searchKeyWord: '',
      isTableLoading: false,
      isCountLoading: false,
      isSendStandardHistory: false,
      currentPage: 1,
      pageCount: 1,
      pageSize: 10,
      dataType: 'all',
      tableData: [],
      bkBizCount: 0,
      projectCount: 0,
      resultTableCount: 0,
      cleanDataCount: 0,
      isApplyPermissionShow: false,
      tag_code: 'virtual_data_mart',
      platFormNameList: {
        bk_data: this.$t('IEG数据平台'),
        tdw: 'TDW',
      },
      resultTableIcons: {
        clean: 'icon-clean-result',
        stream: 'icon-batch-result',
        batch: 'icon-offline-calc',
        batch_model: 'icon-modelflow-table',
        stream_model: 'icon-modelflow-table',
        model: 'icon-abnormal-table',
        transform: 'icon-transform-table',
      },
      tableHeaderTips: {
        [this.$t('热度评分')]: this.$t('热度评分tips'),
        [this.$t('广度评分')]: this.$t('广度评分tips'),
        [this.$t('价值评分')]: this.$t('价值评分tips'),
        [this.$t('热度')]: this.$t('热度评分tips'),
        [this.$t('广度')]: this.$t('广度评分tips'),
        [this.$t('重要度')]: this.$t('重要度tips'),
        [this.$t('存储成本')]: this.$t('存储成本tips'),
        [this.$t('价值成本')]: this.$t('价值成本tips'),
      },
      rateDetail: {
        range_metric: {
          node_count: 0,
          biz_count: 0,
          project_count: 0,
        },
      },
      heatDetail: {
        heat_metric: {
          count: 0,
        },
      },
      // 0默认无选中，1降序，2升序
      explorer: '', // 浏览器类型
      isSwitchStandard: true,
      isShowRate: true,
      sensitiveObj: {
        public: this.$t('公开'),
        private: this.$t('业务私有'),
        confidential: this.$t('业务机密'),
        topsecret: this.$t('业务绝密'),
      },
      openRequsetTimer: null,
      sortCondition: {
        // 用来计数表示是否排序，升序和降序, 0默认无选中，1降序，2升序
        count: 0,
        // 中文标签
        label: '',
        // 英文标签
        name: '',
      },
      tableFilteField: [
        'sensitivity',
        'asset_value_metric.asset_value_score',
        'cost_metric.format_capacity',
        'created_by'
      ],
      treeList: [
        {
          name: this.$t('选择展示列名'),
          expanded: true,
          id: 1,
          children: [
            {
              name: this.$t('敏感度'),
              field: 'sensitivity',
              id: 1,
              checked: true,
            },
            {
              name: this.$t('数据价值'),
              expanded: true,
              id: 2,
              children: [
                {
                  name: this.$t('价值评分'),
                  id: 1,
                  field: 'asset_value_metric.asset_value_score',
                  checked: true,
                },
                { name: this.$t('热度'), id: 2, field: 'heat_metric.heat_score' },
                {
                  name: this.$t('广度'),
                  id: 3,
                  parentId: 1,
                  field: 'range_metric.normalized_range_score',
                },
                {
                  name: this.$t('重要度'),
                  id: 4,
                  parentId: 1,
                  field: 'importance_metric.importance_score',
                },
              ],
            },
            {
              name: this.$t('存储成本'),
              field: 'cost_metric.format_capacity',
              id: 3,
              checked: true,
            },
            {
              name: this.$t('收益比（价值/成本）'),
              field: 'assetvalue_to_cost',
              id: 4,
            },
            {
              name: this.$t('创建者'),
              field: 'created_by',
              id: 5,
              checked: true,
            },
          ],
        },
      ],
      isStandard: '',
      isShowDataSys: true,
    };
  },
  computed: {
    pagination() {
      return {
        current: Number(this.currentPage),
        count: this.pageCount,
        limit: this.pageSize,
      };
    },
    ...mapState({
      userName: state => state.common.userName,
    }),
  },
  watch: {
    dataType(newVal) {
      this.getDictData(true);
    },
    isShowStandard: {
      immediate: true,
      handler(val) {
        const type = 'only_standard';
        if (val) {
          if (!this.searchType.includes(type)) {
            this.searchType.push(type);
          }
        } else {
          if (this.searchType.includes(type)) {
            let index = this.searchType.findIndex(item => item === type);
            this.searchType.splice(index, 1);
          }
        }
        this.getDictData(true);
      },
    },
    platform: {
      immediate: true,
      handler(val) {
        if (val === 'tdw') {
          this.dataType = 'all';
          this.$refs.table && this.clearSort(); // 清除最近修改时间排序状态
          this.sortCondition = {
            count: 0,
            label: '',
            name: '',
          };
        }
      },
    },
  },
  mounted() {
    this.getDictData();

    this.explorer = getExplorer();

    this.isSwitchStandard = this.$modules.isActive('standard');
    this.isShowRate = this.$modules.isActive('lifecycle');
  },
  beforeDestroy() {
    clearTimeout(this.openRequsetTimer);
  },
  methods: {
    standardReset() {
      this.isStandard = '';
      this.searchType = ['standard'];
      this.getDictData(true);
      this.$refs.standardPopover.hideHandler();
    },
    filterStandard() {
      if (typeof this.isStandard === 'boolean') {
        if (this.isStandard) {
          this.searchType = ['standard', 'only_standard'];
        } else {
          this.searchType = ['standard', 'only_not_standard'];
        }
      }
      Bus.$emit('disabledCreatedBy', this.searchType.includes('mine') ? true : false);
      this.getDictData(true);
      this.$refs.standardPopover.hideHandler();
    },
    radioChange(value) {
      this.isStandard = value;
    },
    renderStandard() {
      return (
        <bkdata-popover
          ref="standardPopover"
          trigger="manual"
          ext-cls="popover-style"
          placement="bottom-end"
          theme="light bk-table-setting-popover-content"
          distance={-2}>
          <div class="standard-field">
            {$t('是否标准')}
            <span
              class="icon-funnel"
              style={{ color: typeof this.isStandard === 'boolean' ? '#63656e' : '#c4c6cc' }}
              onClick={this.showTableField('standardPopover')}></span>
          </div>
          <div slot="content">
            <div class="standard-wrapper">
              <span class="standard-field">{$t('是否标准')}</span>
              <bkdata-radio-group onChange={this.radioChange} value={this.isStandard}>
                <bkdata-radio ext-cls="radio-style" value={true}>
                  {$t('是')}
                </bkdata-radio>
                <bkdata-radio disabled={!!this.advanceStandardParams.standard_name} ext-cls="radio-style" value={false}>
                  {$t('否')}
                </bkdata-radio>
              </bkdata-radio-group>
              <div class="content-options">
                <bkdata-button class="mr10" size="small" theme="primary" onClick={this.filterStandard}>
                  {$t('确定')}
                </bkdata-button>
                <bkdata-button theme="default" size="small" onClick={this.standardReset}>
                  {$t('重置')}
                </bkdata-button>
              </div>
            </div>
          </div>
        </bkdata-popover>
      );
    },
    showTableField(ref) {
      const that = this;
      return () => {
        if (ref === 'standardPopover' && that.advanceStandardParams.standard_name) {
          that.isStandard = '';
          postMethodWarning(this.$t('已选中数据标准，查询结果皆为该标准下的标准化数据'), 'warning', {
            delay: 3000,
          });
        }
        that.$refs[ref] && that.$refs[ref].showHandler();
      };
    },
    tableFieldFilter() {
      this.$refs.setPopver.hideHandler();
      this.$nextTick(() => {
        const that = this;
        that.tableFilteField = [];
        function recursive(list) {
          for (let index = 0; index < list.length; index++) {
            if (list[index].checked) {
              that.tableFilteField.push(list[index].field);
            }
            if (list[index].children) {
              recursive(list[index].children);
            }
          }
        }
        recursive(this.treeList);
        this.fixDataSys();
      });
    },
    tableFieldReset() {
      this.tableFilteField = [
        'sensitivity',
        'asset_value_metric.asset_value_score',
        'cost_metric.format_capacity',
        'created_by'
      ];
      function recursive(list, tableFilteField) {
        for (let index = 0; index < list.length; index++) {
          list[index].checked = tableFilteField.includes(list[index].field) ? true : false;
          if (list[index].children) {
            recursive(list[index].children, tableFilteField);
          }
        }
      }
      recursive(this.treeList, this.tableFilteField);
      this.$refs.setPopver.hideHandler();
      this.fixDataSys();
    },
    fixDataSys() {
      this.isShowDataSys = false;
      this.$nextTick(() => {
        this.isShowDataSys = true;
      });
    },
    renderSetting() {
      return (
        <bkdata-popover
          ref="setPopver"
          trigger="manual"
          ext-cls="setting-popover-style"
          placement="left-end"
          theme="light bk-table-setting-popover-content"
          distance={-2}>
          <i title={$t('点击可配置更多列信息')} onClick={this.showTableField('setPopver')} class={'icon-cog-shape '}></i>
          <div slot="content">
            <bkdata-tree
              ext-cls={'hidden-icon'}
              data={this.treeList}
              multiple={true}
              node-key={'id'}
              has-border={true}></bkdata-tree>
            <div class="content-options">
              <bkdata-button class="mr10" size="small" theme="primary" onClick={this.tableFieldFilter}>
                {$t('确定')}
              </bkdata-button>
              <bkdata-button theme="default" size="small" onClick={this.tableFieldReset}>
                {$t('重置')}
              </bkdata-button>
            </div>
          </div>
        </bkdata-popover>
      );
    },
    judgeIsShowField(type) {
      return this.tableFilteField.includes(type);
    },
    // 搜索标准化数据时，需要固定参数
    getStandardParams() {
      this.searchType = ['standard', 'only_standard'];
    },
    getEnName(row, enField = 'tag_code', zhField = 'tag_alias') {
      if (this.$i18n.locale === 'en') {
        return row[enField];
      }
      return row[zhField];
    },
    linkToStandard(data) {
      if (data.is_standard !== 1) return;
      this.$router.push({
        name: 'DataStandardDetail',
        query: {
          id: data.standard_id,
        },
      });
    },
    clearSort() {
      this.$refs.table.clearSort();
    },
    summaryCount(label, name) {
      const that = this;
      return () => {
        that.isTableLoading = true;

        that.$nextTick(() => {
          that.isTableLoading = false;

          let count = 0;

          if (that.sortCondition.label === label) {
            count = this.sortCondition.count;
          }

          count += 1;

          if (count === 3) {
            count = 0;
          }

          that.sortCondition = {
            count,
            label,
            name,
          };
          that.getDictData(true);
        });
      };
    },
    rateSort(label, count, name) {
      const that = this;
      return e => {
        window.event ? (window.event.cancelBubble = true) : e.stopPropagation();

        that.isTableLoading = true;
        that.$nextTick(() => {
          that.isTableLoading = false;

          that.sortCondition = {
            count,
            label,
            name,
          };

          that.getDictData(true);
        });
      };
    },
    getDes(des) {
      return des ? `（${des}）` : '';
    },
    getSafeValue(value, placeholder = '') {
      if (typeof value === 'number') {
        return value;
      }
      return value || placeholder;
    },
    getRangeTooltips() {
      return {
        appendTo: document.body,
        placement: 'right',
        extCls: 'dict-rate-tool-content',
        interactive: false,
        content: `<div
                    class="clearfix"
                    style="white-space: normal;max-width: 500px;max-height: 400px;">
                    <div>${this.$t('应用数据')}：<span>${this.rateDetail.range_metric.node_count || 0}个</span></div>
                    <div>${this.$t('应用项目')}：<span>${this.rateDetail.range_metric.project_count || 1}个</span></div>
                    <div>${this.$t('应用业务')}：<span>${this.rateDetail.range_metric.biz_count || 1}个</span></div>
                    <div>${this.$t('应用APP')}：<span>${this.rateDetail.range_metric.app_code_count || 0}个</span></div>
                  </div>`,
      };
    },
    getHeatTooltips() {
      return {
        appendTo: document.body,
        placement: 'right',
        extCls: 'dict-rate-tool-content',
        interactive: false,
        content: `<div>${this.$t('最近7天')} ${this.$t('查询')}：<span>${this.heatDetail.heat_metric.count
          || this.heatDetail.heat_metric.query_count
          || 0}次</span></div>`,
      };
    },
    changeProgressBack(index) {
      if (this.tableData[index].hasOwnProperty('range_metric') && this.tableData[index].range_metric) {
        this.rateDetail = this.tableData[index];
      }
      if (this.tableData[index].hasOwnProperty('heat_metric') && this.tableData[index].heat_metric) {
        this.heatDetail = this.tableData[index];
      }
    },
    renderHeader(h, { column, $index }) {
      const that = this;

      // 0默认无选中，1降序，2升序
      let isClickUp = 0;

      if (this.sortCondition.label === column.label) {
        isClickUp = this.sortCondition.count;
      }

      const name = Object.keys(sortFieldMap).find(item => sortFieldMap[item] === column.label);

      return h(
        'div',
        {
          style: {
            display: 'flex',
            alignItems: 'center',
            paddingLeft: column.label === $t('存储成本') ? '11px' : 0,
          },
          on: {
            click: that.summaryCount(column.label, name),
          },
        },
        [
          h('span', {
            class: ['icon-question-circle'],
            style: {
              margin: '1px 5px 0 0px',
              verticalAlign: 'middle',
              color: column.label === that.$t('热度评分') ? '#fe9c00' : '#3a84ff',
              display: column.property === 'updated_at' ? 'none' : 'inherit',
            },
            directives: [
              {
                name: 'bkTooltips',
                value: that.tableHeaderTips[that.$t(column.label)],
              },
            ],
          }),
          column.label,
          h(
            'span',
            {
              class: ['bk-table-caret-wrapper', that.platform === 'tdw' ? 'hidden' : ''],
              style: {
                width: '20px',
              },
            },
            [
              h('i', {
                class: ['bk-table-sort-caret', 'descending', isClickUp === 1 ? 'descend-clicked' : ''],
                on: {
                  click: that.rateSort(column.label, 1, name),
                },
              }),
              h('i', {
                class: ['bk-table-sort-caret', 'ascending', isClickUp === 2 ? 'ascend-clicked' : ''],
                on: {
                  click: that.rateSort(column.label, 2, name),
                },
              }),
            ]
          ),
        ]
      );
    },
    switchType(name, index) {
      if (this.$route.params.parentNodes.length === 1
        && this.$route.params.parentNodes[0].category_name === 'virtual_data_mart') {
        this.$router.push({
          name: 'DataMap',
        });
      }
      if (this.tag_code === name) return;
      this.$route.params.parentNodes.splice(index + 1);
      this.tag_code = name;
      this.getDictData(true);
    },
    returnTitle(item) {
      if (item.row.data_set_type === 'result_table' || item.row.data_set_type === 'tdw_table') {
        return item.row.processing_type_alias ? item.row.processing_type_alias + this.$t('结果表') : this.$t('结果表');
      } else if (item.row.data_set_type === 'raw_data') {
        return this.$t('数据源');
      }
    },
    addDataType(type) {
      if (this.isTableLoading || !this.isSwitchStandard) return;
      if (this.searchType.includes(type)) {
        let index = this.searchType.findIndex(item => item === type);
        this.searchType.splice(index, 1);
      } else {
        this.searchType.push(type);
      }
      Bus.$emit('disabledCreatedBy', this.searchType.includes('mine') ? true : false);
      this.getDictData(true);
    },
    changeDataType(type) {
      this.dataType = type;
      this.$emit('projDisabled', type === 'raw_data');
    },
    applyPermission(item) {
      if (item.is_not_exist) return;
      let roleId;
      if (item.data_set_type === 'raw_data') {
        roleId = 'raw_data.viewer';
      } else if (item.data_set_type === 'result_table') {
        roleId = 'result_table.viewer';
      } else if (item.data_set_type === 'tdw_table') {
        return window.open(window.TDWSiteUrl + 'db_table?s_menu=db');
      }
      this.isApplyPermissionShow = true;
      this.$nextTick(() => {
        this.$refs.apply
          && this.$refs.apply.openDialog({
            data_set_type: item.data_set_type,
            bk_biz_id: item.bk_biz_id,
            data_set_id: item.data_set_id,
            roleId: roleId,
          });
      });
    },
    handlePageLimitChange(size) {
      this.pageSize = size;
      this.getDictData(true);
    },
    handlePageChange(page) {
      this.currentPage = page;
      this.getDictData();
    },
    linkTo(item) {
      item = JSON.parse(JSON.stringify(item));
      if (item.is_not_exist) return;
      let id = 'result_table_id';
      if (item.data_set_type === 'raw_data') {
        id = 'data_id';
      } else if (item.platform === 'tdw') {
        id = 'tdw_table_id';
        item.data_set_id = item.table_id;
      }
      const standard_id = item.is_standard ? item.standard_id : null;
      this.$router.push({
        name: 'DataDetail',
        query: {
          dataType: item.data_set_type,
          [id]: item.data_set_id,
          bk_biz_id: item.bk_biz_id,
          standard_id,
          project_id: item.project_id,
        },
      });
    },
    getDictData(isResetPage) {
      if (this.isTableLoading) {
        cancel1 && cancel1();
      }
      if (this.isCountLoading) {
        cancel2 && cancel2();
      }
      if (this.isSendStandardHistory) {
        cancel3 && cancel3();
      }
      if (isResetPage) {
        // 是否重置当前页
        this.currentPage = 1;
      }
      this.isTableLoading = true;
      this.isCountLoading = true;
      this.isSendStandardHistory = true;

      const options = {
        params: {
          bk_biz_id: this.bizId || null,
          project_id: this.projectId || null,
          tag_ids: this.labelList,
          keyword: this.keyword,
          tag_code: this.tag_code,
          me_type: 'tag',
          has_standard: 1,
          cal_type: this.searchType,
          data_set_type: this.dataType,
          page: this.currentPage,
          page_size: this.pageSize,
          platform: this.platform || 'all',
        },
      };
      // 排序条件
      const sortIndex = sortParams[this.sortCondition.count];
      if (sortIndex) {
        options.params = Object.assign({}, options.params, {
          [this.sortCondition.name]: sortIndex,
        });
      }

      if (Object.keys(this.advanceStandardParams).length) {
        // 数据标准高级搜索
        for (const key in this.advanceStandardParams) {
          if (!this.advanceStandardParams[key] && this.advanceStandardParams[key] !== 0) {
            delete this.advanceStandardParams[key];
          }
        }
        options.params = Object.assign({}, options.params, this.advanceStandardParams);
      }
      if (Object.keys(this.historySearchParams).length) {
        // 历史搜索参数
        for (const key in this.historySearchParams) {
          if (!this.historySearchParams[key]) {
            delete this.historySearchParams[key];
          }
          if (Object.keys(sortFieldMap).includes(key) && this.historySearchParams[key]) {
            this.sortCondition = {
              count: Number(Object.keys(sortParams).find(item => sortParams[item] === this.historySearchParams[key])),
              label: sortFieldMap[key],
              name: key,
            };
          }
        }
        // 数据类型和标准化数据的回填
        this.dataType = this.historySearchParams.data_set_type;
        if (typeof this.historySearchParams.is_standard === 'boolean') {
          if (this.historySearchParams.is_standard) {
            this.searchType = ['standard', 'only_standard'];
          } else {
            this.searchType = ['standard', 'only_not_standard'];
          }
        } else {
          this.searchType = ['standard'];
        }
        options.params = Object.assign({}, options.params, this.historySearchParams);
        this.$emit('cancelHistorySearch'); // 解除历史搜索状态
      }
      if (this.searchType.includes('mine')) {
        // 我的数据
        options.params['is_filterd_by_created_by'] = true;
        options.params['created_by'] = this.$store.state.common.userName;
      }
      if (isNumber(options.params.tag_code)) {
        // 标准数据时，清空节点树
        options.params.me_type = 'standard';
        this.$emit('clearNodeTree');
      }
      ajax
        .post('/datamart/datadict/dataset_list/', options.params, {
          cancelToken: new CancelToken(function executor(c) {
            cancel1 = c;
          }),
        })
        .then(res => {
          if (res.result) {
            this.tableData = res.data.results;
            this.tableData.forEach(item => {
              this.$set(item, 'isCollect', false);
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.fixDataSys();
          this.isTableLoading = false;
        });
      ajax
        .post('v3/datamanage/datamap/retrieve/get_data_dict_count/', options.params, {
          cancelToken: new CancelToken(function executor(c) {
            cancel2 = c;
          }),
        })
        .then(res => {
          if (res.result) {
            this.pageCount = res.data.count;
            this.bkBizCount = res.data.bk_biz_count;
            this.projectCount = res.data.project_count;
            this.resultTableCount = res.data.dataset_count;
            this.cleanDataCount = res.data.data_source_count;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.isCountLoading = false;
        });
      // 标准化数据时，需要传is_standard给后端
      options.params.is_standard = this.searchType.includes('only_standard');
      ajax
        .post('datamart/datadict/dataset_list/set_search_history/', options.params, {
          cancelToken: new CancelToken(function executor(c) {
            cancel3 = c;
          }),
        })
        .then(res => {
          if (res.result) {
            // 在处理历史搜索条件回填时，会触发多次请求，需要控制
            this.openRequsetTimer = setTimeout(() => {
              this.$emit('reOpenRequest');
            }, 1000);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.isSendStandardHistory = false;
        });
    },
  },
};
</script>
<style lang="scss">
.dict-rate-tool-content {
  .tippy-content {
    transform: scale(0.95);
  }
}
</style>
<style lang="scss" scoped>
.is-selected {
  background-color: #e1ecff !important;
  border-color: #3a84ff !important;
  color: #3a84ff !important;
  position: relative !important;
  z-index: 1 !important;
}
::v-deep .hidden {
  display: none;
}
::v-deep .ascend-clicked {
  border-bottom-color: #63656e !important;
}
::v-deep .descend-clicked {
  border-top-color: #63656e !important;
}
@media screen and (max-width: 1685px) {
  .table-data-name-safari {
    max-width: 240px;
  }
  .proj-name-safari {
    max-width: 120px;
  }
}
@media screen and (min-width: 1685px) {
  .table-data-name-safari {
    max-width: 360px;
  }
  .proj-name-safari {
    max-width: 178px;
  }
}
::v-deep .range-rate-star-wrap,
::v-deep .heat-rate-star-wrap {
  .bk-rate-star {
    margin-right: 0;
  }
}
::v-deep .range-rate-star-wrap {
  .bk-rate-stars .bk-rate-star.bk-yellow {
    fill: #3a84ff;
  }
}
.data-dict {
  .data-table {
    position: relative;
    .table-item {
      display: inline-block;
      width: 100%;
    }
    .click-style {
      cursor: pointer;
      color: #3a84ff;
    }
    .text-right {
      text-align: right;
      padding-right: 17px;
    }
    .pr-storage {
      width: 105px;
      padding-right: 30px;
    }
    .rate-progress {
      outline: none;
      .rate-score {
        transform: scale(0.9);
        margin-left: -5px;
        color: #3a84ff;
      }
      .heat-color {
        color: #ff9c01;
      }
    }
    span {
      outline: none;
    }
    .disabled {
      color: #0e193a;
      cursor: not-allowed !important;
    }
    .font-weight7 {
      font-weight: 700;
    }
    .icon-star-shape,
    .icon-star {
      cursor: pointer;
      font-size: 18px;
      color: #ff9c01;
    }
    .icon-check-circle-fill {
      font-size: 20px;
      color: #2dcb56;
    }
    .icon-ellipsis {
      font-size: 18px;
      color: #3a84ff;
      font-weight: bolder;
    }
    .data-count {
      position: absolute !important;
      z-index: 101;
      left: 0;
      bottom: 0;
      background: white;
      height: 62px;
      line-height: 62px;
      padding-left: 15px;
      border-left: 1px solid #dfe0e5;
      border-bottom: 1px solid #dfe0e5;
      span {
        color: #3a84ff;
      }
    }
    .data-count-bottom {
      bottom: -1px;
    }
    .bk-table {
      .cell {
        -webkit-line-clamp: inherit;
      }
      .bk-form-checkbox {
        margin-right: 0;
      }
      ::v-deep .table-data-name {
        position: relative;
        padding: 2px 0px;
        display: flex;
        align-items: center;
        .data-dict-badge {
          position: absolute;
          display: inline-block;
          left: -15px;
          top: -15px;
          border: 16px solid #30d878;
          width: 0px;
          height: 0px;
          border-bottom-color: transparent;
          border-right-color: transparent;
        }
        .table-data-left {
          font-size: 22px;
          vertical-align: unset;
          color: #c4c6cc;
        }
        .platform {
          display: flex;
          align-items: center;
          .platform-name {
            max-width: 110px;
          }
          .platform-item {
            display: inline-block;
            max-width: 69px;
            min-width: 34px;
            margin-left: 10px;
            padding: 2px 4px;
            border: 1px solid #ddd;
          }
          .platform-item:first-child {
            margin-left: 0px;
          }
          .platform-item-ellipsis {
            display: flex;
            align-items: center;
            color: #63656e;
            height: 23px;
            display: inline-block;
            padding: 0px 4px;
            margin-left: 10px;
            border: 1px solid #ddd;
          }
          .platform-online {
            background-color: #3a84ff;
            border-color: #3a84ff;
            color: white;
          }
        }
        .table-data-right {
          margin-left: 5px;
          .biz-name {
            margin-bottom: 3px;
            .data-name {
              font-size: 14px;
              font-weight: bold;
              cursor: pointer;
              color: #3a84ff;
            }
            .biz-count {
              border-left: 1px solid #e6e6e6;
              padding-left: 10px;
              margin-left: 10px;
              font-weight: 400;
            }
            .no-click {
              cursor: not-allowed;
            }
          }
        }
      }
      .created {
        width: 90px;
      }
      .data-sensitive,
      .data-warning {
        display: flex;
        justify-content: center;
        width: 90px;
        padding: 2px 20px;
        border-radius: 20px;
        white-space: nowrap;
      }
      .data-sensitive {
        border: 1px solid #2dcb56;
        color: #2dcb56;
      }
      .data-warning {
        border: 1px solid #ff9c01;
        color: #ff9c01;
      }
    }
  }
  .data-table-border {
    border: 1px solid #dfe0e5;
  }
  .sub-head {
    display: flex;
    justify-content: space-between;
    margin-bottom: 10px;
    .node-tree {
      li {
        float: left;
        padding: 5px 6px 0px 0px;
        cursor: pointer;
        p {
          display: inline-block;
          color: #3a84ff;
        }
      }
    }
    .bkdata-button-group {
      display: flex;
      .search-input {
        margin-left: 40px;
        display: flex;
        align-items: flex-end;
        .search-input-container {
          width: 390px;
          margin-right: 20px;
        }
        .data-type-wrap {
          li {
            position: relative;
            float: left;
            margin-right: 12px;
            border: 1px solid #c4c6cc;
            padding: 3px 6px;
            cursor: pointer;
            font-size: 12px;
            color: rgb(102, 107, 180);
            outline: none;
            .corner-mark {
              position: absolute;
              width: 0;
              height: 0px;
              display: inline-block;
              right: 0;
              bottom: 0px;
              border: 7px solid transparent;
              .icon-check-1 {
                position: absolute;
                right: -9px;
                bottom: -10px;
                font-size: 12px;
                color: white;
                transform: scale(0.7);
              }
            }
            .border-clicked {
              border-color: transparent #3a84ff #3a84ff transparent;
            }
          }
        }
      }
    }
  }
}
::v-deep .icon-cog-shape {
  display: inline-block;
  vertical-align: middle;
  width: 24px;
  height: 24px;
  line-height: 24px;
  font-size: 14px;
  color: #979ba5;
  cursor: pointer;
}
::v-deep .hidden-icon {
  .tree-drag-node {
    display: flex;
    align-items: center;
    .tree-expanded-icon {
      margin-right: 5px;
    }
    .icon-folder-open,
    .icon-file,
    .icon-folder {
      display: none;
    }
    .bk-form-checkbox,
    .bk-form-half-checked {
      margin-right: 0 !important;
    }
  }
}
.popover-style,
.setting-popover-style {
  .content-options {
    display: flex;
    justify-content: center;
    margin: 10px 0;
  }
  .standard-wrapper {
    .standard-field {
      display: inline-block;
      font-size: 14px;
      margin: 5px 0;
    }
    .bk-form-control {
      .bk-form-radio {
        width: 100%;
        margin-right: 0;
        line-height: 32px;
      }
    }
  }
}
::v-deep .standard-field {
  .icon-funnel {
    margin-left: 5px;
    height: 20px;
    line-height: 20px;
    font-size: 14px;
    text-align: center;
    cursor: pointer;
    color: #c4c6cc;
  }
}
</style>
