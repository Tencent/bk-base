

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
  <div class="search-container">
    <bkdata-tab v-show="isShowAdvancedSearch"
      :active="tab"
      :class="'node-editing'"
      @tab-change="tabChange">
      <template v-if="tab === 'search'"
        slot="setting">
        <div class="clear-button">
          <bkdata-button :theme="'default'"
            :title="$t('清空')"
            class="mr10"
            @click="clearAll">
            {{ $t('清空') }}
          </bkdata-button>
        </div>
      </template>
      <bkdata-tab-panel :label="$t('高级搜索')"
        name="search">
        <form class="bk-form">
          <div class="bk-form-item mt20">
            <label class="bk-label pr15">{{ $t('存储类型') }}：</label>
            <div class="bk-form-content">
              <bkdata-selector
                class="platform-select"
                searchable
                :allowClear="true"
                :popoverOptions="getBoundary()"
                :isLoading="loading.isStorageLoading"
                :selected.sync="localValue.storage_type"
                :displayKey="'alias'"
                :placeholder="$t('请选择存储类型')"
                :list="storageTypeList"
                :settingKey="'name'"
                searchKey="alias"
                @item-selected="changeStorageType"
                @clear="clearStorageType" />
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('热度评分') }}：</label>
            <div class="bk-form-content width300">
              <bkdata-selector class="platform-select"
                searchable
                :allowClear="true"
                :selected.sync="localValue.heat_operate"
                :displayKey="'name'"
                :placeholder="$t('选择条件')"
                :popoverOptions="getBoundary()"
                :list="rateCondition"
                :settingKey="'code'"
                searchKey="name" />
              <bkdata-input v-model="localValue.heat_score"
                :clearable="true"
                type="number"
                :max="100"
                :min="0"
                :precision="2" />
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('价值评分') }}：</label>
            <div class="bk-form-content width300">
              <bkdata-selector class="platform-select"
                searchable
                :allowClear="true"
                :selected.sync="localValue.asset_value_operate"
                :displayKey="'name'"
                :placeholder="$t('选择条件')"
                :popoverOptions="getBoundary()"
                :list="rateCondition"
                :settingKey="'code'"
                searchKey="name" />
              <bkdata-input v-model="localValue.asset_value_score"
                :clearable="true"
                type="number"
                :max="100"
                :min="0"
                :precision="2" />
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('存储成本') }}：</label>
            <div class="bk-form-content width300">
              <bkdata-selector class="platform-select"
                searchable
                :allowClear="true"
                :selected.sync="localValue.storage_capacity_operate"
                :displayKey="'name'"
                :placeholder="$t('选择条件')"
                :popoverOptions="getBoundary()"
                :list="rateCondition"
                :settingKey="'code'"
                searchKey="name" />
              <bkdata-input v-model="localValue.storage_capacity"
                :clearable="true"
                :type="storageInputType"
                :max="100"
                :min="0"
                :precision="2" />
              <bkdata-selector class="platform-select"
                disabled
                :selected.sync="selectedStorageUnit"
                :displayKey="'name'"
                :placeholder="$t('选择条件')"
                :popoverOptions="getBoundary()"
                :list="storageUnit"
                :settingKey="'code'"
                searchKey="name" />
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('广度评分') }}：</label>
            <div class="bk-form-content width300">
              <bkdata-selector class="platform-select"
                searchable
                :allowClear="true"
                :selected.sync="localValue.range_operate"
                :displayKey="'name'"
                :placeholder="$t('选择条件')"
                :popoverOptions="getBoundary()"
                :list="rateCondition"
                :settingKey="'code'"
                searchKey="name" />
              <bkdata-input v-model="localValue.range_score"
                :clearable="true"
                extCls="range-rate-style"
                type="number"
                :max="100"
                :min="0"
                :precision="2" />
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('收益比(价值/成本)') }}：</label>
            <div class="bk-form-content width300">
              <bkdata-selector class="platform-select"
                searchable
                :allowClear="true"
                :selected.sync="localValue.assetvalue_to_cost_operate"
                :displayKey="'name'"
                :placeholder="$t('选择条件')"
                :popoverOptions="getBoundary()"
                :list="rateCondition"
                :settingKey="'code'"
                searchKey="name" />
              <bkdata-input v-model="localValue.assetvalue_to_cost"
                :clearable="true"
                type="number"
                :min="0"
                :precision="2" />
            </div>
          </div>
        </form>
        <form class="bk-form">
          <div class="bk-form-item mt20">
            <label class="bk-label pr15">{{ $t('数据标准') }}：</label>
            <div class="bk-form-content">
              <bk-cascade ref="cascadeInstance"
                v-model="cascadeName"
                :list="standardData"
                :checkAnyLevel="true"
                clearable
                @change="cascadeChange" />
              <!-- <bkdata-selector
                                class="platform-select"
                                searchable
                                :allow-clear="true"
                                :is-loading="loading.isStandardLoading"
                                :selected.sync="standard_name"
                                :display-key="'name'"
                                :placeholder="$t('请选择标准名称')"
                                :popover-options="getBoundary()"
                                :list="standardData"
                                :setting-key="'id'"
                                search-key="name"
                                @item-selected="changeStandardName"
                                @clear="clearStandard"
                            ></bkdata-selector> -->
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('重要度评分') }}：</label>
            <div class="bk-form-content width300">
              <bkdata-selector class="platform-select"
                searchable
                :allowClear="true"
                :selected.sync="localValue.importance_operate"
                :displayKey="'name'"
                :placeholder="$t('选择条件')"
                :popoverOptions="getBoundary()"
                :list="rateCondition"
                :settingKey="'code'"
                searchKey="name" />
              <bkdata-input v-model="localValue.importance_score"
                :clearable="true"
                type="number"
                :max="100"
                :min="0"
                :precision="2" />
            </div>
          </div>
          <!-- <div class="bk-form-item mt20">
                        <label class="bk-label pr15"
                            >{{ $t('创建时间') }}：</label
                        >
                        <div
                            class="bk-form-content date-width"
                            v-if="isShowDatePick"
                        >
                            <bkdata-date-picker
                                v-model="createdTime"
                                :shortcut-close="true"
                                :use-shortcut-text="true"
                                :shortcuts="shortcuts"
                                :transfer="true"
                                format="yyyy-MM-dd HH:mm"
                                :placeholder="$t('选择日期时间范围')"
                                :type="'datetimerange'"
                            ></bkdata-date-picker>
                        </div>
                    </div> -->
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('创建者') }}：</label>
            <div class="bk-form-content auto-width">
              <bkdata-tag-input v-model="singleSelect"
                v-bk-tooltips="tipContent()"
                style="width: 200px;"
                :disabled="isMemberDisabled"
                :placeholder="$t('请输入并按Enter结束')"
                :hasDeleteIcon="true"
                :list="allUsersForTag"
                :maxData="1"
                :tpl="tpl"
                @change="changeMember" />
              <div class="recent-date">
                <a href="javascript:void(0)"
                  @click="changeUser">
                  {{ $t('我的数据') }}
                </a>
              </div>
            </div>
          </div>
          <div v-if="isShowCreatedTime"
            class="bk-form-item mt20">
            <label class="bk-label pr15">{{ $t('创建时间') }}：</label>
            <div v-if="isShowDatePick"
              class="bk-form-content date-width">
              <bkdata-date-picker v-model="createdTime"
                :shortcutClose="true"
                :useShortcutText="true"
                :shortcuts="shortcuts"
                :transfer="true"
                format="yyyy-MM-dd HH:mm"
                :placeholder="$t('选择日期时间范围')"
                :type="'datetimerange'"
                @change="changeCreatedTime" />
            </div>
          </div>
        </form>
        <div class="operation-panel mt20 mb20">
          <bkdata-button :theme="'primary'"
            :title="$t('确定')"
            class="mr30"
            @click="confirm">
            {{ $t('确定') }}
          </bkdata-button>
          <bkdata-button :theme="'default'"
            type="submit"
            :title="$t('取消')"
            @click="cancel">
            {{ $t('取消') }}
          </bkdata-button>
        </div>
      </bkdata-tab-panel>
      <bkdata-tab-panel :label="$t('历史搜索')"
        name="history">
        <div v-if="tab === 'history'"
          v-bkloading="{ isLoading: loading.isHistoryLoading }"
          class="history-container bk-scroll-y">
          <template v-if="historyData.length">
            <div v-for="(item, idx) in historyData"
              :key="idx"
              class="history-info">
              <span class="time-str">{{ item.time }}</span>
              <div class="info-wrap">
                <div v-for="(key, index) in Object.keys(item).filter(i => !extraFeild.includes(i))"
                  :key="index"
                  class="info-item"
                  @click="historySearch(idx)">
                  <span v-if="key === 'platform'">{{ `${historyFields[key]}：TDW` }}；</span>
                  <span v-else-if="key === 'tag_ids'">
                    {{ `${historyFields[key]}：${getLabels(item.tag_ids_alias)}` }}；
                  </span>
                  <span v-else-if="Object.values(dataValueOperateMap).includes(key)">
                    {{
                      `${historyFields[key]}：${getRateOperate(item, key)}
                                        ${key === 'storage_capacity' ? `${item[key]}MB` : item[key]}` }}；
                  </span>
                  <span v-else-if="sortFields.includes(key)">
                    {{ `${historyFields[key]}：${orderFields[item[key]]}` }}；
                  </span>
                  <span v-else-if="key === 'tag_code'">
                    {{ `${isNumber(item[key])
                      ? $t('数据标准') : $t('分类')}：${isNumber(item[key])
                      ? item.standard_name : item.category_alias}` }}；
                  </span>
                  <span v-else-if="key === 'standard_content_id'">
                    {{ `${historyFields[key]}：${item['standard_content_name']}` }}；
                  </span>
                  <span v-else-if="key === 'data_set_type' && item[key] !== 'all'">
                    {{ `${historyFields[key]}：${item[key] === 'raw_data'
                      ? $t('数据源') : $t('结果表')}` }}；
                  </span>
                  <span v-else-if="key === 'created_at_start'">
                    {{ `${historyFields[key]}：${item[key]} - ${item.created_at_end}` }}；
                  </span>
                  <span v-else-if="key === 'is_standard'">
                    {{ `${historyFields[key]}：${$t('是')}` }}；
                  </span>
                  <span v-else-if="key === 'keyword'"
                    :title="`${historyFields[key]}：${item[key]}`"
                    class="max200 text-overflow">
                    {{ `${historyFields[key]}：${item[key]}` }}；
                  </span>
                  <span v-else>{{ `${historyFields[key]}：${item[key]}` }}；</span>
                </div>
              </div>
            </div>
          </template>
          <div v-else
            class="no-history">
            {{ $t('暂无历史搜索记录') }}
          </div>
        </div>
      </bkdata-tab-panel>
    </bkdata-tab>
  </div>
</template>

<script>
import { getAllUsers } from '@/common/api/base';
import { showMsg } from '@/common/js/util.js';
import Bus from '@/common/js/bus';
import { bkCascade } from 'bk-magic-vue';
import { dataValueOperateMap } from '@/pages/datamart/common/config';

export default {
  components: { bkCascade },
  model: {
    prop: 'value',
    event: 'change',
  },
  props: {
    value: {
      type: Object,
      default: () => ({}),
    },
    isShowAdvancedSearch: {
      type: Boolean,
      default: true,
    },
    tab: {
      type: String,
      default: 'search',
    },
  },
  data() {
    return {
      createdTime: [],
      loading: {
        projectLoading: false,
        bizLoading: false,
        isPlatformLoading: false,
        isStandardLoading: false,
        isStorageLoading: false,
        isRangeLoading: false,
        isHeatLoading: false,
        isHistoryLoading: false,
      },
      channelList: [],
      singleSelect: [],
      allUsers: [],
      activeTab: 'search',
      standardData: [],
      standard_name: '',
      storageTypeList: [],
      storageType: '',
      rateCondition: [
        {
          code: 'eq',
          name: this.$t('等于_'),
        },
        {
          code: 'ge',
          name: this.$t('大于等于'),
        },
        {
          code: 'le',
          name: this.$t('小于等于'),
        },
        {
          code: 'gt',
          name: this.$t('大于'),
        },
        {
          code: 'lt',
          name: this.$t('小于'),
        },
      ],
      rangeRateData: [],
      heatRateData: [],
      shortcuts: [
        {
          text: this.$t('最近1天'),
          value() {
            const end = new Date();
            const start = new Date();
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 1);
            return [start, end];
          },
        },
        {
          text: this.$t('最近1周'),
          value() {
            const end = new Date();
            const start = new Date();
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 7);
            return [start, end];
          },
        },
      ],
      originHeatValue: 0,
      originRangeValue: 0,
      isShowDatePick: true,
      searchHistoryData: [],
      historyFields: {
        platform: this.$t('数据所在系统'),
        bk_biz_id: this.$t('所属业务'),
        project_id: this.$t('所属项目'),
        data_set_type: this.$t('数据类型'),
        storage_type: this.$t('存储类型'),
        tag_code: this.$t('数据标准'),
        standard_content_id: this.$t('标准内容'),
        is_standard: this.$t('标准化数据'),
        tag_ids: this.$t('标签'),
        keyword: this.$t('关键词'),
        created_by: this.$t('创建者'),
        created_at_start: this.$t('创建时间'),
        order_heat: this.$t('按热度评分排序'),
        order_range: this.$t('按广度评分排序'),
        order_time: this.$t('按时间排序'),
        order_asset_value: this.$t('按价值评分排序'),
        order_importance: this.$t('按重要度评分排序'),
        order_storage_capacity: this.$t('按存储成本排序 '),
        order_assetvalue_to_cost: this.$t('按收益比排序'),
        heat_score: this.$t('热度评分区间'),
        range_score: this.$t('广度评分区间'),
        asset_value_score: this.$t('价值评分区间'),
        storage_capacity: this.$t('存储成本区间'),
        importance_score: this.$t('重要度评分区间'),
        assetvalue_to_cost: this.$t('收益比区间'),
      },
      dataSystem: {
        bk_data: this.$t('IEG数据平台'),
        tdw: 'TDW',
      },
      orderFields: {
        asc: this.$t('升序'),
        desc: this.$t('降序'),
      },
      isMemberDisabled: false,
      extraFeild: [
        // 不需要展示的属性
        'heat_operate',
        'range_operate',
        'time',
        'tag_ids_alias',
        'standard_name',
        'bk_biz_name',
        'project_name',
        'created_at_end',
        'category_alias',
        'standard_content_name',
        'asset_value_operate',
        'storage_capacity_operate',
        'importance_operate',
        'assetvalue_to_cost_operate',
      ],
      cascadeName: [],
      sortFields: [
        'order_time',
        'order_heat',
        'order_range',
        'order_asset_value',
        'order_importance',
        'order_storage_capacity',
        'order_assetvalue_to_cost'
      ],
      storageInputType: 'number',
      storageUnit: [
        {
          code: 'MB',
          name: 'MB',
        },
      ],
      selectedStorageUnit: 'MB',
      isShowCreatedTime: true,
    };
  },
  computed: {
    localValue: {
      get() {
        return this.value;
      },
      set(val) {
        // eslint-disable-next-line vue/no-mutating-props
        this.value = val;
      },
    },
    dataValueOperateMap() {
      return dataValueOperateMap;
    },
    historyData() {
      const bizProjObj = {
        bk_biz_id: 'bk_biz_name',
        project_id: 'project_name',
      };
      const filterFieldsData = JSON.parse(JSON.stringify(this.searchHistoryData));
      filterFieldsData.map(item => {
        for (const key in item) {
          if (
            (key === 'data_set_type' && item[key] === 'all') // 过滤data_set_type为all的情况
            || (key === 'platform' && item[key] === 'bk_data') // 过滤数据所在系统为IEG数据平台的情况
            || (key === 'tag_code' // 过滤数据地图分类的情况
              && item[key] === 'virtual_data_mart')
            || (key === 'is_standard' && !item[key])
            || ![
              // 过滤掉不需要的属性
              ...Object.keys(this.historyFields), // this.historyFields为需要展示并且排序了的属性数组
              ...this.extraFeild,
            ].includes(key)
            || [null, ''].includes(item[key]) // 过滤所有值不存在的属性
            || (key === 'tag_ids' && !item[key].length) // 过滤tag_ids为空数组的情况
          ) {
            delete item[key];
          }
        }
        return item;
      });
      const orderFields = {}; // 创建数组排序用到的对象
      Object.keys(this.historyFields).forEach((item, index) => {
        orderFields[item] = index;
      });
      return filterFieldsData.map(item => {
        const newArr = Object.keys(item)
          .filter(child => !this.extraFeild.includes(child)) // 过滤掉不需要展示的属性
          .sort((a, b) => orderFields[a] - orderFields[b]);
        let resultArr = [...newArr, ...this.extraFeild];
        const obj = {};
        resultArr.forEach(r => {
          if (Object.keys(bizProjObj).includes(r)) {
            obj[r] = item[bizProjObj[r]];
          } else {
            // 部分字段值为0，不能过滤掉
            if (item[r] || item[r] === 0) {
              obj[r] = item[r];
            }
          }
        });
        return obj;
      });
    },
    allUsersForTag() {
      return (this.allUsers || []).map(user => {
        return {
          id: user,
          name: user,
        };
      });
    },
  },
  watch: {
    tab(val) {
      if (val === 'history') {
        this.getSearchHistory();
      }
    },
    value: {
      deep: true,
      immediate: true,
      handler(val) {
        if (Object.keys(val).length) {
          this.backFillData(val);
        }
      },
    },
  },
  mounted() {
    this.initDataMember();
    this.getStandardName();
    this.getStorageType();
    Bus.$on('disabledCreatedBy', isDisabled => {
      if (isDisabled) {
        this.changeUser();
      } else {
        this.singleSelect = [];
      }
      this.isMemberDisabled = isDisabled;
    });
  },
  methods: {
    tpl(node, ctx) {
      let parentClass = 'bkdata-selector-node';
      let textClass = 'text';
      return (
        <div class={parentClass}>
          <span class={textClass}>{node.name}</span>
        </div>
      );
    },
    changeCreatedTime(date) {
      if (date.every(item => !item)) {
        // 清空时间时去除相关属性
        delete this.localValue['created_at_start'];
        delete this.localValue['created_at_end'];
      } else {
        const getDate = date.map(item => this.formateDate(item));
        this.localValue['created_at_start'] = getDate[0];
        this.localValue['created_at_end'] = getDate[1];
      }
    },
    changeMember(data) {
      const user = data[0];
      if (user) {
        this.localValue['created_by'] = user;
      } else {
        this.localValue['created_by'] = '';
        this.$emit('change', this.localValue);
      }

    },
    clearAll() {
      this.storageType = '';
      // this.clearStorageType();
      this.cascadeName = [];
      // this.clearStandard();
      this.singleSelect = [];
      this.createdTime = [];
      // 清空操作需要重置创建时间面板，但是选中最近一周等快捷方式时，通过清空数组无效
      this.clearCreatedTime();
      // this.$refs.cascadeInstance && this.$refs.cascadeInstance.clearData();
      this.$emit('change', {
        heat_operate: '',
        heat_score: '',
        asset_value_operate: '',
        asset_value_score: '',
        storage_capacity_operate: '',
        storage_capacity: '',
        range_operate: '',
        range_score: '',
        importance_operate: '',
        importance_score: '',
        assetvalue_to_cost: '',
        assetvalue_to_cost_operate: '',
        storage_type: '',
        created_by: '',
        me_type: '',
        standard_name: '',
        tag_code: '',
        standard_content_id: '',
        standard_content_name: '',
        cal_type: []
      });
    },
    clearCreatedTime() {
      this.isShowCreatedTime = false;
      this.$nextTick(() => {
        this.isShowCreatedTime = true;
      });
    },
    cascadeChange(newVal, oldVal, [selectedItem]) {
      this.$nextTick(() => {
        // 只选第一级
        if (newVal.length > 0) {
          // 标准内容相关参数并且选了两级
          if (newVal.length > 1) {
            this.localValue['standard_content_id'] = newVal[1];
            this.localValue['standard_content_name'] = selectedItem.children
              .find(item => item.id === newVal[1]).name;
          } else {
            this.localValue['standard_content_id'] = '';
            this.localValue['standard_content_name'] = '';
            // this.deleteAttr(['standard_content_id', 'standard_content_name']);
          }
          this.localValue['standard_name'] = selectedItem.name;
          this.localValue['tag_code'] = newVal[0];
          this.localValue['me_type'] = 'standard';
          this.localValue['cal_type'] = ['standard', 'only_standard'];
          this.$emit('change', this.localValue);
        } else {
          this.clearStandard();
        }
      });
    },
    isNumber(value) {
      return !Number.isNaN(Number(value));
    },
    tipContent() {
      if (this.isMemberDisabled) {
        return {
          placement: 'top',
          content: this.$t('输入框已所锁定当前用户'),
          boundary: 'window',
        };
      }
      return {};
    },
    historySearch(index) {
      const params = this.searchHistoryData[index];
      const {
        project_id,
        bk_biz_id,
        tag_code,
        created_by,
        created_at_start,
        created_at_end,
        storage_type,
        heat_operate,
        heat_score,
        range_operate,
        range_score,
        asset_value_operate,
        asset_value_score,
        importance_operate,
        importance_score,
        assetvalue_to_cost_operate,
        assetvalue_to_cost,
        storage_capacity_operate,
        storage_capacity,
        standard_content_id,
        standard_content_name,
      } = params;
      const backFillParams = {
        selectedParams: {
          // 这里数据回填时，值为null时，placeholder会消失，需要置为''
          project_id: project_id || '',
          biz_id: bk_biz_id ? `${bk_biz_id}` : '',
          channelKeyWord: params.platform || '',
        },
        tagParam: {
          selectedTags: params.tag_ids,
          searchTest: params.keyword || '',
          advanceSearch: {
            tag_code: tag_code === 'virtual_data_mart' ? '' : tag_code,
            created_by,
            created_at_start,
            created_at_end,
            standard_name: params.standard_name,
            storage_type,
            heat_operate,
            heat_score,
            range_operate,
            range_score,
            asset_value_operate,
            asset_value_score,
            importance_operate,
            importance_score,
            assetvalue_to_cost_operate,
            assetvalue_to_cost,
            storage_capacity_operate,
            storage_capacity,
            standard_content_id,
            standard_content_name,
          },
        },
      };
      for (const key in backFillParams.tagParam.advanceSearch) {
        if (['', null, undefined].includes(backFillParams.tagParam.advanceSearch[key])) {
          delete backFillParams.tagParam.advanceSearch[key];
        }
      }
      this.$emit('changeAdvanceParams', backFillParams);
      // 高级搜索参数回填
      // this.backFillData(backFillParams.tagParam.advanceSearch)
      if (params.tag_code !== 'virtual_data_mart') {
        const { advanceSearch } = backFillParams.tagParam;
        this.changeStandardName(advanceSearch.tag_code, advanceSearch);
      }
      this.$emit('historySearch', backFillParams, params);
    },
    getSearchHistory() {
      this.loading.isHistoryLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getRecentSearchRecord')
        .then(res => {
          if (res.result) {
            this.searchHistoryData = res.data;
            this.$emit('getHistoryCount', this.searchHistoryData.length);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.isHistoryLoading = false;
        });
    },
    getRateOperate(item, key) {
      const operate = Object.keys(dataValueOperateMap).find(item => dataValueOperateMap[item] === key);
      const target = this.rateCondition.find(child => child.code === item[operate]);
      return target ? target.name : '';
    },
    getLabels(data) {
      let label = '';
      data
        && data.forEach(item => {
          label += `${item}、`;
        });
      return label.slice(0, label.length - 1);
    },
    tabChange(tab) {
      this.$emit('tabChange', tab);
    },
    getBoundary() {
      return {
        boundary: document.body,
      };
    },
    sliceNum(num) {
      const numArr = String(num).split('');
      const index = numArr.findIndex(item => item === '.');
      if (index > 0) {
        return numArr.slice(0, index + 1).join() + numArr.slice(index + 1, index + 3);
      }
      return num;
    },
    backFillData(val) {
      // 清空cascade组件选中的文本
      // this.$refs.cascadeInstance && this.$refs.cascadeInstance.clearData();
      // 数据回填
      this.storageType = val.storage_type || ''; // 存储类型
      this.cascadeName = val.standard_content_id
        ? [Number(val.tag_code), val.standard_content_id]
        : val.tag_code ? [Number(val.tag_code)] : []; // 数据标准
      this.singleSelect = val.created_by ? [val.created_by] : []; // 创建者
      this.createdTime = val.created_at_end // 创建时间
        ? [val.created_at_start, val.created_at_end]
        : [];
      // 评分相关搜索条件回填
      for (const key in dataValueOperateMap) {
        if (val[key]) {
          const valueField = dataValueOperateMap[key];
          this[key] = val[key];
          this[valueField] = val[valueField];
        }
      }
    },
    formateDate(time) {
      const preArr = Array.apply(null, Array(10)).map((elem, index) => {
        return '0' + index;
      });
      let date = new Date(time);
      let year = date.getFullYear();
      let month = date.getMonth() + 1; // 月份是从0开始的
      let day = date.getDate();
      let hour = date.getHours();
      let min = date.getMinutes();
      let sec = date.getSeconds();

      return year + '-'
            + (preArr[month] || month) + '-' + (preArr[day] || day)
            + ' ' + (preArr[hour] || hour) + ':'
            + (preArr[min] || min) + ':' + (preArr[sec] || sec);
    },
    clearStorageType() {
      // this.deleteAttr(['storage_type']);
      // this.$emit('change', this.localValue);
    },
    clearStandard() {
      // this.deleteAttr([
      //     'tag_code',
      //     'me_type',
      //     'cal_type',
      //     'standard_name',
      //     'standard_content_id',
      //     'standard_content_name'
      // ]);
      this.localValue.tag_code = '';
      this.localValue.me_type = '';
      this.localValue.cal_type = [];
      this.localValue.standard_name = '';
      this.localValue.standard_content_id = '';
      this.localValue.standard_content_name = '';
      this.$emit('change', this.localValue);
    },
    changeStandardName(id, item) {
      this.localValue['tag_code'] = id;
      this.localValue['me_type'] = 'standard';
      this.localValue['cal_type'] = ['standard', 'only_standard'];
      this.localValue['standard_name'] = item.standard_name;
      this.$emit('change', this.localValue);
    },
    changeStorageType(type) {
      this.localValue.storage_type = type;
      this.$emit('change', this.localValue);
    },
    changeUser() {
      this.singleSelect = [this.$store.state.common.userName];
      this.localValue['created_by'] = this.$store.state.common.userName;
    },
    addField(data) {
      return data.map((item, index) => {
        return {
          id: index,
          name: item,
        };
      });
    },
    getStandardName() {
      this.loading.isStandardLoading = true;
      this.bkRequest
        .httpRequest('dataStandard/getStandardCascadeInfo')
        .then(res => {
          if (res.result) {
            this.standardData = res.data.standard_list;
            this.$nextTick(() => {
              if (Object.keys(this.localValue).length) {
                this.backFillData(this.localValue); // 数据回填时需要查找数据标准名称，所以要在拿到标砖名称列表后进行
              }
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.isStandardLoading = false;
        });
    },
    getStorageType() {
      this.loading.isStorageLoading = true;
      this.bkRequest
        .httpRequest('dataStandard/getStorageType')
        .then(res => {
          if (res.result) {
            this.storageTypeList = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.isStorageLoading = false;
        });
    },
    // 校验评分相关字段条件完整输入
    completeRateCondition() {
      const keys = Object.keys(dataValueOperateMap);
      const values = Object.values(dataValueOperateMap);
      const paramsKeys = Object.keys(this.localValue);
      for (let index = 0; index < paramsKeys.length; index++) {
        if (this.localValue[paramsKeys[index]]) {
          if (keys.includes(paramsKeys[index])) {
            const fielName = dataValueOperateMap[paramsKeys[index]];
            if (!this.localValue[fielName]) {
              const msg = `${$t('请输入')}${this.historyFields[fielName]}${$t('条件值')}`;
              showMsg(msg, 'warning', { delay: 2000 });
              return true;
            }
          } else if (values.includes(paramsKeys[index])) {
            const keyName = keys.find(item => dataValueOperateMap[item] === paramsKeys[index]);
            if (!this.localValue[keyName]) {
              showMsg(`${$t('请输入')}${this.historyFields[paramsKeys[index]]}${$t('选择条件')}`, 'warning', {
                delay: 2000,
              });
              return true;
            }
          }
        }
      }
      return false;
    },
    confirm() {
      if (this.completeRateCondition()) return;
      this.isShowDatePick = false; // 时间选择器选中时间但没有确定时，页面跳转还会保留下拉选择组件，所以需要重新渲染
      this.$nextTick(() => {
        this.isShowDatePick = true;
      });
      this.$emit('change', this.localValue);
      this.$emit('confirm');
    },
    deleteAttr(attrArr = []) {
      attrArr.forEach(item => {
        delete this.localValue[item];
      });
    },
    cancel() {
      this.$emit('close');
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
  },
};
</script>

<style lang="scss" scoped>
// 媒体查询低分辨率下日期时间组件使用不同的宽度
@media screen and (max-width: 1366px) {
  .date-width {
    width: 260px !important;
  }
  ::v-deep .bk-rtx-member-selector {
    .bk-tag-selector {
      min-height: 32px;
      width: 200px;
    }
  }
}
@media screen and (min-width: 1366px) {
  .date-width {
    width: auto !important;
  }
  ::v-deep .bk-rtx-member-selector {
    .bk-tag-selector {
      min-height: 32px;
      width: 200px;
    }
  }
}
.search-container {
  max-width: 1300px;
  min-width: 1076px;
  margin: 0 auto;
  margin-bottom: 20px;
  .clear-button {
    height: 100%;
    display: flex;
    align-items: center;
    ::v-deep button {
      border: none;
      background-color: #fafbfd;
    }
  }
  .operation-panel {
    display: flex;
    justify-content: center;
  }
  .history-container {
    font-size: 12px;
    max-height: 320px;
    padding: 8px 20px;
    .history-info {
      position: relative;
      display: flex;
      flex-wrap: wrap;
      color: #3a84ff;
      line-height: 18px;
      padding-top: 5px;
      &:first-child {
        padding-top: 0;
      }
      .time-str {
        margin-right: 20px;
        color: #63656e;
      }
      .info-wrap {
        flex: 1;
        display: flex;
        flex-wrap: wrap;
      }
      .info-item {
        cursor: pointer;
        .max200 {
          max-width: 200px;
          display: inline-block;
        }
      }
      .name {
        width: 61px;
        height: 20px;
        font-weight: 400;
        font-size: 14px;
        text-align: left;
        line-height: 20px;
      }
    }
    .no-history {
      text-align: center;
      line-height: 36px;
      color: #63656e;
      font-size: 12px;
    }
  }
  .bk-form {
    .bk-form-item {
      float: left;
      .bk-label {
        width: 150px;
      }
      .bk-form-content {
        line-height: normal;
        margin-left: 150px;
        width: 250px;
        .recent-date {
          height: 32px;
          line-height: 32px;
          font-size: 12px;
          margin-left: 10px;
          white-space: nowrap;
          a:first-child {
            margin-right: 5px;
          }
        }
        ::v-deep .bk-input-number {
          height: 32px;
          .input-number-option {
            top: 2px;
          }
        }
        .bk-cascade {
          width: 100%;
        }
      }
      .auto-width {
        width: auto;
      }
      .width300 {
        width: 250px;
      }
      .width342 {
        width: 342px;
      }
    }
  }
}
::v-deep .bk-tab {
  .bk-tab-section {
    padding: 0;
    overflow-y: inherit;
  }
  .bk-tab-label {
    font-size: 12px !important;
  }
}
</style>
<style lang="scss">
.bk-cascade-dropdown-content {
  .bk-cascade-options {
    .bk-option-content {
      .bk-option-name {
        white-space: nowrap;
        display: inline-block;
        overflow: hidden;
        text-overflow: ellipsis;
        width: calc(100% + 20px);
      }
    }
  }
}
</style>
