

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
  <div class="query-panel">
    <div class="results-table">
      <p class="title">
        {{ $t('请先选择业务') }}
      </p>
      <div class="results-select">
        <bkdata-selector
          :selected.sync="bizs.selected"
          :placeholder="bizs.placeholder"
          :isLoading="bizs.isLoading"
          :searchable="true"
          :list="bizs.list"
          :searchKey="'bk_biz_name'"
          :settingKey="'bk_biz_id'"
          :displayKey="'bk_biz_name'"
          @item-selected="bizChange" />
      </div>
      <p class="title">
        {{ $t('再选择结果数据表_以项目维度分类') }}
      </p>
      <div class="results-select">
        <bkdata-selector
          :selected.sync="results.selected"
          :placeholder="results.placeholder"
          :isLoading="results.isLoading"
          :searchable="true"
          :hasChildren="true"
          :list="resultsLists"
          :settingKey="'result_table_id'"
          :displayKey="'name'"
          @item-selected="resultsChange" />
      </div>
    </div>
    <QueryHistory ref="queryHistory" />
  </div>
</template>
<script>
import QueryHistory from './Components/QueryHistory';
import { postMethodWarning } from '@/common/js/util.js';
import { mapState, mapGetters } from 'vuex';
import mixin from './RtSelect.mixin';
import bkStorage from '@/common/js/bkStorage.js';
import Bus from '@/common/js/bus.js';
export default {
  components: {
    QueryHistory,
  },
  mixins: [mixin],
  data() {
    return {
      results: {
        // 结果数据表下拉列表默认值
        selected: '',
        searchable: true,
        placeholder: this.$t('请输入结果数据表'),
        resultList: [],
        isLoading: false,
      },
      bizs: {
        selected: '',
        placeholder: this.$t('请输入业务相关的关键字'),
        list: [],
        isLoading: false,
      },
      bkStorageKey: {
        bizId: 'dataAccessBizId',
        resultId: 'dataAccessresultId',
      },
    };
  },
  computed: {
    ...mapState({
      historyList: state => state.dataQuery.hsitory.list,
    }),
  },
  watch: {
    resultTable(val) {
      if (this.results.selected !== val) {
        this.results.selected = val;
      }
    },
    bizId(val) {
      if (this.bizs.selected !== val) {
        this.bizs.selected = val;
        this.getResultsList(val);
      }
    },
  },
  created() {
    // 读取默认bizId或用户上次操作的bizId
    const setBizId = this.$route.params.defaultBizId || bkStorage.get(this.bkStorageKey.bizId);
    if (setBizId && /^\d+$/.test(setBizId)) {
      const id = Number(setBizId);
      this.$set(this.bizs, 'selected', id);
      this.getResultsList(id);
    }

    const resultTableId = this.$route.params.defaultResultId || bkStorage.get(this.bkStorageKey.resultId);
    if (resultTableId) {
      this.$set(this.results, 'selected', resultTableId);
      this.$store.commit('dataQuery/setResultTable', resultTableId);
      this.resultsChange(resultTableId);
    }
    Bus.$on('updateHistoryList', id => {
      this.results.selected = id;
      this.resultsChange(id);
    });
  },
  methods: {
    /*
                选择结果数据表下拉选择
              */
    async resultsChange(id, item) {
      bkStorage.set(this.bkStorageKey.resultId, id);
      this.$store.commit('dataQuery/setResultTable', id);
      await this.saveHistory();
    },
    /*
                  保存选择结果表历史
              */
    async saveHistory(noUpdate = false) {
      await this.axios.post('result_tables/' + this.results.selected + '/set_selected_history/').then(res => {
        if (res.result) {
          if (noUpdate) return;
          this.$store.dispatch('dataQuery/getMinResultHistory').then(_ => {
            const activeItemIndex = this.historyList.findIndex(
              item => item.name.result_table_id === this.results.selected
            );
            activeItemIndex >= 0
              && this.$refs.queryHistory.handHistoryQuery(this.historyList[activeItemIndex], activeItemIndex);
          });
        } else {
          postMethodWarning(res.message, 'error');
        }
      });
    },
    /** 选择业务 */
    bizChange(bizId) {
      bkStorage.set(this.bkStorageKey.bizId, bizId);
      this.getResultsList(bizId);
    },

    /*
     * 获取业务列表
     */
    getBizList() {
      let actionId = 'result_table.query_data';
      let dimension = 'bk_biz_id';
      this.bizs.isLoading = true;
      this.bkRequest
        .httpRequest('meta/getMineBizs', { params: { action_id: actionId, dimension: dimension } })
        .then(res => {
          if (res.result) {
            this.bizs.placeholder = this.$t('请先选择业务');
            let businesses = res.data;
            if (businesses.length === 0) {
              this.noData = true;
            } else {
              this.noData = false;
              businesses.forEach(v => {
                this.bizs.list.push({
                  bk_biz_id: parseInt(v.bk_biz_id),
                  bk_biz_name: v.bk_biz_name,
                });
              });
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.bizs.isLoading = false;
        });
    },

    /*
                  获取选择结果表下拉
              */
    async getResultsList(bizId) {
      this.results.resultList = [];
      this.results.placeholder = this.$t('数据加载中');
      this.results.isLoading = true;
      this.bkRequest
        .httpRequest('meta/getMineResultTables', { params: { bk_biz_id: bizId }, query: { is_query: 1 } })
        .then(res => {
          if (res.result) {
            /** 用于触发select的赋值 */
            const preSelected = this.results.selected;
            this.results.selected = '';
            this.results.resultList = res.data;
            this.results.selected = preSelected;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.results.placeholder = this.$t('请选择');
          this.results.isLoading = false;
        });
    },
  },
};
</script>
<style lang="scss" scoped>
.query-panel {
  height: calc(100% - 30px);
  .results-table {
    padding: 0 20px 30px;
    border-bottom: 1px solid #ddd;
    background: #f5f5f5;

    .title {
      margin-top: 14px;
      color: #737987;
      font-size: 14px;
      font-weight: bold;
    }
    .results-select {
      margin-top: 15px;
      input::-webkit-input-placeholder {
        color: #c3cdd7;
      }
    }
  }
}
</style>
