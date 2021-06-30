/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

/* eslint-disable radix */
/* eslint-disable no-restricted-syntax */
/* eslint-disable no-underscore-dangle */
import { mapState } from 'vuex';
export default {
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
    };
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

  computed: {
    ...mapState({
      resultTable: state => state.dataQuery.resultTable,
      bizId: state => state.dataQuery.activeBizId,
    }),
    /**
         * 按照业务进行分组,组装结果表列表
         */
    resultsLists() {
      const rawRtList = this.results.resultList;
      const mProjectRts = {};
      for (let i = 0; i < rawRtList.length; i++) {
        const _rt = rawRtList[i];
        const _projectIndex = `${_rt.project_name}`;
        if (mProjectRts[_projectIndex] === undefined) {
          mProjectRts[_projectIndex] = [];
        }

        mProjectRts[_projectIndex].push({
          result_table_id: _rt.result_table_id,
          name: `${_rt.result_table_id} (${_rt.description})`,
        });
      }
      const tempArr = [];
      for (const _projectIndex in mProjectRts) {
        tempArr.push({
          name: _projectIndex,
          children: mProjectRts[_projectIndex],
        });
      }
      return tempArr.sort((a, b) => a.name < b.name);
    },
  },

  methods: {
    /** 选择业务 */
    bizChange(bizid) {
      this.getResultsList(bizid);
    },

    /*
         * 获取业务列表
         */
    getBizList() {
      const actionId = 'result_table.query_data';
      const dimension = 'bk_biz_id';
      this.bizs.isLoading = true;
      this.bkRequest
        .httpRequest('meta/getMineBizs', { params: { action_id: actionId, dimension } })
        .then((res) => {
          if (res.result) {
            this.bizs.placeholder = this.$t('请先选择业务');
            const businesses = res.data;
            if (businesses.length === 0) {
              this.noData = true;
            } else {
              this.noData = false;
              businesses.forEach((v) => {
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
        .finally(() => {
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
        .httpRequest('meta/getMineResultTables', { params: { bk_biz_id: bizId } })
        .then((res) => {
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
        .finally(() => {
          this.results.placeholder = this.$t('请选择');
          this.results.isLoading = false;
        });
    },
  },
  created() {
    this.getBizList();
    this.$route.params.defaultBizId
            && this.$set(this.bizs, 'selected', Number(this.$route.params.defaultBizId))
            && this.getResultsList(this.$route.params.defaultBizId);
    this.$route.params.clusterType
            && this.$store.commit('dataQuery/setClusterType', this.$route.params.clusterType);
    const resultTableId = this.$route.params.defaultResultId;
    if (resultTableId) {
      this.$set(this.results, 'selected', resultTableId);
      this.$store.commit('dataQuery/setResultTable', resultTableId);
      this.resultsChange(resultTableId);
    }
  },
};
