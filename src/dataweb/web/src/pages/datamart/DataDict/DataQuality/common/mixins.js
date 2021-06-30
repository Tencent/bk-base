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

/* eslint-disable no-param-reassign */
import { dataTypeIds } from '@/pages/datamart/common/config';
import moment from 'moment';

const dataQualityMixins = {
  data() {
    return {
      layout: {
        font: {
          size: 10,
        },
        legend: {
          orientation: 'h',
          x: 0.3,
          y: 1.15,
          font: {
            size: 10,
          },
        },
        width: 537,
        height: 400,
        xaxis: {
          tickangle: 30,
          autorange: true,
        },
        margin: {
          l: 30,
          r: 30,
          // b: 10,
          // t: 10
        },
      },
      config: {
        operationButtons: ['autoScale2d', 'resetScale2d'],
        responsive: true,
        // staticPlot: true
      },
    };
  },
  computed: {
    detailId() {
      return this.$route.query[dataTypeIds[this.$route.query.dataType]];
    },
  },
  methods: {
    getNowTime(data) {
      if (!data.length) {
        const nowTime = new Date();
        const minute = nowTime.getMinutes();
        return `00:00 ~ ${nowTime.getHours()}:${minute < 10 ? `0${minute}` : minute}`;
      }
      const target = JSON.parse(JSON.stringify(data.find(item => item.name === this.$t('今日'))));
      const x = target.x.reverse();
      const y = target.y.reverse();

      for (let index = 0; index < x.length; index++) {
        if (y[index]) {
          return `00:00 ~ ${x[index]}`;
        }
      }
    },
    handleRound(num) {
      // 数据丢失丢弃里，后端的一个问题：num值为-1时，需要手动改为0
      if (num === -1) return 0;
      return Number(num.toFixed(2)) || 0;
    },
    handleTableData(res, type) {
      return [
        {
          label: this.$t('今日'),
          num: this.handleRound(res.data[`today_sum_${type}`]),
          colspan: 2,
        },
        {
          label: this.$t('昨日'),
          num: this.handleRound(res.data[`last_day_sum_${type}`]),
          colspan: 2,
        },
        {
          label: this.$t('上周'),
          num: this.handleRound(res.data[`last_week_sum_${type}`]),
          colspan: 2,
        },
        {
          label: this.$t('最大'),
          num: this.handleRound(res.data[`max_${type}_same_period`]),
          isShowLeftLabel: true,
        },
        {
          label: this.$t('最小'),
          num: this.handleRound(res.data[`min_${type}_same_period`]),
        },
        {
          label: this.$t('平均'),
          num: this.handleRound(res.data[`avg_${type}_same_period`]),
        },
      ];
    },
    handleTipsData(res, type) {
      return [
        {
          label: this.$t('今日'),
          num: this.handleRound(res.data[`today_sum_${type}`]),
          colspan: 2,
        },
        {
          label: this.$t('昨日'),
          num: this.handleRound(res.data[`last_day_${type}_whole_day`]),
          colspan: 2,
        },
        {
          label: this.$t('上周'),
          num: this.handleRound(res.data[`last_week_${type}_whole_day`]),
          colspan: 2,
        },
        {
          label: this.$t('最大'),
          num: this.handleRound(res.data[`max_${type}_whole_day`]),
          isShowLeftLabel: true,
        },
        {
          label: this.$t('最小'),
          num: this.handleRound(res.data[`min_${type}_whole_day`]),
        },
        {
          label: this.$t('平均'),
          num: this.handleRound(res.data[`avg_${type}_whole_day`]),
        },
        // 今日，昨日，7天前当前数据量
        {
          label: this.$t('今日'),
          num: this.handleRound(res.data[`today_${type}_current`]),
        },
        {
          label: this.$t('昨日'),
          num: this.handleRound(res.data[`last_day_${type}_current`]),
        },
        {
          label: this.$t('上周'),
          num: this.handleRound(res.data[`last_week_${type}_current`]),
        },
      ];
    },
    getDataQualitySummary(type) {
      return this.bkRequest
        .httpRequest('dataDict/getDataQualitySummary', {
          params: {
            measurement: type,
          },
          query: {
            data_set_id: this.detailId,
            with_current: true,
            with_whole: true,
          },
        })
        .then((res) => {
          if (res.result) {
            if (!Object.keys(res.data).length) return {};
            return res;
          }
          this.getMethodWarning(res.message, res.code);
          return {
            data: [],
          };
        });
    },
    formateTime(time, formate = 'HH:mm') {
      return moment(time * 1000).format(formate);
    },
    getDmonitorMetrics(type, id) {
      const dayTotalSeconds = 24 * 60 * 60;
      const getParams = (num) => {
        const todayZero = new Date(new Date().toLocaleDateString()).getTime() / 1000;
        const startTime = todayZero - num * dayTotalSeconds;
        return {
          params: {
            measurement: type,
          },
          query: {
            data_set_ids: [id],
            format: 'series',
            start_time: `${startTime}s`,
            end_time: `${startTime + dayTotalSeconds - 1}s`,
          },
        };
      };
      const getResData = (data, res) => {
        const target = res.data.find(item => item.storage_cluster_type === 'kafka');
        if (!target) {
          return {
            name: data.keyWord,
            data: [],
          };
        }
        if (data.num) {
          target.series.forEach((item) => {
            item.time += data.num * dayTotalSeconds;
          });
        }
        return {
          name: data.keyWord,
          data: target.series,
        };
      };
      const dateRequests = [
        {
          num: 0,
          keyWord: this.$t('今日'),
        },
        {
          num: 1,
          keyWord: this.$t('昨日'),
        },
        {
          num: 7,
          keyWord: this.$t('上周'),
        },
      ];
      const monitorCenterUrl = 'dmonitorCenter/getDmonitorMetrics';
      return Promise.all(dateRequests.map(item => this.bkRequest.httpRequest(monitorCenterUrl, getParams(item.num))
        .then((res) => {
          if (res.result) {
            return getResData(item, res);
          }
          this.getMethodWarning(res.message, res.code);
          return {
            name: '',
            data: [],
          };
        }))).then(res => res);
    },
  },
};

export default dataQualityMixins;
