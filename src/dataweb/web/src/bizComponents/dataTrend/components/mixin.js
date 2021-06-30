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

import moment from 'moment';
export default {
  props: {
    id: {
      type: [String, Number],
      default: '',
    },
  },
  data() {
    return {
      pickerOptions: {
        shortcuts: [
          {
            text: this.$t('最近一周'), // 最近一周日期选择
            onClick(picker) {
              const end = new Date();
              const start = new Date();
              start.setTime(start.getTime() - 3600 * 1000 * 24 * 7);
              picker.$emit('pick', [start, end]);
            },
          },
          {
            text: this.$t('最近一个月'), // 最近一个月日期选择
            onClick(picker) {
              const end = new Date();
              const start = new Date();
              start.setTime(start.getTime() - 3600 * 1000 * 24 * 30);
              picker.$emit('pick', [start, end]);
            },
          },
        ],
      },
    };
  },
  computed: {
    ranges() {
      const dateConf = {};
      const yesterKey = this.$t('昨天');
      const weekKey = this.$t('最近一周');
      const monthKey = this.$t('最近一个月');
      // let monthsKey = this.$t('最近三个月')
      dateConf[yesterKey] = [moment().subtract(1, 'days'), moment()];
      dateConf[weekKey] = [moment().subtract(7, 'days'), moment()];
      dateConf[monthKey] = [moment().subtract(1, 'month'), moment()];
      // dateConf[monthsKey] = [moment().subtract(3, 'month'), moment()]
      return dateConf;
    },
  },
  methods: {
    // 获取开始日期结束日期
    getDateDiff(start, end) {
      const date1 = new Date(start);
      const date2 = new Date(end);
      const timeDiff = Math.abs(date2.getTime() - date1.getTime());
      const diffDays = Math.ceil(timeDiff / (1000 * 3600 * 24));
      return diffDays;
    },
    /**
     * 动态获取y轴坐标刻度
     * */
    getYaxisDtick(dataArr) {
      const MAX = Math.ceil(Math.max(...dataArr) / 4);
      const STEP = 10 ** MAX.toString().length - 1;
      return Math.floor(MAX / STEP) * STEP;
    },

  },
};
