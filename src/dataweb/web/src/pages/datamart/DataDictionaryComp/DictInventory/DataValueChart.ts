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

import { Component, Vue } from 'vue-property-decorator';
import DictOveriew from '../DictOveriew/index.vue';
import DataHeat from './DataValue/DataHeat';
import DataImportance from './DataValue/DataImportance';
import DataRange from './DataValue/DataRange';
import ValueCost from './DataValue/ValueCost';

@Component({
  components: {
    DictOveriew,
    ValueCost,
    DataHeat,
    DataImportance,
    DataRange,
  },
})
export default class DataValueChart extends Vue {
  public commonChartOptions = {
    responsive: true,
    animation: false,
    maintainAspectRatio: false,
    title: {
      display: true,
      text: $t('数据开发（DataFlow）后继依赖节点个数分布'),
    },
    scales: {
      y: {
        scaleLabel: {
          display: true,
          labelString: $t('数据表个数（个）'),
        },
        type: 'linear',
        display: true,
        position: 'left',
      },
      y1: {
        scaleLabel: {
          display: true,
          labelString: $t('数据表占比（%）'),
        },
        type: 'linear',
        display: true,
        position: 'right',
        min: 0,
        max: 1,
        gridLines: {
          drawOnChartArea: false,
        },
      },
    },
  };

  public mounted() {
    const observeTargetMap = [
      {
        ele: document.querySelector('.data-range-container'),
        refName: 'dataRange',
        isRequest: false,
        timer: '',
      },
      {
        ele: document.querySelector('.data-importance-container'),
        refName: 'dataImportance',
        isRequest: false,
        timer: '',
      },
      {
        ele: document.querySelector('.data-heat-container'),
        refName: 'dataHeat',
        isRequest: false,
        timer: '',
      },
      // {
      //     ele: document.querySelector('.value-cost-container'),
      //     refName: 'valueCost',
      //     isRequest: false
      // },
      {
        ele: document.querySelector('.overiew-container'),
        refName: 'dictOveriew',
        isRequest: false,
        timer: '',
      },
    ];
    const options = {
      root: document.getElementsByClassName('layout-body')[0],
      threshold: 0,
    };
    const observer = new IntersectionObserver(instances => {
      instances.forEach(instance => {
        if (instance.isIntersecting) {
          const refName = observeTargetMap.find(item => item.ele === instance.target).refName;
          !this.$refs[refName].isRequested && this.$refs[refName].getData();
        }
      });
    }, options);

    observeTargetMap.forEach(item => {
      item.ele && observer.observe(item.ele);
    });
  }
}
