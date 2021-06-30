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

// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue';
import store from './store';
import Index from './App.Index.vue';
import Perfume from 'perfume.js';
import BKMonitor from './common/js/monitor';
import { bkRequest } from '@/common/js/ajax';
import getI18N from './i18n/layout';

Vue.config.productionTip = false;
window.$bk_perfume = new Perfume({
  logPrefix: '🍹 BKMonitor:',
  logging: false,
  analyticsTracker: ({ metricName, duration, eventProperties }) => {
    window.BKMonitor = new BKMonitor(metricName, duration, eventProperties);
    // console.log('analyticsTracker', window.BKMonitor)
    bkRequest
      .httpRequest('monitor/pushMonitorData', {
        params: { content: JSON.stringify(window.BKMonitor) },
      })
      .then(res => {
        // console.log(res.data, '数据上报')
      });
  },
});
Vue.prototype.$Perfume = window.$bk_perfume;
window.userPic = 'signature.png';
(function() {
  let len = document.getElementById('app');
  if (!len) {
    const elm = document.createElement('div');
    elm.id = 'app';
    document.body.appendChild(elm);
  }
})();

const i18n = getI18N();
const vueInstance = new Vue({
  el: '#app',
  store,
  i18n,
  components: { Index },
  template: '<Index/>',
});

window.$t = vueInstance.$t;
