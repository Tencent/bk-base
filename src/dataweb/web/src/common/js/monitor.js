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

import Cookies from 'js-cookie';
import Vue from 'vue';

class BKMonitor {
  constructor(moduleName = '', duration = '', data = {}) {
    this.UserName = Cookies.get('bk_uid');
    this.ModuleName = moduleName;
    this.CreatedTime = new Date().toISOString();
    this.duration = duration;
    this.Data = data;
    this.Screen = this.getScreenParams();
    this.Navigator = this.getNavigatorParams();
    this.Location = this.getLocationParams();
    this.routeDwellTime = {};
  }

  getScreenParams() {
    const { width, height } = window.screen;
    return { width, height };
  }

  getNavigatorParams() {
    const { appCodeName, appName, appVersion, language, platform } = window.navigator;
    return { appCodeName, appName, appVersion, language, platform };
  }

  getLocationParams() {
    const { href, hostname, origin, hash } = window.location;
    return { href, hostname, origin, hash };
  }
  // 路由停留时间
  routeDwellTimeStart() {
    window.$bk_perfume.start('Router_Change');
    Object.assign(this.routeDwellTime, {
      start: new Date().getTime(),
      duration: this.routeDwellTime.end ? new Date().getTime() - this.routeDwellTime.end : 0,
    });
  }
  routeDwellTimeEnd(to, from) {
    Object.assign(this.routeDwellTime, {
      end: new Date().getTime(),
    });

    window.$bk_perfume.end('Router_Change', {
      data: { to: this.getRouteData(to), from: this.getRouteData(from), time: this.routeDwellTime },
    });
    return this.routeDwellTime;
  }

  getRouteData(route) {
    const { name, meta, path, query, params, fullPath } = route;
    // 格式化空meta
    let formatMeta = meta;
    if (!Object.keys(meta || {}).length) {
      formatMeta = {
        alias: '',
      };
    }
    return { name, meta: formatMeta, path, query, params, fullPath };
  }
}

export default BKMonitor;

Vue.directive('monitor', {
  bind(el, binding) {
    window.$bk_perfume.start('User_Action');
    const { value } = binding;
    Object.assign(value, {
      time: new Date().toISOString(),
      className: el.className,
    });
    el.addEventListener(value.event, () => {
      // 绑定click事件，触发后进行数据上报
      window.$bk_perfume.end('User_Action', { data: value });
    });
  },
});
