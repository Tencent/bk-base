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
/* eslint-disable array-callback-return */
import { directive as bkTooltips } from './components/tooltips';
import focus from './utils/focus';
import locale from './locale';
import bkCollapse from './components/collapse';
import bkCollapseItem from './components/collapse-item';
import Tree from './components/tree/index';
import Combobox from './bkdata-components/combobox/index';

const install = (Vue) => {
  const components = [bkCollapse, bkCollapseItem, Combobox, Tree];

  const formComponents = [];

  components.map((component) => {
    Vue.component(component.name, component);
  });

  formComponents.map((component) => {
    Vue.component(component.name, component);
  });

  Vue.use(bkTooltips);
  Vue.use(focus);
  Vue.prototype.t = locale.t;

  Vue.prototype.setLang = (lang) => {
    locale.use(lang);
  };
};

export default {
  version: '1.0.0',
  install,
  Tree,
};
