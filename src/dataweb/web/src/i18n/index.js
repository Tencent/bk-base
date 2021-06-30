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

import Vue from 'vue';
import VueI18n from 'vue-i18n';
import Cookies from 'js-cookie';
import zhCn from './zh-cn/index.js';
import en from './en/index.js';
import { locale, lang } from 'bk-magic-vue';

function getI18N() {
  Vue.use(VueI18n);
  const curLang = Cookies.get('blueking_language') || 'zh-cn';
  locale.use((/^zh/gi.test(curLang) && lang.zhCN) || lang.enUS);

  const i18n = new VueI18n({
    locale: curLang.toLowerCase(),
    messages: {
      en: {
        ...Object.assign(lang.enUS, en),
        // ...enLocae
      },
      'zh-cn': {
        ...Object.assign(lang.zhCN, zhCn),
        // ...zhLocale
      },
    },
    // 屏蔽未翻译警告信息，正式发布前一定要去掉
    silentTranslationWarn: true,
  });

  // ElementLocale.i18n((key, value) => i18n.t(key, value))
  locale.i18n((key, value) => i18n.t(key, value));
  Vue.prototype.$setLocale = function (locale) {
    this.$i18n.locale = locale;
  };
  Vue.prototype.$getLocale = function () {
    return this.$i18n.locale;
  };
  Vue.prototype.$refreshLocale = function (locale) {
    locale = Cookies.get('blueking_language');
    if (locale === undefined || locale === '') {
      locale = 'zh-cn';
    }
    this.$setLocale(locale);
  };

  Vue.mixin(locale.mixin);

  return i18n;
}

export default getI18N;
