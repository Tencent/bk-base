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

/*
 * 根据runVersion配置页面相关内容
 */
const versionConfigCn = {
  openpaas: {
    logoImg: './static/dist/img/logo/logo_cn@2x.png',
    docsUrl: '',
    dataType: '',
  },
  tgdp: {
    logoImg: './static/dist/img/logo/logo_cn1.png',
  },
  ieod: {
    logoImg: './static/dist/img/logo/logo_cn1.png',
  },
  clouds: {
    logoImg: './static/dist/img/logo/logo_cn1.png',
  },
};
const versionConfigEn = {
  openpaas: {
    logoImg: './static/dist/img/logo/logo_en@2x.png',
  },
  tgdp: {
    logoImg: './static/dist/img/logo/logo_en@2x.png',
  },
  ieod: {
    logoImg: './static/dist/img/logo/logo_en@2x.png',
  },
  clouds: {
    logoImg: './static/dist/img/logo/logo_en@2x.png',
  },
};

const defaultConfig = {
  logoImg: './static/dist/img/logo/logo_en@2x.png',
  docsUrl: '',
  dataType: '',
};

/**
 * 根据所属环境&语言去配置页面相关内容
 * @param {Object} version 当前环境
 * @param {Object} type 需要匹配的内容类型
 * @param {String} languages 中文&英文，默认显示中文
 **/

const getVersionConfig = (version, type, languages = 'zh-cn') => {
  const data = languages === 'en' ? versionConfigEn : versionConfigCn;
  return data[version][type] || defaultConfig;
};

export { getVersionConfig };
