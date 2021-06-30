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

const TempCommonData = {
  icons: {
    result_table: 'icon-resulttable',
    data_processing: 'icon-datasource',
    raw_data: 'icon-datasource',
  },
  orValue(data) {
    return data || '暂无数据';
  },
  labelMap(arr, field1, field2) {
    let str = '';
    if (field1 && field2 && arr && arr.length) {
      arr.forEach((item) => {
        str += `${item[field2]}-${item[field1]}、`;
      });
      str = str.substr(0, str.length - 1);
    } else if (field1 && arr && arr.length) {
      arr.forEach((item) => {
        str += item[field1] ? `${item[field1]}、` : '';
      });
      str = str.substr(0, str.length - 1);
    }
    return str;
  },
  switchName(nodeData) {
    if (nodeData.type === 'data_processing') {
      return window.bloodToolTips.isSwitchChinese && nodeData.description ? nodeData.description : nodeData.name;
    }
    if (nodeData.type === 'raw_data') {
      return window.bloodToolTips.isSwitchChinese && nodeData.raw_data_alias
        ? nodeData.raw_data_alias
        : `（${nodeData.name}）${nodeData.raw_data_name ? nodeData.raw_data_name : ''}`;
    }
    if (nodeData.type === 'result_table') {
      return window.bloodToolTips.isSwitchChinese && nodeData.alias ? nodeData.alias : nodeData.name;
    }
  },
  nodeIcon(nodeData) {
    if (nodeData.processing_type === 'clean') {
      return 'icon-clean-result';
    }
    if (nodeData.processing_type === 'stream') {
      return 'icon-batch-result';
    }
    if (nodeData.processing_type === 'batch') {
      return 'icon-offline-calc';
    }
    return 'icon-resulttable';
  },
  switchLanguage(word) {
    if (!window.gVue) return;
    if (window.gVue.$i18n.locale === 'zh-cn') {
      return word;
    }
    return window.gVue.$t(word);
  },
};
export default TempCommonData;
