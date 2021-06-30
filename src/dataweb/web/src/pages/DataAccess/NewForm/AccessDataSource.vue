

<!--
  - Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
  - Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
  - BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
  -
  - License for BK-BASE 蓝鲸基础平台:
  - -------------------------------------------------------------------
  -
  - Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  - documentation files (the "Software"), to deal in the Software without restriction, including without limitation
  - the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
  - and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  - The above copyright notice and this permission notice shall be included in all copies or substantial
  - portions of the Software.
  -
  - THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
  - LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
  - NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  - WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  - SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
  -->

<!--eslint-disable-next-line vue/valid-template-root-->
<template />
<script>
export default {
  props: {
    /** 接入类型ID */
    dataScenarioId: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {};
  },
  watch: {
    /** 监控数据类型改变，重新调用接口请求数据来源 */
    dataScenarioId(val) {
      this.getDataSrc(val);
    },
  },
  mounted() {
    this.$parent.$nextTick(() => {
      this.getAccessTypes();
      this.getBizLists();
      this.getDataSourceCtg();
      this.getEncodeLists();
    });
  },
  methods: {
    /**
     * 统一处理请求返回结果
     * return Promise：处理后状态为200的数据
     */
    resolveRequest(url, options) {
      return new Promise(resolve => {
        this.bkRequest
          .httpRequest(url, options)
          .then(res => {
            if (res.result) {
              resolve(res.data);
            } else {
              resolve([]);
              this.getMethodWarning(res.message, res.code);
            }
          })
          ['catch'](() => {
            resolve([]);
          });
      });
    },

    emitDataSourceLoaded(key, data) {
      this.$emit('dataSourceLoaded', {
        key: key,
        data: data,
      });
    },

    /**
     * 获取接入类型
     * return 解析返回Common 和 Other
     */
    getAccessTypes() {
      this.$emit('getAccessTypeStatus', true);
      this.resolveRequest('/dataAccess/getScenarioList', { mock: false }).then(data => {
        const items = data;
        this.emitDataSourceLoaded('accessTypes', {
          common: items.filter(item => item.type === 'common'),
          other: items.filter(item => item.type === 'other'),
        });
        this.$emit('getAccessTypeStatus', false);
      });
    },

    /**
     * 全部业务列表
     */
    getBizLists() {
      this.resolveRequest('/dataAccess/getMineBizList').then(data => {
        this.emitDataSourceLoaded('allBizInfo', data);
      });
    },

    /**
     * 获取数据来源
     */
    getDataSrc(accessScenarioId) {
      this.resolveRequest('/dataAccess/getSourceList', { params: { access_scenario_id: accessScenarioId } }).then(
        data => {
          this.emitDataSourceLoaded('dataOrigin', data || []);
        }
      );
    },

    /**
     * 获取字符编码
     */
    getEncodeLists() {
      this.resolveRequest('/dataAccess/getEncodeList').then(data => {
        this.emitDataSourceLoaded('encodeList', data);
      });
    },

    /**
     * 获取数据源分类
     */
    getDataSourceCtg() {
      this.resolveRequest('/dataAccess/getCategoryConfigs').then(data => {
        this.formatDataSourceCtg(data);
        this.emitDataSourceLoaded('originCategory', data);
      });
    },

    /** 清洗数据分类，移除空的 sub_list */
    formatDataSourceCtg(root) {
      if (Array.isArray(root)) {
        root.forEach(list => {
          this.formatDataSourceCtg(list);
        });
      } else {
        if (root['sub_list'] && root['sub_list'].length) {
          this.formatDataSourceCtg(root['sub_list']);
        } else {
          delete root['sub_list'];
        }
      }
    },
  },
};
</script>
