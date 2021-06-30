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

export default {
  data() {
    return {
      hasPower: '',
      enableTdw: false,
      tdwPowerLoading: false,
      applicationGroupLoading: false,
      applicationGroupList: [],
      applicationSelect: {
        placeholder: this.$t('请选择应用组'),
        list: [],
        disabled: false,
        multiple: false,
        filterable: false,
      },
      sourceServerSelect: {
        placeholder: this.$t('请选择源服务器'),
        list: [],
        disabled: false,
        multiple: false,
        filterable: false,
      },
      clusterVersionSelect: {
        placeholder: this.$t('请选择集群版本'),
        list: [],
        disabled: false,
        multiple: false,
        filterable: false,
      },
      computingClusterSelect: {
        placeholder: this.$t('请选择计算集群'),
        list: [],
        disabled: false,
        multiple: false,
        filterable: false,
      },
    };
  },
  methods: {
    changeCheckTdw(event) {
      const enableTdw = event;
      this.enableTdw = enableTdw;
      if (enableTdw) {
        this.tdwPowerLoading = true;
        this.requestApplication();
      } else {
        this.resetTdwParams();
        this.hasPower = '';
      }
    },
    requestApplication() {
      // 以应用组结果作为权限
      this.applicationGroupLoading = true;
      this.$store
        .dispatch('tdw/getApplicationGroup')
        .then((res) => {
          if (res.result) {
            this.hasPower = 'hasPower';
            this.applicationGroupList = res.data;
          } else {
            this.hasPower = 'notPower';
          }
        })
        .finally(() => {
          this.tdwPowerLoading = false;
          this.applicationGroupLoading = false;
        });
    },
    resetTdwParams() {
      if (Object.prototype.hasOwnProperty.call(this.params, 'tdw_conf')) {
        this.params.tdw_conf.tdwAppGroup = '';
        this.params.tdw_conf.sourceServer = '';
        this.params.tdw_conf.task_info.spark.spark_version = '';
        this.params.tdw_conf.gaia_id = '';
        this.applicationSelect.list = [];
        this.sourceServerSelect.list = [];
        this.clusterVersionSelect.list = [];
        this.computingClusterSelect.list = [];
      }
    },
  },
};
