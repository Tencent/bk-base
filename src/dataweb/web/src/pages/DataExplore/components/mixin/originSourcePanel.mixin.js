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

import Bus from '@/common/js/bus.js';

export default {
  data() {
    return {
      originSourceValue: '',
      originResultLoading: false,
      filesList: [],
      iconMap: {
        csv: 'icon-csv',
        xlsx: 'icon-excel',
      },
    };
  },
  watch: {
    tabActived(val) {
      if (val === 'ST') {
        this.getOriginSourceList();
      }
    },
  },
  computed: {
    displayFilesList() {
      return this.filesList.filter(this.handleFilter);
    },
  },
  methods: {
    handleFilter(item) {
      return this.originSourceValue ? item.displayName.includes(this.originSourceValue) : true;
    },
    handleShowUpload() {
      Bus.$emit('notebook-upload-show');
    },
    getFileIcon(item) {
      return this.iconMap[item.file_type];
    },
    async getCommonSourceList() {
      const res = await this.bkRequest.httpRequest('dataExplore/getFilesList', {
        params: {
          project_id: this.dataExplore.projectId,
        },
        query: {
          action_id: 'raw_data.query_data',
        },
      });

      return res;
    },
    async getPersonalSourceList() {
      let res = {
        result: true,
        data: [],
      };
      const rawDataIds = await this.bkRequest.httpRequest('dataAccess/getAccessList', {
        query: {
          data_scenario: 'offlinefile',
          bk_biz_id: this.bk_biz_id,
          page: 1,
          show_display: 1,
        },
      });

      if (rawDataIds.result && rawDataIds.data.results.length) {
        res = await this.bkRequest.httpRequest('dataAccess/getDeployPlanList', {
          query: {
            data_scenario: 'offlinefile',
            bk_biz_id: this.bk_biz_id,
            raw_data_ids: rawDataIds.data.results.map(item => item.id),
          },
        });
      }

      return res;
    },

    async getOriginSourceList() {
      this.originResultLoading = true;
      const res = this.projectType === 'common'
        ? await this.getCommonSourceList() : await this.getPersonalSourceList();
      console.log(res);
      if (res.result) {
        const list = res.data
          .map((item) => {
            if (item && item.access_conf_info) {
              return item.access_conf_info.resource.scope.map((file) => {
                const fileName = file.file_name || '';
                const fileType = (fileName.split('.').length && fileName.split('.')[1]).includes('csv')
                  ? 'csv'
                  : 'xlsx';
                return {
                  id: item.id,
                  raw_data_alias: item.raw_data_alias,
                  displayName: `[${item.id}]${item.raw_data_alias}`,
                  file_name: fileName,
                  file_type: fileType,
                  bk_biz_id: item.bk_biz_id,
                  description: item.description,
                  updated_at: item.updated_at,
                  created_at: item.created_at,
                };
              });
            }
            return null;
          })
          .flat();
        this.$set(this, 'filesList', list);

        console.log(this.filesList, 'filesList', list, res);
      } else {
        this.getMethodWarning(res.message, res.code);
      }

      this.originResultLoading = false;
    },
  },
};
