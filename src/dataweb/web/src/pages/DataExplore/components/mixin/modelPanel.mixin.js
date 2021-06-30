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

/* eslint-disable no-undef */
export default {
  data() {
    return {
      modelList: [],
      modelInfoMap: [
        {
          title: $t('输入'),
          key: 'inputs',
        },
        {
          title: $t('输出'),
          key: 'outputs',
        },
        {
          title: $t('参数'),
          key: 'args',
        },
        {
          title: $t('说明'),
          key: 'description',
        },
      ],
      originData: {},
      modelLoading: false,
    };
  },
  created() {
    this.getModelList();
  },
  methods: {
    getModelList() {
      this.modelLoading = true;
      this.bkRequest
        .httpRequest('dataExplore/getAlgorithmList')
        .then((res) => {
          if (res.result) {
            this.$set(this, 'originData', res.data);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        .finally(() => {
          this.modelLoading = false;
        });
    },
    changeModelName(name) {
      console.log(name);
      this.modelList = this.originData[name].map((group) => {
        const content = group.alg_info.map((item) => {
          const data = {
            inputs: item.inputs.join('\n') || this.$t('无输入'),
            outputs: item.outputs.join('\n') || this.$t('无输出'),
            args: item.args.join('\n') || this.$t('无参数'),
          };
          return Object.assign({}, item, data);
        });
        // Object.assign({}, item, {
        //     inputs: item.inputs.join('\n') || this.$t('无输入'),
        //     outputs: item.outputs.join('\n') || this.$t('无输出'),
        //     args: item.args.join('\n') || this.$t('无参数'),
        // })
        return {
          group: group.alg_group,
          content,
        };
      });
    },
  },
};
