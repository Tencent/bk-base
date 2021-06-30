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
  props: {
    params: {
      type: Array,
      default: () => [],
    },
    bizList: {
      type: Array,
      default: () => [],
    },
    fullParams: {
      type: Object,
      default: () => ({}),
    },
  },
  methods: {
    initOutput() {
      this.params.length === 0 && this.addOutput();
    },
    async addOutput() {
      if (this.isAddDisabled) return;
      const result = await this.validateForm();
      if (!result) return;

      this.processOutput();
      this.activeOutput = (this.params.length - 1).toString();
    },
    processOutput() {
      const output = {
        bk_biz_id: this.fullParams.config.bk_biz_id || this.bizList[0].biz_id,
        fields: [],
        output_name: '',
        table_name: '',
        table_custom_name: '',
        validate: {
          table_name: {
            // 输出编辑框 数据输出
            status: false,
            errorMsg: '',
          },
          output_name: {
            // 输出编辑框 中文名称
            status: false,
            errorMsg: '',
          },
          field_config: {
            status: false,
            errorMsg: this.$t('必填项不可为空'),
          },
        },
      };
      this.params.push(output);
    },
  },
};
