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

import { postMethodWarning, confirmMsg } from '@/common/js/util.js';
export default {
  methods: {
    editUDF(name, openUDFCallback) {
      this.bkRequest
        .httpRequest('udf/editUDF', {
          params: {
            function_name: name,
            ignore_locked: false,
          },
        })
        .then((res) => {
          if (res.result) {
            if (res.data.locked === 1) {
              const self = this;
              const content = (this.$t('该函数已被')
                + res.data.locked_by
                + this.$t('于')
                + res.data.locked_at
                + this.$t('锁定_是否强制编辑'));
              confirmMsg(self.$t('函数已被锁'), content, () => {
                self.forceEdit(name, openUDFCallback);
              });
            } else {
              openUDFCallback(res.data);
            }
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        .catch((err) => {
          postMethodWarning(err, 'error');
        });
    },
    forceEdit(name, openUDFCallback) {
      this.bkRequest
        .httpRequest('udf/editUDF', {
          params: {
            function_name: name,
            ignore_locked: true,
          },
        })
        .then((res) => {
          if (res.result) {
            openUDFCallback(res.data);
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        .catch((err) => {
          postMethodWarning(err, 'error');
        });
    },
  },
};
