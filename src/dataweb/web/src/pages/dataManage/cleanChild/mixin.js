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

import { generateId } from '@/common/js/util.js';

const defaultConfig = {
  Json: {
    result: '',
  },
  Regx: {
    result: '',
    regex: '',
  },
  Csv: {
    result: '',
  },
  Url: {
    result: '',
  },
  Iteration: {
    result: '',
    iteration_type: 'list',
  },
  Split: {
    result: '',
    delimiter: '',
    limit: '',
  },
  Replace: {
    result: '',
    from: '',
    to: '',
  },
  Access: {
    assign_method: 'index',
    location: '',
    result: '',
  },
  Assign: {
    assign_method: 'index',
    fields: [
      {
        id: generateId('field'),
        type: '',
        assign_to: '',
        location: '',
        highlight: false,
      },
    ],
  },
  Splitkv: {
    result: '',
    record_split: '',
    kv_split: '',
  },
  Zip: {
    result: '',
    list_key_a: '',
    list_key_b: '',
  },
  Remove: {
    assign_method: 'index',
    type: '',
    result: '',
  },
};

export default {
  methods: {
    resetParams(comp) {
      this.$set(this, 'innerParam', defaultConfig[comp]);
    },
  },
};
