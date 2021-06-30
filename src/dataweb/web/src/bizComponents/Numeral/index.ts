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

import { Component, Prop, Vue } from 'vue-property-decorator';
const Numeral = require('numeral');

@Component({
  components: {},
})
export default class numeral extends Vue {
  @Prop({ default: 0 }) num: any;
  get str() {
    return this.num || 0;
  }
  get attr() {
    if (this.str < 1000) {
      return {};
    }
    return {
      'v-bk-tooltips': this.str,
    };
  }

  /**
     * 按Numbers格式处理
     */
  public numFormat(num: string | number, digits = 4): string | number {
    if (!num || isNaN(Number(num))) {
      return 0;
    }

    const format = num >= 10000 ? `0.[${String('').padStart(digits - 1, '0')}]a` : '0';
    return Numeral(this.toExponential(num)).format(format);
  }

  /** 有效数字保留 */
  public toExponential(num: string | number, digits = 4) {
    if (isNaN(Number(num))) {
      return 0;
    }

    return Number(num).toExponential(digits - 1);
  }
}
