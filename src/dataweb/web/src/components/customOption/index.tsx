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

import { Component, Vue, Prop } from 'vue-property-decorator';

@Component({})
export default class CustomOption extends Vue {
  @Prop({ default: () => {} }) item: any;

  /** 取值对应key */
  @Prop({ default: 'scene_alias' }) aliasKey: string;
  @Prop({ default: 'scene_name' }) sceneNameKey: string;

  itemSetting: any = {
    custom: { disabled: false, icon: 'bkdata-icon icon-customize-line' },
    timeseries_anomaly_detect: { disabled: false, icon: 'bkdata-icon icon-detect-abnormal-line' },
    correlation_analysis: { disabled: true, icon: 'bkdata-icon icon-relate-line' },
    prediction: { disabled: true, icon: 'bkdata-icon icon-predict-line' },
  };

  get calcItem() {
    return {
      name: this.item[this.aliasKey],
      disabled: false,
      id: this.item[this.sceneNameKey],
      icon: this.itemSetting[this.item[this.sceneNameKey]]?.icon,
      description: this.item.description.split(/\n|\r/).filter((str: String) => str.replace(/\s/g, '') !== ''),
    };
  }
}
