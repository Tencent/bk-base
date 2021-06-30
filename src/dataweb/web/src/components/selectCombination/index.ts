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

import { Vue, Component, Prop, PropSync, Emit, Watch } from 'vue-property-decorator';

@Component({})
export default class selectCombination extends Vue {
  @Prop({ default: 83 }) width!: number;
  @Prop({ default: 'field' }) firstSelected!: string;
  @Prop({ default: 'field' }) input!: string;
  @Prop({ default: 'field' }) selected!: string;
  @Prop({ default: () => [] }) list!: Array<object>;
  @Prop({ default: false }) clearable!: boolean;

  public initType: string = this.firstSelected;
  public initSelected: string = this.selected;
  public initInput: string = this.input;

  public typeList: Array<object> = [
    {
      id: 'constant',
      name: '常量映射',
    },
    {
      id: 'field',
      name: '字段选择',
    },
  ];

  @Emit()
  clickOutside() {
    return {
      input: this.initInput,
      selected: this.initSelected,
      type: this.initType,
    };
  }

  @Emit()
  typeChange() {
    return {
      input: this.initInput,
      selected: this.initSelected,
      type: this.initType,
    };
  }

  @Emit()
  changeStatus() {
    return {
      input: this.initInput,
      selected: this.initSelected,
      type: this.initType,
    };
  }

  @Emit()
  secondSelectedItem() {}

  @Emit()
  secondSelectedChange() {
    return {
      input: this.initInput,
      selected: this.selected,
      type: this.initType,
    };
  }
}
