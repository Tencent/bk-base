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

import styled from '@tencent/vue-styled-components';
import { bkForm, bkFormItem, bkTab, bkComposeFormItem, bkSelect, bkInput, bkDatePicker } from 'bk-magic-vue';

const inputProps = {
  width: {
    default: 'calc(100% - 72px) !important',
    type: String | Number,
  },
};

const selectProps = {
  width: {
    default: '72px !important',
    type: String | Number,
  },
};

const formProps = {
  inputWidth: {
    default: 'calc(100% - 72px)',
    type: String | Number,
  },
  selectWidth: {
    default: '72px',
    type: String,
  },
  selectColor: {
    default: '#fafbfd',
    type: String,
  },
};

export const ProcessFormItem = styled('div', {})`
  display: inline-block;
  width: calc(50% - 20px);
  margin-top: 0px !important;
`.withComponent(bkFormItem);

export const NoPaddingTabPanel = styled('div', {})`
  .bk-tab-section {
    padding: 20px 0;
  }
`.withComponent(bkTab);

export const windowComposeFormItem = styled('div', formProps)`
  width: 100%;
  font-size: 12px;
  display: flex;
  .bk-input-wrapper-small {
    line-height: 24px !important;
    width: ${props => props.inputWidth};
  }
  .bk-select-small {
    height: 26px;
    width: ${props => props.selectWidth};
    background-color: ${props => props.selectColor};
  }
  .bk-select-main {
    width: ${props => props.inputWidth};
  }
`.withComponent(bkComposeFormItem);

export const FlexForm = styled('div', {})`
    display: flex;
    align-items: center;
}
`.withComponent(bkForm);

export const windowFormItem = styled('div', {}, { clsName: 'bk-form-item' })`
    /deep/.bk-label {
        font-size: 12px;
    }
    .bk-date-picker {
        .icon-wrapper {
            height: 26px;
        }
        .bk-date-picker-editor {
            height: 26px !important
            line-height: 26px !important
        }
    }
`.withComponent(bkFormItem);

export const offsetTip = styled('div', {}, { clsName: 'inline-offset-tip' })`
  width: 64px;
  height: 26px;
  text-align: center;
  background: #fafbfd;
  border: 1px solid #c4c6cc;
  border-radius: 2px 0px 0px 2px;
  line-height: 28px;
  font-size: 12px;
  color: #63656e;
  float: left;
`;

export const unitSelect = styled('select', {})`
  background-color: #fafbfd;
  height: 26px;
  width: ${props => props.width};
`.withComponent(bkSelect);

export const fullWidthSelect = styled('select', {}, { clsName: 'full-select' })`
  width: 100% !important;
`.withComponent(bkSelect);

export const mainInput = styled(bkInput, inputProps)`
  width: ${props => props.width};
  line-height: 24px !important;
`;

export const MiniDatePicker = styled('div', {})`
    .icon-wrapper {
        height: 26px;
    }
    .bk-date-picker-editor {
        height: 26px !important
        line-height: 26px !important
    }
`.withComponent(bkDatePicker);
