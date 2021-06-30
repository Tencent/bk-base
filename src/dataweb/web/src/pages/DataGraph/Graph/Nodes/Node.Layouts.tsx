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

/**
 * 此组件为基础组件，用于数据开发画布节点布局使用
 * 谨慎修改，所有修改需要进行 merge request 评审
 */

import styled from '@tencent/vue-styled-components';

const wrapperProps = {
  height: {
    default: '100%',
    type: String | Number,
  },
};

/**
 * 定义节点布局外层容器
 * @param prop :{ height: String|Number }
 */
const LayoutWrapper = styled('section', wrapperProps)`
  height: ${props => props.height};
`;

const headerProps = {};

/** 定义节点头部容器 */
const LayoutHeader = styled('div', headerProps)`
  height: 85px;
  background: rgb(250, 251, 253);
`;

const bodyProps = {};

/** 定义节点Body */
const LayoutBody = styled('div', bodyProps)`
  height: calc(100% - 140px);
  background: #fff;
  border-bottom: 1px solid #dcdee5;
`;

const footerProps = {};

/** 定义节点Footer */
const LayoutFooter = styled('div', footerProps)`
  height: 55px;
  background: rgb(250, 251, 253);
  display: flex;
  align-items: center;
  justify-content: flex-end;
  padding: 0 30px;
`;

export { LayoutWrapper, LayoutHeader, LayoutBody, LayoutFooter };
