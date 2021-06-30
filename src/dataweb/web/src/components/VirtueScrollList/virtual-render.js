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

import { throttled } from '@/common/js/util.js';

function visibleRender(e, wrapper, binding) {
  const { lineHeight = 30, callback } = binding.value;
  const { scrollTop, offsetHeight, startIndex, endIndex } = wrapper;

  const targetStartIndex = Math.floor(scrollTop / lineHeight);
  const targetEndIndex = Math.ceil(offsetHeight / lineHeight) + targetStartIndex;
  if (startIndex !== targetStartIndex || endIndex !== targetEndIndex) {
    typeof callback === 'function' && callback(e, targetStartIndex, targetEndIndex, scrollTop);
  }
}

function throttledRender(e, wrapper, binding) {
  throttled(visibleRender)(e, wrapper, binding);
}

export default {
  inserted(el, binding) {
    const wrapper = el.parentNode;
    wrapper.addEventListener('scroll', e => throttledRender(e, wrapper, binding));
  },
  componentUpdated(el, binding) {
    const wrapper = el.parentNode;
    throttledRender(null, wrapper, binding);
  },
  unbind(el) {
    if (el) {
      const wrapper = el.parentNode;
      wrapper && wrapper.removeEventListener('scroll', throttledRender);
    }
  },
};
