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

/* eslint-disable no-param-reassign */
export default {
  /**
   * 清除bk-message
   */
  clearMessage() {
    for (const item of document.querySelectorAll('.bk-message')) {
      item.remove();
    }
  },
  /**
   * 展示消息
   * @param {*} theme 显示主题颜色
   * @param {*} msg 消息
   * @param {*} context 上下文实例vue
   */
  showMessage(theme, msg, context) {
    this.clearMessage();
    const self = this;
    context.$bkMessage({
      message: msg,
      theme,
      hasCloseIcon: true,
      delay: 0,
      onClose() {
        self.clearMessage();
      },
      onShow() {
        self.clearMessage();
      },
    });
  },
  // 返回拦截数据
  hookDemoResult(data) {
    return new Promise((resolve) => {
      setTimeout(resolve, 0, {
        result: true,
        data,
      });
    });
  },
  showElement(selector) {
    for (const item of document.querySelectorAll(selector)) {
      item.style.display = 'block';
    }
  },
  hideElement(selector) {
    for (const item of document.querySelectorAll(selector)) {
      item.style.display = 'none';
    }
  },
  /**
   * 修改元素不透明度
   * @param {*} selector 选择器
   * @param {*} op 透明度
   */
  setElementOpacity(selector, op) {
    for (const item of document.querySelectorAll(selector)) {
      item.style.opacity = op;
    }
  },
  showGuideContent(onGuideLoad) {
    let iframe = document.getElementById('guide-iframe');
    if (iframe) {
      iframe.width = '100%';
      iframe.height = '100%';
      onGuideLoad();
    } else {
      iframe = document.createElement('iframe');
      iframe.id = 'guide-iframe';
      iframe.scrolling = 'no';
      iframe.style.display = 'block';
      iframe.style['border-width'] = 0;
      iframe.width = '0';
      iframe.height = '0';
      iframe.style.overflow = 'hidden';
      document.body.appendChild(iframe);
      // iframe.src = 'http://localhost:8380/#/dataflow/ide/demo'
      let src = '';
      if (/localhost/i.test(window.location.hostname)) {
        src = 'http://localhost:8380/#/dataflow/ide2/demo';
      } else {
        src = `${window.BKBASE_Global.siteUrl}#/dataflow/ide/demo`;
      }
      iframe.src = src;
      iframe.onload = () => {
        iframe.width = '100%';
        iframe.height = '100%';
        iframe.style.position = 'absolute';
        iframe.style.top = '0';
        iframe.style.left = '0';
        iframe.style['z-index'] = '1000';
        onGuideLoad();
      };
    }
  },
  getTimeList(endStr = '00', type = 1) {
    return new Array(24).fill('')
      .map((t, index) => {
        if (!type && index < 2) {
          endStr = window.$t(endStr.replace(/S/i, ''));
        }
        const name = (type && `${`0${index}`.slice(-2)}:${endStr}`) || `${index}${endStr}`;
        return {
          id: index,
          name,
        };
      });
  },
};
