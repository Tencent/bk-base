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
  data() {
    return {
      logData: [],
      getLogTimer: null,
    };
  },
  methods: {
    onScrollHandle(event) {
      if (this.logLoading || this.getLogTimer) return;
      const direction = event.deltaY > 0 ? 'down' : 'up';
      const el = document.querySelector('.bk-table-body-wrapper');
      const curScrollPos = el.scrollTop;
      const oldScroll = el.scrollHeight - el.clientHeight;
      /** 如果向上滚动， 获取日志并重制滚动条位置 */
      if (curScrollPos === 0 && this.logPosInfo.process_start !== 0 && direction === 'up') {
        this.logLoading = true;
        this.getLogData('backward').then((res) => {
          this.getLogTimer = setTimeout(() => {
            this.getLogTimer = null;
          }, 2000);

          this.logData.unshift(...res.data.formatLog);
          this.$refs.logTable.calLogDataSet(true);

          /** 实图更新scroll位置 */
          this.$nextTick(() => {
            const newScroll = el.scrollHeight - el.clientHeight;
            el.scrollTop = curScrollPos + (newScroll - oldScroll);
            this.logLoading = false;
            this.checkAllLogData(true);
          });
        });

        /** 向下滚动，加载新日志内容 */
      } else if (curScrollPos === oldScroll && curScrollPos !== 0 && direction === 'down') {
        /** 实时日志永远可以请求新数据，不设限 */
        if (this.logPosInfo.process_end < this.currentLogSize - 1 || this.taskType === 'stream') {
          this.getLogData('forward').then((res) => {
            this.getLogTimer = setTimeout(() => {
              this.getLogTimer = null;
            }, 2000);
            this.logData.push(...res.data.formatLog);
            this.checkAllLogData();
          });
        }
      }
    },
  },
};
