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
/* eslint-disable no-unused-vars */
export default {
  data() {
    return {
      tableFilter: [
        { text: 'INFO', value: 'INFO' },
        { text: 'WARN', value: 'WARN' },
        { text: 'ERROR', value: 'ERROR' },
      ],
      calTableData: [],
      searchTimeout: null,
      getLogTimer: null,
    };
  },
  watch: {
    data: {
      deep: true,
      immediate: true,
      handler() {
        this.calLogDataSet();
      },
    },
    search() {
      clearTimeout(this.searchTimeout);
      this.searchTimeout = setTimeout(this.calLogDataSet, 300);
    },
  },
  computed: {
    maxTextAline() {
      return /windows|win32/i.test(navigator.userAgent) ? 99 : 103;
    },
    /** 是否为建模的日志 */
    isModelLog() {
      return this.logType === 'model';
    },
    modelName() {
      return this.isModelLog ? '模块' : '来源';
    },
    filterListData() {
      return this.filterList.length > 0 ? this.filterList : this.tableFilter;
    },
    isShowPage() {
      return this.isModelLog && !this.loading && this.calTableData.length > 0;
    },
  },
  methods: {
    renderHeader(h, data) {
      const directive = {
        content: this.$t('产生日志数据的类名等信息'),
        placement: 'right',
        theme: 'light',
      };
      return this.isModelLog ? (
        <div class="custom-header-cell">
          <span>{data.column.label}</span>
        </div>
      ) : (
        <div class="custom-header-cell">
          <span>{data.column.label}</span>
          <span v-bk-tooltips={directive} class="icon-info ml8 icon"></span>
        </div>
      );
    },
    filterChange(filters) {
      /** 当过滤条件改变，改变滚动条位置，防止自动加载数据 */
      const el = document.querySelector('.bk-table-body-wrapper');
      el.scrollTop = 1;
      this.$emit('filterChange', filters);
    },
    foldIconClick(row, event) {
      let target = event.srcElement;
      while (!target.className.includes('cell')) {
        target = target.parentElement;
      }
      this.tableFoldAction(target, row);
    },
    cellDbClickHandle(row, column, cell) {
      const el = cell.querySelector('div');
      this.tableFoldAction(el, row);
    },
    tableFoldAction(el, row) {
      const { scrollHeight } = el;
      const { clientHeight } = el;
      if (!row.isOver) return;
      row.fold = !row.fold;
      if (row.fold) {
        el.style.height = 'auto';
        el.classList.remove('log-unfold');
      } else {
        el.style.height = `${scrollHeight + 36}px`; // 动态修改height，无法直接写入css样式
        el.classList.add('log-unfold');
      }
      this.$refs.logTable.doLayout();
    },
    cellMouseLeave(row, column, cell) {
      cell.querySelector('.icon-active') && cell.querySelector('.icon-active').classList.remove('icon-active');
    },
    cellMouseEnter(row, column, cell) {
      cell.querySelector('.icon-group') && cell.querySelector('.icon-group').classList.add('icon-active');
    },
    levelFilterMethod(value, row, column) {
      const level = column.property;
      return row[level] === value;
    },
    calLogDataSet(clearFold = false) {
      const key = this.search.trim().split(' ');
      /** 计算过后，需要根据fold，对是否折叠进行一些适配 */
      this.calTableData = this.data.map((log) => {
        const copyLog = JSON.parse(JSON.stringify(log));
        copyLog.fold = true;
        copyLog.log = key[0]
          ? log.log.replace(new RegExp(`(${key.join('|')})`, 'gi'), '<span class="highlight">$1</span>')
          : log.log;
        copyLog.isOver = log.log.length > this.maxTextAline; // 一行能容纳的最大长度为113个字符
        return copyLog;
      });
      if (clearFold) {
        const els = document.querySelectorAll('.log-unfold');
        els.forEach((element) => {
          element.remove('log-unfold');
          element.style.height = 'auto';
        });
      }
    },
    pageChange(page) {
      this.$emit('pageChange', page);
    },
  },
};
