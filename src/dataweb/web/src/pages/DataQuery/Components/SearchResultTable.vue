

<!--
  - Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
  - Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
  - BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
  -
  - License for BK-BASE 蓝鲸基础平台:
  - -------------------------------------------------------------------
  -
  - Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  - documentation files (the "Software"), to deal in the Software without restriction, including without limitation
  - the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
  - and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  - The above copyright notice and this permission notice shall be included in all copies or substantial
  - portions of the Software.
  -
  - THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
  - LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
  - NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  - WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  - SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
  -->

<template>
  <div ref="searchResultContainer"
    :style="search_table_bg"
    class="table search-result-table clearfix">
    <div @click.stop="handleSearchTableCilck">
      <bkdata-table
        id="searchResultContainer"
        class="bk-table-transparent"
        :outerBorder="false"
        :data="currentPageData"
        :stripe="true"
        :emptyText="$t('暂无数据')"
        :pagination="calcPagination"
        :headerCellStyle="{ background: '#f1f2f5' }"
        @page-change="handlePageChange"
        @page-limit-change="handlePageLimitChange">
        <template v-if="!serveSidePage">
          <bkdata-table-column :label="$t('序号')"
            prop="_$index"
            minWidth="60"
            align="center" />
        </template>
        <template v-for="(item, index) in resultList.select_fields_order">
          <bkdata-table-column :key="index"
            :label="item"
            minWidth="100">
            <div
              slot-scope="props"
              class="table-cell-container"
              @dblclick.stop="e => handleCelldbclick(e, props.row[item], item)">
              <!--eslint-disable vue/no-v-html-->
              <template v-if="isHtmlContentAvailable(props.row[item])">
                <span
                  v-bk-overflow-tips="{ content: $t('双击查看更多'), interactive: false }"
                  class="searchrt-table-cell"
                  v-html="props.row[item]" />
              </template>
              <template v-else>
                <span
                  v-bk-overflow-tips="{ content: $t('双击查看更多'), interactive: false }"
                  class="searchrt-table-cell">
                  {{ props.row[item] }}
                </span>
              </template>
            </div>
          </bkdata-table-column>
        </template>
      </bkdata-table>
    </div>
    <div ref="tableCellPoptext"
      class="table-cell-poptext"
      :style="popText.style"
      v-html="popText.content" />
  </div>
</template>
<script>
import { bkOverflowTips } from 'bk-magic-vue';
import { mapGetters, mapState } from 'vuex';
// import cellClick from '@/components/cellClick/cellClick.vue';
export default {
  directives: {
    bkOverflowTips,
  },
  components: {},
  props: {
    serveSidePage: {
      type: Boolean,
      default: false,
    },
    resultList: {
      type: Object,
      default: () => ({}),
    },
    pagination: {
      type: Object,
      default: () => ({
        current: 1,
        limit: 30,
      }),
    },
  },
  data() {
    return {
      serachKey: '',
      headerFixed: false, // 定位
      scrollShow: false, // 滚动条显示
      scroll: {
        isScroll: false, // 滚动时禁止拖动选中
        scrollTop: false, // 是否滚动到顶部
      },
      moreLoading: false,
      noMore: false,
      search_table_bg: {
        // 查询历史水印
        background: 'url(' + window.userPic + ') repeat',
      },

      localPagination: this.pagination,
      popText: {
        content: '',
        style: {
          display: 'none',
        },
      },
      searchContainer: null,
      isFirstLoad: true,
    };
  },
  computed: {
    ...mapState({
      searchLoading: state => state.dataQuery.search.isLoading,
    }),
    calcPagination() {
      const curPagination = Object.assign({}, this.pagination, this.localPagination);
      return {
        current: curPagination.current,
        count: this.serveSidePage ? curPagination.count : this.tabelList.length,
        limit: curPagination.limit,
        limitList: [10, 20, 30, 40, 50, 100, 200],
      };
    },
    tabelList() {
      if (this.serachKey) {
        return this.resultList.list.filter(item => {
          return Object.keys(item).some(key => {
            return item[key].toString().includes(this.serachKey);
          });
        });
      } else {
        return this.resultList.list;
      }
    },
    currentPageData() {
      return this.serveSidePage
        ? this.tabelList
        : this.tabelList
          .map((item, index) => Object.assign(item, { _$index: index + 1 }))
          .slice(
            (this.calcPagination.current - 1) * this.calcPagination.limit,
            this.calcPagination.current * this.calcPagination.limit
          );
    },
    toCsvData() {
      let header = {};
      let ret = [...this.tabelList];
      this.resultList.select_fields_order.forEach((item, idx) => {
        header[idx] = item.value;
      });
      ret.unshift(header);
      return ret;
    },
  },
  methods: {
    isHtmlContentAvailable(content) {
      return !/<\w+/.test(content);
    },
    stringToRegExp(pattern, flags) {
      return new RegExp(
        pattern.replace(/[\\[\]\\{}()+*?.$^|]/g, function (match) {
          return '\\' + match;
        }),
        flags
      );
    },
    handleSearchTableCilck(e) {
      // !Array.prototype.some.call(e.target.classList, cls => /table-cell-poptext/.test(cls))
      let targetNode = e.target;
      if (new RegExp(`^<${e.target.tagName}>`, 'i').test(this.popText.content)) {
        targetNode = e.target.parentNode;
      }
      const targetClassList = targetNode.classList || [];
      if (!Array.prototype.some.call(targetClassList, cls => /table-cell-poptext/.test(cls))) {
        this.popText.style = { display: 'none' };
      }
    },
    handleCelldbclick(e, cellValue, item) {
      let targetNode = e.target;
      if (new RegExp(`^<${e.target.tagName}>`, 'i').test(cellValue)) {
        targetNode = e.target.parentNode;
      }
      const targetClassList = targetNode.classList || [];
      const reg = /table-cell-container|searchrt-table-cell/;
      if (Array.prototype.some.call(targetClassList, cls => reg.test(cls))) {
        this.popText.content = cellValue;
        this.popText.style = {
          left: e.target.offsetLeft + 'px',
          top: e.target.offsetTop + 'px',
          minHeight: e.target.offsetHeight + 'px',
          whiteSpace: 'nowrap',
          display: 'block',
          padding: '3px',
          boxShadow: '0 0 15px rgba(33, 34, 50, 0.5)',
          borderRadius: '2px',
        };

        e.target.appendChild(this.$refs.tableCellPoptext);
        this.$nextTick(() => {
          const targetRect = e.target.getBoundingClientRect();
          const popRect = this.$refs.tableCellPoptext.getBoundingClientRect();
          const popStyle = {};

          if (!this.searchContainer) {
            const containerRect = document
              .querySelector('#searchResultContainer div.bk-table-body-wrapper')
              .getBoundingClientRect();
            this.searchContainer = {
              left: containerRect.left,
              top: containerRect.top,
              right: containerRect.right,
            };
          }

          if (popRect.width > this.searchContainer.right - this.searchContainer.left) {
            let popWidth = 0;
            let left = e.target.offsetLeft;
            let top = 0;
            const targetWidth = targetRect.right - targetRect.left;
            const targetLeftRect = targetRect.left - this.searchContainer.left - 30;
            const targrtRight = this.searchContainer.right - targetRect.right + targetWidth;
            if (popRect.width <= targetLeftRect) {
              popWidth = targetLeftRect;
            } else {
              if (targetLeftRect > targrtRight) {
                left = targetLeftRect - e.target.offsetWidth;
                popWidth = targetLeftRect;
              } else {
                popWidth = targrtRight;
              }
            }

            Object.assign(popStyle, {
              left: -left + 'px',
              top: e.target.offsetTop + 'px',
              minHeight: e.target.offsetHeight + 'px',
              width: popWidth + 'px',
              whiteSpace: 'normal',
            });
          } else {
            if (popRect.right > this.searchContainer.right) {
              popStyle.left = this.searchContainer.right - popRect.right + 'px';
            }
          }

          Object.assign(this.popText.style, popStyle);
        });
      }
    },
    setSearchKey(val) {
      this.serachKey = val;
    },
    searchTable(key) {
      this.serachKey = key;
    },
    handlePageChange(page) {
      if (this.calcPagination.current !== page) {
        this.$set(this.localPagination, 'current', page);
        this.$emit('changed', this.calcPagination);
      }
    },
    handlePageLimitChange(pageSize, preLimit) {
      if (this.localPagination.limit !== pageSize) {
        this.$set(this.localPagination, 'current', 1);
        this.$set(this.localPagination, 'limit', pageSize);
        this.$emit('changed', this.calcPagination);
      }
    },
    exportToCsv(filename, rows) {
      var processRow = function (row) {
        var finalVal = '';
        debugger;
        Object.keys(row).forEach((key, idx) => {
          var innerValue = row[key] === null ? '' : row[key].toString();
          if (row[key] instanceof Date) {
            innerValue = row[key].toLocaleString();
          }
          var result = innerValue.replace(/"/g, '""');
          if (result.search(/("|,|\n)/g) >= 0) {
            result = '"' + result + '"';
          }
          finalVal += `${result},`;
        });
        return finalVal + '\n';
      };

      const csvFile = rows.map(r => processRow(r)).join('');
      var blob = new Blob([csvFile], { type: 'text/csv;charset=utf-8;' });
      if (navigator.msSaveBlob) {
        // IE 10+
        navigator.msSaveBlob(blob, filename);
      } else {
        var link = document.createElement('a');
        if (link.download !== undefined) {
          // feature detection
          // Browsers that support HTML5 download attribute
          var url = URL.createObjectURL(blob);
          link.setAttribute('href', url);
          link.setAttribute('download', filename);
          link.style.visibility = 'hidden';
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);
        }
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.table-cell-poptext {
  background: #fff;
  position: absolute;
  top: 0;
  left: 0;
  width: auto;
  height: auto;
  border: solid 1px #ddd;
  z-index: 999;
  padding: 1px;
}
.result-toolbar {
  display: flex;
  justify-content: space-between;
  padding: 0 15px 15px 15px;
}
.search-result-table {
  .bk-table-header-wrapper {
    .bk-table-header {
      th {
        height: 30px;
        .cell {
          height: 30px;
          line-height: 30px;
        }
      }
    }
  }
  .bk-table-pagination-wrapper {
    padding: 5px;

    .bk-page-count-right {
      top: 5px;
    }
  }

  .bk-table-body-wrapper {
    height: 30vh;
    overflow-y: auto;
    &::-webkit-scrollbar {
      width: 8px;
      background-color: transparent;
    }
    &::-webkit-scrollbar-thumb {
      border-radius: 8px;
      background-color: #a0a0a0;
    }
    .bk-table-row {
      td {
        height: 27px;
        em {
          color: #000034;
          background: #ff0;
          font-style: normal;
        }
        .searchrt-table-cell {
          width: 100%;
          overflow: hidden;
          display: inline-block;
          white-space: nowrap;
          text-overflow: ellipsis;
        }
      }
    }
  }
}
</style>
