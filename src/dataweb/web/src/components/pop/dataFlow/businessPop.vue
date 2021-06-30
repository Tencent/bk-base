

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
  <div class="businessPop">
    <a href="javascript:;"
      class="close-pop"
      @click="closePop">
      <i class="bk-icon icon-close" />
    </a>
    <p class="title">
      {{ $t('接入业务详情') }}
    </p>
    <div class="serch-box" />
    <div v-bkloading="{ isLoading: loading }"
      class="tableList">
      <table class="bk-table">
        <thead>
          <tr>
            <td colspan="5"
              class="search-box">
              <form class="bk-form"
                style="width: 272px">
                <div class="bk-form-item">
                  <input
                    v-model="searchText"
                    v-focus
                    type="text"
                    class="bk-form-input"
                    autofocus
                    :placeholder="$t('搜索')"
                    @input="search">
                  <span class="search-icon"><i class="bk-icon icon-search" /></span>
                </div>
              </form>
            </td>
          </tr>
          <tr id="thead">
            <th v-for="(item, index) in tableFields"
              :key="index"
              :width="item.width"
              @click="sort(item)">
              {{ item.title }}
              <!-- <i><img src="../../../common/images/icon/order-reverse.png" /></i> -->
              <template v-if="item.sortable">
                <i v-if="item.is_reverse === -1">
                  <img src="../../../common/images/icon/order-sort2.png">
                </i>
                <i v-else-if="item.is_reverse === 0">
                  <img src="../../../common/images/icon/order-sort.png">
                </i>
                <i v-else-if="item.is_reverse === 1">
                  <img src="../../../common/images/icon/order-reverse2.png">
                </i>
                <i v-else><img src="../../../common/images/icon/order-reverse.png"></i>
              </template>
            </th>
          </tr>
        </thead>
        <tbody v-if="businessDetails.length > 0">
          <template v-if="businessDetails.length > 0">
            <tr v-for="(item, index) of businessDetails"
              :key="index">
              <td>{{ item.biz_name }}</td>
              <td @click="omitted(item)">
                <span class="business-owners"
                  :title="item.maintainers"
                  :class="{ business: item.business }">
                  {{ item.maintainers }}
                </span>
              </td>
              <td>
                {{ item.created_at }}
              </td>
              <td>{{ item.data_ids }}</td>
            </tr>
          </template>
          <tr>
            <td colspan="5"
              class="page-box mt10">
              <!-- <template class="mt10"> -->
              <bk-paging :curPage.sync="page.page"
                :size="'small'"
                :totalPage="page.totalPage" />
              <!-- </template> -->
            </td>
          </tr>
        </tbody>
      </table>
      <template v-if="businessDetails.length === 0">
        <div class="no-data">
          <img src="../../../common/images/no-data.png"
            alt="">
          <p>{{ $t('暂无数据') }}</p>
        </div>
      </template>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    data: {
      type: Object,
    },
  },
  data() {
    return {
      business: false,
      searchText: '',
      loading: false,
      dialogShow: '',
      searching: false,
      businessSearch: [],
      businessDetails: [],
      businessAllList: [],
      list: [],
      page: {
        page: 1,
        page_size: 5, // 每页数量
        totalpage: 10,
      },
      tableFields: [
        {
          width: 25 + '%',
          name: 'biz_name',
          title: this.$t('业务名称'),
          sortable: true,
          is_reverse: -2,
        },
        {
          width: 25 + '%',
          name: 'maintainers',
          title: this.$t('业务负责人'),
          sortable: true,
          is_reverse: -2,
        },
        {
          width: 30 + '%',
          name: 'created_at',
          title: this.$t('更新时间'),
          sortable: true,
          is_reverse: -2,
        },
        {
          width: 20 + '%',
          name: 'data_ids',
          title: this.$t('原始数据数量'),
          sortable: true,
          is_reverse: -2,
        },
      ],
    };
  },
  watch: {
    data: function (newVal, oldVal) {
      // eslint-disable-next-line vue/no-mutating-props
      this.data = newVal;
    },
    'paging.totalpage': function (newVal, oldVal) {
      this.paging.totalpage = newVal;
    },
    'page.page': function (newVal, oldVal) {
      if (this.searching) {
        this.page.page = newVal;
        this.businessDetails.length = 0;
        this.businessDetails = this.businessSearch.slice(
          (newVal - 1) * this.page.page_size,
          newVal * this.page.page_size
        );
      } else {
        this.page.page = newVal;
        this.businessDetails.length = 0;
        this.businessDetails = this.businessAllList.slice(
          (newVal - 1) * this.page.page_size,
          newVal * this.page.page_size
        );
      }
    },
  },
  methods: {
    sort(data) {
      this.page.page = 1;
      if (data.sortable) {
        // 箭头逻辑处理
        for (var i = 0; i < this.tableFields.length; i++) {
          if (this.tableFields[i].name !== data.name) {
            if (this.tableFields[i].is_reverse === -1) {
              this.tableFields[i].is_reverse = 0;
            } else if (this.tableFields[i].is_reverse === 1) {
              this.tableFields[i].is_reverse = -2;
            }
          }
        }
        if (data.is_reverse === -1) {
          data.is_reverse = 1;
        } else if (data.is_reverse === -2) {
          data.is_reverse = 1;
        } else if (data.is_reverse === 1) {
          data.is_reverse = -1;
        } else {
          data.is_reverse = -1;
        }
        this.sorting(data.name, data.is_reverse);
      }
    },
    /*
     * 排序传参 name排序对象， reverse 排序 1顺逆 -1逆序
     * */
    sorting(name, reverse) {
      this.businessSearch = this.businessSearch.sort(function (a, b) {
        if (a[name] > b[name]) {
          return reverse;
        }
        if (a[name] < b[name]) {
          return -reverse;
        }
      });
      let totalPage = Math.ceil(this.businessSearch.length / this.page.page_size);
      this.businessDetails = this.businessSearch.slice(0, this.page.page_size);
      // 总页数不能为0
      this.page.totalPage = totalPage !== 0 ? totalPage : 1;
    },
    /*
     * 是否超出隐藏
     * */
    omitted(item) {
      item.business = !item.business;
    },
    closePop() {
      this.$emit('close-pop');
    },
    /*
     * 搜索
     * */
    search() {
      this.page.page = 1;
      this.searching = true;
      this.businessSearch.splice(0);
      if (this.searchText.length === 0) {
        for (var key in this.businessAllList) {
          this.businessSearch[key] = this.businessAllList[key];
        }
        let totalPage = Math.ceil(this.businessAllList.length / this.page.page_size);
        this.businessDetails = this.businessAllList.slice(0, this.page.page_size);
        // 总页数不能为0
        this.page.totalPage = totalPage !== 0 ? totalPage : 1;
        return;
      }
      this.businessAllList.map(item => {
        for (let key in item) {
          if (JSON.stringify(item[key]).indexOf(this.searchText) > -1 && key !== 'can_remove') {
            this.businessSearch.push(item);
            break;
          }
        }
      });
      let totalPage = Math.ceil(this.businessSearch.length / this.page.page_size);
      this.businessDetails = this.businessSearch.slice(0, this.page.page_size);
      // 总页数不能为0
      this.page.totalPage = totalPage !== 0 ? totalPage : 1;
    },
    init() {
      this.searchText = '';
      this.loading = true;
      this.searching = false;
      if (this.$route.params.pid) {
        this.axios.get('projects/' + this.$route.params.pid + '/list_biz/').then(res => {
          if (res.result) {
            for (let i = 0; i < this.tableFields.length; i++) {
              if (this.tableFields[i].is_reverse === -1) {
                this.tableFields[i].is_reverse = 0;
              } else if (this.tableFields[i].is_reverse === 1) {
                this.tableFields[i].is_reverse = -2;
              }
            }
            for (var i = 0; i < res.data.length; i++) {
              res.data[i].business = false;
            }
            this.businessAllList = res.data;
            for (var key in this.businessAllList) {
              this.businessSearch[key] = this.businessAllList[key];
            }
            let totalPage = Math.ceil(res.data.length / this.page.page_size);
            this.businessDetails = res.data.slice(0, this.page.page_size);
            // 总页数不能为0
            this.page.totalPage = totalPage !== 0 ? totalPage : 1;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.loading = false;
        });
      } else {
        this.axios.get('projects/' + this.data.id + '/list_biz/').then(res => {
          if (res.result) {
            for (let i = 0; i < this.tableFields.length; i++) {
              if (this.tableFields[i].is_reverse === -1) {
                this.tableFields[i].is_reverse = 0;
              } else if (this.tableFields[i].is_reverse === 1) {
                this.tableFields[i].is_reverse = -2;
              }
            }
            for (let i = 0; i < res.data.length; i++) {
              res.data[i].business = false;
            }
            this.businessAllList = res.data;
            for (var key in this.businessAllList) {
              this.businessSearch[key] = this.businessAllList[key];
            }
            let totalPage = Math.ceil(res.data.length / this.page.page_size);
            this.businessDetails = res.data.slice(0, this.page.page_size);
            // 总页数不能为0
            this.page.totalPage = totalPage !== 0 ? totalPage : 1;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.loading = false;
        });
      }
    },
  },
};
</script>

<style media="screen" lang="scss" scoped>
.businessPop {
  .tableList {
    margin-top: 25px;
    width: 100%;
    border: 1px solid #e6e9f0;
    .bk-table {
      border: none;
      thead {
        color: #212232;
        td {
          border: none;
          height: 70px;
          line-height: 68px;
          padding: 0px 20px;
        }
        th {
          text-align: left;
          padding-left: 20px;
          font-weight: normal;
          i {
            float: right;
          }
        }
      }
      tbody {
        td {
          padding: 14px 20px;
          color: #212232;
          height: 48px;
          /*line-height: 48px;*/
          &.opreate {
            color: #3a84ff;
            cursor: pointer;
          }
          &.page-box {
            padding-right: 20px;
            text-align: right;
            box-sizing: border-box;
            padding-top: 17px;
            height: 70px;
          }
        }
        .business-owners {
          width: 255px;
          display: inline-block;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .business {
          white-space: inherit;
        }
      }
    }
    li {
      border-bottom: 1px solid #e6e9f0;
      padding: 0 10px;
      height: 61px;
      line-height: 60px;
      &:last-of-type {
        border: none;
      }
    }
    .search-box {
      .search-icon {
        position: absolute;
        right: 0px;
        top: 21px;
        width: 34px;
        height: 30px;
        line-height: 30px;
        text-align: center;
        text-decoration: none;
        color: #4e4e4e;
      }
    }

    .no-data {
      padding: 10px;
      text-align: center;
      p {
        color: #cfd3dd;
      }
    }
    .list-left {
      float: left;
      .business-image {
        display: inline-block;
        vertical-align: middle;
        height: 60px;
        margin-right: 10px;
        img {
          height: 58px;
        }
      }
    }
    .list-right {
      float: right;
      .apply {
        color: #3a84ff;
        margin-left: 25px;
      }
      .bk-icon {
        margin-left: 5px;
      }
    }
  }
}
</style>
