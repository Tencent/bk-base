

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
  <div v-bkloading="{ isLoading: isLoading }">
    <bkdata-table :data="tableData"
      :emptyText="$t('暂无数据')"
      :pagination="pagination"
      @page-change="handlePageChange"
      @page-limit-change="handlePageLimitChange"
      @sort-change="sortChange">
      <bkdata-table-column type="expand"
        width="30"
        align="center">
        <template slot-scope="props">
          <bkdata-table
            :data="[...props.row.detaildata, ...props.row.indicator]"
            :emptyText="$t('暂无数据')"
            :outerBorder="false"
            :headerCellStyle="{
              background: '#fff',
              borderRight: 'none',
            }">
            <bkdata-table-column :minWidth="250"
              :label="$t('名称')">
              <div slot-scope="subprops"
                class="text-overflow click-style"
                :title="subprops.row.standard_content_name"
                @click="linkToDetail(subprops.row.standardId, subprops.row.id)">
                {{ subprops.row.standard_content_name }}
              </div>
            </bkdata-table-column>
            <bkdata-table-column :minWidth="220"
              :label="$t('描述')">
              <div slot-scope="subprops"
                class="text-overflow"
                :title="subprops.row.description">
                {{ subprops.row.description }}
              </div>
            </bkdata-table-column>
            <bkdata-table-column :width="140"
              prop="type"
              :label="$t('类型')" />
            <bkdata-table-column :width="220"
              prop="linked_data_count"
              :label="$t('关联数据')">
              <span slot-scope="subprops"
                :class="[subprops.row.linked_data_count > 0 ? 'click-style' : '']"
                @click="linkToDiction(subprops.row)">
                {{ subprops.row.linked_data_count || 0 }}
              </span>
            </bkdata-table-column>
            <bkdata-table-column prop="desc"
              :label="$t('操作')"
              :width="125">
              <template slot-scope="subprops">
                <bkdata-button class="mr10"
                  theme="primary"
                  text
                  @click="linkToDetail(subprops.row.standardId, subprops.row.id)">
                  {{ $t('详情') }}
                </bkdata-button>
                <a target="_blank"
                  :href="$store.getters['docs/getPaths'].dataMartFeedback">
                  {{ $t('反馈') }}
                </a>
              </template>
            </bkdata-table-column>
            <bkdata-table-column prop="standard_version" />
            <bkdata-table-column :width="100"
              prop="updated_by" />
            <bkdata-table-column :width="150"
              prop="updated_at" />
            <bkdata-table-column width="90"
              fixed="right" />
          </bkdata-table>
        </template>
      </bkdata-table-column>
      <bkdata-table-column :minWidth="250"
        :label="$t('标准名称')"
        prop="standard_name">
        <div slot-scope="props"
          class="text-overflow click-style"
          :title="props.row.standard_name"
          @click="linkToDetail(props.row.id)">
          {{ props.row.standard_name }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column :minWidth="220"
        :label="$t('标准描述')"
        prop="description">
        <div slot-scope="props"
          class="text-overflow"
          :title="props.row.description">
          {{ props.row.description }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column :width="140"
        :label="$t('标签')">
        <div v-if="props.row.hasOwnProperty('tag_list') && props.row.tag_list.length"
          slot-scope="props"
          class="flex">
          <span v-for="(item, index) in props.row.tag_list.slice(0, 2)"
            :key="index"
            :title="item.alias"
            class="tag text-overflow">
            {{ item.alias }}
          </span>
          <bkdata-popover placement="top">
            <span v-if="props.row.tag_list.length > 2"
              class="icon-ellipsis platform-item-ellipsis" />
            <div slot="content"
              class="clearfix"
              style="white-space: normal; max-width: 500px; max-height: 400px">
              <span v-for="(item, index) in props.row.tag_list.slice(2)"
                :key="index"
                class="data-platform-item">
                {{ item.alias }}
              </span>
            </div>
          </bkdata-popover>
        </div>
      </bkdata-table-column>
      <bkdata-table-column :width="220"
        :renderHeader="renderHeader">
        <div slot-scope="props">
          {{ `【${props.row.detaildata.length}】/【${props.row.indicator.length}】` }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column :width="135"
        :label="`${$t('关联数据')}（${$t('个')}）`"
        sortable>
        <span slot-scope="props"
          :class="[props.row.linked_data_count > 0 ? 'click-style' : '']"
          @click="linkToDiction(props.row)">
          {{ props.row.linked_data_count || 0 }}
        </span>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('版本号')"
        prop="standard_version" />
      <bkdata-table-column :width="100"
        :label="$t('修改者')"
        prop="updated_by" />
      <bkdata-table-column :width="150"
        :label="$t('修改时间')"
        prop="updated_at" />
      <bkdata-table-column :label="$t('操作')"
        width="120"
        fixed="right">
        <template slot-scope="props">
          <bkdata-button class="mr10"
            theme="primary"
            text
            @click="linkToDetail(props.row.id)">
            {{ $t('详情') }}
          </bkdata-button>
          <a target="_blank"
            :href="CESiteUrl">
            {{ $t('反馈') }}
          </a>
        </template>
      </bkdata-table-column>
    </bkdata-table>
  </div>
</template>

<script>
export default {
  data() {
    return {
      page: 1,
      pageSize: 15,
      pageCount: 1,
      isLoading: false,
      tableData: [],
      params: {
        page: this.page,
        page_size: this.pageSize,
        keyword: '',
        tag_ids: [],
      },
      linkDataSort: '',
      CESiteUrl: window.BKBASE_Global.CESiteUrl + 'products/view/2282?&tab_id=all_issues_btn#create-feedback',
    };
  },
  computed: {
    pagination() {
      return {
        current: this.page,
        count: this.pageCount,
        limit: this.pageSize,
        limitList: [10, 15, 20],
      };
    },
  },
  mounted() {
    this.getData();
  },
  methods: {
    sortChange(data) {
      if (data.order === 'descending') {
        this.linkDataSort = 'desc';
      } else if (data.order === 'ascending') {
        this.linkDataSort = 'asc';
      } else {
        this.linkDataSort = '';
      }
      this.getData();
    },
    linkToDetail(data, id) {
      this.$router.push({
        name: 'DataStandardDetail',
        query: {
          id: data,
        },
        params: {
          treeId: id,
        },
      });
    },
    linkToDiction(data) {
      if (!data.linked_data_count) return;
      const { standard_version_id, standard_name, standard_content_name, standard_content_id } = data;
      this.$router.push({
        name: 'DataSearchResult',
        params: {
          standard_version_id,
          standard_name,
          standard_content_name,
          standard_content_id,
        },
      });
    },
    renderHeader(h, { column, $index }) {
      const that = this;
      return h(
        'div',
        {
          class: ['custom-head'],
        },
        [
          h('span', [that.$t('标准内容')]),
          h(
            'span',
            {
              class: ['custom-child'],
            },
            [`[${that.$t('明细数据')}] / [${that.$t('原子指标标准')}](${$t('个')})`]
          ),
        ]
      );
    },
    updateData(params) {
      Object.keys(params).forEach(key => {
        this.params[key] = params[key];
      });
      this.page = 1;
      this.pageSize = 15;
      this.getData();
    },
    handlePageLimitChange(pageSize, preLimit) {
      this.page = 1;
      this.pageSize = pageSize;
      this.getData();
    },
    handlePageChange(page) {
      this.page = page;
      this.getData();
    },
    getData() {
      const params = {
        page: this.page,
        page_size: this.pageSize,
      };
      if (this.linkDataSort) {
        // 关联数据排序
        params.order_linked_data_count = this.linkDataSort;
      }
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataStandard/getStandardTable', {
          params: Object.assign({}, this.params, params),
        })
        .then(res => {
          if (res.result) {
            this.tableData = res.data.standard_list;
            this.tableData.forEach(child => {
              for (const key in child) {
                if (key === 'indicator') {
                  child[key].forEach(item => {
                    item.type = '原子指标';
                  });
                  addAttr(child, key);
                } else if (key === 'detaildata') {
                  child[key].forEach(item => {
                    item.type = '明细数据';
                  });
                  addAttr(child, key);
                }
              }
            });
            function addAttr(child, key) {
              child[key].forEach(item => {
                item.standardId = child.id;
                item['standard_content_id'] = item.id;
                item['standard_version_id'] = child.standard_version_id;
                item['standard_name'] = child.standard_name;
              });
            }
            this.pageCount = res.data.standard_count;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isLoading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .bk-table-small {
  .bk-table {
    .bk-table-header-wrapper {
      thead > tr > th {
        font-size: 12px;
      }
      .bk-table-header {
        th {
          height: 42px;
          background-color: #fafbfd !important;
          .cell {
            height: 42px;
            line-height: 42px;
          }
        }
      }
    }

    .bk-table-body-wrapper {
      .bk-table-row {
        td {
          height: 42px;
        }
      }
    }
  }
}
::v-deep .custom-head {
  display: flex;
  align-items: center;
  flex-direction: column;
  justify-content: center;
  line-height: normal;
  height: 100%;
  .custom-child {
    transform: scale(0.95);
    font-weight: normal;
  }
}
.flex {
  display: flex;
  align-items: center;
}
.tag {
  display: inline-block;
  max-width: 69px;
  min-width: 34px;
  margin-left: 10px;
  padding: 2px 4px;
  border: 1px solid #ddd;
  &:first-child {
    margin-left: 0;
  }
}
.platform-item-ellipsis {
  display: inline-block;
  max-width: 69px;
  min-width: 34px;
  margin-left: 10px;
  padding: 2px 4px;
  border: 1px solid #ddd;
  font-size: 18px;
  font-weight: bolder;
}
.data-platform-item {
  white-space: nowrap;
  margin-left: 10px;
  padding: 2px 4px;
  border: 1px solid #ddd;
  float: left;
  &:first-child {
    margin-left: 0px;
  }
}
.click-style {
  color: #3a84ff;
  cursor: pointer;
}
</style>
