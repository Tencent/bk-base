

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
  <div class="bk-form">
    <bkdata-table
      style="margin-top: 15px"
      :size="'small'"
      :pagination="calcPagination"
      :outerBorder="false"
      :border="false"
      :colBorder="false"
      :data="calTableList"
      :emptyText="$t('暂无数据')"
      @page-change="handlePageChange"
      @page-limit-change="handlePageLimitChange"
      @sort-change="changeDataOrder">
      <bkdata-table-column
        sortable="custom"
        :minWidth="'160px'"
        :sortOrders="['ascending', 'descending']"
        :label="$t('状态')">
        <div slot-scope="props"
          class="bk-table-inlineblock">
          <span
            :class="[
              'sum-status',
              /started|running/i.test(props.row.status) && !props.row.has_exception
                ? 'running'
                : props.row.status === 'started' && props.row.has_exception
                  ? 'exception'
                  : '',
            ]">
            {{
              /started|running/i.test(props.row.status) && !props.row.has_exception
                ? $t('运行中')
                : props.row.status === 'started' && props.row.has_exception
                  ? $t('运行异常')
                  : $t('未运行')
            }}
          </span>
        </div>
      </bkdata-table-column>
      <bkdata-table-column
        sortable="custom"
        :minWidth="'200px'"
        :sortOrders="['ascending', 'descending']"
        :label="projectNameText"
        :renderHeader="renderProjectHedaer">
        <div slot-scope="props"
          class="bk-table-inlineblock">
          <span class="project-task"
            :class="{ isJumpable: props.row.flow_id !== flowid }"
            @click="jumpFlow(props.row)">
            <span class="icon">
              <i class="bk-icon icon-folder" />
            </span>
            <span class="ml5">{{ props.row.project_name ? props.row.project_name : props.row.project_id }}</span>
            /
            <span class="">{{ props.row.flow_name ? props.row.flow_name : props.row.flow_id }}</span>
          </span>
        </div>
      </bkdata-table-column>
      <bkdata-table-column
        :sortOrders="['ascending', 'descending']"
        :minWidth="'160px'"
        sortable="custom"
        :label="$t('创建人')"
        prop="created_by" />
    </bkdata-table>
  </div>
</template>
<script>
export default {
  inject: ['handleSliderClose'],
  model: {
    prop: 'value',
    event: 'taskLoaded',
  },
  props: {
    rtid: {
      type: [String, Number],
      default: '',
    },
    value: {
      type: Number,
      default: 0,
    },
    nodeId: {
      type: [String, Number],
      default: '',
    },
  },
  data() {
    return {
      curPage: 1,
      dataCount: 0,
      pageSize: 10,
      tableLabelMap: {
        'bk-table_1-column-1': 'sumStatus',
        'bk-table_1-column-2': 'project_name',
        'bk-table_1-column-3': 'created_by',
      },
      projectNameText: `${this.$t('关联项目')}/${this.$t('任务')}`,
      relatedTask: {
        // 关联任务
        loading: false,
        isShow: true,
        tableList: [],
      },
    };
  },
  computed: {
    calTableList() {
      return this.relatedTask.tableList.slice(this.pageSize * (this.curPage - 1), this.pageSize * this.curPage) || [];
    },
    calcPagination() {
      return {
        current: this.curPage,
        count: this.dataCount,
        limit: this.pageSize,
      };
    },
    flowid() {
      return Number(this.$route.params.fid);
    },

    projectId() {
      return Number(this.$route.query.project_id);
    },
  },
  watch: {
    rtid: {
      immediate: true,
      handler(val) {
        val && this.getRelatedTask(val);
      },
    },
  },
  methods: {
    renderProjectHedaer(h, data) {
      const tip = {
        name: 'bkTooltips',
        content: this.$t('引用了该计算节点的项目/任务'),
        placement: 'right',
      };

      return (
        <a class="custom-header-cell" v-bk-tooltips={tip}>
          {data.column.label}
        </a>
      );
    },
    handlePageChange(page) {
      this.curPage = page;
    },
    handlePageLimitChange(pageSize) {
      this.curPage = 1;
      this.pageSize = pageSize;
    },
    changeDataOrder(options) {
      let { column, order } = options;
      this.relatedTask.tableList = this.relatedTask.tableList.slice().sort((a, b) => {
        if (order === 'ascending') {
          return a[this.tableLabelMap[column.id]] < b[this.tableLabelMap[column.id]] ? -1 : 1;
        } else {
          return a[this.tableLabelMap[column.id]] < b[this.tableLabelMap[column.id]] ? 1 : -1;
        }
      });
    },
    jumpFlow(task) {
      if (task.flow_id !== this.flowid) {
        this.$router.push({
          name: 'dataflow_ide', // 'dataflow_detail',
          params: {
            fid: task.flow_id,
          },
          query: {
            project_id: task.project_id,
            NID: task.related_nodes.length && task.related_nodes[0].node_id,
          },
        });
        this.handleSliderClose();
      }
    },
    /*
     *   获取关联任务列表
     */
    getRelatedTask(rtid) {
      if (rtid) {
        const userName = this.$store.getters.getUserName;
        this.relatedTask.loading = true;
        this.bkRequest
          .httpRequest('dataFlow/listRelaTaskByRtid', {
            query: {
              result_table_id: rtid,
              bk_username: userName,
            },
          })
          .then(res => {
            if (res.result) {
              this.relatedTask.tableList = res.data.filter(item => item.flow_id !== this.flowid);
              this.relatedTask.tableList.forEach((x, i) => {
                let sumStatus =                  x.status === 'started' && !x.has_exception
                  ? 'running'
                  : x.status === 'started' && x.has_exception
                    ? 'exception'
                    : 'stopping';
                this.$set(x, 'sumStatus', sumStatus);
              });
              this.$emit('taskLoaded', this.relatedTask.tableList.length);
            } else {
              this.getMethodWarning(res.message, res.code);
            }
          })
          ['finally'](() => {
            this.relatedTask.loading = false;
            this.dataCount = this.relatedTask.tableList.length;
          });
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.bk-form {
  ::v-deep .bk-table-header-label .custom-header-cell {
    color: inherit;
    text-decoration: underline;
    text-decoration-style: dashed;
    text-decoration-color: #c4c6cc;
    text-underline-position: under;
  }
  .project-task {
    display: inline-block;
    color: #3a84ff;
    &.isJumpable {
      cursor: pointer;
      border-bottom: solid 1px transparent;
      &:hover {
        border-bottom: solid 1px #3a84ff;
      }
    }
  }
}
</style>
