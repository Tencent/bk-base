

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
  <div>
    <bkdata-sideslider :isShow.sync="isShow"
      extCls="slide-style"
      :showMask="false"
      :width="580"
      @hidden="close">
      <div slot="header"
        class="head-container">
        <i class="icon-cog" />
        <div class="head-text">
          <div class="title">
            {{ $t('剖析配置') }}
          </div>
          <p>
            对当前结果表（<span class="primary-color">{{ $route.query.result_table_id }}</span>）进行数据剖析，若有任何疑问请直接联系
            <a href="wxwork://message/?username=TGDP">{{ $t('TGDP助手') }}</a>
            协助解决。
          </p>
        </div>
      </div>
      <div slot="content">
        <form class="bk-form"
          :class="{ width100: $i18n.locale !== 'en' }">
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('结果表') }}:</label>
            <div class="bk-form-content">
              <bkdata-selector
                :selected.sync="selectedResultTable"
                :list="resultTableList"
                :settingKey="'id'"
                :displayKey="'name'" />
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('开始时间') }}:</label>
            <div class="bk-form-content">
              <bkdata-date-picker v-model="startTime"
                :placeholder="$t('选择日期时间范围')"
                :type="timeType" />
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('结束时间') }}:</label>
            <div class="bk-form-content">
              <bkdata-date-picker v-model="endTime"
                :placeholder="$t('选择日期时间范围')"
                :type="timeType" />
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('数据字段') }}:</label>
            <div class="bk-form-content">
              <bkdata-selector
                :placeholder="$t('默认选择所有字段')"
                :selected.sync="selectedResultTable"
                :list="resultTableList"
                :settingKey="'id'"
                :displayKey="'name'" />
            </div>
          </div>
        </form>
        <bkdata-collapse v-model="activeName"
          extCls="collapse-background-style">
          <bkdata-collapse-item :name="'0'">
            {{ $t('剖析选项') }}
            <div slot="content"
              class="f13">
              <bkdata-tree
                ref="tree1"
                :showIcon="false"
                :multiple="true"
                :data="treeListOne"
                :nodeKey="'id'"
                :hasBorder="true"
                @on-click="nodeClickOne"
                @on-expanded="nodeExpandedOne" />
            </div>
          </bkdata-collapse-item>
        </bkdata-collapse>
      </div>
    </bkdata-sideslider>
  </div>
</template>

<script>
export default {
  data() {
    return {
      activeName: '0',
      isShow: false,
      resultTableList: [],
      selectedResultTable: '',
      startTime: '',
      endTime: '',
      timeType: 'datetime',
      treeListOne: [
        {
          name: 'tree node1',
          title: 'tree node1',
          expanded: true,
          id: 1,
          children: [
            {
              name: 'tree node 1-1',
              title: 'tree node 1-1',
              expanded: true,
              children: [
                { name: 'tree node 1-1-1', title: 'tree node 1-1-1', id: 2 },
                { name: 'tree node 1-1-2', title: 'tree node 1-1-2', id: 3 },
                { name: 'tree node 1-1-3', title: 'tree node 1-1-3', id: 4 },
              ],
            },
            {
              title: 'tree node 1-2',
              name: 'tree node 1-2',
              id: 5,
              expanded: true,
              children: [
                { name: 'tree node 1-2-1', title: 'tree node 1-2-1', id: 6 },
                { name: 'tree node 1-2-2', title: 'tree node 1-2-2', id: 7 },
              ],
            },
          ],
        },
      ],
    };
  },
  methods: {
    open() {
      this.isShow = true;
      this.$nextTick(() => {
        window.__bk_pop_manager.hideModalMask();
      });
    },
    nodeClickOne(node) {
      console.log(node);
    },
    nodeExpandedOne(node, expanded) {
      console.log(node);
      console.log(expanded);
    },
    close() {
      this.$emit('close');
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .tree-drag-node {
  display: flex;
  align-items: center;
  .tree-node {
    .node-icon {
      top: 0px;
    }
    .node-title {
      margin-left: 8px;
    }
  }
}
.bk-form {
  padding: 20px;
  .bk-form-item {
    .no-float {
      float: inherit;
    }
    .bk-form-content {
      margin-left: 100px;
      .bk-date-picker {
        width: 100%;
      }
    }
    .height450 {
      width: 500px;
      height: 450px;
      margin-left: 24px;
    }
  }
}
.width100 {
  .bk-label {
    width: 100px;
  }
}
::v-deep .slide-style {
  position: absolute;
  top: -1px;
  bottom: -1px;
  right: -442px;
  pointer-events: none;
  border: 1px solid rgba(195, 205, 215, 0.6);
  .bk-sideslider-wrapper {
    border-left: 1px solid rgba(195, 205, 215, 0.6);
    pointer-events: all;
  }
  .bk-sideslider-header {
    height: auto !important;
    .bk-sideslider-title {
      padding: 0 0 0 30px !important;
      height: auto !important;
      .head-container {
        display: flex;
        line-height: normal;
        font-size: 12px;
        .icon-cog {
          float: left;
          font-size: 22px;
          width: 22px;
          margin: 20px 0 0 10px;
        }
        .head-text {
          width: calc(100% - 32px);
          padding: 10px;
          .title {
            font-size: 16px;
            line-height: 28px;
            margin-bottom: 10px;
          }
          p {
            font-size: 14px;
            color: #979ba5;
            font-weight: normal;
          }
          .primary-color {
            color: #3a84ff;
          }
        }
      }
    }
  }
  .bk-sideslider-content {
    padding: 0 15px;
  }
}
</style>
