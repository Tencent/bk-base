

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
  <bkdata-tree ref="tree1"
    :data="treeData"
    :nodeKey="'id'"
    :multiple="false"
    :hasBorder="true"
    @on-click="nodeClickOne"
    @on-expanded="nodeExpandedOne" />
</template>
<script>
import Bus from '@/common/js/bus.js';
export default {
  props: {
    standardContent: {
      type: Object,
      default: () => ({}),
    },
    treeId: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
      treeData: [
        {
          name: '',
          title: '',
          id: '',
          children: [],
          expanded: true,
          selected: false,
        },
      ],
    };
  },
  watch: {
    treeId: {
      immediate: true,
      handler(val) {
        if (val) {
          this.lightNode(this.treeData, val);
        }
      },
    },
    standardContent: {
      immediate: true,
      handler(val) {
        if (Object.keys(val).length && val.standard_content.length) {
          // 第一层
          this.treeData[0].name = this.treeData[0].title = val.basic_info.standard_name;
          const treeItem = this.treeData[0].children;
          const standardContent = val.standard_content;
          // 第二层
          this.treeData[0].children = [
            {
              name: standardContent[0].standard_info.standard_content_name,
              title: standardContent[0].standard_info.standard_content_name,
              expanded: true,
              level: 2,
              id: standardContent[0].standard_info.id,
              children: [],
              selected: false,
            },
          ];
          // 第三层
          val.standard_content.forEach((item, index) => {
            if (index > 0) {
              if (item.standard_fields.length) {
                this.treeData[0].children[0].children.push({
                  name: item.standard_info.standard_content_name,
                  title: item.standard_info.standard_content_name,
                  expanded: true,
                  level: 2,
                  id: item.standard_info.id,
                  selected: false,
                });
              }
            }
          });
          // 从数据字典详情页跳转到标准详情页，携带的contentId是标准内容下对应的指标id
          const { contentId } = this.$route.query;
          if (contentId) {
            return this.lightNode(this.treeData, contentId);
          }
          this.lightNode(this.treeData, this.treeId);
        }
      },
    },
  },
  methods: {
    lightNode(data, id) {
      data.forEach(item => {
        item.selected = item.id === id;
        if (item.selected) {
          Bus.$emit('openCollapse', item.id);
        }
        if (item.children && item.children.length) {
          this.lightNode(item.children, id);
        }
      });
    },
    nodeClickOne(node) {
      this.lightNode(this.treeData, node.id);
    },
    nodeExpandedOne(node, expanded) {
      this.treeData[0].expanded = true;
      this.treeData[0].children[0].expanded = true;
      if (Number(node.level) === 2) {
        Bus.$emit('openCollapse', node.id);
      }
    },
  },
};
</script>
<style lang="scss" scoped>
::v-deep .tree-drag-node {
  .tree-node {
    .node-icon {
      top: 0px;
    }
    .node-title {
      margin-left: 8px;
    }
  }
}
</style>
