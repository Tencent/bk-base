

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
    <div class="tree-menu tree-addition"
      :class="{ 'storage-unexpand': !isSidebarExpand }">
      <my-tree v-for="menuItem in searchedNodes"
        :key="menuItem.id"
        :model="menuItem" />
    </div>
  </div>
</template>

<script>
import { mapState, mapGetters } from 'vuex';
import Bus from '@/common/js/bus';
import myTree from '@/components/tree/treeMenu.vue';
import { copyObj } from '@/common/js/util';
export default {
  name: 'flow-outline',
  components: {
    myTree,
  },
  inject: ['activeNodeById'],
  props: {
    searchValue: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      searchContent: '',
      renderList: [],
      models: {},
    };
  },
  computed: {
    ...mapState({
      isSidebarExpand: state => state.ide.isSidebarExpand,
      graphData: state => state.ide.graphData || {},
    }),
    ...mapGetters({
      getNodeConfigByNodeWebType: 'ide/getNodeConfigByNodeWebType',
    }),
    /**
     * 搜索匹配
     */
    searchedNodes() {
      let searchContent = this.searchValue;
      const renderList = copyObj(this.models);
      Object.keys(renderList).forEach(model => {
        let arr = renderList[model].children.filter(m => {
          return m.menuName.includes(searchContent);
        });
        renderList[model].children = arr;
      });

      return renderList;
    },
  },
  watch: {
    graphData: {
      handler(newVal) {
        if (newVal && newVal.locations) {
          this.models = {};
          for (let location of newVal.locations) {
            let group = this.getType(location.node_type);
            if (group) {
              let temp = {
                menuName: location.node_name || '未配置',
                types: 'child',
                isActive: false,
              };
              let obj = Object.assign(temp, location);
              if (this.models[group.node_group_name]) {
                this.models[group.node_group_name].children.push(obj);
              } else {
                this.models[group.node_group_name] = {
                  id: group.node_group_name,
                  menuName: group.node_group_display_name,
                  types: 'parant',
                  children: [obj],
                };
              }
            }
          }
        }
      },
      deep: true,
      immediate: true,
    },
  },

  mounted() {
    Bus.$on('focusNode', model => {
      this.focusNode(model);
    });
  },
  destroyed() {
    Bus.$off('focusNode');
  },
  methods: {
    /**
     * 判断是属于哪种类型节点
     */
    getType(type) {
      return this.getNodeConfigByNodeWebType(type);
    },
    /**
     * 点击outline节点事件响应
     */
    focusNode(model) {
      this.activeNodeById(model.id);
    },
  },
};
</script>

<style lang="scss">
.tree-addition {
  opacity: 1;
  transition: all 0.3s ease-in;
}
</style>
