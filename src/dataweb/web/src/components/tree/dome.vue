

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
  <div class="true">
    <div class="bk-form-item pl15 pr15 pt20">
      <div class="bk-form-content">
        <input
          v-model="searchContent"
          type="text"
          class="bk-form-input pr30 mb15"
          :placeholder="$t('请输入节点关键字')">
        <i class="bk-icon icon-search" />
      </div>
    </div>
    <div class="tree-List">
      <div class="tree-ul">
        <template v-for="(models, ind) in searchResult">
          <bk-tree :key="ind"
            class="item"
            :search="searchContent"
            :model="models" />
        </template>
      </div>
    </div>
  </div>
</template>
<script>
import bkTree from '@/components/tree/tree.vue';
export default {
  components: {
    bkTree,
  },
  data() {
    return {
      searchContent: '',
      data: [],
      dataTmp: [],
    };
  },
  computed: {
    searchResult: function () {
      let con = this.searchContent.toLowerCase();
      if (!con.trim()) {
        return this.dataTmp;
      }
      const d = JSON.parse(JSON.stringify(this.dataTmp));
      return d.filter(function (oneLevel) {
        // let b = oneLevel.children.filter(function(twoLevel) {
        //     let a = twoLevel.children.filter(function(threeLevel) {
        //         console.log(threeLevel.name.toLowerCase().includes(con))
        //         return threeLevel.name.toLowerCase().includes(con)
        //     })
        //     console.log('a', a)
        //     return a.length > 0
        // })
        // console.log('b', b)
        // return b.length > 0
        const outer = oneLevel.children.filter(twoLevel => {
          const inner = twoLevel.children.filter(threeLevel => {
            return threeLevel.name.toLowerCase().includes(con);
          });
          twoLevel.children.splice(0, twoLevel.children.length, ...inner);
          return twoLevel.children;
        });
        oneLevel.children.splice(0, oneLevel.children.length, ...outer);
        return oneLevel;
      });
    },
  },
  mounted() {
    this.getTreeData();
  },
  methods: {
    getTreeData() {
      let treeList = [];
      this.axios.get('v3/dataflow/flow/bksql/bksql_func_info/').then(res => {
        if (res.result) {
          let groupsData = [];
          res.data.forEach(groups => {
            let listgroup = [];
            groups.func_groups.forEach(group => {
              group.func.forEach(third => {
                this.$set(third, 'level', 3);
              });
              listgroup.push({
                children: group.func,
                name: group.group_name,
                level: 2,
              });
            });
            groupsData.push({
              children: listgroup,
              name: groups.type_name,
              level: 1,
            });
          });
          this.data.splice(0, this.data.length, ...groupsData);
          this.dataTmp.splice(0, this.dataTmp.length, ...groupsData);
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
  },
};
</script>
<style lang="scss">
.tree-List {
  .tree-ul {
    text-align: left;
    padding-left: 1em;
    line-height: 1.5em;
  }
  .item {
    cursor: pointer;
  }
}
</style>
