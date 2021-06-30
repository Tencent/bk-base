

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
    <!-- <div class="bk-form-item pl15 pr15 pt20">
            <div class="bk-form-content">
                <bkdata-input type="text"
                    class="mb15"
                    v-model="searchValue"
                    :placeholder="$t('请输入函数名')"
                    :right-icon="'bk-icon icon-search'" />
            </div>
        </div> -->
    <div class="tree-List">
      <div class="tree-ul">
        <template v-for="models in searchResult">
          <bk-tree :key="models.name"
            class="item"
            :search="searchValue"
            :model="models" />
        </template>
      </div>
    </div>
    <div v-if="$modules.isActive('udf')"
      class="create-fn"
      @click="createFn()">
      <span> <i class="bk-icon bk-icon-style icon-udf" /> {{ $t('创建自定义函数') }} </span>
    </div>
    <div v-if="showUDFProcess"
      class="udfWrapper">
      <udf-slider ref="udfSlider"
        @udfSliderClose="closeSlider" />
    </div>
    <MemberManagerWindow
      ref="member"
      :roleIds="['function.manager', 'function.developer']"
      :isOpen.sync="udfMemberParams.isOpen"
      :objectClass="udfMemberParams.objectClass"
      :scopeId="udfMemberParams.scopeId" />
  </div>
</template>
<script>
import udfSlider from '@/pages/DataGraph/Graph/Components/UDF/sliderIndex';
import MemberManagerWindow from '@/pages/authCenter/parts/MemberManagerWindow';
import bkTree from '@/components/tree/tree.vue';
import allData from './data.json';
import allDataEn from './data-en.json';
import Cookies from 'js-cookie';
import Bus from '@/common/js/bus.js';
export default {
  components: {
    bkTree,
    udfSlider,
    MemberManagerWindow,
  },
  props: {
    noip: {
      type: Boolean,
      default: false,
    },
    searchValue: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      showUDFProcess: false,
      data: [],
      dataTmp: [],
      udfMemberParams: {
        isOpen: false,
        objectClass: 'function',
        scopeId: '',
        roleIds: ['function.manager', 'function.developer'],
      },
    };
  },
  computed: {
    searchResult: function () {
      let con = this.searchValue.toLowerCase();
      if (!con.trim()) {
        return this.dataTmp;
      }
      const d = JSON.parse(JSON.stringify(this.dataTmp));
      return d.filter(function (oneLevel) {
        const children1 = oneLevel.children || [];
        /** 自定义函数没有三层，直接过滤 */
        if (children1.length && children1[0].level === 3) {
          const inner = children1.filter(threeLevel => {
            return threeLevel.name.toLowerCase().includes(con);
          });
          children1.splice(0, children1.length, ...inner);
          /** 常规函数多层，因此要多层过滤 */
        } else {
          const outer = children1.filter(twoLevel => {
            const childrend2 = twoLevel.children || [];
            const inner = childrend2.filter(threeLevel => {
              return threeLevel.name.toLowerCase().includes(con);
            });
            childrend2.splice(0, childrend2.length, ...inner);
            return childrend2;
          });
          children1.splice(0, children1.length, ...outer);
        }
        return oneLevel;
      });
    },
  },
  mounted() {
    this.getTreeData();
    Bus.$on('openUDFSlider', this.createFn);
    Bus.$on('getUDFAuth', this.getAuth);
  },
  methods: {
    getAuth(funcName) {
      this.udfMemberParams.scopeId = funcName;
      this.udfMemberParams.isOpen = true;
    },
    closeSlider() {
      this.udfInit(); // 初始化UDF
      this.showUDFProcess = false;
    },
    udfInit() {
      this.$store.commit('udf/changeInitStatus', false); // 初始化UDF状态
      this.$store.commit('udf/saveDevParams'); // 清空配置
    },
    createFn() {
      this.showUDFProcess = true;
      setTimeout(() => {
        this.$refs.udfSlider.isShow = true;
      }, 0);
    },
    getfuncUdf(released) {
      let statusText = ['开发中', '已发布'];
      return {
        children: [],
        level: 2,
        display: statusText[released],
        name: statusText[released],
        open: false,
      };
    },
    getTreeData() {
      let treeList = [];
      const url = this.$modules.isActive('udf')
        ? 'v3/dataflow/flow/list_function_doc/'
        : '/v3/dataflow/flow/bksql/bksql_func_info/';
      if (this.noip) {
        let list = allData['function'];
        if (Cookies.get('blueking_language') === 'en') {
          list = allDataEn['function'];
        }
        this.data = list;
        this.dataTmp = list;
        return;
      }
      this.axios.get(url).then(res => {
        if (res.result) {
          let groupsData = [];
          res.data.forEach(groups => {
            let listgroup = [];
            groups.func_groups
              && groups.func_groups.forEach(group => {
                if (group.func) {
                  group.func.forEach(third => {
                    this.$set(third, 'level', 3);
                  });
                  listgroup.push({
                    open: false,
                    display: group.display,
                    children: group.func,
                    name: group.group_name,
                    level: 2,
                  });
                } else {
                  this.$set(group, 'level', 3);
                  listgroup.push(group);
                }
              });
            if (groups.hasOwnProperty('func')) {
              if (groups.func.length !== 0) {
                listgroup = [this.getfuncUdf(0), this.getfuncUdf(1)];
                groups.hasOwnProperty('func')
                  && groups.func.forEach(udf => {
                    this.$set(udf, 'level', 3);
                    listgroup[udf.released].children.push(udf);
                  });
                listgroup.reverse();
              } else {
                listgroup.push({
                  open: false,
                  display: this.$t('暂无自定义函数'),
                  children: [],
                  level: 2,
                });
              }
            }
            groupsData.push({
              open: true,
              display: groups.display,
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
.true {
  display: flex;
  flex-flow: column;
  height: 100%;
  .tree-List {
    .tree-ul {
      text-align: left;
      /* padding-left: 1em; */
      line-height: 28px;

      .item {
        .tree-li {
          .tree-content {
            .model-name {
              font-family: SourceCodePro, Menlo, Monaco, Consolas, 'Courier New', monospace;
            }

            &.bold {
              font-weight: bold;
              .model-name {
                font-family: SourceCodePro, Menlo, Monaco, Consolas, 'Courier New', monospace;
              }
            }
          }
        }
      }
    }
    .item {
      cursor: pointer;
    }
  }
  .create-fn {
    width: 300px;
    height: 34px;
    box-shadow: 0px 3px 4px rgba(0, 0, 0, 0.32);
    background: rgba(58, 132, 255, 1);
    display: flex;
    justify-content: center;
    flex-shrink: 0;
    align-items: center;
    margin-bottom: 4px;
    position: absolute;
    bottom: 0;
    cursor: pointer;
    span {
      font-size: 14px;
      font-family: PingFangSC-Regular;
      font-weight: 400;
      color: rgba(255, 255, 255, 1);
      line-height: 20px;
    }
  }
}
</style>
