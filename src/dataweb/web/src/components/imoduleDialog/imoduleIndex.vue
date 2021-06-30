

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
  <div class="imodule-container">
    <span class="module-select mr15"
      @click="handleSelectModule()">
      <i class="bk-icon icon-cc-square mr5" />{{ $t('选择模块和IP') }}
    </span>
    <bkdata-dialog
      v-model="iModuleVisiable"
      :width="900"
      :theme="'primary'"
      :maskClose="false"
      :okText="$t('保存')"
      :cancelText="$t('取消')"
      @confirm="confirm">
      <template v-if="iModuleVisiable">
        <bkdata-ipmodule-selector
          ref="imodule"
          v-model="scope"
          :bizid="bizid"
          :sourceModuleList="sourceModuleList"
          :sourceCloudArea="sourceCloudArea"
          :cloudAreaSelected="sourceCloudAreaSelected" />
      </template>
    </bkdata-dialog>
  </div>
</template>
<script>
import bkdataIpmoduleSelector from './imodule';
export default {
  components: { bkdataIpmoduleSelector },
  model: {
    prop: 'value',
    event: 'change',
  },
  props: {
    bizid: {
      type: Number,
      default: 0,
    },
    value: {
      type: Object,
    },
    beforeOpen: {
      type: Function,
      default: () => true,
    },
  },
  data() {
    return {
      scope: {},
      iModuleVisiable: false,
      sourceModuleList: [],
      sourceCloudArea: [],
      sourceCloudAreaSelected: '',
    };
  },
  watch: {
    bizid: {
      immediate: true,
      handler(val) {
        val && this.getModuleList();
      },
    },
  },

  mounted() {
    this.cloneDataFromModel();
    this.getCloudArea();
  },
  methods: {
    handleSelectModule() {
      this.cloneDataFromModel();
      if (typeof this.beforeOpen === 'function') {
        this.iModuleVisiable = !!this.beforeOpen();
      } else {
        this.iModuleVisiable = true;
      }
      this.setModule();
    },

    confirm() {
      const target = {
        module_scope: this.scope['module_scope'].map(item => {
          return Object.keys(item)
            .filter(key => key !== 'child')
            .reduce((pre, current) => Object.assign(pre, { [current]: item[current] }), {});
        }),
        host_scope: this.scope['host_scope'],
      };

      this.$emit('change', Object.assign({}, this.value, target));
    },
    /** 获取模块 */
    getModuleList() {
      this.isLoading = true;
      this.sourceModuleList = [];
      this.bkRequest
        .httpRequest('meta/getModuleTree', {
          params: {
            bizId: this.bizid,
          },
        })
        .then(res => {
          if (res.result) {
            this.sourceModuleList = res.data;
            this.updateNodeUniqueIds(this.sourceModuleList);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.setModule();
          this.isLoading = false;
        });
    },

    setModule() {
      this.$nextTick(() => {
        this.$refs.imodule && this.$refs.imodule.setData(this.scope);
      });
    },

    /** 生成唯一ID */
    updateNodeUniqueIds(root) {
      Array.isArray(root)
        && root.forEach(item => {
          this.$set(item, 'uid', `${item.bk_obj_id}_${item.bk_inst_id}`);
          item.child && this.updateNodeUniqueIds(item.child);
        });
    },

    /** 获取云区域 */
    getCloudArea() {
      this.$store.dispatch('api/getAreaLists').then(res => {
        if (res.result) {
          this.sourceCloudArea = res.data;
          this.sourceCloudAreaSelected = res.data[0].bk_cloud_id;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },

    /** 复制结果集，防止修改引起的内存溢出 */
    cloneDataFromModel() {
      this.scope = JSON.parse(JSON.stringify(this.value));
      this.updateNodeUniqueIds(this.scope['module_scope'] || {});
    },
  },
};
</script>
<style lang="scss" scoped>
.imodule-container {
  display: inline-block;
  .module-select {
    padding: 5px;
    border: 1px solid #c3cdd7;
    background: #fff;
    cursor: pointer;
    .bk-icon {
      font-size: 14px;
    }
  }
}
</style>
