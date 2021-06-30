

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
  <div class="clusters">
    <bkdata-selector
      class="cluster-selector"
      :colorLevel="colorLevel"
      :hasChildren="true"
      :list="clusterList"
      :disabled="isDisabled || disabled"
      :placeholder="placeholder"
      :settingKey="'cluster_name'"
      :displayKey="'alias'"
      :optionTip="colorLevel"
      :toolTipTpl="colorLevel ? getColorLevelTpl : ''"
      :customIcon="colorLevel ? 'circle-shape' : ''"
      :selected.sync="currentValue"
      @item-selected="handleClusterSelect">
      <!-- <div class="bk-table-inlineblock" slot-scope="props">
                <div class="bkdata-selector-create-item" @click="applyClusterAccess">
                    <i class="text">{{$t('申请集群组权限')}}</i>
                </div>
            </div> -->
    </bkdata-selector>
    <i
      v-bk-tooltips="$t('如要使用无权限集群组请联系管理员')"
      class="bk-icon icon-question-circle icon-no-cluster-apply" />
  </div>
</template>

<script>
import { mapState, mapGetters } from 'vuex';
export default {
  model: {
    prop: 'value',
    event: 'change',
  },
  props: {
    clusterType: {
      type: String,
      default: 'es',
    },
    selfConfig: {
      type: [Object, String],
      default: () => ({}),
    },
    value: {
      type: String,
    },
    clusterLoading: {
      type: Boolean,
    },
    disabled: {
      type: Boolean,
    },
    colorLevel: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      clusterList: [],
      selectedType: '',
      placeholder: '',
      isDisabled: false,
      currentValue: '',
    };
  },
  computed: {
    ...mapState({
      projectId: state => state.ide.flowData.project_id,
    }),
    ...mapGetters({
      getActiveProject: 'getProjectById',
    }),
    activeProject() {
      return (this.projectId && this.getActiveProject(this.projectId)) || {};
    },
    geogArea() {
      return (
        (this.activeProject.tags
          && this.activeProject.tags.manage
          && this.activeProject.tags.manage.geog_area
          && this.activeProject.tags.manage.geog_area[0].code)
        || 'inland'
      );
    },
    loading: {
      get() {
        return this.clusterLoading;
      },
      set(newVal) {
        this.$emit('update:clusterLoading', newVal);
      },
    },
  },
  watch: {
    value: {
      immediate: true,
      handler(val) {
        if (this.currentValue !== val) {
          this.currentValue = val;
        }
      },
    },
    /** selfConfig回填，发送集群的请求
     * 新节点，带参数enable； 编辑节点时，不带该参数，返回所有
     */
    selfConfig: {
      immediate: true,
      handler(val) {
        (val.hasOwnProperty('id') || val.hasOwnProperty('node_id'))
          && this.getClusterList(val.hasOwnProperty('node_id'));
      },
    },
  },

  methods: {
    handleClusterSelect(val, item) {
      this.$emit('change', val, item);
      this.$emit('handlerCopyStorage', item.isEnableReplica);
    },
    async getClusterList(isEdit) {
      this.loading = true;
      this.placeholder = this.$t('数据加载中');
      this.isDisabled = true;
      const tags = isEdit ? [this.geogArea, 'usr'] : [this.geogArea, 'usr', 'enable'];
      await this.bkRequest
        .httpRequest('auth/getFlowClusterWithAccess', {
          params: {
            id: this.projectId,
            clusterType: this.clusterType,
          },
          query: {
            tags: tags,
          },
        })
        .then(res => {
          if (this.colorLevel) {
            res.forEach(item => {
              item.children
                && item.children.forEach(child => {
                  if (child.priority >= 0 && child.priority <= 10) {
                    child.colorClass = 'color-danger';
                    child.colorTip = this.$t('容量不足');
                  } else if (child.priority > 10 && child.priority <= 30) {
                    child.colorClass = 'color-warning';
                    child.colorTip = this.$t('容量紧张');
                  } else {
                    child.colorClass = 'color-success';
                    child.colorTip = this.$t('容量充足');
                  }
                  if (item.isReadonly) {
                    child.colorTip = this.$t('权限不足');
                  }
                });
            });
          }
          this.clusterList.splice(0);
          this.clusterList = res;
          if (this.clusterList.length) {
            if (!this.value) {
              this.selectedType =                (this.clusterList[0].children
                  && this.clusterList[0].children[0]
                  && this.clusterList[0].children[0]['cluster_name'])
                || '';
            }
          }
        })
        ['finally'](() => {
          if (this.clusterList.length) {
            this.placeholder = this.$t('请选择集群');
          } else {
            this.placeholder = this.$t('暂无数据');
          }
          this.isDisabled = false;
          this.loading = false;
          isEdit && this.$emit('getClusterList', this.clusterList); // 编辑状态，触发tdbank相关内容的回填
        });
    },
    getColorLevelTpl(option) {
      return `<span style="white-space: pre-line;">${option.colorTip || ''}</span>`;
    },
  },
};
</script>

<style lang="scss">
.clusters {
  width: 100%;
  position: relative;
  // display: flex;
  .bkdata-selector-create-item {
    padding: 10px 10px;
    background-color: #fafbfd;
    cursor: pointer;
    .text {
      font-style: normal;
    }
    &:hover {
      background: #cccccc;
    }
  }
  .icon-no-cluster-apply {
    position: absolute;
    top: 50%;
    transform: translate(calc(100% + 10px), -50%);
    right: 0;
  }
}

.select-option-wrapper {
  ::v-deep .color-danger,
  .color-warning,
  .color-success {
    transform: scale(0.84); // 10px;
  }
  ::v-deep .color-danger {
    color: #ea3636;
  }
  ::v-deep .color-warning {
    color: #ff9c01;
  }
  ::v-deep .color-success {
    color: #2dcb56;
  }
}

.vue-tooltip.tooltip-custom {
  z-index: 1000;
}
</style>
