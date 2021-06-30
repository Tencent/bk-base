

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
  <bkdata-dialog
    v-model="isShow"
    width="1000"
    :loading="loading"
    extCls="generatorIndicator-config"
    @confirm="$emit('confirm', emitResultTable)"
    @cancel="$emit('confirm', [{ list: '' }])">
    <template slot="header">
      <div class="header-title">
        <span class="h1-title">{{ $t('批量生成指标构建节点') }}</span> -
        <span class="content-title">{{ $t('维度模型应用节点') }}</span>
      </div>
    </template>
    <div v-bkloading="{ isLoading: treeLoading }"
      class="content">
      <div v-if="treeData.length"
        class="content-wrapper">
        <div class="left-panel panel bk-scroll-y">
          <div class="title">
            {{ $t('选择指标') }}
          </div>
          <bkdata-input v-model="filter"
            placeholder="搜索指标名称"
            rightIcon="bk-icon icon-search"
            extCls="mb15" />
          <bkdata-big-tree
            ref="tree"
            :data="treeData"
            :defaultExpandAll="true"
            :showLinkLine="true"
            :showCheckbox="true"
            :options="{ folderKey: 'folder' }"
            extCls="node-generate-tree"
            @check-change="checkHandler"
            @disable-change="disabledHandler">
            <div
              slot-scope="{ node, data }"
              class="node-content-wrapper"
              style="display: flex; justify-content: space-between">
              <div class="name text-overflow">
                <i :class="getIcon(node)" />
                <span>{{ `${data.alias}` }}</span>
                <span>{{ `(${node.name})` }}</span>
              </div>
              <div v-if="data.count"
                class="count">
                {{ data.count }}
              </div>
            </div>
          </bkdata-big-tree>
        </div>
        <div class="right-panel panel bk-scroll-y">
          <div class="title">
            {{ $t('结果预览') }}
          </div>
          <div v-for="(item, index) in resultTable"
            :key="index"
            class="result-form-item mb20">
            <div class="title">
              <i class="icon-down-shape" />
              <span>{{ $t('已选') }}</span>
              <strong class="high-light-number"
                style="color: #3a84ff">
                {{ item.list.length }}
              </strong>
              <span>{{ $t('个') + item.name }}</span>
            </div>
            <div
              v-for="(node, xindex) in item.list"
              :key="xindex"
              :class="['select-item', { disabled: node.disabled }]">
              <span class="node-name">{{ node.name }}</span>
              <i v-if="!node.disabled"
                class="icon-close-line-2 icon"
                @click="removeNode(node.name, item.name)" />
            </div>
          </div>
        </div>
      </div>
      <div v-else
        class="empty-content">
        <EmptyView :tips="$t('当前数据模型暂无指标')">
          <div class="empty-tip">
            <p>{{ $t('您可以选择') }}:</p>
            <p>1.{{ $t('在Dataflow上该节点后拖动模型实例指标构建') }}</p>
            <p>
              2.{{ $t('前往该') }}
              <span class="link"
                @click="linkToModel">
                {{ $t('数据模型') }}
              </span>
              {{ $t('创建指标') }}
            </p>
          </div>
        </EmptyView>
      </div>
    </div>
  </bkdata-dialog>
</template>

<script lang="ts">
import { Component, Prop, Vue, PropSync, Watch, Mixins, Emit } from 'vue-property-decorator';
import EmptyView from '@/components/emptyView/EmptyView.vue';
import emptyImg from '../../../../common/images/empty.png';

interface ITreeData {
  id: string;
  alias: string;
  name: string;
  type: string;
  folder: boolean;
  disabled: boolean;
  count?: number;
  children: ITreeData[];
}

interface ITableConfig {
  name: string;
  list: any[];
}

@Component({
  components: { EmptyView },
})
export default class GeneratorIndicator extends Vue {
  isShow: boolean = false;
  filter: string = '';
  loading: boolean = false;
  nodeId: string | number = '';
  treeLoading: boolean = false;
  treeData: Array<ITreeData> = [];
  streamIndicator: Array<string> = [];
  batchIndicator: Array<string> = [];
  disabledNodes: Array<string> = [];
  expandedNodes: Array<string> = [];
  emptyImg: object = emptyImg;
  modelId: number | null = null;
  projectId: number | null = null;

  resultTable: Array<ITableConfig> = [
    {
      name: this.$t('实时指标').toString(),
      list: [],
    },
    {
      name: this.$t('离线指标').toString(),
      list: [],
    },
  ];

  get emitResultTable() {
    return this.resultTable.map(item => {
      const table = JSON.parse(JSON.stringify(item));
      table.list = table.list.filter((item: any) => !item.disabled).map((item: any) => item.name);
      return table;
    });
  }

  @Watch('isShow')
  onShowChanged(val: boolean) {
    if (!val) {
      this.resultTable.forEach(item => {
        item.list = [];
      });
    }
  }

  linkToModel() {
    const url = this.$router.resolve({
      name: 'dataModelView',
      params: {
        modelId: this.modelId?.toString() ?? '',
      },
      query: {
        project_id: this.projectId?.toString() ?? '',
      },
    });
    window.open(url.href, '_blank');
  }

  initTreeData(nodeId: string) {
    this.nodeId = nodeId;
    this.isShow = true;
    this.treeLoading = true;
    this.bkRequest
      .httpRequest('dataFlow/indicatorMenuTree', {
        query: {
          node_id: nodeId,
        },
      })
      .then(res => {
        if (res.result) {
          this.treeData = Array.isArray(res.data.tree) ? res.data.tree : [res.data.tree];
          this.disabledNodes = this.expandedNodes = res.data.selected_nodes;
          this.projectId = res.data.project_id;
          this.modelId = res.data.model_id;
          this.$nextTick(() => {
            res.data.selected_nodes.forEach((item: any) => {
              this.$refs.tree.setChecked(item, { checked: true, emitEvent: true });
              setTimeout(() => {
                this.$refs.tree.setDisabled(item, { disabled: true, emitEvent: true });
              }, 500);
            });
          });
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      })
      ['finally'](() => {
        this.treeLoading = false;
      });
  }

  removeNode(node: string, group: string) {
    const resultGroup = this.resultTable.find(item => item.name === group);
    const nodeIndex = resultGroup?.list.findIndex(item => item.name === node);

    resultGroup?.list.splice(nodeIndex ?? 0, 1);
    this.$refs.tree.setChecked(node, { checked: false });
  }

  disabledHandler(treeNodes: any) {
    const group = this.resultTable.find(table => {
      const node = table.list.find(item => item.name === treeNodes.id);
      if (node) {
        node.disabled = true;
        return true;
      }
      return false;
    });
  }

  checkHandler(ids: Array<string>, checked: boolean) {
    console.log(ids);
    this.resultTable.forEach(table => (table.list = []));
    const nodes = ids.map(id => {
      return this.flatTreeData.find(item => {
        if (item.id === id && item.type.includes('indicator')) {
          const index = item.type.includes('batch') ? 1 : 0;
          this.resultTable[index].list.push({ name: id, disabled: this.disabledNodes.includes(id) });
          return true;
        }
        return false;
      });
    });
  }

  getIcon(node: any) {
    const type = node.data.type;
    if (type === 'batch_indicator') {
      return 'icon-offline-indicators';
    } else if (type === 'stream_indicator') {
      return 'icon-realtime-indicators';
    } else if (type === 'calculation_atom') {
      return 'icon-statistic-caliber';
    } else {
      return 'icon-fact-model';
    }
  }

  get flatTreeData(): Array<ITreeData> {
    let stack: any[] = [];
    let result: any[] = [];
    stack = stack.concat(this.treeData);
    while (stack.length > 0) {
      var node = stack.pop();
      result.push(node);
      if (node.children) {
        stack = stack.concat(node.children.reverse());
      }
    }
    return result;
  }
}
</script>

<style lang="scss" scoped>
.generatorIndicator-config {
  .header-title {
    color: #313237;
    font-size: 16px;
    line-height: 26px;
    text-align: left;
    .h1-title {
      font-size: 20px;
    }
  }
  .content {
    height: 505px;
    width: 100%;
    display: flex;
    .empty-content {
      width: 100%;
      .empty-tip {
        text-align: left;
        width: 292px;
        margin: 0 auto;
        color: #979ba5;
        margin-top: 20px;
        line-height: 24px;
        .link {
          color: #3a84ff;
          cursor: pointer;
        }
      }
    }
    .content-wrapper {
      width: 100%;
      display: flex;
    }
    .left-panel {
      flex: 1 1 468px;
    }
    .right-panel {
      flex: 1 1 532px;
      background-color: #f0f1f5;
    }
    .panel {
      padding: 20px 24px;
      .title {
        font-size: 16px;
        margin-bottom: 16px;
      }
      .result-form-item {
        .select-item {
          width: 100%;
          display: flex;
          align-items: center;
          .node-name {
            display: inline-block;
            width: 458px;
            height: 32px;
            background-color: #fff;
            border-radius: 2px;
            box-shadow: 0px 1px 2px 0px;
            padding: 0 18px;
            line-height: 32px;
            margin-bottom: 4px;
            margin-right: 6px;
          }
          .icon {
            font-size: 16px;
            color: #c4c6cc;
            cursor: pointer;
          }
        }
      }
    }
    ::v-deep .node-generate-tree {
      .node-content-wrapper {
        display: flex;
        justify-content: space-between;
        align-items: center;
        .name {
          width: 350px;
        }
        .count {
          background-color: rgb(240, 241, 245);
          color: rgb(151, 155, 165);
          width: 20px;
          height: 16px;
          font-size: 12px;
          border-radius: 2px;
          line-height: 16px;
          text-align: center;
        }
      }
    }
  }
  ::v-deep .bk-dialog-body {
    border-top: 1px solid #dcdee5;
  }
}

::v-deep .bk-dialog {
  .bk-dialog-header {
    transform: translateY(-13px);
    padding-bottom: 0;
  }
  .bk-dialog-body {
    border-top: 1px solid #dcdee5;
    padding: 0;
  }
}
</style>
