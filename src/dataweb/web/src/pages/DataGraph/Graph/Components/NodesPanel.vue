

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
  <bkdata-collapse v-model="calcActiveName">
    <template v-for="group in searchedNodes">
      <bkdata-collapse-item :key="group.group_type_name"
        :name="group.group_type_name">
        <div class="group-header">
          <i class="bk-icon"
            :class="[group.groupIcon]" /><label>{{ group.group_type_alias }}</label>
        </div>
        <div slot="content">
          <div v-if="group['isExpand']"
            class="data-type"
            :class="{ 'toolbar-expand': group['isExpand'] }">
            <template v-for="instance in group.instances">
              <span
                :key="instance.node_type_instance_name"
                class="bk-primary data-item tool"
                :class="{ disabled: !isNodeDragable, [listType]: true }"
                :data-type="instance.webNodeType"
                :data-tmpl-group="group.groupTemplate"
                :data-tmpl-icon="instance.iconName"
                :data-alias="instance.node_type_instance_alias"
                :draggable="isNodeDragable"
                @mouseover.stop="showTips($event, instance)"
                @mouseleave.stop="fireExistInstance"
                @dragstart="handleDragstart">
                <span class="data-item-icon">
                  <i :class="['bk-icon', instance.iconName]" />
                </span>
                <span class="data-item-name">{{ instance.node_type_instance_alias }}</span>
              </span>
            </template>
          </div>
        </div>
      </bkdata-collapse-item>
    </template>
  </bkdata-collapse>
</template>

<script>
import { mapState, mapGetters } from 'vuex';
import tippy from 'tippy.js';
export default {
  name: 'Nodes-Panel',
  props: {
    listType: {
      type: String,
      default: 'table',
    },
    searchValue: {
      type: String,
      default: '',
    },
  },
  data() {
    // Round square rectangle
    return {
      activeName: [],
      tippyInstance: [],
      tippyTimer: 0,
    };
  },
  computed: {
    ...mapState({
      flowNodeConfig: state => state.ide.flowNodeConfig,
    }),
    ...mapGetters({
      beingDebug: 'ide/isBeingDebug',
    }),
    isNodeDragable() {
      return this.$route.params.fid && !this.beingDebug;
    },
    formatNodeConfig() {
      return this.flowNodeConfig
        .map((conf, index) => {
          this.$set(conf, 'isExpand', true);
          return conf;
        })
        .sort((a, b) => a.order - b.order);
    },
    searchedNodes() {
      const copyNodes = JSON.parse(JSON.stringify(this.formatNodeConfig));
      return copyNodes
        .map(group => {
          group.instances = group.instances
            .sort((a, b) => a.order - b.order)
            .filter(
              instance => !instance.disable
                            && new RegExp(this.searchValue, 'ig').test(instance.node_type_instance_alias)
            );
          return group;
        })
        .filter(group => group.instances.length !== 0);
    },
    calcActiveName: {
      get() {
        return (
          (this.searchValue !== '' && this.formatNodeConfig.map(node => node.group_type_name))
                      || (this.activeName && this.activeName.length && this.activeName)
                      || [(this.formatNodeConfig[0] || {}).group_type_name]
        );
      },

      set(val) {
        this.activeName = val;
      },
    },
  },
  methods: {
    showTips(e, nodeInstance) {
      this.fireExistInstance();
      const targetEl = e.target.closest('span.data-item');
      const instance = tippy(targetEl, {
        delay: [500, 0],
        trigger: 'manual',
        content: this.generateTipsString(nodeInstance),
        arrow: true,
        placement: 'right',
        multiple: true,
        maxWidth: 300,
        boundary: 'window',
      });
      this.tippyTimer = setTimeout(() => {
        instance.show();
        this.tippyInstance.push(instance);
      }, 500);
    },
    fireExistInstance() {
      this.tippyTimer && clearTimeout(this.tippyTimer);
      this.tippyTimer = 0;
      while (this.tippyInstance.length) {
        if (this.tippyInstance[0]) {
          this.tippyInstance[0].hide();
          this.tippyInstance[0].destroy();
        }
        this.tippyInstance.shift();
      }
    },
    handleDragstart(event) {
      event.dataTransfer.setData('text/plain', null);
      event.dataTransfer.dropEffect = 'copy';
      const { tmplGroup, type, tmplIcon } = event.target.dataset;
      event.dataTransfer.setData('data-node-type', type);
      event.dataTransfer.setData('data-node-group', tmplGroup);
      event.dataTransfer.setData('data-node-icon', tmplIcon);
    },
    handleItemClick(item, index) {
      item.isExpand = !item.isExpand;
    },
    getIconNameFirstChar(name) {
      return name.substring(0, 1).toUpperCase() + name.substring(1, 2).toLocaleLowerCase();
    },

    /**
     * 生成tips的文字信息
     * @param {*} type
     */
    generateTipsString(nodeInstance) {
      return (
        (nodeInstance
          && `<div>${
            nodeInstance.node_type_instance_alias
          }</div><div style="white-space: pre-line;">${nodeInstance.description
            || nodeInstance.node_type_instance_alias}</div><div style="font-weight: bold">${
            nodeInstance.node_type_instance_name === 'kv_source' ? '建议使用新的「关联数据源」' : ''
          }</div>`)
        || ''
      );
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .bk-collapse-item-content {
  border-top: solid 1px #dcdee5;
}

::v-deep .bk-collapse-item-header {
  height: 35px;
  line-height: 35px;
}
.group-header {
  padding: 0 10px;
  margin: 0 -10px;
  border-top: solid 1px #dcdee5;
  background: #fafbfd;
  label {
    margin-left: 15px;
    cursor: pointer;
  }
}
.data-type {
  display: flex;
  flex-wrap: wrap;
  margin: 10px 0;

  span.data-item {
    width: 33.3%;
    position: relative;
    padding: 10px;
    border-radius: 2px;
    text-align: center;
    color: #63656e;
    cursor: pointer;
    &.disabled {
      cursor: no-drop;
      &:hover {
        background: #ddd;
      }
    }

    &:hover {
      background: #f0f1f5;
      color: #3a84ff;
      cursor: pointer;
    }

    &:focus {
      outline: none;
    }

    &.list {
      width: 100%;
      display: flex;
      align-items: center;
      .data-item-icon {
        width: 35px;
        padding-top: 0;

        .bk-icon {
          font-size: 24px;
        }
      }

      .data-item-name {
        // font-size: 14px;
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
        font-size: 12px;
        pointer-events: none;
      }
    }
    .data-item-icon {
      width: 100%;
      padding-top: 55%;
      display: inline-block;
      position: relative;
      pointer-events: none;

      .bk-icon {
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        font-size: 30px;
      }
    }

    .data-item-name {
      white-space: pre-line;
      word-break: break-word;
      &:focus {
        outline: none;
      }
    }
  }

  margin-bottom: 15px;
}
</style>
