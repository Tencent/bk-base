

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
  <div class="alert-receiver-config-form">
    <div v-bkloading="{ isLoading: isLoading }"
      class="bk-user-permission">
      <bkdata-tag-input ref="userManager"
        v-model="receivers"
        :placeholder="$t('告警接收人')"
        :hasDeleteIcon="true"
        :list="roleMembers"
        :tpl="tpl"
        @change="changeReceivers"
        @removeAll="clearReceivers" />
    </div>
  </div>
</template>

<script>
export default {
  props: {
    value: {
      type: Array,
      required: true,
    },
    disabled: {
      type: Boolean,
      required: false,
      default: false,
    },
    objectClass: {
      type: String,
      required: false,
    },
    scopeId: {
      type: Boolean,
      required: false,
    },
    roles: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      isLoading: false,
      receivers: [],
      roleLists: [],
    };
  },
  computed: {
    roleMembers() {
      return (this.roleLists || []).map(user => {
        return {
          id: user,
          name: user,
        };
      });
    },
  },
  watch: {
    value: {
      immediate: true,
      handler(value) {
        if (!value.length) return;
        this.receivers = [];
        this.value.forEach(item => {
          this.receivers.push(item.username);
        });
      },
    },
  },
  mounted() {
    this.getProjectMember();
  },
  methods: {
    getProjectMember() {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('meta/getProjectMember')
        .then(res => {
          if (res.result) {
            this.roleLists = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isLoading = false;
        });
    },
    tpl(node, ctx) {
      let parentClass = 'bkdata-selector-node';
      let textClass = 'text';
      let imgClass = 'avatar';
      return (
        <div class={parentClass}>
          <span class={textClass}>{node.name}</span>
        </div>
      );
    },
    changeReceivers(value) {
      let receiverConfig = [];
      for (let username of value) {
        receiverConfig.push({
          receiver_type: 'user',
          username: username,
        });
      }
      for (let receiver of this.value) {
        if (receiver.receiver_type !== 'user') {
          receiverConfig.push(receiver);
        }
      }
      this.$emit('input', receiverConfig);
    },
    clearReceivers(value) {
      this.changeReceivers([]);
    },
  },
};
</script>

<style lang="scss" scoped>
.alert-receiver-config-form {
  display: inline-flex;
  align-items: center;
}

.bk-user-permission {
  box-shadow: none;
  width: 789px;
  min-height: 80px;

  ::v-deep .bk-tag-input {
    width: 600px;
    min-height: 80px;
  }
}
</style>
