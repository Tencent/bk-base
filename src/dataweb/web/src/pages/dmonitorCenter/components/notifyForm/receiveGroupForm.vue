

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
  <div class="alert-receiver-group-form">
    <bkdata-selector :multiple="true"
      :searchable="true"
      :loading="isLoading"
      :displayKey="'receive_group_alias'"
      :allowClear="true"
      :settingKey="'receive_group_id'"
      :list="receiveGroupList"
      :placeholder="$t('告警接收群组')"
      :selected.sync="receiveGroups"
      style="width: 240px" />
  </div>
</template>

<script>
export default {
  props: {
    value: {
      type: Array,
      required: true,
    },
  },
  data() {
    return {
      isLoading: false,
      receiveGroupList: [],
      receiveGroups: [],
    };
  },
  watch: {
    receiveGroups: {
      handler(value) {
        let receiveGroupConfig = [];
        for (let receive_group_id of value) {
          receiveGroupConfig.push({
            receiver_type: 'group',
            receive_group_id: receive_group_id,
          });
        }
        for (let receiver of this.value) {
          if (receiver.receiver_type !== 'group') {
            receiveGroupConfig.push(receiver);
          }
        }
        this.$emit('input', receiveGroupConfig);
      },
    },
  },
  mounted() {
    this.getReceiveGroupList();
    this.value.forEach(receiver => {
      if (receiver.receiver_type === 'group') {
        this.receivers.push(receiver.receive_group_id);
      }
    });
  },
  methods: {
    getReceiveGroupList() {
      this.isLoading = true;
      this.bkRequest.httpRequest('dmonitorCenter/getReceiveGroupList').then(res => {
        if (res.result) {
          this.receiveGroupList = res.data;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.isLoading = false;
      });
    },
  },
};
</script>

<style lang="scss">
.alert-receiver-group-form {
  display: inline-flex;
  align-items: center;
}
</style>
