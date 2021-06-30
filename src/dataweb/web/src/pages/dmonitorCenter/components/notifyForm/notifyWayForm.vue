

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
  <div class="alert-notify-way-config">
    <bkdata-checkbox-group v-model="notifyWays"
      class="alert-notify-way-checkbox-group"
      @change="changeNotifyWay">
      <bkdata-checkbox v-for="notifyWay in notifyWayList"
        :key="notifyWay.notify_way"
        :value="notifyWay.notify_way"
        :disabled="disabled">
        <link rel="icon"
          type="image/png"
          sizes="16x16"
          :href="notifyWay.icon">
        {{ $i18n.locale === 'en' ? notifyWay.notify_way_name : notifyWay.notify_way_alias }}
      </bkdata-checkbox>
    </bkdata-checkbox-group>
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
  },
  data() {
    return {
      notifyConfigDict: {},
      notifyWayList: [],
      notifyWays: [],
    };
  },
  watch: {
    value: {
      immediate: true,
      handler(value) {
        this.notifyWays = JSON.parse(JSON.stringify(value));
      },
    },
  },
  mounted() {
    this.getNotifyWayList();
  },
  methods: {
    changeNotifyWay(data) {
      this.$emit('input', data);
    },
    getNotifyWayList() {
      // this.filterForm.notifyWayHoder = this.$t('数据加载中')
      this.bkRequest.httpRequest('dmonitorCenter/getNotifyWayList').then(res => {
        if (res.result) {
          this.notifyWayList = res.data;
          for (let notifyWay of this.notifyWayList) {
            if (this.value.includes(notifyWay.notify_way)) {
              this.notifyConfigDict[notifyWay.notify_way] = true;
            } else {
              this.notifyConfigDict[notifyWay.notify_way] = false;
            }
          }
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
  },
};
</script>

<style lang="scss">
.alert-notify-way-config {
  display: inline-flex;
  align-items: center;

  .alert-notify-way-checkbox-group {
    display: inline-flex;
    align-items: center;
  }
}
</style>
