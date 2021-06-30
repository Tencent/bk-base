

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
  <bkdata-select
    v-model="myZoneID"
    v-tooltip.notrigger="{ content: validate.content, visible: validate.visible, class: validate.class }"
    searchable
    :placeholder="placeholder"
    :loading="loading"
    style="width: 100%"
    @selected="zoneChangeHandle"
    @change="validator">
    <bkdata-option v-for="(item, index) in zoneList"
      :id="item.zone_id"
      :key="index"
      :name="item.zone_name" />
  </bkdata-select>
</template>

<script>
export default {
  props: {
    clusterName: {
      type: String,
      default: '',
    },
    zoneID: {
      type: [String, Number],
      default: '',
    },
  },
  data() {
    return {
      myZoneID: '',
      zoneList: [],
      loading: false,
      validate: {
        content: window.$t('不能为空'),
        visible: false,
        class: 'error-red ide-node-edit',
      },
    };
  },
  computed: {
    placeholder() {
      return this.clusterName ? this.$t('请选择') : this.$t('请先选择集群');
    },
  },
  watch: {
    clusterName: {
      immediate: true,
      handler(val) {
        this.getGameZoneList(val);
      },
    },
    zoneID(val) {
      this.myZoneID = val;
    },
  },
  methods: {
    getGameZoneList(clusterName) {
      this.loading = true;
      this.bkRequest
        .httpRequest('dataAccess/getGameZoneList', {
          query: {
            cluster_name: clusterName,
          },
        })
        .then(res => {
          if (res.result) {
            this.zoneList = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading = false;
        });
    },
    zoneChangeHandle(val, option) {
      const zone = this.zoneList.find(item => item.zone_id === val);
      this.$emit('update: zoneID', val);
      this.$emit('zoneselect', zone);
    },
    validator() {
      this.validate.visible = !this.myZoneID;
      return !this.validate.visible;
    },
  },
};
</script>
