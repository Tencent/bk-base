

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
  <div>
    <div v-if="accessOverView.showDetails.includes(details.data_scenario)"
      class="shadows access-overview">
      <div class="type">
        <span>
          {{ $t('接入总览') }}
        </span>
        <div class="total-count fr">
          {{ $t('IP总数') }}
          <span class="count name">
            {{ num.total ? num.total : 0 }}
          </span>
        </div>
      </div>

      <div class="process-section">
        <div @click="filterIplist('success')">
          <data-process :title="titles.success"
            :percent="percent.success"
            :num="num.success"
            :config="config1" />
        </div>

        <div @click="filterIplist('failed')">
          <data-process :title="titles.failed"
            :percent="percent.failed"
            :num="num.failed"
            :config="config2" />
        </div>

        <div @click="filterIplist('running')">
          <data-process :title="titles.running"
            :num="num.running"
            :percent="percent.running"
            :config="config3" />
        </div>

        <div @click="filterIplist('waiting')">
          <data-process :title="titles.waiting"
            :num="num.waiting"
            :percent="percent.waiting"
            :config="config4" />
        </div>
      </div>
    </div>
    <div v-else
      class="shadows access-overview">
      <div class="type">
        <span>
          {{ $t('接入总览') }}
        </span>
      </div>

      <div class="process-section-custom">
        <div class="custom">
          <i class="bk-icon icon-check-1 f20" />
        </div>
        <div>
          {{ $t('接入成功') }}
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import dataProcess from './../dataProcess.vue';
export default {
  components: {
    dataProcess,
  },
  props: {
    details: {
      type: Object,
    },
    num: {
      type: Object,
      default() {
        return {
          success: 0,
          failed: 0,
          running: 0,
          waiting: 0,
        };
      },
    },
    percent: {
      type: Object,
      default() {
        return {
          success: 0,
          failed: 0,
          running: 0,
          waiting: 0,
        };
      },
    },
  },
  data() {
    return {
      accessOverView: this.$store.getters.getAccessOverView,
      validDataScenario: this.$store.getters.getValidDataScenario,
      titles: {
        success: this.$t('成功'),
        failed: this.$t('失败'),
        running: this.$t('接入中'),
        waiting: this.$t('等待中'),
      },
      config1: {
        width: 65,
        height: 65,
        strokeWidth: 5,
        bgColor: '#dde4eb',
        activeColor: '#9dcb6b',
        r: 30,
        index: 1,
      },
      config2: {
        width: 65,
        height: 65,
        strokeWidth: 5,
        bgColor: '#dde4eb',
        activeColor: '#fe771d',
        r: 30,
        index: 2,
      },
      config3: {
        width: 65,
        height: 65,
        strokeWidth: 5,
        bgColor: '#dde4eb',
        activeColor: '#3a84ff',
        r: 30,
        index: 3,
      },
      config4: {
        width: 65,
        height: 65,
        strokeWidth: 5,
        bgColor: '#dde4eb',
        activeColor: '#737987',
        r: 30,
        index: 4,
      },
    };
  },
  methods: {
    filterIplist(data) {
      // this.$emit('filterIplist', data)
    },
  },
};
</script>

<style lang="scss" scoped>
.access-overview {
  height: 163px;
  .count {
    height: 15px;
    line-height: 15px;
    width: 36px;
    margin-left: 15px;
  }
  .process-section {
    display: flex;
    justify-content: space-between;
    padding: 0px 15px;
    .custom {
      width: 60px;
      height: 60px;
      border-radius: 50%;
      border: 5px solid #9dcb6b;
    }
  }
  .process-section-custom {
    text-align: center;
    i {
      display: inline-block;
      font-weight: bold;
      color: #9dcb6b;
      margin-top: 13px;
    }
    .custom {
      width: 60px;
      height: 60px;
      border-radius: 50%;
      border: 5px solid #9dcb6b;
      margin: 0 auto;
    }
  }
  .bk-circle {
    cursor: pointer;
  }
}
</style>
