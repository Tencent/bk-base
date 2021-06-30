

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
  <div class="f12">
    <div id="running-body"
      v-bkloading="{ isLoading: loading }"
      class="info-log__body">
      <div v-if="infoList.length <= 0"
        class="no-warning">
        <img src="../../../../../common/images/no-data.png"
          alt="">
        <p>{{ $t('暂无运行信息') }}</p>
      </div>
      <ul v-else
        class="info-log__report">
        <li
          v-for="(item, index) of infoList"
          :key="index"
          class="info-log__report-item"
          :class="item.level"
          @click="expandDebugDetail($event)">
          <div class="info-log__report-title"
            :class="item.status">
            <span class="title">
              {{ getRunningInfoTitle(item) }}
            </span>
          </div>
        </li>
      </ul>
    </div>
  </div>
</template>

<script>
import $ from 'jquery';
import { CONSOLE_ACTIVE_TYPE as CONSOLE } from '../../Config/Console.config.js';
import { mapState } from 'vuex';
export default {
  name: 'flow-running-info',
  data() {
    return {
      loading: false,
    };
  },
  computed: {
    ...mapState({
      infoList(state) {
        this.loading = true;
        let error = 0;
        let temp = state.ide.runningInfo.infoList;
        for (let item of temp) {
          let level = item.level.toLowerCase();
          if (level === 'error') {
            error += 1;
          }
        }
        this.$emit('changeWarningCount', error, CONSOLE.RUNNING);
        this.$nextTick(() => {
          this.loading = false;
          this.scrollToButtom();
        });
        return temp;
      },
    }),
  },

  methods: {
    progressProcess(progress) {
      if (progress && progress < 10) {
        return `0${progress}`;
      }
      return progress;
    },
    getRunningInfoTitle(log) {
      const progress = this.progressProcess(log.progress);
      return progress === null ? `[${log.time}] ${log.message}` : `[${log.time}][${progress}%] ${log.message}`;
    },
    expandDebugDetail(e) {
      $(e.target)
        .closest('.info-log__report-item')
        .find('.title')
        .toggleClass('toggleExpand');
    },
    scrollToButtom() {
      this.$nextTick(() => {
        let infoLogBody = this.$el.querySelector('#running-body');
        infoLogBody.scrollTop = 200000;
      });
    },
  },
};
</script>

<style></style>
