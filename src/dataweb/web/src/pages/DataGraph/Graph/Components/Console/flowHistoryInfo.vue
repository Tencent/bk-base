

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
    <div id="infoLogBody3"
      v-bkloading="{ isLoading: loading }"
      class="info-log__body">
      <div v-if="renderList.length === 0"
        class="no-warning">
        <img src="../../../../../common/images/no-data.png"
          alt="">
        <p>{{ $t('暂无执行历史') }}</p>
      </div>
      <ul v-else
        class="info-log__report">
        <li
          v-for="(row, index) of renderList"
          :key="index"
          class="info-log__report-item"
          :class="{ expanded: row.expanded }">
          <div
            :class="[{ error: row.status === 'failure' }, 'info-log__report-title']"
            @click.stop="toggleHistoryExpand(index)">
            <span class="down-icon"
              :class="{ down: row.expanded }">
              <i class="bk-icon icon-right-shape" />
            </span>
            <span class="title">{{ getHistoryTitle(row) }}</span>
            <span class="Operator">
              - 【{{ operationMap[row.action] }}操作】{{ $t('操作人员') }}: {{ row.created_by }}
            </span>
          </div>
          <div v-for="(log, logIndex) of row.logs"
            v-show="row.expanded"
            :key="logIndex"
            class="expand-body">
            <p class="expand-content"
              :class="{ error: log.level === 'ERROR' }">
              {{ getHistoryLogInfo(log) }}
            </p>
          </div>
          <div v-show="row.expanded"
            v-if="row.logs.length === 0"
            class="expand-body">
            <p class="expand-content">
              {{ $t('暂无日志') }}
            </p>
          </div>
        </li>
      </ul>
    </div>
  </div>
</template>

<script>
import { CONSOLE_ACTIVE_TYPE as CONSOLE } from '../../Config/Console.config.js';
import EditUtil from '@/pages/DataGraph/Common/utils';
export default {
  name: 'flow-history-info',
  data() {
    return {
      loading: false,
      renderList: [],
      map: {
        terminated: this.$t('终止'),
        success: this.$t('成功'),
        finished: this.$t('完成'),
        failure: this.$t('失败'),
        pending: this.$t('等待执行'),
        running: this.$t('执行中'),
      },
      operationMap: {
        start: this.$t('启动'),
        restart: this.$t('重启'),
        stop: this.$t('停止'),
        custom_calculate: this.$t('补算'),
      },
      error: 0,
    };
  },
  methods: {
    getHistoryTitle(row) {
      const progress = this.progressProcess(row.progress);
      return progress === null
        ? `[${row.created_at}] (${this.map[row.status]})`
        : `[${row.created_at}][${progress}%] (${this.map[row.status]})`;
    },
    getHistoryLogInfo(log) {
      const progress = this.progressProcess(log.progress);
      return progress === null ? `[${log.time}] ${log.message}` : `[${log.time}][${progress}%] ${log.message}`;
    },
    progressProcess(progress) {
      if (progress && progress < 10) {
        return `0${progress}`;
      }
      return progress;
    },
    /**
     * 点击历史子项的事件操作
     */
    toggleHistoryExpand(index) {
      let list = this.renderList;
      for (let [_index, item] of list.entries()) {
        if (index === _index) {
          item.expanded = !item.expanded;
        } else {
          item.expanded = false;
        }
      }
    },
    /**
     * 更新历史信息
     */
    async updateHistoryInfo() {
      if (!this.$route.params.fid) return;
      this.loading = true;
      await this.$store
        .dispatch('api/flows/getListDeployInfo', {
          flowId: this.$route.params.fid,
        })
        .then(resp => {
          if (resp.result) {
            let list = [];
            this.error = 0;
            for (let item of resp.data) {
              item.expanded = false;
              list.push(item);
              if (item.status === 'failure') {
                this.error++;
              }
            }
            this.$emit('changeWarningCount', this.error, CONSOLE.HISTORY);
            this.renderList = list;
            // this.statusWatcher(resp.data)
          } else {
            EditUtil.clearMessage();
            this.getMethodWarning(resp.message, resp.code);
          }
        })
        ['finally'](() => {
          this.loading = false;
        });
    },

    statusWatcher(data) {
      const item = data[0] || {};
      if (item.status === 'pending' || item.status === 'running') {
        setTimeout(async () => {
          await this.updateHistoryInfo();
        }, 3000);
      }
    },
  },
};
</script>

<style></style>
