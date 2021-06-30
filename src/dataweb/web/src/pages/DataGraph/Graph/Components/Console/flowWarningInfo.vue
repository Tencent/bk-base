

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
    <div class="filter-box">
      <div class="monitor-select">
        <bkdata-selector
          :displayKey="'name'"
          :allowClear="true"
          :list="alertLevelList"
          :placeholder="$t('告警级别')"
          :selected.sync="params.selectedAlertLevel"
          :settingKey="'key'" />
      </div>
      <div class="identification-input">
        <bkdata-input v-model="params.searchContent"
          :placeholder="$t('告警内容_回车直接检索')"
          type="text" />
      </div>
    </div>
    <div id="infoLogBody"
      v-bkloading="{ isLoading: loading }"
      class="info-log__body">
      <div v-if="warningList.length === 0"
        class="no-warning">
        <img src="../../../../../common/images/no-data.png"
          alt="">
        <p>{{ $t('暂无告警信息') }}</p>
      </div>
      <ul v-else
        class="info-log__report">
        <li
          v-for="(item, index) of warningShowList"
          :key="index"
          class="info-log__report-item"
          :class="[{ checked: item.checked }]"
          @click="nodeFocus(item)">
          <div class="info-log__report-title"
            :class="[item.level]"
            @click="focusWarningNode(item.node_id, item)">
            <span
              class="nodes-status bk-icon icon-exclamation-triangle-shape"
              :style="{ color: warningStatus[item.alert_level] || '#737987 ' }"
              :class="[item.level]" />
            <span class="title">
              [{{ item.time }}] {{ item.msg }}
              {{ item.alert_status == 'recovered' ? '(' + $t('恢复') + ')' : '' }}
            </span>
          </div>
        </li>
      </ul>
    </div>
  </div>
</template>

<script>
import { mapGetters } from 'vuex';
import { CONSOLE_ACTIVE_TYPE as CONSOLE } from '../../Config/Console.config.js';
import Bus from '@/common/js/bus';

export default {
  name: 'flow-warning-info',
  inject: ['activeNodeById'],
  data() {
    return {
      warningList: [],
      warningShowList: [],
      loading: false,
      warningStatus: {
        danger: '#ea3636',
        warning: '#ff9c01',
      },
      warningNodes: {},
      params: {
        searchContent: '',
        selectedAlertLevel: '',
      },
      alertLevelList: [
        {
          name: this.$t('警告'),
          key: 'warning',
        },
        {
          name: this.$t('严重'),
          key: 'danger',
        },
      ],
    };
  },
  computed: {
    ...mapGetters({
      getNodeDataByNodeId: 'ide/getNodeDataByNodeId',
    }),
    graphData() {
      return this.$store.state.ide.graphData;
    },
  },
  watch: {
    'params.selectedAlertLevel'() {
      this.getFilterResultList();
    },
    'params.searchContent'() {
      this.getFilterResultList();
    },
  },
  mounted() {
    // 告警信息只在拿到节点数据后获取一次
    Bus.$on('updateWarningInfo', graphData => {
      if (graphData.locations.length) {
        this.updateWarningInfo();
      }
    });
  },
  beforeDestroy() {
    Bus.$off('updateWarningInfo');
  },
  methods: {
    getFilterResultList() {
      // 存在关键字过滤时需进行关键字过滤，反之则跳过这项过滤
      const contentFilterCondition = item => this.params.searchContent
        ? item.full_message.includes(this.params.searchContent)
        : true;
      // 存在告警级别过滤时需进行类型过滤，反之则跳过这项过滤
      const leaveFilterCondition = item => this.params.selectedAlertLevel
        ? item.alert_level === this.params.selectedAlertLevel
        : true;

      this.warningShowList = this.warningList.filter(
        item => contentFilterCondition(item) && leaveFilterCondition(item)
      );
    },
    getNodeAlertCout() {
      this.warningNodes = {};
      this.warningList.forEach(item => {
        if (this.warningNodes.hasOwnProperty(item.node_id)) {
          this.warningNodes[item.node_id]++;
        } else {
          this.warningNodes[item.node_id] = 1;
        }
      });
      // 这里拿到所有节点的告警数量之后，需要更新画布的数据，然后把节点数量展示在画布节点的上方
      if (Object.keys(this.warningNodes).length) {
        const graphData = JSON.parse(JSON.stringify(this.graphData));
        graphData.locations
          && graphData.locations.forEach(item => {
            if (Object.keys(this.warningNodes).includes(String(item.node_id))) {
              item.alertCount = this.warningNodes[item.node_id];
            }
          });
        this.$store.commit('ide/setGraphData', graphData);
        // 在Graph.index.vue组件里监听事件，然后重新渲染画布
        Bus.$emit('updateALertCountGraph');
      }
    },
    addDomId() {
      this.warningList.forEach(item => {
        if (this.getNodeDataByNodeId(item.node_id)) {
          item.domId = this.getNodeDataByNodeId(item.node_id).id;
        }
      });
    },
    nodeFocus(item) {
      this.activeNodeById(item.domId);
    },
    // 点击告警信息突出显示节点
    focusWarningNode(nodeId, item) {
      let node = this.getNodeDataByNodeId(nodeId);
      let id = node.id;
      let checked = false;
      this.warningList.forEach((warning, i) => {
        if (item === warning) {
          item.checked = !item.checked;
        } else {
          warning.checked = false;
        }
      });
      this.warningList.forEach(warning => {
        if (warning.checked) {
          checked = true;
        }
      });
      if (checked) {
        for (let item of document.querySelectorAll('.focus-status')) {
          item.classList.remove('focus-status');
        }
        document.getElementById(id).classList.add('focus-status');
      } else {
        document.getElementById(id).classList.remove('focus-status');
      }
    },

    getAlertConfigByFlowID() {
      const params = {
        flow_id: this.$route.params.fid,
      };
      return this.bkRequest.httpRequest('dmonitorCenter/getAlertConfigByFlowID', { params }).then(res => {
        if (res && res.result) {
          return res.data.id;
        } else {
          res && this.getMethodWarning(res.message, res.code);
        }
      });
    },
    /**
     * 更新警告信息
     */
    async updateWarningInfo() {
      if (!this.$route.params.fid) return;
      this.warningList = [];
      this.loading = true;
      const id = await this.getAlertConfigByFlowID();
      this.$store
        .dispatch('api/flows/getListAlert', {
          flowId: this.$route.params.fid,
          alert_config_id: id,
        })
        .then(resp => {
          if (resp.result) {
            for (let item of resp.data) {
              item.checked = false;
              this.warningList.push(item);
            }
            this.warningShowList = this.warningList;
            this.$emit('changeWarningCount', this.warningList.length, CONSOLE.WARNING);
            if (!this.warningList.length) return;
            this.addDomId();
            this.getNodeAlertCout();

            Bus.$emit('dataFlowAlertCount', this.warningList.length);
          } else {
            this.getMethodWarning(resp.message, resp.code);
          }
        })
        ['finally'](() => {
          this.loading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.nodes-status {
  font-size: 16px;
}
.filter-box {
  display: flex;
  position: relative;
  z-index: 999;
  padding: 8px;
  background-color: white;
  border-bottom: 1px solid #eaeaea;
  .monitor-select {
    width: 180px;
    margin-right: 10px;
  }
  .identification-input {
    width: 180px;
    .bk-form-input {
      display: block;
    }
  }
}
</style>
