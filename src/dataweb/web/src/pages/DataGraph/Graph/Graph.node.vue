

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
    <bkdata-sideslider
      class="bk-graph-node-container"
      :class="{
        'bksql-edit-expand': !isEditCollapse,
        'full-screen': isFullScreen,
        'node-v2': nodeConfig.node_type === 'batchv2',
      }"
      :isShow.sync="isNodeShowDetail"
      :width="sliderWidth">
      <div slot="header">
        <NodeHeader :nodeName="nodeConfig.node_type"
          :doc="originConfig.doc"
          @headerFullScreenClick="fullScreenHandle">
          <slot name="node-header" />
        </NodeHeader>
      </div>
      <template v-if="nodeConfig.node_type !== 'batchv2'">
        <div slot="content"
          class="bkdata-node-action">
          <component
            :is="activeNodeInstance"
            ref="nodeDetails"
            :nodeDisname="originConfig.node_type_instance_alias"
            :nodeType="originConfig.webNodeType"
            :project="flowData"
            :projectId="flowData.project_id"
            @sqlEditCollapse="handleSqlEditCollapse"
            @add-location="showAddLocationPop"
            @applyData="applyData"
            @applyTdw="applyTdw"
            @routerLeave="handleRouterLeave"
            @nodeDetailMounted="handleNodeDetailMounted"
            @closeSideslider="isNodeShowDetail = false" />
        </div>
        <div slot="footer"
          class="bkdata-node-footer">
          <bkdata-button :loading="isSaving"
            :style="'width: 120px'"
            theme="primary"
            @click="handleBtnSave">
            {{ isSaveNode ? $t('修改') : $t('保存') }}
          </bkdata-button>
          <bkdata-button :disabled="isSaving"
            theme="default"
            :style="'width: 86px'"
            @click="handlebtnClose">
            {{ $t('关闭') }}
          </bkdata-button>
        </div>
      </template>
      <template v-else
        slot="content">
        <component
          :is="activeNodeInstance"
          ref="nodeDetails"
          :nodeDisname="originConfig.node_type_instance_alias"
          description="desc"
          :nodeType="originConfig.webNodeType"
          :project="flowData"
          :projectId="flowData.project_id"
          :isSaving="isSaving"
          @nodeDetailMounted="handleNodeDetailMounted"
          @submit="handleBtnSave"
          @cancel="handlebtnClose" />
      </template>
    </bkdata-sideslider>

    <!-- 确定是否添加结果数据的关联节点start -->
    <bkdata-dialog
      v-model="addLocationPop.isShow"
      :extCls="'start-process dialog-pop bkdata-dialog'"
      :hasFooter="false"
      :title="addLocationPop.title"
      :okText="$t('确定')"
      :cancelText="$t('取消')"
      padding="10px"
      @confirm="addLocation"
      @cancel="addLocationPopCancel">
      <div slot="content">
        <div class="star-content">
          <div v-show="addLocationPop.data.data.has_hdfs"
            class="radio">
            <label class="bk-form-checkbox">
              <input v-model="addLocationPop.locationType"
                name="offline"
                type="checkbox"
                value="offline">
              <i class="bk-checkbox-text">{{ $t('离线计算') }}</i>
            </label>
          </div>
          <div v-show="addLocationPop.data.data.has_kafka"
            class="radio">
            <label class="bk-form-checkbox">
              <input v-model="addLocationPop.locationType"
                name="realtime"
                type="checkbox"
                value="realtime">
              <i class="bk-checkbox-text">{{ $t('实时计算') }}</i>
            </label>
          </div>
        </div>
      </div>
    </bkdata-dialog>
    <!-- 确定是否添加结果数据的关联节点end -->
    <div class="apply-biz-data">
      <apply-biz
        v-if="isApplyBizShow"
        ref="applyBiz"
        :bkBizId="bkBizId"
        @applySuccess="applySuccess"
        @closeApplyBiz="closeApplyBiz" />
      <add-tdw
        v-if="applyType === 'tdw'"
        ref="applyTdw"
        :commitType="commitType"
        :project="flowData"
        :bkBizId="bkBizId"
        :resultTableId="resultTableId"
        @addTdwSuccess="updateList"
        @applySuccess="applySuccess"
        @closeApplyBiz="closeApplyBiz" />
    </div>
  </div>
</template>
<script>
import NodeComponentsManager from './Config/Node.Config';
import NodeHeader from './Children/components/NodeHeader';
import { showMsg } from '@/common/js/util.js';
import { mapState, mapGetters } from 'vuex';
import addTdw from './Components/Dialog/flowAddTdw';
import applyBiz from '@/pages/userCenter/components/applyBizData';
import { fullScreen, exitFullscreen, isFullScreen } from '@/common/js/util.js';

export default {
  name: 'Graph-Node-Info',
  components: {
    NodeHeader,
    addTdw,
    applyBiz,
  },
  provide() {
    const self = this;
    return {
      handleSqlEditCollapse: function (val) {
        self.handleSqlEditCollapse(val);
      },
      handleSliderClose: function () {
        self.handlebtnClose();
      },
    };
  },
  props: {
    nodeConfig: {
      type: Object,
      default: () => ({}),
    },
    originConfig: {
      type: Object,
      default: () => ({}),
    },
    isShow: {
      type: Boolean,
      default: false,
    },
    graphData: {
      type: Object,
      default: () => ({}),
    },
    // 打开详情的标志位，常态为config，关联任务为referTask
    openType: {
      type: String,
      default: 'config',
    },
  },
  data() {
    return {
      // SQL编辑器是否收起
      isEditCollapse: true,

      /**
       * 动态注册节点组件
       * 设置Mounted事件和beforeDestroy事件
       * beforeDestroy事件用于还原Mounted事件中备份的原生Vue事件
       */
      NodeComponentsManager: new NodeComponentsManager([
        {
          callback: this.nodeInstanceMounted,
          fnName: 'mounted',
        },
      ]),

      isSaving: false,
      addLocationPop: {
        // 结果数据的弹窗的相关参数
        isShow: false,
        title: this.$t('是否添加相关节点'),
        locationType: [],
        data: {
          data: {
            has_hdfs: false,
            has_kafka: false,
          },
        },
      },
      applyType: 'biz',
      isApplyBizShow: false,
      bkBizId: 0,
      resultTableId: '',
      configData: [],
      commitType: 'add',
      configCacheTimer: null,
      isFullScreen: false,
    };
  },
  computed: {
    ...mapState({
      flowData: state => state.ide.flowData,
    }),
    isNodeShowDetail: {
      get() {
        return this.isShow;
      },

      set(val) {
        this.$emit('update:isShow', val);
        if (!val) {
          const id = this.nodeConfig.node_id || this.nodeConfig.id;
          this.saveCacheConfig(id).then(() => {
            this.$emit('saveGraphCache');
          });
          clearInterval(this.configCacheTimer);
          this.configCacheTimer = null;
        }
      },
    },
    /** 当前绑定组件节点详情配置项 */
    currentNodeComponentConfig() {
      return this.NodeComponentsManager.getComponentConfig(this.nodeConfig) || {};
    },

    /** 当前节点详情Vue组件实例，动态加载 */
    activeNodeInstance() {
      return (this.isNodeShowDetail && this.currentNodeComponentConfig.component) || null;
    },

    /** 节点详情样式配置信息 */
    activeNodeLayout() {
      return this.currentNodeComponentConfig.layout || {};
    },

    /** 弹框宽度 */
    sliderWidth() {
      return this.activeNodeLayout.width;
    },

    flowId() {
      return this.$route.params.fid;
    },

    // 区分当前节点是保存还是修改
    isSaveNode() {
      return Object.keys(this.nodeConfig).length && this.nodeConfig.hasOwnProperty('node_id');
    },
  },
  mounted() {},
  methods: {
    startCatchTimer(time, id) {
      if (!this.configCacheTimer) {
        this.configCacheTimer = setInterval(() => {
          this.saveCacheConfig(id);
        }, time);
      }
    },
    saveCacheConfig(id) {
      this.$refs.nodeDetails.customValidateForm && this.$refs.nodeDetails.customValidateForm();
      return this.bkRequest.httpRequest('dataFlow/setCatchData', {
        params: {
          key: 'node_' + id,
          content: (this.$refs.nodeDetails && this.$refs.nodeDetails.params.config) || {},
        },
      });
    },
    fullScreenHandle() {
      const ele = document.getElementsByClassName('bk-sideslider-wrapper')[0];
      isFullScreen() ? exitFullscreen() : fullScreen(ele);
      this.isFullScreen = !isFullScreen();
    },
    handleNodeDetailMounted(instance) {
      this.getNodeInfoByActiveId(instance);
    },
    handleRouterLeave(params) {
      this.isNodeShowDetail = false;
      this.$nextTick(() => {
        this.$router.push(params);
      });
    },
    nodeInstanceMounted(instance) {
      instance.$emit('nodeDetailMounted', instance);
    },
    getNodeInfoByActiveId(instance) {
      if (this.nodeConfig['node_id']) {
        this.bkRequest
          .httpRequest('dataFlow/getNodeConfigInfo', {
            params: { fid: this.flowId, node_id: this.nodeConfig['node_id'] },
          })
          .then(resp => {
            if (resp.result) {
              this.setNodeContent(instance, resp.data);
            } else {
              this.isNodeShowDetail = false;
              this.getMethodWarning(resp.message, 'error');
            }
          });
      } else {
        const extObj = {
          frontend_info: {
            x: parseInt(this.nodeConfig.x),
            y: parseInt(this.nodeConfig.y),
          },
        };
        const self = Object.assign({}, this.nodeConfig, extObj);
        this.setNodeContent(instance, self);
      }
    },

    /** 详情页面SQL编辑器展开收起事件 */
    handleSqlEditCollapse(isExpand) {
      this.isEditCollapse = !isExpand;
    },

    /** 回填节点数据 */
    setNodeContent(instance, config) {
      instance = instance || this.$refs.nodeDetails;
      if (!instance) {
        return;
      }
      const { source, fromLink } = this.getClickNodeParentConfig();
      this.startCatchTimer(20000, config.node_id); // 开启定时节点配置缓存
      instance.setConfigBack(config, source, fromLink, { elType: this.openType });
    },

    /** 保存修改 */
    async handleBtnSave() {
      try {
        const validateResult = await this.$refs.nodeDetails.validateFormData();
        if (validateResult) {
          if (this.isSaveNode) {
            this.refreshNode(this.$refs.nodeDetails.params);
          } else {
            this.createNode(this.$refs.nodeDetails.params);
          }
        }
      } catch (error) {
        console.warn(error);
      }
    },

    /** 关闭弹出页 */
    handlebtnClose() {
      this.isNodeShowDetail = false;
    },
    getClickNodeParentConfig() {
      const id = this.nodeConfig.id;
      let parentIds = [];
      let source = [];
      let fromLink = [];
      let curItemConfig = {};

      // 在lines里找到所有父节点的id
      for (let item of this.graphData.lines) {
        if (id === item.target.id) {
          parentIds.push(item.source.id);
          fromLink.push(item);
        }
      }

      for (let item of this.graphData.locations) {
        // 找到当前节点的config
        if (id === item.id) {
          curItemConfig = item;
        }

        // 找到所有父节点的config
        for (let _item of parentIds) {
          if (item.id === _item) {
            source.push(item);
          }
        }
      }

      fromLink.forEach(link => {
        const _source = this.graphData.locations.find(node => String(node.id) === String(link.source.id));
        const _target = this.graphData.locations.find(node => String(node.id) === String(link.target.id));

        _source && Object.assign(link.source, { arrow: 'Left', node_id: _source.node_id || '' }, link.source);
        _target && Object.assign(link.target, { arrow: 'Left', node_id: _target.node_id || '' }, link.target);
        const { source, target } = link;
        link = { source, target };
      });

      source.forEach(item => {
        if (!item.bk_biz_id && item.config && item.config.bk_biz_id) {
          item.bk_biz_id = item.config.bk_biz_id;
        }
      });

      return { source, fromLink };
    },

    /** 创建节点 */
    createNode(params) {
      params.flow_id = this.flowId;
      params.config.origin_id = this.nodeConfig.id;
      params.node_type = this.nodeConfig.type;
      this.isSaving = true;
      this.$store
        .dispatch('api/flows/createNode', { params: params })
        .then(resp => {
          this.answerNodeInfo(resp, params);
        })
        ['finally'](_ => {
          this.isSaving = false;
        });
    },

    /** 修改节点信息 */
    refreshNode(params) {
      this.isSaving = true;
      this.$store
        .dispatch('api/flows/updateNode', { nodeId: this.nodeConfig.node_id, params: params })
        .then(resp => {
          let isRefreshFlag = true;
          this.answerNodeInfo(resp, params, isRefreshFlag);
        })
        ['finally'](() => {
          this.isSaving = false;
        });
    },

    /** 更新节点信息 */
    updateNode(params) {
      this.$store.dispatch('api/flows/updateNode', { nodeId: params.node_id, params }).then(resp => {
        let isRefreshFlag = true;
        this.answerNodeInfo(resp, params, isRefreshFlag);
      });
    },

    /** 处理node创建或更新完成数据 */
    answerNodeInfo(resp, params, isRefreshFlag) {
      if (resp.result) {
        this.$set(resp.data, 'config', Object.assign({}, params.config, resp.data.node_config));
        this.$set(resp.data, 'id', resp.data.id || this.nodeConfig.id || `ch_${resp.data.node_id}`);
        resp.data['is_data_corrected'] = resp.data.node_config ? resp.data.node_config.is_open_correct : false;
        this.$emit('updateNode', resp.data);
        showMsg(this.$t(this.isSaveNode ? '修改成功' : '创建成功'), 'success', { delay: 1500 });
        this.isNodeShowDetail = false;
      } else {
        this.getMethodWarning(resp.message, 'error');
      }
    },

    /**
     * 申请业务数据
     */
    applyData(data, bkBizId, type) {
      this.isApplyBizShow = true;
      this.applyType = 'biz';
      this.bkBizId = bkBizId;
      this.$nextTick(() => {
        this.$refs.applyBiz.show(data, type);
      });
    },

    updateList(config) {
      this.$refs.nodeDetail.getBizsByProjectId(this.flowData.project_id, config);
    },

    /**
     * 新增TDW表
     */
    applyTdw(params) {
      this.isApplyBizShow = true;
      this.applyType = 'tdw';
      this.commitType = params.type;
      this.bkBizId = params.bkBizId;
      this.resultTableId = params.resultTableId;
      const activeDialog = (this.applyType === 'tdw' && 'applyTdw') || 'applyBiz';
      this.$nextTick(() => {
        this.$refs[activeDialog].show(params.data);
      });
    },
    applySuccess() {
      this.$refs.nodeDetails.getAuthRslList
        && this.$refs.nodeDetails.getAuthRslList(this.$refs.nodeDetails.params.config.bk_biz_id);
      this.$refs.nodeDetails.getBizsByProjectId
        && this.$refs.nodeDetails.getBizsByProjectId(this.$refs.nodeDetails.project.project_id);
    },
    showAddLocationPop(data) {
      let location = this.getNodeDataById(this.clickId);
      if (!(location.type === 'rtsource')) {
        return;
      }
      let lines = this.graphData.lines;
      let hasTarget = false;
      let hasKafka = data.data.has_kafka;
      let hasHdfs = data.data.has_hdfs;
      let position = document.getElementsByClassName('#' + location.id).getBoundingClientRect();
      let res = hasHdfs && hasKafka;
      lines.map(line => {
        if (line.source.id === location.id) {
          hasTarget = true;
        }
      });
      // 当hdfs和kafka都可以连接的时候就显示弹窗
      if (res) {
        this.addLocationPop.isShow = true;
        this.addLocationPop.data = data;
        return;
      }
      if (!hasTarget && hasKafka) {
        const nodeConfig = this.getNodeConfigByNodeWebType('realtime');
        let locationOption = {
          iconName: nodeConfig.iconName,
          type: nodeConfig.groupTemplate,
          dataType: 'realtime',
          x: parseFloat(position.left) + 100,
          y: parseFloat(position.top) + 10,
        };
        this.$emit('addReletedLocation', locationOption, location.id);
      } else if (!hasTarget && hasHdfs) {
        const nodeConfig = this.getNodeConfigByNodeWebType('offline');
        let locationOption = {
          iconName: nodeConfig.iconName,
          type: nodeConfig.groupTemplate,
          dataType: 'offline',
          x: parseFloat(position.left) + 150,
          y: parseFloat(position.top) + 10,
        };
        this.$emit('addReletedLocation', locationOption, location.id);
      }
    },
    addLocationPopCancel() {
      this.addLocationPop.isShow = false;
    },
    loadDataFlowCanavas() {
      this.$emit('loadDataFlowCanavas');
    },
    addLocation() {
      let location = this.getNodeDataById(this.clickId);
      let hasKafka = this.addLocationPop.data.data.has_kafka;
      let hasHdfs = this.addLocationPop.data.data.has_hdfs;
      let position = document.getElementsByClassName('#' + location.id).getBoundingClientRect();
      if (this.addLocationPop.locationType.indexOf('realtime') > -1) {
        const nodeConfig = this.getNodeConfigByNodeWebType('realtime');
        let locationOption = {
          iconName: nodeConfig.iconName,
          type: nodeConfig.groupTemplate,
          dataType: 'realtime',
          x: parseFloat(position.left) + 130,
          y: parseFloat(position.top) - 30,
        };
        this.$emit('addReletedLocation', locationOption, location.id);
        this.addLocationPop.isShow = false;
      }
      if (this.addLocationPop.locationType.indexOf('offline') > -1) {
        const nodeConfig = this.getNodeConfigByNodeWebType('offline');
        let locationOption = {
          iconName: nodeConfig.iconName,
          type: nodeConfig.groupTemplate,
          dataType: 'offline',
          x: parseFloat(position.left) + 130,
          y: parseFloat(position.top) + 50,
        };
        this.$emit('addReletedLocation', locationOption, location.id);
        this.addLocationPop.isShow = false;
      }
    },
    closeApplyBiz() {
      this.isApplyBizShow = false;
    },
  },
};
</script>
<style lang="scss">
@import '~@/pages/DataGraph/Scss/graph.node.scss';
</style>
