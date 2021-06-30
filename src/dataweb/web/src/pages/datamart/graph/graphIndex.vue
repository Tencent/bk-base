

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
  <div id="canvas-flow" />
</template>
<script>
import { postMethodWarning } from '@/common/js/util.js';
import FlowNodeToolKit from './newGraphToolKit.js';
import nodeAttrMethods from './nodeAttr';
import D3Graph from '@blueking/bkflow.js';
import Bus from '@/common/js/bus';
import tempSelect from './template';
import tippy from 'tippy.js';
import { mapState } from 'vuex';
// import repData from './mock.js'

let canvas = null;
let graphData = null;
export default {
  name: 'bk-graph',
  props: {
    flowId: {
      type: [String, Number],
      default: '',
    },
    isSwitchChinese: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      totalNodes: 0,
      nodeCounter: 0,
      editable: true,
      linkRulesConfig: {},
      toolOption: {
        position: 'right',
        direction: 'column',
      },
      tools: [
        {
          type: 'zoomIn',
          text: '放大',
          cls: 'tool-item',
          /** 显示类型
           * text: 文本显示
           * icon：icon显示，需要传递icon
           * full: icon + 文本
           */
          showType: 'icon',
          icon: 'bk-icon icon-plus-circle',
        },
        {
          type: 'zoomOut',
          text: '缩小',
          cls: 'tool-item',
          showType: 'icon',
          icon: 'bk-icon icon-minus-circle',
        },
        {
          type: 'resetPosition',
          text: '重置',
          cls: 'tool-item',
          showType: 'icon',
          icon: 'bk-icon icon-restart',
        },
      ],
      graphLocaData: null,
      timer: null,
      isFullScreen: false,
    };
  },
  computed: {
    ...mapState({
      graphData: state => state.ide.graphData,
    }),
    calcData: {
      get() {
        return {
          nodes: this.graphData.locations || [],
          lines: this.graphData.lines || [],
        };
      },

      set(val) {
        this.$store.commit('ide/setGraphData', val);
      },
    },
  },
  watch: {
    nodeCounter(value) {
      if (value !== 0 && value === this.totalNodes) {
        this.$forceUpdate();
        this.$refs.jsFlow.renderData();
        Bus.$emit('bloodLoading', false);
      }
    },
    isSwitchChinese(val) {
      window.bloodToolTips.isSwitchChinese = val;
      canvas && canvas.renderGraph(graphData);
    },
  },
  mounted() {
    // 注册接受是否全屏事件
    Bus.$on('isFullScreen', status => {
      this.isFullScreen = status;
    });
    const that = this;
    window.bloodToolTips = {
      toolTipsClearTimer: null,
      tippyInstance: [],
      nodeData: null,
      isSwitchChinese: false,
      linkToDictionary: function (nodeData) {
        let str = '';
        let query = {};
        if (window.bloodToolTips.nodeData.type === 'result_table') {
          str = that.$t('离开此页面_将跳转到对应结果表详情');
          query = {
            dataType: 'result_table',
            result_table_id: window.bloodToolTips.nodeData.name,
            bk_biz_id: window.bloodToolTips.nodeData.bk_biz_id,
          };
        } else if (window.bloodToolTips.nodeData.type === 'raw_data') {
          str = that.$t('离开此页面_将跳转到对应数据源详情');
          query = {
            dataType: 'raw_data',
            data_id: window.bloodToolTips.nodeData.name,
            bk_biz_id: window.bloodToolTips.nodeData.bk_biz_id,
          };
        } else {
          return;
        }
        let res = confirm(window.gVue.$t(str));
        if (!res) return;
        that.$router.push({
          name: 'DataDetail',
          query,
        });
        Bus.$emit('reloadDataDetail', query);
      },
    };
    canvas = new D3Graph('#canvas-flow', {
      onNodeRender: node => tempSelect(node).temp,
      renderVisibleArea: true,
      background: 'white',
      zoom: {
        scaleExtent: [0.5, 5],
        controlPanel: true,
        tools: this.tools,
      },
      lineConfig: {
        canvasLine: true, // 使用Canvas增强性能
        color: '#ddd',
      },
      nodeConfig: [
        { type: 'default', group: 'default', width: 168, height: 44, radius: '4px', background: 'white' }
      ],
    })
      .on('nodeMouseEnter', function (param, event) {
        canvas.activeCanvasLines({ id: param.id, color: '#3a84ff' });
        window.bloodToolTips.nodeData = param;
        while (window.bloodToolTips.tippyInstance.length) {
          if (window.bloodToolTips.tippyInstance[0]) {
            window.bloodToolTips.tippyInstance[0].hide();
            window.bloodToolTips.tippyInstance[0].destroy();
          }
          window.bloodToolTips.tippyInstance.shift();
        }
        const options = {
          trigger: 'mouseenter',
          content: tempSelect(param).toolTipsContent,
          arrow: true,
          placement: 'top',
          interactive: true,
          multiple: true,
          appendTo: () => document.body,
        };
        if (that.isFullScreen) {
          options.appendTo = () => document.getElementById('canvas-flow');
        }
        const instance = tippy(event.target, options);
        instance.show(100);
        window.bloodToolTips.tippyInstance.push(instance);
      })
      .on('nodeMouseLeave', function (param) {
        canvas.activeCanvasLines();
      });
    this.getGraphData();
    // 鼠标滚轮缩放事件
    // document.getElementsByClassName('canvas-flow-wrap')[0].addEventListener('mousewheel', e => {
    //     if (this.timer) return
    //     this.timer = setTimeout(() => {
    //         this.changeZoom(e)
    //         this.timer = null
    //     }, 200)
    // })
  },
  beforeDestroy() {
    clearTimeout(this.timer);
    delete window.bloodToolTips;
    canvas && canvas.destroy();
    canvas = null;
    graphData = null;
  },
  methods: {
    getGraphData(flowId, isFilterData = false) {
      let id;
      if (this.$route.query.result_table_id) {
        id = this.$route.query.result_table_id;
      } else if (this.$route.query.data_id) {
        id = this.$route.query.data_id;
      } else if (this.$route.query.tdw_table_id) {
        id = this.$route.query.tdw_table_id;
      }
      const options = {
        query: {
          type: this.$route.query.dataType,
          qualified_name: id,
        },
      };
      if (isFilterData) {
        options.query.is_filter_dp = 1;
      }
      this.bkRequest.httpRequest('dataDict/getlineageSubGraghData', options).then(resp => {
        if (resp.result) {
          // resp = repData
          this.graphLocaData = resp.data;
          nodeAttrMethods.methods.label_plus_minus(resp.data.locations);
          this.totalNodes = resp.data.locations.length;
          if (!this.totalNodes) {
            let message = '';
            if (this.$route.query.dataType === 'result_table') {
              message = this.$t('该结果表没有血缘信息');
            } else if (this.$route.query.dataType === 'raw_data') {
              message = this.$t('该数据源没有血缘信息');
            } else if (this.$route.query.dataType === 'tdw_table') {
              message = this.$t('该TDW表没有血缘信息');
            }
            postMethodWarning(message, 'warning');

            return null;
          }
          this.processData(this.graphLocaData);
          if (!isFilterData) return;
          // })
        } else {
          this.getMethodWarning(resp.message, resp.code);
        }
        Bus.$emit('bloodLoading', false);
      });
    },
    processData(data) {
      let id;
      if (this.$route.query.result_table_id) {
        id = this.$route.query.result_table_id;
      } else {
        id = this.$route.query.data_id;
      }
      let currentId;
      if (this.$route.query.dataType === 'raw_data') {
        currentId = this.$route.query.data_id;
      } else if (this.$route.query.dataType === 'result_table') {
        currentId = this.$route.query.result_table_id;
      }
      let toolkit = new FlowNodeToolKit(data, `${this.$route.query.dataType}_${currentId}`);
      graphData = toolkit.updateFlowGraph(id);
      canvas.renderGraph(graphData);
      Bus.$emit('bloodLoading', false);
    },
    changeGraphParam(flag) {
      Bus.$emit('bloodLoading', true);
      if (flag) {
        this.getGraphData(undefined, true);
      } else {
        this.getGraphData();
      }
    },
    changeZoom(e) {
      if (e.wheelDeltaY < 0) {
        this.$refs.jsFlow.zoomIn(1.1);
      } else {
        this.$refs.jsFlow.zoomOut(0.9);
      }
    },
  },
};
</script>
<style lang="scss">
#canvas-flow {
  position: absolute;
  top: 43px;
  left: 0;
  bottom: 0;
  right: 0;
  overflow: hidden;
  .node-current {
    display: flex;
    align-items: center;
    width: 100%;
    height: 100%;
    padding-left: 10px;
    background-color: #3a84ff !important;
    color: white;
    outline: none;
    .node-name {
      width: 90%;
    }
    .icon-more {
      font-size: 20px;
    }
  }
  .chinese-name {
    position: absolute;
    left: 0;
    right: 0;
    height: 30px;
    line-height: 30px;
    text-align: center;
  }
  .node-storage-container {
    position: relative;
    display: inline-block;
    width: 168px;
    height: 44px;
    background-color: white;
    border: 1px solid #313238;
    border-radius: 4px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    .icon-minus-square-shape,
    .icon-plus-square-shape {
      cursor: pointer;
    }
    .icon-minus-circle::before {
      content: '\E9AA';
      color: white;
      background: #c4c6cc;
    }
    .node-icon {
      display: flex;
      align-items: center;
      font-size: 20px;
      border-right: 1px solid #979ba5;
      padding: 0px 3px;
      margin-right: 4px;
    }
    .border-white {
      border-right-color: white;
    }
    .node-name {
      flex: 1;
      font-size: 12px;
    }
    .pl8 {
      padding-left: 8px;
    }
    .bl {
      border-left: 1px solid #979ba5;
      border-right: none;
    }
  }
  .data-processing {
    border-radius: 44px;
  }
  .node-background {
    background-color: #e1ecff;
  }
  .border-color-none {
    border-color: #3a84ff;
  }
  .reverse {
    flex-direction: row-reverse;
  }
  .icon-right {
    position: absolute;
    right: -16px;
    z-index: 1;
  }
  .icon-left {
    position: absolute;
    left: -20px;
    z-index: 1;
  }
}
</style>
<style lang="scss">
.focus {
  animation: scale 1s;
}
@keyframes scale {
  15% {
    box-shadow: 0px 0px 10px #333333;
  }
  30% {
    box-shadow: 0px 0px 0px transparent;
  }
  45% {
    box-shadow: 0px 0px 10px #333333;
  }
  60% {
    box-shadow: 0px 0px 10px transparent;
  }
  75% {
    box-shadow: 0px 0px 10px #333333;
  }
  100% {
    box-shadow: 0px 0px 10px transparent;
  }
}
</style>

<style lang="scss">
#blood-tooltip-demo {
  max-width: 445px;
  font-size: 12px;
  .bk-form-item {
    .bk-label {
      width: 135px;
      height: 20px;
      font-size: 12px;
      padding: 0;
      color: white;
      line-height: 20px;
      font-weight: normal;
    }
    .bk-form-content {
      margin-left: 135px;
      height: 20px;
      line-height: 20px;
      min-height: 20px;
    }
  }
  .mark-color {
    color: #a3c5fd;
    font-weight: 900;
    .bk-label {
      color: #a3c5fd;
      font-weight: 900;
    }
  }
}
.blood-tooltips {
  max-width: 828px;
  .tooltips-title {
    border-bottom: 1px solid #ddd;
    margin-bottom: 5px;
    padding-bottom: 5px;
    font-weight: 900;
  }
  .click-color {
    color: #699df4;
    cursor: pointer;
  }
}
</style>
