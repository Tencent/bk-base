/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

/* eslint-disable no-param-reassign */
class NodeTemplate {
  constructor(data) {
    this.nodeData = data;
    this.mode = 'edit';
    this.action = 'normal';
    this.iconMap = {
      failure: 'icon-close-circle-shape',
      warning: 'icon-exclamation-circle-shape',
      success: 'icon-check-circle-shape',
      running: 'icon-circle-4-1',
    };

    this.debugIconMap = {
      success: 'icon-check-1',
      failure: 'icon-close',
      warning: 'icon-info-tip',
      running: 'icon-circle-4-1',
      disabled: 'icon-minus-circle',
    };
    this.nodeStatus = this.nodeData.status;
    this.debugInfo = this.nodeData.debugInfo;
    this.isDebuging = !!this.debugInfo;
    this.debugStaus = (this.action === 'debug' && ((this.debugInfo || {}).status || 'running')) || 'normal';
    this.debugDisplay = (this.debugInfo || {}).status_display || '待调试';
  }

  updateStaus() {
    this.nodeStatus = this.nodeData.status;
    this.debugInfo = this.nodeData.debugInfo;
    this.isDebuging = !!this.debugInfo;
    this.debugStaus = (this.action === 'debug' && ((this.debugInfo || {}).status || 'running')) || 'normal';
    this.debugDisplay = (this.debugInfo || {}).status_display || '待调试';
  }

  getTemplateById(id, mode = 'edit', action = 'normal') {
    this.mode = mode;
    this.action = action;
    this.updateStaus();
    const template = {
      'graph-square': () => this.getGraphSquareTemplate(),
      'graph-round': () => this.getGraphRoundTemplate(),
      'graph-ractangle': () => this.getGraphRactangleTemplate(),
    };

    return (template[id] && template[id]()) || '';
  }

  getGraphSquareTemplate() {
    return `<div class="node-container ${this.mode}" id="${this.nodeData.id}">
                <div class="node-storage-layer node-running-layer ${this.nodeStatus}">
                    <div class="node-storage-content node-content">
                        <div class="node-storage-body">
                            <i class="icons ${this.nodeData.iconName}"></i>
                        </div>
                        <div class="node-storage-footer node-footer">
                            <div class="footer-line-container">
                                <span class="line-left ${this.nodeStatus}"></span>
                                <span class="line-right ${this.nodeStatus}"></span>
                            </div>
                        </div>
                    </div>
                    ${this.getNodeActionsHtml()}
                    <div class="node-name-label" title="${this.nodeData.node_name}">
                        ${this.nodeData.node_name}
                    </div>
                    ${(this.nodeData.monitor && this.getCommonInputHtml(this.nodeData.monitor.formatData)) || ''}
                    <div class="node-status-container">
                        ${(this.isDebuging
        && `<span class="node-debug-status ${this.debugStaus}" title="${this.debugDisplay}">
                                <i class="bk-icon ${this.debugIconMap[this.debugStaus]}"></i>
                            </span> `)
      || ''}
                            ${(Object.prototype.hasOwnProperty.call(this.nodeData, 'alertCount')
        && `<span class="flow-alert-container">
                                        <span class="icon-alert"></span>${this.nodeData.alertCount}
                                    </span>`)
      || ''}
                        </div>
                </div>
            </div>`;
  }

  getGraphRoundTemplate() {
    return `<div class="node-container ${this.mode}" id="${this.nodeData.id}">
                    <div class="node-source-layer node-running-layer ${this.nodeStatus}">
                        <div class="node-source-content node-content">
                            <i class="icons ${this.nodeData.iconName}"></i>
                        </div>
                        ${this.getNodeActionsHtml()}
                        <div class="node-name-label">
                            ${this.nodeData.node_name}
                        </div>
                        ${(this.nodeData.monitor && this.getCommonInputHtml(this.nodeData.monitor.formatData)) || ''}
                        <div class="node-status-container">
                            ${(this.isDebuging
        && `<span class="node-debug-status ${this.debugStaus}" title="${this.debugDisplay}">
                                    <i class="bk-icon ${this.debugIconMap[this.debugStaus]}"></i>
                                </span>`)
      || ''}
                            ${(Object.prototype.hasOwnProperty.call(this.nodeData, 'alertCount')
        && `<span class="flow-alert-container">
                                        <span class="icon-alert"></span>${this.nodeData.alertCount}
                                    </span>`)
      || ''}
                        </div>
                    </div>
                </div>`;
  }

  getGraphRactangleTemplate() {
    const monitorData = (this.nodeData.monitor && this.nodeData.monitor.formatData) || {};
    return `<div class="node-container ${this.mode}" id="${this.nodeData.id}">
                <div class="node-calculate-layer node-running-layer ${this.nodeStatus}">
                    <div class="node-calculate-content">
                        <div class="node-icon ${this.nodeStatus}">
                            <i class="icons ${this.nodeData.iconName}"></i>
                        </div>
                        <div class="node-detail">
                            <span class="node-name" title="${this.nodeData.node_name}">
                                ${this.nodeData.node_name || ''}
                            </span>
                            <!--<span class="action-more">
                                <i class="bk-icon icon-more"></i>
                            </span>-->
                            ${(this.isDebuging
        && `<span class="node-debug-status ${this.debugStaus}" title="${this.debugDisplay}">
                                <i class="bk-icon ${this.debugIconMap[this.debugStaus]}"></i>
                            </span>`)
      || ''}
                        </div>
                        <div class="flow-upper-status test">
                            ${(this.nodeData.related
        && `<span class="icon-chain" data-related="${this.nodeData.related}"></span>`)
      || ''}
                            ${(Object.prototype.hasOwnProperty.call(this.nodeData, 'alertCount')
        && `<span class="flow-alert-container">
                                        <span class="icon-alert"></span>${this.nodeData.alertCount}
                                    </span>`)
      || ''}
                            ${(this.nodeData.custom_calculate_status === 'applying'
        && `<span class="icon-recal-pending" title="${window.$t('补算审批中')}"
              data-tooltips="${window.$t('补算审批中')}"></span>`)
      || ''}
                            ${(this.nodeData.custom_calculate_status === 'ready'
        && `<span class="icon-recal-execution" title="${window.$t('补算已审批_待执行')}"
              data-tooltips="${window.$t('补算已审批_待执行')}"></span>`)
      || ''}
                            ${(this.nodeData.custom_calculate_status === 'running'
        && `<i class="recal-loader" title=${window.$t('补算中')}></i>`)
      || ''}
                            ${this.nodeData.is_data_corrected
        ? `<span class="icon-data-amend" title=${window.$t('已修正')}></span>`
        : ''
      }
                        </div>
                    </div>
                    ${this.getNodeActionsHtml()}
                    ${(this.nodeData.monitor && this.getMonitorIntervalHtml(monitorData)) || ''}
                </div>
            </div>`;
  }

  /**
       * 打点监控Interval HTML，区别于通用HTML
       * @param {*} monitor ：打点数据
       */
  getMonitorIntervalHtml(monitor) {
    if (monitor.interval !== 'null' && monitor.interval !== '-') {
      return `<div class="node-option-monitor monitor-input">
                <span class="info-text">${monitor.start_time}</span>
            </div>
            <div class="node-option-monitor monitor-output">
                <span class="info-text">${monitor.interval}</span>
                <span class="point-debug-unit info-label">
                    <i class="bk-icon icon-clock"></i>
                </span>
            </div>`;
    }
    return `${this.getCommonInputHtml(monitor)} ${this.getCommonOutputHtml(monitor)}`;
  }

  /**
       * 打点监控通用HTML
       * @param {*} monitor ：打点信息
       */
  getCommonInputHtml(monitor) {
    const localmonitor = monitor || {};
    return `<div class="node-option-monitor monitor-input">
            <span class="point-debug-unit info-label">
                <i class="bk-icon icon-input-2"></i>
            </span>
            <span class="info-text">${localmonitor.input_data_count || '-'}</span>
        </div>`;
  }

  getCommonOutputHtml(monitor) {
    const localmonitor = monitor || {};
    return `<div class="node-option-monitor monitor-output">
            <span class="info-text">${localmonitor.output_data_count || '-'}</span>
            <span class="point-debug-unit info-label">
                <i class="bk-icon icon-output"></i>
            </span>
        </div>`;
  }

  /**
       * 节点操作项：复制、删除...
       */
  getNodeActionsHtml() {
    return `<div class="node-option-container ${this.mode}">
            <span class="bkdata-node-extend">
                <i class="bk-icon icon-clipboard-shape  bk-copy __bk-node-setting" title="复制"></i>
                <i class="bk-icon icon-close3-shape bk-delete __bk-node-setting"  title="删除"></i>
                ${(this.hasMoreIcon()
        && `<i class="bk-icon icon-more-2  __bk-node-setting" node-extend-more-tippy title="更多"></i>
                        <ul class="bk-node-select"><li class="bk-generate-node">批量生成 指标构建 节点</li></ul>`)
      || ''}
            </span>
        </div>`;
  }

  hasMoreIcon() {
    return (
      this.nodeData.node_type === 'data_model_app'
      && Object.prototype.hasOwnProperty.call(this.nodeData, 'node_id')
    );
  }
  getExtendSelectDrop() {
    return;
  }
}

export default NodeTemplate;
