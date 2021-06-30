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

/**
 * 此组件为基础组件，用于数据开发画布节点基类，所有节点详情可基于此类扩展
 * 谨慎修改，所有修改需要进行 merge request 评审
 */

import Bus from '@/common/js/bus.js';
import { exitFullscreen, fullScreen, isFullScreen } from '@/common/js/util.js';
import { deduplicate } from '@/common/js/util.js';
import { bkButton } from 'bk-magic-vue';
import styled from '@tencent/vue-styled-components';
import { CreateElement } from 'vue';
import { Component, Emit, Model, Prop, PropSync, ProvideReactive, Vue, Watch } from 'vue-property-decorator';
import { mapGetters } from 'vuex';
import { INodeParams } from '../../interface/INodeParams';
import GraphNodeLayout from './GraphNode.Layout';
import StyledVue from './StyledVue';

const bkbuttonProps = {};

/** 节点详情组件 */
const StyledRoot = {
  /** 节点头部组件 */
  Header: styled('section', {})`
    display: flex;
    flex-direction: column;
    justify-content: flex-start;
    align-items: flex-start;
    padding: 15px 20px;
    position: relative;
    line-height: normal;
  `,
  /** 节点标题(名称) */
  Title: styled('div', {})`
    font-size: 18px;
    margin-bottom: 10px;
    color: #333;
    display: flex;
    align-items: center;
    font-weight: 400;
    position: relative;
    width: 100%;
  `,
  /** 节点子标题(描述) */
  SubTitle: styled('div', {})`
    display: flex;
    font-size: 14px;
    font-weight: 400;
    color: #888;
    width: 100%;
  `,
  /** 节点Icon */
  TitleIcon: styled('i', {})`
    margin-right: 5px;
  `,
  /** 工具区域组件 */
  TitleTool: styled('div')`
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
  `,
  /** 工具条目 */
  ToolItem: styled('span')`
    font-size: 14px;
    line-height: 19px;
    margin-right: 20px;
    cursor: pointer;
    color: #979ba5;
  `,
  /** 工具Icon */
  ToolIcon: styled('i')`
    margin-right: 5px;
    color: #c4c6cc;
  `,
  FooterButton: styled('button', bkbuttonProps, { clsName: 'bkdata-button' })`
    width: 120px;
    margin-right: 10px;
  `.withComponent(bkButton),
};

@Component({
  components: {
    GraphNodeLayout,
  },
  computed: {
    ...mapGetters({
      allBizList: 'global/getAllBizList',
    }),
  },
})
export default class GraphNodeBase extends StyledVue {
  @ProvideReactive() public parentResultTables = [];

  /** 节点类型 */
  @Prop({ default: 'Node Type' }) nodeType!: string;

  /** 描述 */
  @Prop({ default: 'Description' }) description!: string;

  /** 显示Icon */
  @Prop({ default: 'icon-nodata' }) IconName!: string;

  /** 帮助文档 */
  @Prop({ default: undefined }) helpUrl!: string;

  /** 保存按钮loading */
  @Prop({ default: false }) public isSaving: boolean = false;

  /** 是否为全屏 */
  public isFullScreen: boolean = false;

  public selfConfig: object = {};
  public parentConfig: object = {};
  public bizList: object[] = [];
  public loading: boolean = false;
  public params: INodeParams = {
    config: {
      name: 'string',
      bk_biz_id: '',
      dedicated_config: {},
      node_type: '',
    },
  };

  /**
   * 提交表单信息
   * 不同节点的表单信息略有差异
   * 根据继承的节点自动扩展
   */
  public formData: any = {
    params: {},
  };

  /**
   * 提交表单数据校验设置
   * 根据不同节点类型设置
   */
  public validateForm: any = {};

  get fullScreenIcon() {
    return this.isFullScreen ? 'icon-un-full-screen' : 'icon-full-screen';
  }

  public mounted() {
    this.$emit('nodeDetailMounted', this);
  }

  public getBizNameByBizId(id: string | number) {
    id = String(id);
    const result = (this.allBizList || []).find(biz => biz.bk_biz_id === id) || {};
    return result.bk_biz_name || '';
  }

  public initOutput() { }

  public customValidateForm() { } // 自定义校验，由子组件实现
  public customSetConfigBack() { } // 自定义回填，由子组件实现,且必须返回一个Promise

  public validateFormData() {
    this.params.config.bk_biz_id = this.params.config.outputs[0].bk_biz_id; // 没有什么意义，之后考虑去掉

    /** 更新节点时需要flowid */
    if (this.selfConfig.hasOwnProperty('node_id')) {
      this.params.flow_id = this.$route.params.fid;
    }
    return this.customValidateForm();
  }

  public async setConfigBack(self, source, fl, option = {}) {
    this.loading = true;
    /** 画布缓存数据处理 */
    let tempConfig = this.$store.state.ide.tempNodeConfig.find(item => {
      return item.node_key === self.id;
    });

    const cacheConfig = await this.bkRequest.httpRequest('dataFlow/getCatchData', {
      query: { key: 'node_' + self.id },
    });

    if (cacheConfig.result) {
      tempConfig = Object.assign({ config: {} }, tempConfig, { config: cacheConfig.data });
    }

    if (tempConfig && !self.hasOwnProperty('node_id')) {
      self.node_config = tempConfig.config;
    }

    /** 节点数据回填 */

    this.activeTab = option.elType === 'referTask' ? 'referTasks' : 'config';
    this.selfConfig = self;
    this.parentConfig = deduplicate(JSON.parse(JSON.stringify(source)), 'id');

    this.params.frontend_info = self.frontend_info;
    this.params.from_links = fl;
    const parentList = this.parentConfig.reduce((output, config) => [...output, ...config.result_table_ids], []);
    await this.getResultList(parentList);

    if (self.hasOwnProperty('node_id') || self.isCopy || tempConfig) {
      for (const key in self.node_config) {
        // 去掉 null、undefined, [], '', fals
        if (key === 'inputs') {
          continue;
        }
        if (self.node_config[key] !== null) {
          this.params.config[key] = self.node_config[key];
          if (key === 'outputs') {
            this.params.config[key].forEach(output => {
              this.$set(output, 'validate', {
                table_name: {
                  // 输出编辑框 数据输出
                  status: false,
                  errorMsg: '',
                },
                output_name: {
                  // 输出编辑框 中文名称
                  status: false,
                  errorMsg: '',
                },
                field_config: {
                  status: false,
                  errorMsg: this.$t('必填项不可为空'),
                },
              });
            });
          }
        }
      }
    }

    this.parentConfig.forEach(parent => {
      // 在离线节点的父节点是离线节点时，传来的父节点对象里result_table_id为null，需要赋值
      if (!parent.result_table_id) {
        parent.result_table_id = parent.result_table_ids[0];
      }

      const parentRts = parent.result_table_ids;

      /** 数据输入相关 */
      this.params.config.inputs.push({
        id: parent.node_id,
        from_result_table_ids: parentRts,
      });

      const selectInputList = {
        id: parent.node_id,
        name: parent.node_name,
        children: [],
      };
      parentRts.forEach(item => {
        const listItem = {
          id: `${item}(${parent.node_id})`,
          name: `${item}(${parent.node_id})`,
          disabled: parentRts.length === 1,
          // name: `${item}(${parent.output_description})`
        };
        selectInputList.children.push(listItem);
      });
      // this.dataInputList.push(selectInputList)

      /** 根据parent配置biz_list */
      const bizId = parent.bk_biz_id;
      if (!this.bizList.some(item => item.biz_id === bizId)) {
        this.bizList.push({
          name: this.getBizNameByBizId(bizId),
          biz_id: bizId,
        });
      }
    });

    this.$nextTick(() => {
      this.initOutput();
    });

    this.customSetConfigBack().then(resolve => {
      this.loading = false;
      Bus.$emit('nodeConfigBack');
    });
  }

  public async getResultList(resultTableIds = []) {
    return this.getAllResultList(resultTableIds)
      .then(res => {
        this.$set(this, 'parentResultTables', res);
      })
      ['catch'](err => {
        this.getMethodWarning(err.message, 'error');
      });
  }

  public getAllResultList(resultTableIds = []) {
    const resultList = {};
    return Promise.all(resultTableIds.map(id => this.getResultListByResultTableId(id)))
      .then(res => {
        return Promise.resolve(
          res.reduce(
            (pre, current) => Object.assign(pre, {
              [current.resultTableId]: {
                fields: current.fields,
                resultTable: {
                  id: current.result_table_id,
                  name: current.result_table_name,
                  alias: current.result_table_name_alias,
                },
              },
            }),
            resultList
          )
        );
      })
      ['catch'](err => {
        this.getMethodWarning(err.message, 'error');
        return Promise.reject(err);
      });
  }

  /*
        获取结果字段表
    */
  public getResultListByResultTableId(id) {
    return this.bkRequest.httpRequest('meta/getResultTablesWithFields', { params: { rtid: id } }).then(res => {
      if (res.result) {
        return Promise.resolve({
          fields: res.data.fields
            .filter(item => item.field_name !== '_startTime_' && item.field_name !== '_endTime_')
            .map(field => ({
              name: field.field_name,
              type: field.field_type,
              alias: field.field_alias,
              des: field.description,
            })),
          resultTableId: id,
        });
      } else {
        return Promise.reject(res);
      }
    });
  }

  public getCode() {
    const Inputs = this.parentResultTables;
    const rtId = Object.keys(Inputs)[0];
    const parentResult = Inputs[rtId];
    let sql = '';
    for (let i = 0; i < parentResult.fields.length; i++) {
      sql += parentResult.fields[i].name + ', ';
      if (i % 3 === 0 && i !== 0) {
        sql += '\n    ';
      }
    }
    const index = sql.lastIndexOf(',');
    sql = sql.slice(0, index);
    if (!sql) {
      sql = '*';
    }
    return 'select ' + sql + '\nfrom ' + rtId;
  }

  /** 全屏操作 */
  public handleFullscreenClick() {
    this.isFullScreen = !isFullScreen();
    !this.isFullScreen ? exitFullscreen() : fullScreen(this.$el);
  }

  /** 提交表单 */
  public handleSubmitClick() {
    if (this.beforeSubmit()) {
      this.$emit('submit', this.params);
    }
  }

  /** 取消逻辑 */
  public handleCancelClick() {
    this.$emit('cancel', this.formData);
  }

  /**
   * 表单提交之前钩子
   * 子组件可以重写此函数，用于单独的校验逻辑
   */
  public beforeSubmit(): Boolean {
    return true;
  }

  /** 渲染Header适配器 */
  public getHeaderAdapter(h: CreateElement) {
    return (
      <StyledRoot.Header>
        <StyledRoot.Title>
          <StyledRoot.TitleIcon class={`bk-icon ${this.IconName}`}></StyledRoot.TitleIcon>
          {this.nodeType}
          <StyledRoot.TitleTool>
            <StyledRoot.ToolItem onClick={this.handleFullscreenClick}>
              <StyledRoot.ToolIcon class={`bkdata-icon ${this.fullScreenIcon}`}></StyledRoot.ToolIcon>
              {this.$t('全屏')}
            </StyledRoot.ToolItem>
            {this.helpUrl && (
              <StyledRoot.ToolItem>
                <StyledRoot.ToolIcon class="bkdata-icon icon-help-fill"></StyledRoot.ToolIcon>
                <a href={this.helpUrl} target="_blank">
                  {this.$t('帮助文档')}
                </a>
              </StyledRoot.ToolItem>
            )}
          </StyledRoot.TitleTool>
        </StyledRoot.Title>
        <StyledRoot.SubTitle>{this.description}</StyledRoot.SubTitle>
      </StyledRoot.Header>
    );
  }

  /**
   * 渲染body适配器
   * @param h
   */
  public getBodyAdapter(h: CreateElement) {
    return this.$slots['default'];
  }

  /** 渲染Footer适配器 */
  public getFooterAdapter(h: CreateElement) {
    return (
      <div>
        <StyledRoot.FooterButton theme="primary" loading={this.isSaving} onClick={this.handleSubmitClick}>
          {this.$t('保存')}
        </StyledRoot.FooterButton>
        <StyledRoot.FooterButton onClick={this.handleCancelClick}>{this.$t('取消')}</StyledRoot.FooterButton>
      </div>
    );
  }

  public render(h: CreateElement) {
    return (
      <GraphNodeLayout>
        <template slot="header">{this.getHeaderAdapter(h)}</template>
        <template slot="default">
          <div className="node-content-wrapper"
            style="height: 100%"
            v-bkloading={{ isLoading: this.loading }}>
            {this.getBodyAdapter(h)}
          </div>
        </template>
        <template slot="footer">{this.getFooterAdapter(h)}</template>
      </GraphNodeLayout>
    );
  }
}
