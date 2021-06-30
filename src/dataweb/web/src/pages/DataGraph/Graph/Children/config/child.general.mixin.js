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

/* eslint-disable radix */
/* eslint-disable no-restricted-syntax */
/* eslint-disable prefer-destructuring */
/* eslint-disable no-param-reassign */
import { deduplicate } from '@/common/js/util.js';
export default {
  computed: {
    hasNodeId() {
      return Object.prototype.hasOwnProperty.call(this.selfConfig, 'node_id');
    },
  },
  methods: {
    /*
            获取结果字段表
        */
    async getResultList(resultTableIds = []) {
      return this.getAllResultList(resultTableIds)
        .then((res) => {
          this.parentResultList = res;
          this.$emit('updateInputRTList', res);
        })
        .catch((err) => {
          this.getMethodWarning(err.message, 'error');
        });
    },
    async setConfigBack(self, source, fl, option = {}) {
      let tempConfig = this.$store.state.ide.tempNodeConfig.find(item => item.node_key === self.id);

      /** 只有新建节点才获取缓存数据 */
      if (!Object.prototype.hasOwnProperty.call(self, 'node_id')) {
        const cacheConfig = await this.bkRequest.httpRequest('dataFlow/getCatchData', {
          query: { key: `node_${self.id}` },
        });

        if (cacheConfig.result) {
          tempConfig = Object.assign({ config: {} }, tempConfig, { config: cacheConfig.data });
        }
      }

      console.log(tempConfig, 'tempConfig in store');

      if (tempConfig && !Object.prototype.hasOwnProperty.call(self.hasOwnProperty, 'node_id')) {
        self.node_config = tempConfig.config;
      }

      this.activeTab = option.elType === 'referTask' ? 'referTasks' : 'config';
      this.selfConfig = self;
      this.parentConfig = deduplicate(JSON.parse(JSON.stringify(source)), 'id');

      this.parentConfig.forEach((parent) => {
        // 在离线节点的父节点是离线节点时，传来的父节点对象里result_table_id为null，需要赋值
        if (!parent.result_table_id) {
          parent.result_table_id = parent.result_table_ids[0];
        }

        const parentRts = parent.result_table_ids;

        /** 数据输入相关 */
        this.params.config.from_nodes.push({
          id: parent.node_id,
          from_result_table_ids: parentRts[0],
        });

        const selectInputList = {
          id: parent.node_id,
          name: parent.node_name,
          children: [],
        };
        parentRts.forEach((item) => {
          const listItem = {
            id: `${item}(${parent.node_id})`,
            name: `${item}(${parent.node_id})`,
            disabled: parentRts.length === 1,
            // name: `${item}(${parent.output_description})`
          };
          selectInputList.children.push(listItem);
        });
        this.dataInputList.push(selectInputList);

        /** 根据parent配置biz_list */
        const bizId = parent.bk_biz_id;
        if (!this.bizList.some(item => item.biz_id === bizId)) {
          this.bizList.push({
            name: this.getBizNameByBizId(bizId),
            biz_id: bizId,
          });
        }
      });

      this.params.frontend_info = self.frontend_info;
      this.params.from_links = fl;
      this.rtId = this.parentConfig[0].result_table_ids[0];
      this.$refs.dataInput.initSelectedInput(); // 根据数据输入，初始化选项
      await this.getResultList(this.parentConfig
        .reduce((output, config) => [...output, ...config.result_table_ids], []));
      // this.judgeBizIsEqual()

      if (Object.prototype.hasOwnProperty.call(self, 'node_id') || self.isCopy || tempConfig) {
        for (const key in self.node_config) {
          // 去掉 null、undefined, [], '', fals
          if (key === 'from_nodes') continue;
          if (self.node_config[key] !== null) {
            this.params.config[key] = self.node_config[key];
            if (key === 'outputs') {
              this.params.config[key].forEach((output) => {
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

      this.$nextTick(() => {
        this.$refs.outputPanel.initOutput();
      });
    },
    formatParams() {
      clearInterval(this.configCatchTimer);
      this.configCatchTimer = null;

      /** 根据dataInput的下拉选择，修改from_nodes */
      const regx = /(.+?)\((\d+)\)/;
      const { selectedInput } = this.$refs.dataInput;
      this.params.config.from_nodes = [];
      selectedInput.forEach((input) => {
        const rtName = regx.exec(input)[1];
        const id = regx.exec(input)[2];

        const node = this.params.config.from_nodes.find(node => node.id === parseInt(id)) || {
          id: parseInt(id),
          from_result_table_ids: [],
        };
        node.from_result_table_ids.push(rtName);
        this.params.config.from_nodes.push(node);
      });

      this.params.config.bk_biz_id = this.params.config.outputs[0].bk_biz_id;

      /** 更新节点时需要flowid */
      if (Object.prototype.hasOwnProperty.call(this.selfConfig, 'node_id')) {
        this.params.flow_id = this.$route.params.fid;
      }
    },
  },
};
