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

import { showMsg } from '@/common/js/util.js';
import Bus from '@/common/js/bus.js';
import { getNodeConfig } from './nodeConfig.js';

let copyCorrectConfig = '';
let initCorrectSwitch = false;

export default {
  data() {
    return {
      // 节点数据修正调试成功的标记
      correctFlag: false,
    };
  },
  mounted() {
    // 数据修正调试不通过，禁止修改节点配置
    Bus.$on('changeCorrectFixStatus', (debugFlag) => {
      this.correctFlag = debugFlag;
    });
  },
  methods: {
    checkCorrectChange() {
      if (this.params.config.data_correct) {
        if (initCorrectSwitch) {
          // 修正开关打开时，初始修正配置可以直接保存节点
          if (copyCorrectConfig === JSON.stringify(this.params.config.data_correct.correct_configs)) {
            return true;
          }
        }
        if (!this.correctFlag) {
          showMsg('保存节点之前需要数据修正调试成功！', 'warning');
        }
      }
      return this.correctFlag;
    },
    // 存储初始修正配置的拷贝
    async copyCorrectConfig(self) {
      if (self.node_id) {
        if (self.node_config && self.node_config.data_correct) {
          copyCorrectConfig = JSON.stringify(self.node_config.data_correct.correct_configs);
          initCorrectSwitch = self.node_config.data_correct.is_open_correct;
        }
      } else {
        const nodeConfig = await getNodeConfig();
        copyCorrectConfig = JSON.stringify(nodeConfig[self.type].config.data_correct.correct_configs);
      }
    },
  },
  beforeDestroy() {
    Bus.$off('changeCorrectFixStatus');
  },
};
