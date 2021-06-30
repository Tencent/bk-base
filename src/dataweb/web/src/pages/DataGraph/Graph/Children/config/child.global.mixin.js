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
/* eslint-disable prefer-destructuring */
/* eslint-disable radix */
import { mapGetters } from 'vuex';
export default {
  computed: {
    ...mapGetters({
      allBizList: 'global/getAllBizList',
    }),
    shouldProcessFields() {
      return ['elastic_storage', 'tspider_storage', 'mysql_storage'].includes(this.nodeType);
    },
  },
  methods: {
    /** 存储节点根据父节点获取bk_biz_id
         *  1. 旧节点，返回parent.bk_biz_id
         *  2. 新节点，返回parent.outputs[0].bk_biz_id
         *  3. 新节点支持多输出后,遍历outputs(toDo)
         */
    getBizIdByParent() {},
    /** 过期时间相关
         * formatExpiresConfig 提交表单时，对expiresSelected进行分割
         * getStorageclusterExpiresConfigs 根据nodeType获取过期时间列表
         * getClusterExpiresConfig 格式化过期时间列表
         */
    formatExpiresConfig() {
      if (parseInt(this.expiresSelected) === -1) {
        this.params.config.expires = -1;
        this.params.config.expires_unit = '';
        return;
      }
      this.params.config.expires_unit = this.expiresSelected.match(/[a-z|A-Z]+/gi)[0];
      this.params.config.expires = Number(this.expiresSelected.match(/\d+/gi)[0]);
      return;
    },
    getExpiresConfigByCluster(nodeType) {
      this.expiresList = [];
      this.$store
        .dispatch('api/flows/getStorageClusterExpiresConfigs', { type: nodeType, name: this.cluster })
        .then((res) => {
          if (res.result) {
            res.data.expires = JSON.parse(res.data.expires);
            this.expiresConfig = res.data;
            this.getClusterExpiresList();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    getClusterExpiresList() {
      if (this.expiresConfig) {
        this.expiresList = this.expiresConfig.expires.list_expire.map(d => ({ id: d.value, text: d.name }));
        const currentExpire = this.expiresList.find(item => item.id === this.expiresSelected);
        if (currentExpire === undefined) {
          this.expiresSelected = this.expiresList[0].id;
        }
      } else {
        this.expiresSelected = '';
      }
    },
    /** 过期时间相关，由于不同节点过期时间的单位改造没有统一，这里把高度重合的部分抽离 */
    getExpiresListCommon(hasUnit = false, key) {
      const expiresKey = key && Object.prototype.hasOwnProperty.call(this, key)
        ? this[key]
        : this.params.config.expires;
      if (this.expiresConfig) {
        if (this.nodeTypeLower === 'tdw' || this.nodeType === 'tdw_storage') {
          this.params.config.expires_unit = this.expiresConfig.expires.list_expire[0].value.replace(
            /^\d+(\w)/,
            '$1',
          ); // tdw存储添加expires_unit参数
        }
        this.expiresList = this.expiresConfig.expires.list_expire.map(d => ({
          id: hasUnit ? d.value : parseInt(d.value.replace('d', '')),
          text: d.name,
        }));
        const currentExpire = this.expiresList.find(item => item.id === expiresKey);
        if (currentExpire === undefined) {
          key ? (this[key] = this.expiresList[0].id) : (this.params.config.expires = this.expiresList[0].id);
        }
      } else {
        if (!expiresKey) {
          key ? (this[key] = null) : (this.params.config.expires = null);
        }
      }
    },

    getFromResultTableIds(parentConfig) {
      let resultTableIds = [];
      for (const item of parentConfig) {
        resultTableIds = [...new Set(resultTableIds.concat(item.result_table_ids))];
      }

      return resultTableIds;
    },

    getBizInfoByBizId(id) {
      const targetId = String(id);
      return (this.allBizList || []).find(biz => biz.bk_biz_id === targetId) || {};
    },

    getBizNameByBizId(id) {
      return this.getBizInfoByBizId(id).bk_biz_name || '';
    },

    /** 根据BizId获取BizName（返回结果不包含BizId） */
    getBizNameWithoutBizIdByBizId(id) {
      return this.getBizInfoByBizId(id).source_biz_name || '';
    },

    getAllResultList(resultTableIds = []) {
      const resultList = {};
      return Promise.all(resultTableIds.map(id => this.getResultListByResultTableId(id)))
        .then(res => Promise.resolve(res.reduce((pre, current) => {
          const data = {
            [current.resultTableId]: {
              fields: current.fields,
              resultTable: {
                id: current.result_table_id,
                name: current.result_table_name,
                alias: current.result_table_name_alias,
              },
            },
          };
          return Object.assign(pre, data);
        }, resultList,
          // Object.assign(pre, {
          //     [current.resultTableId]: {
          //         fields: current.fields,
          //         resultTable: {
          //             id: current.result_table_id,
          //             name: current.result_table_name,
          //             alias: current.result_table_name_alias,
          //         },
          //     },
          // }),
        )))
        .catch((err) => {
          this.getMethodWarning(err.message, 'error');
          return Promise.reject(err);
        });
    },

    /*
            获取结果字段表
        */
    getResultListByResultTableId(id) {
      return this.bkRequest.httpRequest('meta/getResultTablesWithFields', { params: { rtid: id } }).then((res) => {
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
        }
        return Promise.reject(res);
      });
    },
    /** config回填 */
    paramsConfigSet(selfConfig, parentConfig, params) {
      // eslint-disable-next-line no-restricted-syntax
      for (const key in selfConfig) {
        // 去掉 null、undefined, [], '', fals
        if (selfConfig[key] !== null && selfConfig[key] !== undefined) {
          // from_nodes根据父级，不回填，防止断掉连线后带来的问题
          if (key === 'from_nodes') {
            /** 对于from_nodes， 如果父级包含selfConfig配置，回填，否则按照父级内容填充 */
            let canSetBack = false;
            for (let i = 0; i < parentConfig.length; i++) {
              const currentRt = (
                selfConfig[key].length && selfConfig[key][i].from_result_table_ids[0]
              )
                            || 0;
              canSetBack = parentConfig[i].result_table_ids.includes(currentRt);
              if (!canSetBack) break;
            }
            if (canSetBack) {
              params.config[key] = JSON.parse(JSON.stringify(selfConfig[key]));
              params.config[key].forEach((item) => {
                /** 回填要求from_result_table_ids为String类型 */
                if (Array.isArray(item.from_result_table_ids)) {
                  item.from_result_table_ids = item.from_result_table_ids[0];
                }
              });
            }
          } else {
            params.config[key] = JSON.parse(JSON.stringify(selfConfig[key]));
          }
        }
      }
    },
  },
};
