

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
  <div id="collect-range"
    class="bk-form-item mt15">
    <ScopeItem
      v-for="(item, index) of params"
      :key="index"
      v-bkloading="{ isLoading: item.rowLoad }"
      class="collect-content mb15"
      :step="index + 1"
      :height="'auto'">
      <div class="delete f16">
        <i v-if="params.length - 1"
          class="bk-icon icon-delete"
          @click="deleteOne(index)" />
      </div>
      <Item class="coll-row coll-row-h">
        <label class="bk-label">{{ $t('采集范围') }}：</label>
        <div class="bk-form-content">
          <bkdata-input
            v-model="item.db_host"
            v-tooltip.notrigger="validates[index]['db_host']"
            class="host"
            name="validation_name"
            :placeholder="$t('host_域名')" />
          <span>:</span>
          <bkdata-input
            v-model="item.db_port"
            v-tooltip.notrigger="validates[index]['db_port']"
            class="port mr10"
            name="validation_name"
            :placeholder="$t('端口')" />
          <bkdata-input
            v-model="item.db_user"
            v-tooltip.notrigger="validates[index]['db_user']"
            class="db-user mr10"
            name="validation_name"
            :placeholder="$t('用户名')" />
          <bkdata-input
            v-model="item.db_pass"
            v-tooltip.notrigger="validates[index]['db_pass']"
            type="password"
            class="db-passwd mr10"
            name="validation_name"
            :placeholder="$t('密码')" />
          <i v-bk-tooltips="formatToolTip()"
            class="info f18 bk-icon icon-info-circle" />
        </div>
      </Item>

      <Item class="database-table coll-row-h">
        <label class="bk-label">{{ $t('数据库_表名') }}：</label>
        <div
          v-tooltip.manual.notrigger.left="bizIdValidate"
          :class="['bk-form-content']"
          :data-validate="bizIdValidate.visible">
          <div class="database col1">
            <bkdata-selector
              v-tooltip.notrigger.bottom="validates[index]['db_name']"
              :isLoading="item.loadDb"
              searchable
              :list="item.dataBaseLists"
              :selected.sync="item.db_name"
              :placeholder="placeholder.db_name"
              @visible-toggle="
                _ => {
                  checkDb(_, item, index);
                }
              " />
          </div>
          <span>/</span>
          <div class="database mr10">
            <bkdata-selector
              v-tooltip.notrigger.bottom="validates[index]['table_name']"
              :isLoading="item.loadTb"
              searchable
              :list="item.databaseTables"
              :selected.sync="item.table_name"
              :placeholder="placeholder.table_name"
              @item-selected="
                () => {
                  handleDataTableSelected(item);
                }
              "
              @visible-toggle="
                _ => {
                  checkDbTable(_, item, index);
                }
              " />
          </div>
          <div v-if="selectedFields.length"
            class="search-content">
            <i class="search f20 bk-icon icon-search"
              @click="handleSearchDBRows(item, index)" />
          </div>
        </div>
      </Item>
    </ScopeItem>
    <bkdata-dialog
      v-model="debugShow"
      extCls="bkdata-dialog access-db"
      :width="dialogWidth"
      :content="'component'"
      :hasHeader="false"
      :hasFooter="false"
      :maskClose="false"
      :closeIcon="true">
      <div class="debug-list">
        <bkdata-table :data="searchTables"
          :stripe="true"
          :border="true"
          :emptyText="$t('暂无数据')">
          <bkdata-table-column v-for="(field, index) in selectedFields"
            :key="index"
            :label="field.Field">
            <template slot-scope="props">
              {{ props.row[field.Field] }}
            </template>
          </bkdata-table-column>
        </bkdata-table>
        <!-- <table class="bk-table has-table-bordered">
					<thead>
						<tr>
							<th v-for="(field,index) in selectedFields"
								:key="index">{{ field.Field }}</th>
						</tr>
					</thead>
					<tbody>
						<tr v-for="(row,index) in searchTables"
							:key="index">
							<td v-for="(field,findex) in selectedFields"
								:key="findex">{{ row[field.Field] }}</td>
						</tr>
					</tbody>
				</table> -->
      </div>
    </bkdata-dialog>
    <div class="new-add">
      <span
        v-tooltip.notrigger="{ content: addValidate.content, visible: addValidate.visible, class: 'error-red' }"
        @click="addNew()">
        <i class="bk-icon icon-plus-circle" />
        {{ $t('添加接入对象') }}
      </span>
    </div>
  </div>
</template>

<script>
import Item from '../Item';
import Bus from '@/common/js/bus';
import { validateScope } from '../../SubformConfig/validate.js';
import ScopeItem from '@/pages/DataAccess/Components/Scope/ScopeForm.vue';
import mixin from '@/pages/DataAccess/Config/mixins.js';
export default {
  components: { Item, ScopeItem },
  mixins: [mixin],
  props: {
    bkBizid: {
      type: Number,
      default: 0,
    },
    dbTypeId: {
      type: Number,
      default: 1,
    },
    /** 需要申请权限IP列表 */
    permissionHostList: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      searchTables: [],
      debugShow: false,
      isFirstValidate: true,
      cacheParams: [],
      selectedFields: [],
      param: {
        db_host: '',
        db_port: '',
        db_user: '',
        db_pass: '',
        db_name: '',
        table_name: '',
        dataBaseLists: [],
        databaseTables: [],
        loadDb: false,
        loadTb: false,
        rowLoad: false,
      },
      validate: {
        db_host: {
          regs: [{ required: true, error: window.$t('不能为空') }],
          content: '',
          visible: false,
          class: 'error-red',
        },
        db_port: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { regExp: /^[1-9]\d*$/.source, error: window.$t('只能是正整数') },
          ],
          content: '',
          visible: false,
          class: 'error-red',
        },
        db_user: {
          regs: [{ required: true, error: window.$t('不能为空') }],
          content: '',
          visible: false,
          class: 'error-red',
        },
        db_pass: {
          regs: [{ required: true, error: window.$t('不能为空') }],
          content: '',
          visible: false,
          class: 'error-red',
        },
        db_name: {
          regs: [{ required: true, error: window.$t('不能为空') }],
          content: '',
          visible: false,
          class: 'error-red',
        },
        table_name: {
          regs: [{ required: true, error: window.$t('不能为空') }],
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
      params: [],
      validates: [],
      bizIdValidate: { content: '请选择所属业务', visible: false },
      placeholder: {
        db_name: this.$t('选择数据库表名'),
        table_name: this.$t('请选择表_多个表数据结构需要一致'),
      },
      addValidate: {
        count: 3,
        content: '接入对象添加不能超过3条',
        visible: false,
      },
    };
  },
  computed: {
    dialogWidth() {
      const calLen = this.selectedFields.length * 120;
      return calLen > 1000 ? 1000 : calLen;
    },
  },
  watch: {
    params: {
      handler(newVal) {
        !this.isFirstValidate && this.validateForm(null, false);
      },
      deep: true,
    },
  },

  created() {
    /* eslint-disable */
    Object.defineProperty(RegExp.prototype, 'toJSON', {
      value: RegExp.prototype.toString,
    });
    this.$set(this.params, 0, JSON.parse(JSON.stringify(this.param)));
    this.$set(this.validates, 0, JSON.parse(JSON.stringify(this.validate)));
  },
  methods: {
    formatToolTip() {
      return {
        container: document.body.querySelector('.collect-range'),
        placement: 'top',
        content: `
                        <div class="temp-wrap">
                              <p>${window.$t('以下IP需要具有SELECT权限')}:</p>
                              ${this.permissionHostList.map(host => `<p>${host.ip}</p>`).join('')}
                        </div>
                    `,
      };
    },

    validateBizId() {
      if (this.bkBizid) {
        this.$set(this.bizIdValidate, 'visible', false);
      } else {
        this.$set(this.bizIdValidate, 'visible', true);
      }
      return this.bkBizid;
    },
    validateForm(validateFunc, isSubmit = true) {
      if (isSubmit) {
        this.isFirstValidate = false;
      }
      let isValidate = true;
      this.params.forEach((param, index) => {
        if (!validateScope(param, this.validates[index], [])) {
          isValidate = false;
        }
      });
      this.$forceUpdate();
      return isValidate;
    },
    formatFormData() {
      return {
        group: 'access_conf_info',
        identifier: 'resource',
        data: {
          scope: this.params.map(p => {
            return Object.assign({}, p, { db_type_id: Number(this.dbTypeId) });
          }),
        },
      };
    },
    renderData(data) {
      const key = 'scope';
      const defaultParam = Object.assign({}, this.params);
      this.$set(this, 'params', data['access_conf_info']['resource'][key] || defaultParam);

      this.params.forEach((param, index) => {
        let _param = {};
        Object.keys(this.param).forEach(key => {
          this.$set(param, key, param[key] || this.param[key]);
        });
        this.$set(this.validates, index, JSON.parse(JSON.stringify(this.validate)));
        this.checkDb(true, param);
        this.checkDbTable(true, param);
        this.handleDataTableSelected(param);
      });
    },
    getCachedDbs(param) {
      return this.cacheParams.find(
        ca =>
          ca.db_host === param.db_host &&
          ca.db_port === param.db_port &&
          ca.db_user === param.db_user &&
          ca.db_pass === param.db_pass
      );
    },
    getCachedTbs(param) {
      return this.cacheParams.find(
        ca =>
          ca.db_host === param.db_host &&
          ca.db_port === param.db_port &&
          ca.db_user === param.db_user &&
          ca.db_pass === param.db_pass &&
          ca.db_name === param.db_name
      );
    },
    checkDb(v, param, index) {
      if (v) {
        if (
          !this.validateBizId() ||
          !validateScope(param, this.validates[index], ['db_name', 'table_name', 'validate'])
        ) {
          this.$forceUpdate();
          return false;
        }
        param.loadDb = true;
        const cache = this.getCachedDbs(param);
        if (!cache || !cache.dataBaseLists.length) {
          this.databaseCheck(param, 'db')
            .then(res => {
              const resData = res.data;
              param.dataBaseLists = (resData || ['bkdata_basic']).map(db => {
                return {
                  id: db,
                  name: db,
                };
              });
              this.cacheParams.push(JSON.parse(JSON.stringify(param)));
            })
            .finally(() => {
              param.loadDb = false;
            });
        } else {
          param.dataBaseLists = cache.dataBaseLists;
          param.loadDb = false;
        }
      }
    },
    checkDbTable(v, param, index) {
      if (v) {
        if (!this.validateBizId() || !validateScope(param, this.validates[index], ['table_name', 'validate'])) {
          this.$forceUpdate();
          return false;
        }
        param.loadTb = true;
        const cach = this.getCachedTbs(param);
        if (!cach || !cach.databaseTables || !cach.databaseTables.length) {
          this.databaseCheck(param, 'tb')
            .then(res => {
              const resData = res.data;
              param.databaseTables = (resData || []).map(db => {
                return {
                  id: db,
                  name: db,
                };
              });
              this.cacheParams.push(JSON.parse(JSON.stringify(param)));
              param.table_name && this.handleDataTableSelected(param);
            })
            .finally(() => {
              param.loadTb = false;
            });
        } else {
          param.databaseTables = cach.databaseTables;
          param.loadTb = false;
        }
      }
    },

    handleSearchDBRows(param, index) {
      if (!this.validateBizId() || !validateScope(param, this.validates[index], [])) {
        this.$forceUpdate();
        return false;
      }
      param.rowLoad = true;
      this.databaseCheck(param, 'rows')
        .then(res => {
          this.searchTables = res.data;
          this.debugShow = true;
        })
        .finally(() => {
          param.rowLoad = false;
          this.$forceUpdate();
        });
    },

    /** 检查有没有不相等的字段
     *  return TRUE：有不相等的字段
     *  return False: 没有不相等
     */
    validateFileds(fields) {
      if (this.params.length > 1 && this.selectedFields.length) {
        /** 检查有没有一个不相等的字段 */
        return this.selectedFields.some(field => !fields.some(_f => Object.keys(_f).every(k => field[k] === _f[k])));
      } else {
        this.selectedFields = fields;
        return false;
      }
    },
    handleDataTableSelected(param) {
      this.$nextTick(() => {
        if (param.table_name) {
          param.loadTb = true;
          Bus.$emit('access.obj.dbtableSearching', true);
          this.databaseCheck(param, 'field')
            .then(res => {
              if (res.result) {
                if (!this.validateFileds(res.data)) {
                  Bus.$emit('access.obj.dbtableselected', res.data);
                } else {
                  this.getMethodWarning('表数据结构不一致', 404);
                  param.table_name = '';
                }
              }
            })
            .finally(() => {
              param.loadTb = false;
              Bus.$emit('access.obj.dbtableSearching', false);
            });
        }
      });
    },
    addNew() {
      if (this.params.length >= this.addValidate.count) {
        this.$set(this.addValidate, 'visible', true);
        this.$nextTick(() => {
          setTimeout(() => {
            this.$set(this.addValidate, 'visible', false);
          }, 1000);
        });
        return;
      }
      this.params.push(JSON.parse(JSON.stringify(this.param)));
      this.validates.push(JSON.parse(JSON.stringify(this.validate)));
    },

    /** 删除接入对象 */
    deleteOne(index) {
      this.params.splice(index, 1);
      this.validates.splice(index, 1);
    },
    /**
     * 检索数据库、数据库表、数据库字段
     * @param param:数据库连接配置
     * @param opType：检索类型 "db":尝试获取db列表,"tb":尝试获取表列表,"field":尝试获取字段属性
     */
    databaseCheck(param, opType) {
      let params = {
        data_scenario: 'db',
        db_host: param.db_host,
        db_port: param.db_port,
        db_name: param.db_name,
        db_user: param.db_user,
        db_pass: param.db_pass,
        bk_biz_id: this.bkBizid,
        op_type: opType,
      };
      opType !== 'db' && Object.assign(params, { tb_name: param.table_name });
      return this.bkRequest
        .httpRequest('dataAccess/getDbCheck', {
          mock: false,
          params: params,
        })
        .then(res => {
          if (res.result) {
            return Promise.resolve(res);
          } else {
            this.getMethodWarning(res.message, res.code);
            return Promise.reject({});
          }
        });
    },
  },
};
</script>

<style lang="scss">
#collect-range {
  flex-direction: column;
  justify-content: flex-start;
  align-items: center;
  width: 789px;
  .coll-row {
    margin-top: 0 !important;
  }

  .coll-row-h {
    font-size: 12px;
  }
  .collect-content {
    // background: #f5f5f5;
    // padding: 15px 30px 15px 0;
    position: relative;
    // width: 100%;
    .bk-form-content {
      display: flex;
      margin-left: 0;
      width: 100%;
      input,
      .bkdata-selector-input {
        height: 30px;
        font-size: 12px;
      }
      span {
        align-items: center;
        margin: 0px 5px;
        padding-top: 6px;
      }
    }
    .delete {
      text-align: right;
      height: 22px;
      position: absolute;
      right: 10px;
      cursor: pointer;
      i {
        padding: 3px 5px;
        &:hover {
          background: #3a84ff;
          color: #fff;
        }
      }
    }
    .db-passwd {
      width: 100px;
    }
    .db-user {
      width: 150px;
    }

    .host {
      width: 193px;
    }
    .port {
      width: 60px;
    }
    .database-table {
      width: 735px;
    }
    .database {
      width: 330px;
      &.col1 {
        width: 193px;
      }
    }
    .search-content {
      display: flex;
    }

    .info,
    .search {
      width: 28px;
      height: 28px;
      line-height: 28px;
      text-align: center;
      cursor: pointer;
      &:hover {
        color: #fff;
        background: #3a84ff;
      }
    }
  }
  .new-add {
    text-align: center;
    span {
      border: 1px dashed #c3cdd7;
      display: inline-block;
      padding: 0px 10px;
      text-align: center;
      min-width: 100px;
      cursor: pointer;
      padding: 5px;
      font-size: 12px;
      &:hover {
        background: #3a84ff;
        color: #fff;
        cursor: pointer;
        border: solid 1px #3a84ff;
      }
    }
  }
}

.bkdata-dialog {
  &.access-db {
    .debug-list {
      width: 100%;
      overflow: auto;
      margin-top: 15px;
      padding: 15px;
    }
  }
}
</style>
