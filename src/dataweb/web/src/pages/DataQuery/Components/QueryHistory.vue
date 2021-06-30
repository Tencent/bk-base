

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
  <div class="choose-history">
    <p class="title">
      <i :title="$t('历史记录')"
        class="bk-icon icon-history-record" />
      {{ $t('历史记录') }}
    </p>
    <transition-group
      v-if="historyList.length > 0"
      v-bkloading="{ isLoading: isLoading }"
      class="history-list"
      name="flip-list"
      tag="ul">
      <li v-for="(item, index) in historyList"
        :key="item.name.result_table_id"
        :class="{ active: item.show }">
        <div
          :id="'historyList' + index"
          v-bk-tooltips="getHistoryDesciption(item)"
          class="list-title"
          @click="handViewMoreDetail(item, index, true)">
          <i v-if="!item.show"
            :title="$t('展开')"
            class="bk-icon icon-plus-square left-icon" />
          <i v-else
            :title="$t('收起')"
            class="bk-icon icon-minus-square left-icon" />
          <p class="title-text">
            {{ item.name.result_table_id }}
          </p>
          <div class="button">
            <a
              v-bk-tooltips="$t('查询')"
              class="bk-icon-button bk-default icon-button"
              href="javascript:;"
              @click.stop="handHistoryQuery(item, index, true, true, true)">
              <!-- <span class="tooltip">{{$t('查询')}}</span> -->
              <i :title="$t('查询')"
                class="bk-icon icon-search" />
            </a>
            <a
              v-bk-tooltips="$t('跳转到任务')"
              :class="{ 'is-loading': goToFlowLoading }"
              class="bk-icon-button bk-default icon-button"
              href="javascript:;"
              @click.stop="handGoToTask(item.name.result_table_id)">
              <i :title="$t('跳转到任务')"
                class="bk-icon icon-dataflow" />
              <!-- <span class="tooltip">{{$t('跳转到任务')}}</span> -->
            </a>
            <a
              v-bk-tooltips="$t('删除')"
              :class="{ 'is-loading': deleteLoading }"
              class="bk-icon-button bk-default icon-button delete-button"
              href="javascript:;"
              @click.stop="handDeleteHistory(item)">
              <!-- <span class="tooltip">{{$t('删除')}}</span> -->
              <i :title="$t('删除')"
                class="bk-icon icon-delete" />
            </a>
          </div>
        </div>
        <transition name="slide">
          <div
            v-if="item.show"
            v-bkloading="{ isLoading: item.isloading }"
            :style="{ height: styleObject + 'px' }"
            class="list-content">
            <template v-if="item.info">
              <ol class="content-list">
                <li
                  v-for="(fields, key) in (item.info.storage[currentActiveType] &&
                    item.info.storage[currentActiveType].fields) ||
                    []"
                  :id="'infoList' + key"
                  :key="key"
                  v-bk-tooltips="getHistoryFieldsDesciption(fields)"
                  class="clearfix">
                  <div class="text fl">
                    <p class="text-title">
                      {{ fields.physical_field }}
                    </p>
                  </div>
                  <div class="info fl">
                    <span v-for="(value, num) in fields.icons"
                      :id="value.key + num + key"
                      :key="num"
                      class="tag">
                      <span v-bk-tooltips="{ content: value.description }">
                        <i :class="'bk-icon icon-' + value.key" />
                      </span>
                    </span>
                  </div>
                  <div class="fr field-type">
                    {{ fields.physical_field_type }}
                  </div>
                </li>
              </ol>
            </template>
            <div class="no-data">
              <template v-if="!item.info && !item.isloading">
                <img alt
                  src="../../../common/images/no-data.png">
                <p>{{ $t('暂无数据') }}</p>
              </template>
            </div>
          </div>
        </transition>
      </li>
    </transition-group>
    <div v-if="historyList.length === 0"
      v-bkloading="{ isLoading: isLoading }"
      class="no-data">
      <template v-if="historyList.length === 0 && !isLoading">
        <img alt
          src="../../../common/images/no-data.png">
        <p>{{ $t('暂无数据') }}</p>
      </template>
    </div>
  </div>
</template>
<script>
import { postMethodWarning } from '@/common/js/util.js';
import { mapState, mapGetters } from 'vuex';
import Bus from '@/common/js/bus';
export default {
  data() {
    return {
      deleteLoading: false,
      goToFlowLoading: false,
      currentActiveItem: {},
      isInitDefaultHistory: true,
    };
  },

  computed: {
    ...mapState({
      historyList: state => state.dataQuery.hsitory.list,
      isLoading: state => state.dataQuery.hsitory.isLoading,
      currentActiveType: state => state.dataQuery.hsitory.clusterType,
    }),
    styleObject() {
      const storage = this.currentActiveItem.storage
        ? this.currentActiveItem.storage[this.currentActiveType]
          ? this.currentActiveItem.storage[this.currentActiveType]
          : this.currentActiveItem.storage[0]
        : null;
      return storage !== undefined && storage !== null ? storage.fields.length * 27 + 28 : 100;
    },
  },

  created() {
    this.$store.dispatch('dataQuery/getMinResultHistory').then(_ => {
      this.initDefaultActiveItem();
    });
  },

  methods: {
    async handHistoryQuery(item, index, updateRtid = false, viewMoreDetail = true, queryHistory = false) {
      if (viewMoreDetail) {
        await this.handViewMoreDetail(item, index, false, !item.show);
      }
      this.$store.dispatch('dataQuery/reOrderHistoryList', {
        item,
        index,
      });
      this.$store.dispatch('dataQuery/setActiveHistoryItem', {
        item: this.currentActiveItem,
      });
      if (updateRtid) {
        this.$store.commit('dataQuery/setResultTable', item.name.result_table_id);
        this.$store.commit('dataQuery/setActiveBizid', item.name.bk_biz_id);
      }

      queryHistory
        && this.$nextTick(() => {
          Bus.$emit('DataQuery-QueryResult-History');
        });
    },
    getHistoryDesciption(item) {
      return {
        boundary: document.body,
        appendTo: document.body,
        placement: 'right',
        content: `<div>
                              <div><span class="subtitle">${this.$t('字段名称')}</span> : <span class="text">${
          item.name.result_table_id
        }</span></div>
                              <div><span class="subtitle">${this.$t('中文描述')}</span> : <span class="text">${
          item.name.description
        }</span></div>
                              <div><span class="subtitle">${this.$t('创建时间')}</span> : <span class="text">${
          item.name.created_at
        }</span></div>
                              <div><span class="subtitle">${this.$t('数据更新时间')}</span> : <span class="text">${
          item.name.latest_data_time
        }</span></div>
                          </div>`,
        zIndex: '1001',
      };
    },
    getHistoryFieldsDesciption(fields) {
      return {
        boundary: document.body,
        placement: 'right',
        html: `<div>
                              <div><span class="subtitle">${this.$t('字段名称')}</span> : <span class="text">${
          fields.physical_field
        }</span></div>
                              <div><span class="subtitle">${this.$t('字段类型')}</span> : <span class="text">${
          fields.physical_field_type
        }</span></div>
                              <div><span class="subtitle">${this.$t('字段描述')}</span> : <span class="text">${
          fields.field_alias
        }</span></div>
                          </div>`,
        zIndex: '1001',
      };
    },

    async handViewMoreDetail(item, index, autoSearch = false, triggerShow = true) {
      for (let i = 0; i < this.historyList.length; i++) {
        if (this.historyList[i].name.result_table_id !== item.name.result_table_id) {
          this.historyList[i].show = false;
        }
      }

      if (triggerShow) {
        item.show = !item.show;
      }
      if (item.show) {
        item.isloading = true;
        this.$store.commit('dataQuery/setResultTable', item.name.result_table_id);
        const res = await this.bkRequest.httpRequest('meta/getSchemaAndSqlBase', {
          params: { rtid: item.name.result_table_id },
        });
        if (res.result) {
          const defaultOrder = res.data.order.find(order => !/^es/i.test(order)) || res.data.order[0];
          (!this.currentActiveType || !this.isInitDefaultHistory)
            && this.$store.commit('dataQuery/setClusterType', defaultOrder);
          item.activeType = defaultOrder;
          this.$store.dispatch('dataQuery/setViewOrders', {
            orders: res.data.order,
          });
          item.info = res.data;
          this.currentActiveItem = res.data;
          this.formatStorageData(item.info.storage);
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        item.isloading = false;

        if (this.isInitDefaultHistory) {
          this.isInitDefaultHistory = false;
        }

        autoSearch && this.handHistoryQuery(item, index, false, false);
      }
    },
    formatStorageData(data) {
      Object.keys(data).forEach(key => {
        (data[key].fields || []).forEach(field => {
          let icons = [];

          field.is_dimension
            && icons.push({
              description: window.$t('维度'),
              judge: 'is_dimension',
              key: 'dimension',
            });

          field.is_time
            && icons.push({
              description: window.$t('时间'),
              judge: 'is_time',
              key: 'time',
            });

          const analyzedFields = data[key].config.analyzedFields || [];
          analyzedFields.includes(field.physical_field)
            && icons.push({
              description: window.$t('分词'),
              judge: 'is_analyzed',
              key: 'analyzed',
            });
          Object.assign(field, { icons: icons });
        });
      });
    },
    handGoToTask(resultId) {
      this.$router.push('/routehub/?rtid=' + resultId);
    },
    handDeleteHistory(item) {
      this.deleteLoading = true;
      this.axios['delete']('result_tables/' + item.name.result_table_id + '/delete_selected_history/').then(res => {
        if (res.result) {
          this.historyList.forEach((element, index) => {
            if (element.name.result_table_id === item.name.result_table_id) {
              this.historyList.splice(index, 1);
            }
          });
        } else {
          postMethodWarning(res.message, 'error');
        }
        this.deleteLoading = false;
      });
    },
    initDefaultActiveItem() {
      if (this.$route.params.defaultResultId) {
        this.$nextTick(() => {
          const activeItemIndex = this.historyList.findIndex(
            item => item.name.result_table_id === this.$route.params.defaultResultId
          );
          activeItemIndex >= 0 && this.handHistoryQuery(this.historyList[activeItemIndex], activeItemIndex);
        });
      }
    },
  },
};
</script>
<style>
.z-index-1001 {
  z-index: 1001 !important;
}
</style>

<style lang="scss" scoped>
.choose-history {
  height: calc(100% - 200px);
  overflow-y: auto;
  overflow-x: hidden;
  /* 设置滚动条的样式 */
  &::-webkit-scrollbar {
    width: 5px;
    background-color: #f5f5f5;
  }

  /* 滚动槽 */
  &::-webkit-scrollbar-track {
    -webkit-box-shadow: inset 0 0 6px #eee;
    box-shadow: inset 0 0 6px #eee;
    border-radius: 10px;
    background-color: #f5f5f5;
  }

  /* 滚动条滑块 */
  &::-webkit-scrollbar-thumb {
    border-radius: 10px;
    background-color: rgba(0, 0, 0, 0.3);
  }
  &::-webkit-scrollbar-thumb {
    border-radius: 10px;
    background-color: rgba(0, 0, 0, 0.3);
  }
  .title {
    padding: 24px 11px 20px;
    border-bottom: 1px solid #eee;
    i {
      margin-right: 5px;
      display: inline-block;
      vertical-align: -2px;
      font-size: 18px;
    }
  }
  .slide-enter-active,
  .slide-leave-active {
    overflow: hidden;
  }
  .slide-enter,
  .slide-leave-to {
    height: 0 !important;
  }
  .history-list {
    min-height: 45px;
    padding-bottom: 20px;
    > li {
      border-bottom: 1px solid #eee;
      min-height: 42px;
      &.active {
        .list-title {
          color: #3a84ff;
          background: #e1ecff;
          font-weight: bold;
          &:hover {
            background: #e1ecff;
          }
        }
      }
      .list-title {
        outline: none;
        cursor: pointer;
        padding: 0 10px 0 38px;
        display: -webkit-box;
        position: relative;
        display: flex;
        justify-content: flex-end;
        height: 40px;
        &:hover {
          background: #eee;
          color: #3a84ff;
          .title-text {
            width: 163px;
            margin-right: 10px;
          }
          .button {
            display: block;
          }
        }
      }
      .list-content {
        padding: 0 0px 20px;
        transition: all linear 0.2s;
        background: #fff;
        overflow: hidden;
        .content-title {
          padding: 15px 0 10px;
          font-weight: bold;
        }
        .content-table {
          font-size: 12px;
          color: #737987;
        }
        .content-list {
          padding: 8px 0 0px;
          line-height: 26px;
          cursor: pointer;
          li {
            outline: none;
            padding: 0 20px 0 37px;
            &:hover {
              background: #eee;
              .text {
                color: #3a84ff;
              }
            }
          }
          .text {
            line-height: 26px;
            position: relative;
            .text-title {
              max-width: 130px;
              overflow: hidden;
              text-overflow: ellipsis;
              white-space: nowrap;
            }
            &:hover {
              .text-title {
                color: #3a84ff;
              }
            }
          }
          .tag {
            display: inline-block;
            text-align: center;
            font-size: 10px;
            font-weight: bold;
            position: relative;
            transform: scale(0.8);
            color: #bbbec5;
            .info-tips {
              display: none;
              position: absolute;
              left: 50%;
              bottom: 26px;
              color: #fff;
              background: #212232;
              padding: 9px;
              min-width: 50px;
              border-radius: 2px;
              transform: translate(-50%, 0);
              &:after {
                position: absolute;
                left: 50%;
                bottom: -6px;
                content: '';
                width: 0;
                height: 0px;
                border: 3px solid #fff;
                border-color: #212232 transparent transparent transparent;
                transform: translate(-50%, 0);
              }
            }
            &:hover {
              color: #3a84ff;
              .info-tips {
                display: block;
              }
            }
          }
          .info {
            margin: 0 0 0 10px;
            .bk-icon {
              vertical-align: middle;
            }
          }
          .field-type {
            color: #c3cdd7;
          }
          > .no-data {
            height: 100px;
          }
        }
        .Masking {
          width: calc(100% - 318px);
          height: 20px;
          position: fixed;
          top: 108px;
          // left: 320px;
          background: #fff;
          z-index: 4;
          min-width: 882px;
        }
        .noMain {
          text-align: center;
          position: absolute;
          left: 50%;
          top: 50%;
          transform: translate(-50%, -50%);
          p {
            margin-top: 22px;
            font-size: 18px;
            color: #c3cdd7;
            font-weight: bold;
          }
        }
        table {
          width: 100%;
          tr {
            border: 1px solid #eee;
          }
          td {
            padding: 7px 10px;
            border-right: 1px solid #eee;
          }
          td:first-of-type {
            background: #fafafa;
          }
        }
      }
    }
    .title-text {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      padding: 11px 0;
      width: calc(100% - 108px);
      position: absolute;
      left: 38px;
    }
    .button {
      /*float: right;*/
      display: none;
      padding: 9px 0;
      > .bk-icon {
        display: inline-block;
        width: 30px;
        text-align: center;
        color: #737987;
        line-height: 45px;
      }
      .icon-button {
        text-align: center;
        min-width: 23px;
        height: 22px;
        border-radius: 2px;
        background: none;
        border: none;
        position: relative;
        overflow: initial;
        display: block;
        float: left;
        &:hover {
          background: #3a84ff;
          color: #fff;
        }
        .bk-icon {
          width: 23px;
          height: 22px;
          line-height: 22px;
          text-align: center;
        }
      }
    }
    .left-icon {
      font-size: 14px;
      position: absolute;
      left: 13px;
      top: 14px;
      transition: all linear 0.1s;
    }
  }

  .no-data {
    display: block;
    text-align: center;
    padding: 65px 0;
    p {
      color: #cfd3dd;
    }
  }
}
</style>
