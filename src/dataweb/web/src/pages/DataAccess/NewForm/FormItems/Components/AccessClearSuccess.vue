

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
  <div class="access-modal-layer">
    <div class="access-modal-body">
      <div class="access-modal-content">
        <div class="access-modal-header">
          <div class="access-modal-h1">
            <i class="bk-icon icon-check-circle-shape" />
            <span>{{ $t('提交成功_数据正在清洗中') }}</span>
          </div>
          <div class="access-modal-h2">
            {{ lastTime }}{{ $t('秒后自动跳转到') }} [
            <a href="javascript:void(0);"
              @click="handleEnterDetail">
              {{ $t('清洗详情') }}
            </a>]
            <a :disabled="stopTimer"
              href="javascript:void(0);"
              @click="handleStopTimer">
              {{ $t('停止') }}
            </a>
          </div>
        </div>
        <div class="access-modal-header sub">
          <div class="sub-1">
            {{ $t('接入后数据会放入消息队列缓存_请及时配置计算_入库') }}
          </div>
          <div class="sub-2">
            {{ $t('您可以进行以下操作') }}：
          </div>
        </div>
        <div class="access-option-items">
          <div class="access-option-item">
            <div class="item-header">
              <span class="item-header-content"> <i class="bk-icon icon-order" />{{ $t('清洗详情') }} </span>
            </div>
            <div class="item-line" />
            <div class="item-body">
              <div class="item-row">
                {{ $t('清洗前后数据对比') }}
              </div>
              <div class="item-row">
                {{ $t('清洗任务管理') }}
              </div>
              <div class="item-row is-disabled">
                {{ $t('数据权限管理') }}
              </div>
              <div class="item-row">
                {{ $t('数据流监控') }}
              </div>
            </div>
            <div class="item-footer">
              <a href="javascript:void(0);"
                @click="handleEnterDetail">
                {{ $t('进入') }}
              </a>
            </div>
          </div>
          <div class="access-option-item">
            <div class="item-header">
              <span class="item-header-content"> <i class="bk-icon icon-dataflow" />{{ $t('数据开发') }} </span>
            </div>
            <div class="item-line" />
            <div class="item-body">
              <div class="item-row">
                {{ $t('实时计算') }}
              </div>
              <div class="item-row">
                {{ $t('离线计算') }}
              </div>
              <div class="item-row is-disabled">
                {{ $t('机器学习') }}
              </div>
              <div class="item-row">
                {{ $t('数据流监控') }}
              </div>
              <div class="item-row">
                {{ $t('计算任务管理') }}
              </div>
            </div>
            <div class="item-footer">
              <a href="javascript:void(0);"
                @click="handleEnterFlow">
                {{ $t('进入') }}
              </a>
            </div>
          </div>
          <div class="access-option-item">
            <div class="item-header">
              <span class="item-header-content"> <i class="bk-icon icon-data2-shape" />{{ $t('数据入库') }} </span>
              <span
                v-tooltip="{
                  content: '如果数据结构比较复杂(如多层嵌套的Json)，建议先进行数据清洗，以便后续进行数据开发和分析',
                }"
                class="item-header-tips">
                <i class="bk-icon icon-exclamation-circle-shape" />
              </span>
            </div>
            <div class="item-line" />
            <div class="item-body db-model">
              <div class="item-row">
                {{ $t('关系型数据库') }}：
                <span>Tspider(MySQL)</span>
              </div>
              <div class="item-row">
                {{ $t('时序性数据库') }}：
                <span>InfluxDB、Druid</span>
              </div>
              <div class="item-row">
                {{ $t('分析性数据库') }}：
                <span>Druid、Hermes</span>
              </div>
              <div class="item-row">
                {{ $t('消息队列') }}：
                <span>Redis、kafka</span>
              </div>
              <div class="item-row">
                {{ $t('日志检索') }}：
                <span>ElesticSearch</span>
              </div>
              <div class="item-row">
                {{ $t('文件系统') }}：
                <span>HDFS</span>
              </div>
            </div>
            <div class="item-footer">
              <a href="javascript:void(0);"
                @click="handleEnterStorage">
                {{ $t('进入') }}
              </a>
            </div>
          </div>
          <div class="access-option-item">
            <div class="item-header">
              <span class="item-header-content is-disabled">
                <i class="bk-icon icon-data2-shape" />{{ $t('数据分析(BI)') }}
              </span>
            </div>
            <div class="item-line" />
            <div class="item-body">
              <div class="item-row is-disabled">
                {{ $t('数据可视化') }}
              </div>
              <div class="item-row is-disabled">
                {{ $t('交互式分析') }}
              </div>
              <div class="item-row is-disabled">
                {{ $t('仪表盘') }}
              </div>
              <div class="item-row is-disabled">
                {{ $t('数据报表') }}
              </div>
            </div>
            <div class="item-footer">
              <a disabled="disabled"
                href="javascript:void(0);">
                {{ $t('进入') }}
              </a>
            </div>
          </div>
        </div>
      </div>
    </div>
    <bkdata-dialog
      v-model="cleanUpdateShow"
      theme="primary"
      :maskClose="false"
      :loading="updateLoading"
      headerPosition="left"
      :title="$t('清洗表已更新')"
      :width="750"
      @confirm="updateStorage"
      @after-leave="timeCounter">
      <p class="clean-update-content"
        style="margin-bottom: 20px">
        {{ $t('是否更新当前清洗表对应的入库配置') }}
      </p>
      <bkdata-table :data="cleanExtraData"
        :okText="$t('更新')">
        <bkdata-table-column :label="$t('结果数据表')"
          prop="result_table_id" />
        <bkdata-table-column :label="$t('存储类型')"
          prop="cluster_type" />
        <bkdata-table-column :label="$t('创建人')"
          prop="created_by" />
        <bkdata-table-column :label="$t('修改时间')"
          prop="updated_at" />
      </bkdata-table>
    </bkdata-dialog>
  </div>
</template>
<script>
import { showMsg } from '@/common/js/util.js';
export default {
  name: 'AccessClearSuccess',
  // props: {
  //     rawDataId: {
  //         type: [String, Number],
  //         default: ''
  //     }
  // },
  data() {
    return {
      lastTime: 10,
      stopTimer: false,
      timerId: 0,
      cleanUpdateShow: false,
      updateLoading: false,
    };
  },
  computed: {
    rawDataId() {
      return this.$route.params.rawDataId;
    },
    cleanExtraData() {
      return this.$route.params.cleanExtraData;
    },
  },
  mounted() {
    if (this.cleanExtraData.length) {
      this.cleanUpdateShow = true;
      return;
    }
    this.$nextTick(() => {
      this.timeCounter();
    });
  },
  beforeDestroy() {
    this.timerId && clearTimeout(this.timerId);
  },
  methods: {
    updateStorage() {
      this.updateLoading = true;
      const request = this.cleanExtraData.map(item => {
        return this.bkRequest.httpRequest('dataClear/updateStorageByCleanResult', {
          params: {
            clusterType: item.cluster_type,
            rtID: item.result_table_id,
          },
        });
      });
      Promise.all(request).then(value => {
        const result = value.every(item => item.result);
        if (result) {
          showMsg(this.$t('更新成功'), 'success');
          this.cleanUpdateShow = false;
          this.updateLoading = false;
        } else {
          this.updateLoading = false;
          const errorItem = value.find(item => !item.result);
          this.getMethodWarning(errorItem.message, errorItem.code);
        }
      });
    },
    handleStopTimer() {
      this.stopTimer = true;
    },
    handleEnterStorage() {
      this.clearTimer();
      this.$router.push({
        name: 'data_detail',
        params: {
          did: this.rawDataId,
          tabid: '4',
        },
      });
    },
    handleEnterDetail() {
      this.clearTimer();
      this.$router.push({
        name: 'data_detail',
        params: {
          did: this.rawDataId,
          tabid: '3',
        },
      });
    },
    handleEnterFlow() {
      this.clearTimer();
      this.$router.push({
        name: 'dataflow_ide',
        params: {},
      });
    },
    clearTimer() {
      clearTimeout(this.timerId);
      this.stopTimer = true;
      this.timerId = 0;
    },
    timeCounter() {
      if (!this.stopTimer) {
        this.timerId = setTimeout(() => {
          this.lastTime--;
          if (this.lastTime) {
            this.timeCounter();
          } else {
            !this.stopTimer && this.handleEnterDetail();
          }
        }, 1000);
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.access-modal-layer {
  background: #e9ecf2;
  height: 100%;
  position: relative;
  min-height: 600px;
  display: flex;
  justify-content: center;
  align-items: center;

  .access-modal-body {
    background: #fff;
    width: 1224px;
    height: 524px;
    padding: 0 100px;

    .access-modal-content {
      // width: 762px;
      margin: 35px auto;

      .access-modal-header {
        color: #2699fb;
        margin: 25px 0 50px;
        .access-modal-h1 {
          font-size: 36px;
          line-height: 70px;
          position: relative;
          i {
            position: absolute;
            left: 0;
            top: 50%;
            transform: translate(-100%, -50%);
            padding-right: 15px;
          }
        }

        .access-modal-h2 {
          font-size: 18px;
          line-height: 26px;

          a {
            &[disabled] {
              color: #ddd;
              cursor: not-allowed;
            }
          }
        }

        &.sub {
          margin-bottom: 40px;
          .sub-2 {
            color: #696f72;
            margin: 10px 0;
          }
        }
      }

      .access-option-items {
        display: flex;
        justify-content: space-between;

        .access-option-item {
          min-width: 242px;
          height: 222px;
          border: solid 1px #72bcee;
          background: #f1f9ff;
          padding: 0 5px 25px 5px;
          position: relative;
          margin-right: 18px;
          &:last-child {
            margin-right: 0;
          }

          .item-header {
            height: 42px;
            display: flex;
            align-items: center;
            position: relative;
            .item-header-content {
              display: flex;
              align-items: center;
              font-size: 16px;
              color: #2699fb;
              font-weight: 600;

              &.is-disabled {
                color: #b7bcbf;
              }

              i {
                padding: 0 10px;
                color: #747474;
              }
            }

            .item-header-tips {
              position: absolute;
              right: 10px;
              top: 50%;
              transform: translateY(-50%);
              color: #2699fb;
              &:hover {
                cursor: pointer;
              }
            }
          }

          .item-line {
            height: 1px;
            background: #d1cfd0;
            border: 0;
            width: 100%;
          }

          .item-body {
            margin: 0 35px;
            .item-row {
              margin-top: 8px;
              color: #747474;

              &.is-disabled {
                color: #b7bcbf;
              }
            }

            &.db-model {
              margin: 0 10px;
              .item-row {
                display: flex;
                flex-wrap: nowrap;
                span {
                  color: #2469ad;
                }
              }
            }
          }

          .item-footer {
            position: absolute;
            bottom: 5px;
            left: 5px;
            right: 5px;
            height: 20px;
            display: flex;
            justify-content: flex-end;

            [disabled='disabled'] {
              cursor: not-allowed;
              color: #b7bcbf;
            }
          }
        }
      }
    }
  }
}
</style>
