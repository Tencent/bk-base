

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
            <i class="bk-icon icon-check-circle-shape" /><span>{{ $t('提交成功_数据正在接入中') }}</span>
          </div>
          <!-- <div class="access-modal-h2">部分场景的特殊说明，如：XXXX</div> -->
          <div class="access-modal-h2">
            {{ lastTime }}{{ $t('秒后自动跳转到') }} [<a href="javascript:void(0);"
              @click="handleEnterDetail">
              {{ $t('接入详情') }}
            </a>]
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
              <span class="item-header-content"><i class="bk-icon icon-order" />{{ $t('接入详情') }}</span>
            </div>
            <div class="item-line" />
            <div class="item-body">
              <div class="item-row">
                {{ $t('查看数据接入进度') }}
              </div>
              <div class="item-row">
                {{ $t('查询原始数据') }}
              </div>
              <div class="item-row">
                {{ $t('采集任务管理') }}
              </div>
              <div class="item-row is-disabled">
                {{ $t('数据权限管理') }}
              </div>
              <div class="item-row">
                {{ $t('数据监控') }}
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
              <span class="item-header-content"><i class="bk-icon icon-dataflow" />{{ $t('数据清洗') }}</span>
            </div>
            <div class="item-line" />
            <div class="item-body">
              <div class="item-row">
                {{ $t('数据格式化') }}
              </div>
              <div class="item-row is-disabled">
                {{ $t('数据标准化') }}
              </div>
              <div class="item-row">
                {{ $t('清洗任务管理') }}
              </div>
              <div class="item-row">
                {{ $t('数据流监控') }}
              </div>
            </div>
            <div class="item-footer">
              <a disabled="disabled"
                href="javascript:void(0);">
                {{ $t('进入') }}
              </a>
            </div>
          </div>
          <div class="access-option-item">
            <div class="item-header">
              <span class="item-header-content"><i class="bk-icon icon-data2-shape" />{{ $t('数据入库') }}</span>
              <span
                v-tooltip="{
                  content: $t('如果数据结构比较复杂_建议先进行数据清洗_以便后续进行数据开发和分析'),
                }"
                class="item-header-tips">
                <i class="bk-icon icon-exclamation-circle-shape" />
              </span>
            </div>
            <div class="item-line" />
            <div class="item-body db-model">
              <div class="item-row">
                {{ $t('关系型数据库') }}：<span>Tspider(MySQL)</span>
              </div>
              <div class="item-row">
                {{ $t('时序性数据库') }}：<span>InfluxDB、Druid</span>
              </div>
              <div class="item-row">
                {{ $t('分析性数据库') }}：<span>Druid、Hermes</span>
              </div>
              <div class="item-row">
                {{ $t('消息队列') }}：<span>Redis、kafka</span>
              </div>
              <div class="item-row">
                {{ $t('日志检索') }}：<span>ElesticSearch</span>
              </div>
              <div class="item-row">
                {{ $t('文件系统') }}：<span>HDFS</span>
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
        <div />
      </div>
    </div>
  </div>
</template>
<script>
export default {
  components: {},
  data() {
    return {
      lastTime: 10,
      timeoutId: 0,
    };
  },
  computed: {
    rawDataId() {
      return this.$route.params.rawDataId;
    },
  },
  mounted() {
    this.$nextTick(() => {
      this.timeCounter();
    });
  },
  beforeDestroy() {
    this.timeoutId && clearTimeout(this.timeoutId);
  },
  methods: {
    handleEnterDetail() {
      this.timeoutId && clearTimeout(this.timeoutId);
      this.$router.push({
        name: 'data_detail',
        params: {
          did: this.rawDataId,
          tabid: '2',
        },
        hash: '#list',
        query: { page: 1 },
      });
      // this.$router.push({path: `/data-access/data-detail/${this.rawDataId}`, hash: '#list', query: {page: 1}})
    },
    timeCounter() {
      this.timeoutId && clearTimeout(this.timeoutId);
      this.timeoutId = setTimeout(() => {
        this.lastTime--;
        if (this.lastTime) {
          this.timeCounter();
        } else {
          this.handleEnterDetail();
        }
      }, 1000);
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
    width: 1024px;
    // height: 524px;

    .access-modal-content {
      width: 762px;
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
          width: 242px;
          // height: 222px;
          border: solid 1px #72bcee;
          background: #f1f9ff;
          padding: 0 5px 25px 5px;
          position: relative;

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
