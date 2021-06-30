

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
  <div class="operating">
    <bkdata-dialog v-model="operatingInfor.isShow"
      :padding="0"
      extCls="bkdata-dialog"
      :hasHeader="false"
      :hasFooter="false"
      :maskClose="true"
      :width="operatingInfor.width">
      <div class="operating-infor">
        <div class="hearder pl20">
          <span class="close"
            @click="close">
            <i :title="$t('关闭')"
              class="bk-icon icon-close" />
          </span>
          <!-- <div class="icon fl"><i class="bk-icon icon-folder"></i></div> -->
          <div class="text fl">
            <p class="title">
              {{ $t('最近运行信息') }}
            </p>
            <!-- <p>{{ $t('您的数据将以项目的形式进行组织') }}</p> -->
          </div>
        </div>
        <div v-bkloading="{ isLoading: loading }"
          class="info-body">
          <template v-if="deployInfo.logs">
            <div v-if="deployInfo.logs.length === 0"
              class="no-warning">
              <img src="../../../common/images/no-data.png"
                alt="">
              <p>{{ $t('暂无运行信息') }}</p>
            </div>
            <ul v-else
              class="info-log-report">
              <li v-for="(item, index) of deployInfo.logs"
                :key="index"
                :class="['report-item', item.level]">
                <div class="report-title">
                  <span class="title">[{{ item.time }}}] {{ item.message }}</span>
                </div>
              </li>
            </ul>
          </template>
        </div>
      </div>
    </bkdata-dialog>
  </div>
</template>
<script type="text/javascript">
export default {
  data() {
    return {
      loading: false,
      operatingInfor: {
        isShow: false,
        width: 900,
      },
      deployInfo: [],
    };
  },
  methods: {
    // 获取最近运行信息
    getDeployInfo(flow) {
      if (flow.flow_id) {
        this.loading = true;
        this.axios.get(`flows/${flow.flow_id}/get_latest_deploy_info/`).then(res => {
          if (res.result) {
            this.deployInfo = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.loading = false;
        });
      }
    },
    open(flow) {
      this.getDeployInfo(flow);
      this.operatingInfor.isShow = true;
    },
    close() {
      this.operatingInfor.isShow = false;
    },
  },
};
</script>
<style lang="scss" type="text/css" media="screen">
.operating-infor {
  .hearder {
    height: 60px;
    background: #23243b;
    position: relative;
    line-height: 60px;
    .close {
      display: inline-block;
      position: absolute;
      right: 0;
      top: 0;
      width: 40px;
      height: 40px;
      line-height: 40px;
      text-align: center;
      cursor: pointer;
    }
    .icon {
      font-size: 32px;
      color: #abacb5;
      line-height: 118px;
      width: 142px;
      text-align: right;
      margin-right: 16px;
    }
    .title {
      color: #fafafa;
      font-size: 18px;
      position: absolute;
      top: 50%;
      transform: translate(0, -50%);
    }
  }
  .info-body {
    width: 100%;
    height: 500px;
    background-color: #fff;
    overflow-y: auto;
    position: relative;
  }
  .no-warning {
    text-align: center;
    position: absolute;
    left: 50%;
    top: 50%;
    transform: translate(-50%, -50%);
    p {
      color: #cfd3dd;
      font-size: 14px;
    }
  }
  .info-log-report {
    margin: 15px 0;
    color: #434343;
    .report-item {
      padding: 0 20px;
      word-break: break-all;
      border-top: 1px solid #eaeaea;
      color: #737987;
      &.ERROR {
        color: #fe771d;
      }
      &:hover {
        background: #fff3ed;
      }
      &:last-of-type {
        border-bottom: 1px solid #eaeaea;
      }
    }
    .report-title {
      min-height: 34px;
      line-height: 34px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
      .title {
        white-space: nowrap;
        text-overflow: ellipsis;
        overflow: hidden;
      }
    }
  }
}
</style>
