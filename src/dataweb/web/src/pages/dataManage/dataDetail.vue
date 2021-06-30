

<!--
  -  Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
  -
  -  Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
  -
  -   BK-BASE 蓝鲸基础计算平台 is licensed under the MIT license.
  -
  -  A copy of the MIT License is included in this file.
  -
  -  Terms of the MIT License:
  -  ---------------------------------------------------
  -  Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  -  documentation files (the "Software"), to deal in the Software without restriction, including without limitation
  -  the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
  -  and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  -
  -  The above copyright notice and this permission notice shall be included in all copies or substantial
  -  portions of the Software.
  -
  -  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
  -  LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
  -  NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  -  WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  -  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
  -->

<template>
  <div class="data-detail">
    <!-- 数据start -->
    <!-- <template> -->
    <!-- 数据量start -->
    <div class="box-wrapper mt20">
      <div class="chart-wrapper clearfix">
        <div class="left-chart">
          <minuteData ref="minute"
            :loading="loading"
            :recentWrapperBg="recentWrapperBg"
            :minuteParams="minuteParams" />
        </div>
        <div class="right-chart">
          <dayData ref="dayData"
            :loading="loading"
            :dayParams="dayParams" />
        </div>
      </div>
    </div>
    <!-- 数据量end -->
    <bkdata-tab class="mt20">
      <bkdata-tab-panel :label="$t('最近上报数据')"
        name="dataCount">
        <!-- 最近上报数据start -->
        <div class="box-wrapper">
          <dataReported ref="dataReported"
            :loading="loading"
            :recentWrapperBg="recentWrapperBg"
            :reportedData="reportedData">
            <template slot="content">
              <img alt
                src="../../common/images/no-data.png">
              <p>{{ $t('暂无数据') }}</p>
            </template>
          </dataReported>
        </div>
        <!-- 最近上报数据end -->
      </bkdata-tab-panel>
    </bkdata-tab>
    <!-- </template> -->
    <!-- 数据end -->
    <dataDialog :loading="loading" />
  </div>
</template>

<script type="text/javascript">
import '@/bizComponents/dataTrend/scss/index.scss';
import mixin from '@/bizComponents/dataTrend/mixin.js';
import { postMethodWarning } from '@/common/js/util.js';
import minuteData from '@/bizComponents/dataTrend/components/minuteData.vue';
import dayData from '@/bizComponents/dataTrend/components/dayData.vue';
import dataReported from '@/bizComponents/dataTrend/components/dataReported.vue';
import dataDialog from '@/bizComponents/dataTrend/components/dataDialog.vue';

export default {
  components: {
    minuteData,
    dayData,
    dataReported,
    dataDialog
  },
  mixins: [mixin],
  mounted() {
    this.init();
  },
  methods: {
    init() {
      this.loading.nodeLoading = true;
      /*
                    初始化时间
                */
      let myDate = new Date();
      let minDate = new Date(new Date().getTime() - 24 * 60 * 60 * 1000); // 24小时前
      let minTime = new Date(new Date().getTime() - 91 * 24 * 60 * 60 * 1000); // 3个月前
      this.min_time = minTime.getFullYear() + '-' + (minTime.getMonth() + 1) + '-' + minTime.getDate();
      this.max_time = myDate.getFullYear() + '-' + (myDate.getMonth() + 1) + '-' + myDate.getDate();
      this.minuteParams.start_time = minDate.getFullYear() + '-' + (minDate.getMonth() + 1) + '-'
            + minDate.getDate() + ' ' + minDate.getHours() + ':' + minDate.getMinutes() + ':' + minDate.getSeconds();
      this.minuteParams.end_time = myDate.getFullYear() + '-' + (myDate.getMonth() + 1) + '-' + myDate.getDate()
            + ' ' + myDate.getHours() + ':' + myDate.getMinutes() + ':' + myDate.getSeconds();
      this.minuteParams.date = [this.minuteParams.start_time, this.minuteParams.end_time];
      this.$refs.minute.getMinDataAmount();
      let dayDate = new Date(new Date().getTime() - 6 * 24 * 60 * 60 * 1000); // 一周前面
      this.dayParams.start_time = dayDate.getFullYear() + '-' + (dayDate.getMonth() + 1)
            + '-' + dayDate.getDate() + ' 00:00:00';
      this.dayParams.end_time = myDate.getFullYear() + '-' + (myDate.getMonth() + 1)
            + '-' + myDate.getDate() + ' 23:59:59';
      this.dayParams.date = [this.dayParams.start_time, this.dayParams.end_time];
      this.$refs.dayData.getDayDataAmount();
      /*
                    获取最近上报数据
                */
      this.axios.get('/v3/databus/rawdatas/' + this.$route.params.did + '/tail/').then(res => {
        if (res.result) {
          this.reportedData = [];
          for (let i = 0; i < res.data.length; i++) {
            this.reportedData.push({
              text: this.$refs.dataReported.formatTailData(res.data[i].value),
              source: res.data[i].value,
              show: false,
              copy_Sucess: false,
            });
          }
        } else {
          // this.getMethodWarning(res.message, res.code)
        }
        this.loading.reportedLoading = false;
      });
    },
  },
};
</script>
