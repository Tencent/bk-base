

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
  <Container class="request-example">
    <span class="bk-item-des">{{ $t('URL_时间参数格式__集周期填写后_可以查看完整的轮询过程') }}</span>
    <ul v-bkloading="{ isLoading: httpLoading }"
      class="polling-process">
      <li>
        <span v-for="(item, index) in calUrList"
          :key="index">
          <label class="">{{ $t('第') }}{{ item.index }}{{ $t('次') }}：</label>
          <p class="interval">now+{{ item.start }}~now+{{ item.end }}，</p>
          <p>{{ time }}{{ calFormatTime.display }}，</p>
        </span>
      </li>
      <li>
        <span v-for="(item, index) in calUrList"
          :key="index">
          <p v-bk-tooltips.top="item.url"
            class="polling-url">
            URL: {{ item.url }}
          </p>
          <p class="polling-icon">
            <i
              :class="['bk-icon', (item.success && `icon-${item.icon}`) || 'icon-debug']"
              @click="handleHttpDebug(item)" />
          </p>
        </span>
      </li>
    </ul>
    <ul>
      <li>
        <p>...</p>
      </li>
    </ul>
    <bkdata-dialog
      v-model="debugShow"
      extCls="bkdata-dialog access-http-demo"
      :content="'component'"
      :hasHeader="false"
      :closeIcon="true"
      :hasFooter="false"
      :maskClose="false"
      :width="800"
      @confirm="debugShow = !debugShow"
      @cancel="debugShow = !debugShow">
      <div class="debug-list">
        <pre id="httpResult" />
        <p />
      </div>
    </bkdata-dialog>
  </Container>
</template>
<script>
import Bus from '@/common/js/bus';
import Container from './ItemContainer';
import * as moment from 'moment';
export default {
  components: { Container },
  props: {
    bizid: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
      clickDebug: false,
      httpLoading: false,
      url: '',
      time: 5,
      demoList: [1, 2, 3],
      timeFormat: '',
      timeUnit: 'm',
      formatTime: {
        m: {
          timeParam: 'minutes',
          display: window.$t('分钟'),
        },
        d: {
          timeParam: 'days',
          display: window.$t('天'),
        },
        h: {
          timeParam: 'hours',
          display: window.$t('小时'),
        },
      },
      debugShow: false,
      httpDebugResult: '',
      validate: {
        content: window.$t('请选择时间参数格式'),
        visible: false,
        class: 'error-red',
      },
    };
  },
  computed: {
    calFormatTime() {
      return this.formatTime[this.timeUnit];
    },
    calFormat: {
      get() {
        return this.timeFormat || 'YYYY-MM-DD hh:mm:ss';
      },
      set(newValue) {
        this.timeFormat = newValue;
      },
    },
    calUrList() {
      const _now = new Date();
      const minute = _now.getMinutes();
      return new Array(...this.demoList).map(item => {
        const start = this.time * item;
        const end = this.time * (item + 1);
        switch (this.calFormat) {
          case 'YYYY-MM-DD\'T\'HH:MM:SSXXX':
            this.calFormat = 'YYYY-MM-DD\'T\'HH:MM:SS.sss';
            break;
          case 'UNIX Time Stamp(mins)':
            this.calFormat = 'XX';
            break;
          case 'UNIX Time Stamp(seconds)':
            this.calFormat = 'X';
            break;
          case 'UNIX Time Stamp(milliseconds)':
            this.calFormat = 'XXX';
            break;
        }
        let obj = {
          index: item,
          start: start,
          end: end,
          startTime:
            this.calFormat === 'XX'
              ? Math.floor(this.momentTime(this.addTimes(_now, start), 'X') / 60)
              : this.momentTime(this.addTimes(_now, start), this.calFormat),
          endTime:
            this.calFormat === 'XX'
              ? Math.floor(this.momentTime(this.addTimes(_now, end), 'X') / 60)
              : this.momentTime(this.addTimes(_now, end), this.calFormat),
        };
        const url = this.url.replace('<start>', obj.startTime).replace('<end>', obj.endTime);
        const encodeUrl = this.url
          .replace('<start>', encodeURI(obj.startTime))
          .replace('<end>', encodeURI(obj.endTime));
        return Object.assign({}, obj, { url: url, encodeUrl: encodeUrl });
      });
    },
  },
  watch: {
    // timeFormat (val) {
    //     this.validateParams()
    // },
    bizid(val) {
      this.clickDebug && this.validateParams();
    },
  },
  mounted() {
    Bus.$on('access.http', http => {
      http.url && this.$set(this, 'url', http.url);
      http.timeFormat && this.$set(this, 'timeFormat', http.timeFormat);
      // http.timeUnit && this.$set(this, 'timeUnit', http.timeUnit.split(',').slice(-1)[0])
      this.clickDebug && this.validateParams();
    });

    Bus.$on('access.methods.period', http => {
      let { period, timeUnit } = http;
      if (isNaN(period) || period === null || period < 0) {
        period = 0;
      }
      this.time = period || 0;
      this.timeUnit = timeUnit;
    });
  },
  beforeDestroy() {
    this.validate.visible = false;
  },
  methods: {
    momentTime(time, format) {
      let transFormat = format.replace(/XXX/g, 'Z').replace(/[y{2,4}d{2}]/g, match => {
        return match.toLocaleUpperCase();
      });
      return moment(time).format(transFormat);
    },
    validateParams() {
      let isvalidate = true;
      this.validate.visible = false;

      if (!this.timeFormat) {
        this.validate.visible = true;
        this.validate.content = window.$t('请选择时间参数格式');
        isvalidate = false;
        Bus.$emit('Access-httpExample-DebugValidate1', this.validate);
      }

      if (isvalidate && !this.bizid) {
        this.validate.visible = true;
        this.validate.content = window.$t('请选择所属业务');
        isvalidate = false;
        Bus.$emit('Access-httpExample-DebugValidate2', this.validate);
      }

      return isvalidate;
    },
    addTimes(date, append = '0') {
      let mdate = moment(date);
      const param = this.calFormatTime.timeParam;
      return mdate[param](mdate[param]() + append);
    },
    handleHttpDebug(item) {
      this.clickDebug = true;
      if (!this.validateParams()) {
        return;
      }
      this.httpLoading = true;
      this.bkRequest
        .request('v3/access/collector/http/check/', {
          useSchema: false,
          method: 'POST',
          params: {
            url: item.encodeUrl,
            method: 'get',
            bk_biz_id: this.bizid,
            time_format: this.timeFormat,
          },
        })
        .then(res => {
          this.debugShow = true;
          const codeHtml = this.syntaxHighlight((res.result && res.data) || res.message);
          document.getElementById('httpResult').innerHTML = codeHtml;
          this.$set(item, 'success', true);
          this.$set(item, 'icon', (res.result && 'check-circle') || 'close-circle');
        })
        ['finally'](res => {
          this.httpLoading = false;
        });
    },
    syntaxHighlight(json) {
      if (typeof json !== 'string') {
        json = JSON.stringify(json, undefined, 2);
      }
      json = json
        .replace(/&/g, '&')
        .replace(/</g, '<')
        .replace(/>/g, '>');
      return json.replace(
        /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)/g,
        function (match) {
          var cls = 'number';
          if (/^"/.test(match)) {
            if (/:$/.test(match)) {
              cls = 'key';
            } else {
              cls = 'string';
            }
          } else if (/true|false/.test(match)) {
            cls = 'boolean';
          } else if (/null/.test(match)) {
            cls = 'null';
          }
          return '<span class="' + cls + '">' + match + '</span>';
        }
      );
    },
    renderData(data) {
      let format = data['access_conf_info']['collection_model']['time_format'];
      if (!format) {
        format = '';
      }
      let formatArray = format.split(' ').map(item => {
        return item.replace(/[y{2,4}d{2}]/g, match => {
          return match.toLocaleUpperCase();
        });
      });
      const formatStr = formatArray.join(' ');
      this.timeFormat = formatStr;
    },
  },
};
</script>
<style lang="scss">
.access-http-demo {
  .debug-list {
    width: 780px;
    height: 500px;
    overflow: auto;
    margin-top: 15px;

    #httpResult {
      outline: 1px solid #ccc;
      padding: 5px;
      margin: 5px;

      .string {
        color: green;
      }
      .number {
        color: darkorange;
      }
      .boolean {
        color: blue;
      }
      .null {
        color: magenta;
      }
      .key {
        color: red;
      }
    }
  }
}
</style>

<style lang="scss" scoped>
.request-example {
  background: #f1f9ff;
  padding: 15px;
  .polling-process {
    margin-top: 10px;
  }
  .polling-url {
    width: 500px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .polling-icon {
    margin-left: 5px;
    cursor: pointer;
    .icon-debug {
      color: blue;
    }

    .icon-check-circle {
      color: #30d878;
    }

    .icon-close-circle {
      color: red;
    }
  }
  ul {
    display: flex;
    li {
      display: flex;
      margin-bottom: 5px;
      flex-direction: column;
      span {
        display: flex;
      }
    }
  }
}
</style>
