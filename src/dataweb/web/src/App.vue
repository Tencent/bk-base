

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
  <div class="data-wrapper"
    :class="`bkdata-${language}`">
    <template v-if="!isLoading">
      <template v-if="!isLoginRouter && !isFlowIframe">
        <v-header />
      </template>
      <main :class="{ 'flow-iframe': isFlowIframe }">
        <router-view />
      </main>
      <!-- 登录弹窗 -->
      <div v-if="showLoginModal"
        class="loginDialog">
        <div class="login-wrapper">
          <div class="holeBg" />
          <i class="icon-close closeBtn"
            @click="hideLoginModal" />
          <iframe
            id="login_frame"
            name="login_frame"
            :src="loginUrl"
            scrolling="no"
            border="0"
            :width="autorization['width']"
            :height="autorization['height']" />
        </div>
      </div>
      <bkdata-dialog v-model="updateTipShow"
        :escClose="false"
        :maskClose="false"
        :closeIcon="false"
        :title="$t('资源更新提醒')"
        footerPosition="center"
        theme="warning"
        @confirm="updateConfirm"
        @cancel="updateCancel">
        <p class="resource-update-dialog-content">
          {{ $t('资源更新提醒内容') }}
        </p>
        <p class="resource-update-dialog-warning">
          {{ $t('资源更新警告') }}
        </p>
      </bkdata-dialog>
    </template>
  </div>
</template>

<script>
import vHeader from '@/components/header/header.vue';
import Bus from '@/common/js/bus.js';

export default {
  name: 'app',
  components: {
    vHeader,
  },
  data() {
    return {
      updateTipShow: false,
      language: this.$i18n.locale,
      http404count: 0,
      http500count: 0,
      timeout: 7500,
      stopTimer: false,
      intervalId: 0,
      hasShowWarning: false,
      showLoginModal: false,
      isLoading: false,
      autorization: {},
      siteUrl: '',
      logUrlFromServer: '',
    };
  },
  computed: {
    /**
     * 是否数据开发Iframe
     */
    isFlowIframe() {
      return this.$route.query.flowIframe;
    },
    loginUrl() {
      return this.logUrlFromServer
        ? this.logUrlFromServer
        : `${window.BKBASE_Global.loginUrl}plain/?size=big&c_url=${this.siteUrl}/account/login_success/`;
    },
    isLoginRouter() {
      return this.$route.name === 'login-success' || /from=login/.test(window.location.search);
    },
    unihash() {
      return document.querySelector('meta[name="unihash"]') || {};
    },
    unihashContent() {
      return decodeURIComponent(this.unihash.content || '');
    },
  },
  mounted() {
    this.$nextTick(() => {
      this.checkServeSidePackge();
    });

    Bus.$on('show-login-modal', resp => {
      this.logUrlFromServer = resp.login_url;
      const { width, height } = resp;
      if (width && height) {
        Object.assign(this.autorization, { width, height });
      }
      this.$nextTick(() => {
        this.showLoginModal = true;
      });
    });

    Bus.$on('close-login-modal', () => {
      this.showLoginModal = false;
    });
  },
  created() {
    window.$t = function () {};
    if (/from=login/.test(window.location.search)) {
      this.$router.push({ name: 'login-success' });
    }

    const self = this;
    let url = window.location.origin;
    this.siteUrl = encodeURIComponent(url);
    if (typeof window['close_login_dialog'] !== 'function') {
      window['close_login_dialog'] = function () {
        self.showLoginModal = false;
      };
    }
  },
  methods: {
    checkServeSidePackge() {
      const origin = window.location.origin;
      const fileName = `${origin}${this.unihashContent}.vO7JpshJEESM7o97ohlLdw`;
      if (this.unihashContent) {
        this.intervalId = setInterval(() => {
          this.Axios.get(fileName)
            .then(res => {
              this.http500count = 0;
              if (this.http404count) {
                this.http404count = 0;
              }
              // this.checkServeSidePackge()
            })
            ['catch'](e => {
              if (e.response.status === 404 && !this.stopTimer) {
                this.timeout = 1000;
                this.http404count++;
                if (this.http404count >= 2) {
                  this.updateTipShow = true;
                }
              } else {
                if (e.response.status === 500 || e.response.status === 502) {
                  // this.http500count++
                  console.warn(`Service Error: ${e.response.status}`, new Date());
                  // !this.stopTimer && this.checkServeSidePackge()
                }
              }
            });
        }, this.timeout);
      }
    },
    updateConfirm() {
      this.updateTipShow = false;
      window.location.reload(true);
    },
    updateCancel() {
      this.stopTimer = true;
      console.warn('已取消刷新获取最新资源', new Date());
    },
    hideLoginModal() {
      this.showLoginModal = false;
      window.location.reload();
    },
  },
};
</script>

<style lang="scss" scoped>
.resource-update-dialog-content {
  text-align: center;
}
.resource-update-dialog-warning {
  text-align: center;
  color: red;
  font-weight: 800;
  margin-top: 10px;
}
main {
  height: calc(100% - 60px);
}
::v-deep .flow-iframe {
  height: 101%;
    .header-top {
      top: 46px !important;
    }
    .group-filter-view {
        top: 54px;
    }
    .session-mask {
        top: 94px !important;
        width: calc(100vw - 740px) !important;
        height: calc(100vh - 94px) !important;
        right: 0px !important;
    }
    .custom-publish-right {
        .session-mask {
            width: calc(100vw - 650px) !important;
            height: calc(100vh - 145px) !important;
        }
        .group-filter-backage {
            top: 46px;
        }
    }
}
.loginDialog {
  position: fixed;
  left: 0;
  top: 0;
  width: 100%;
  height: 100%;
  z-index: 99999;

  .holeBg {
    position: fixed;
    left: 0;
    top: 0;
    width: 100%;
    height: 100%;
    background: rgba(0, 0, 0, 0.7);
  }

  .login-wrapper {
    width: 959px;
    height: 627px;
    top: 100px;
    margin: auto;
    position: relative;

    iframe {
      display: block;
      background: #fff;
      border: 0;
      width: 100%;
      height: 100%;
      margin: auto;
      top: 50%;
      position: relative;
      transform: translateY(-50%);
      z-index: 10002;
    }
    .closeBtn {
      position: absolute;
      right: 10px;
      top: 10px;
      width: 16px;
      height: 16px;
      font-size: 17px;
      background: #fff;
      border-radius: 50%;
      z-index: 10009;
      cursor: pointer;

      &:hover {
        box-shadow: 0 0 10px rgba(255, 255, 255, 0.5);
        background-color: #f0f1f5;
      }

      img {
        width: 8px;
        margin-top: 0;
        display: inline-block;
        position: relative;
        top: -1px;
      }
    }
  }
}
</style>
