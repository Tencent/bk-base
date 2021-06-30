

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
  <div class="data-header">
    <header class="header">
      <div class="logo"
        @click="!isPlaceholder && $router.push({ name: homeRouteName })">
        <img src="../../common/images/logo/logo.svg"
          alt="">
        <span class="logo-name">
          {{ getTitleName() }}
        </span>
      </div>
      <div class="nav">
        <template v-if="!isPlaceholder">
          <ul class="nav-items clearfix">
            <template v-for="(item, index) in activeNaviList">
              <li
                v-if="item.children && item.children.length"
                :key="index"
                class="nav-item"
                :class="{ active: item.route.matchActive.includes(routerName) }">
                <bkdata-popover
                  :width="116"
                  theme="light"
                  placement="top"
                  @show="tippyShowCallback"
                  @hide="tippyCloseCallback">
                  <router-link tag="span"
                    exact
                    :to="item.route.to">
                    {{ $t(item.displayName) }}
                  </router-link>
                  <div slot="content"
                    class="bk-dropdown-menu header-dropdown-width">
                    <ul class="bk-dropdown-list">
                      <template v-for="(child, cIndex) in item.children">
                        <li
                          v-if="child.active"
                          :key="cIndex + '_child'"
                          :class="{ active: child.route.matchActive.includes(routerName) }">
                          <a href="javascript:void(0)"
                            @click.stop="handleSubNaviClick(child)">
                            <p>
                              <span>{{ $t(child.displayName) }}</span>
                              <span v-if="child.labelName"
                                class="nav-label">
                                {{ $t(child.labelName) }}
                              </span>
                            </p>
                          </a>
                        </li>
                      </template>
                    </ul>
                  </div>
                </bkdata-popover>
              </li>
              <router-link
                v-else
                :key="index"
                class="nav-item"
                tag="li"
                exact
                :class="{ active: item.route.matchActive.includes(routerName) }"
                :to="item.route.to">
                <span>{{ $t(item.displayName) }}</span>
              </router-link>
            </template>
            <li class="nav-item"
              @click="helpDoc">
              <span>{{ $t('帮助文档') }}</span>
            </li>
          </ul>
        </template>
      </div>

      <div
        id="user-info"
        data="usercenter"
        :class="{ active: ['user_center', 'home', 'SampleDetail'].includes(routerName) }"
        @click="linkToUserCenter"
        @mouseover="isDropDwon = true"
        @mouseout="isDropDwon = false">
        <i class="bk-icon icon-user mr5" />
        <template v-if="!isPlaceholder">
          <ul v-show="isDropDwon"
            class="dropdown-menu"
            style="position: absolute; left: 0; right: auto">
            <li v-for="(item, index) in userCenterList"
              :key="index"
              :class="{ active: item.tab === queryTabName }">
              <a href="jacascript: void(0);"
                @click.stop="handleUsercenterNaviClick($event, item)">
                {{ $t(item.tabName) }}
              </a>
            </li>
          </ul>
          {{ $t('用户中心') }}
        </template>
      </div>
      <div v-if="showHelpAssistant"
        class="help-section fr">
        <a href="wxwork://message/?username=TGDP">
          <i class="icon-weixin" />
          {{ $t('助手') }}
        </a>
      </div>
    </header>
  </div>
</template>
<script>
import Cookies from 'js-cookie';
import HeaderNaviConfiguration from '@/common/js/featureConfig.js';
import { mapState, mapGetters } from 'vuex';
import Bus from '@/common/js/bus.js';

export default {
  props: {
    isPlaceholder: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      lan: 'zh-cn',
      isDropDwon: false,
      activeNaviList: [],
      userCenterList: [],
    };
  },
  computed: {
    ...mapState({
      userName: state => state.common.userName,
      isAdmin: state => state.common.isAdmin,
    }),
    showHelpAssistant() {
      return window.BKBASE_Global.runVersion === 'ieod';
    },
    queryTabName() {
      return this.$route.query.tab || (this.$route.name === 'home' && 'project');
    },
    homeRouteName() {
      return this.$modules.isActive('dataflow') ? 'user_center' : 'home';
    },
    runVersion() {
      return window.BKBASE_Global.runVersion || '';
    },
    link() {
      return this.$store.getters.env;
    },
    isShowAINode() {
      return this.$store.getters.functions;
    },
    routerName() {
      return (this.$route && this.$route.name) || 'null';
    },
  },
  mounted() {
    this.lan = Cookies.get('blueking_language');
    window.addEventListener('click', e => {
      if (e.target.id !== 'user-info') {
        this.isDropDwon = false;
      }
    });
  },
  created() {
    this.initHeaderNavis();
  },
  methods: {
    tippyShowCallback() {
      Bus.$emit('header-nav-tippy-show');
    },
    tippyCloseCallback() {
      Bus.$emit('header-nav-tippy-close');
    },
    handleSubNaviClick(conf) {
      if (typeof conf.onClick === 'function') {
        conf.onClick.call(this);
      } else {
        this.$router.push(conf.route.to);
      }
    },
    handleUsercenterNaviClick(e, item) {
      if (typeof item.onClick === 'function') {
        item.onClick.call(this, e);
        this.isDropDwon = false;
      }
    },
    linkToDataDashbaord(route) {
      if (route.name === this.$route.name) {
        return Bus.$emit('dataDashboardRefresh');
      }
      this.$router.push(route);
    },
    getTitleName() {
      return window.BKBASE_Global.webTitle || $t('基础计算平台');
    },
    select(data) {
      if (data.id === 'data-mining') {
        window.location.href = this.link.MODELFLOW_PLATFORM_ARRE;
      }
    },
    helpDoc() {
      const url = this.$store.getters['docs/getPaths'].helpDocRoot;
      window.open(url);
    },
    linkToUserCenter() {
      this.$router.push({
        path: '/user-center',
        query: {
          tab: 'project',
        },
      });
    },
    linkToTab(e, tab) {
      this.$router.push({
        path: '/user-center',
        query: {
          tab: tab,
        },
      });
      this.isDropDwon = false;
      e.stopPropagation();
    },
    linkToAccess(e) {
      this.$router.push({
        path: '/auth-center/permissions',
        query: {
          objType: 'project',
        },
      });
      e.stopPropagation();
    },
    initHeaderNavis() {
      if (!this.isPlaceholder) {
        const naviConfig = new HeaderNaviConfiguration(this.$modules, this.$route);
        this.activeNaviList = naviConfig.getActiveHeadNaviList().sort((a, b) => a.order - b.order) || [];
        this.userCenterList = (naviConfig.getUserCenterConfig() || [])
          .sort((a, b) => a.order - b.order)
          .filter(navi => navi.active);
      }
    },
  },
  // components: {
  //   // modelflow
  // },
};
</script>
<style lang="scss" type="text/css">
.header-dropdown-width {
  margin: -7px -14px;
  display: flex !important;
  .bk-dropdown-list {
    text-align: center;
    li {
      &.active {
        a {
          background-color: #eaf3ff;
          color: #3a84ff;
        }
      }
    }
    .nav-label {
      font-size: 10px;
      color: rgba(100, 100, 100, 0.6);
    }
  }
}
.data-header {
  z-index: 280;
  header {
    user-select: none;
    min-width: 1200px;
    background-color: #212232;
    z-index: 1005;
    height: 60px;
    line-height: 1;
    padding-left: 10px;
    position: relative;
    span.name {
      font-size: 20px;
      color: #fff;
      position: relative;
      top: -10px;
      font-style: italic;
    }
    .logo {
      margin: 12px 28px 0px 0px;
      float: left;
      cursor: pointer;
      img {
        height: 38px;
        margin-right: 5px;
        vertical-align: middle;
      }
      .logo-name {
        vertical-align: middle;
        font-size: 18px;
        color: #fff;
        font-weight: 800;
        font-style: italic;
        display: inline-block;
        transform: translateY(1px);

      }
    }
    .help-section {
      height: 100%;
      display: flex;
      align-items: center;
      a {
        cursor: pointer;
        padding: 10px 15px;
        i {
          padding-right: 5px;
        }
      }
    }
    .nav {
      display: inline-block;
      margin-top: 10px;
      .nav-items {
        margin: 0px;
        padding: 0px;
        color: #b2bac0;
        .active span {
          background-color: #3a84ff;
          color: #fff;
        }
      }
      .nav-item + .nav-item {
        margin-left: 5px;
      }
      .nav-item {
        float: left;
        height: 50px;
        position: relative;
        &:last-child {
          box-sizing: content-box;
        }
        &:hover .nav-item-list {
          display: block;
        }
        .nav-item-list {
          text-align: center;
          display: none;
          position: absolute;
          left: 50%;
          top: 49px;
          min-width: 118px;
          padding: 15px 0;
          margin: 0 6px;
          background: #e1ecff;
          border: 1px solid #d4d6f3;
          box-shadow: 0px 0px 10px 0px rgba(33, 34, 50, 0.1);
          z-index: 250;
          transform: translate(-53%, 0);
          &:before {
            position: absolute;
            top: -12px;
            left: 50%;
            content: '';
            width: 0;
            height: 0px;
            border: 6px solid;
            border-color: transparent transparent #d4d6f3 transparent;
            transform: translate(-50%, 0);
          }
          &:after {
            position: absolute;
            top: -12px;
            left: 50%;
            content: '';
            width: 0;
            height: 0px;
            border: 6px solid;
            border-color: transparent transparent #e1ecff transparent;
            transform: translate(-50%, 0);
          }
          .list-item {
            display: block;
            /*height: 32px;*/
            /*line-height: 32px;*/
            color: #3a84ff;
            background: #e1ecff;
            padding: 6px 5px;
            &:hover {
              background: #3a84ff;
              color: #fff;
            }
            &.router-link-exact-active {
              background: #3a84ff;
              color: #fff;
            }
          }
        }
        span {
          display: inline-block;
          height: 40px;
          line-height: 40px;
          border-radius: 3px;
          text-align: center;
          list-style: none;
          padding: 0px 20px;
          transition: all 0.5s linear;
          &:hover {
            cursor: pointer;
            color: #fff;
            transition: all 0.5s linear;
            background-color: #63656e;
          }
        }
        .bk-tooltip {
          .bk-tooltip-ref {
            span.active,
            .click-active {
              background-color: #3a84ff;
              color: #fff;
            }
          }
        }
      }
    }
    .admin-header {
      position: relative;
      height: 60px;
      float: right;
      line-height: 60px;
      padding: 0px 28px;
      color: #b2bac0;
      cursor: pointer;
      transition: all 0.5s linear;
      i {
        font-size: 18px;
      }
      &:hover {
        background: #11121e;
        color: #fff;
      }
    }
    #user-info {
      position: relative;
      height: 60px;
      float: right;
      line-height: 60px;
      padding: 0px 28px;
      // color: #b2bac0;
      color: #fff;
      cursor: pointer;
      transition: all 0.5s linear;
      &.active {
        background-color: #3a84ff;
      }

      i {
        font-size: 18px;
      }
      &:hover {
        // background: #11121e;
        color: #fff;
        background-color: #3a84ff;
      }
      .user-name {
        background-color: #323744;
        line-height: 32px;
        color: #fff;
        padding-left: 10px;
        .name-text {
          float: left;
          max-width: 200px;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          line-height: 32px;
        }
        &:hover {
          cursor: pointer;
        }
      }
      .apply-info {
        display: none;
        z-index: 2000;
        width: 180px;
        position: absolute;
        top: 45px;
        right: 0px;
        height: 134px;
        line-height: 40px;
        background-color: #fff;
        cursor: pointer;
        border: 1px solid #dde4eb;
        div {
          text-align: center;
          padding: 13px 0px;
          &:hover {
            color: #3a84ff;
            .apply-icon {
              background: #3a84ff;
              i {
                color: #fff;
              }
            }
          }
          &.check {
            padding-left: 15px;
            text-align: left;
            border-bottom: 1px solid #dde4eb;
            position: relative;
            color: #737987;
            &:last-child {
              border-bottom: none;
            }
          }
        }
        .apply-icon {
          display: inline-block;
          text-align: center;
          border-radius: 2px;
          width: 38px;
          height: 36px;
          line-height: 36px;
          background-color: #f2f4f9;
          vertical-align: middle;
          font-size: 18px;
          .apply-num {
            top: 9px;
            left: 45px;
            border-color: #fff;
          }
        }
        &:before {
          content: '';
          display: inline-block;
          position: absolute;
          border: 8px solid #fff;
          border-top-color: transparent;
          border-left-color: transparent;
          top: -14px;
          right: 8px;
          border-right-color: transparent;
        }
      }
      .image-wraper {
        position: relative;
      }
      .apply-num {
        position: absolute;
        display: inline-block;
        border: 2px solid #212232;
        height: 18px;
        min-width: 16px;
        padding: 0px 3px;
        top: -7px;
        left: 35px;
        -webkit-border-radius: 50%;
        -moz-border-radius: 50%;
        border-radius: 8px;
        background-color: #ff6600;
        color: #fff;
        line-height: 13px;
        font-size: 10px;
        text-align: center;
      }
      .user-photo {
        vertical-align: bottom;
        display: inline-block;
        height: 32px;
        width: 32px;
        margin-left: 10px;
        background-size: 32px 32px;
        img {
          position: relative;
          top: -14px;
          right: -18px;
        }
      }
      .dropdown-menu {
        box-shadow: 0 3px 4px rgba(0, 0, 0, 0.32);
        position: absolute;
        left: 0px;
        right: auto;
        width: 100%;
        text-align: center;
        background: white;
        li:hover {
          background: #f9f9ff;
        }
        li.active {
          a {
            background-color: #eaf3ff;
            color: #3a84ff;
          }
        }
        a {
          display: block;
          width: 100%;
          height: 36px;
          line-height: 36px;
          color: #303133;
        }
      }
    }
    > .active {
      background-color: #3a84ff;
      color: #fff;
    }
    .active > span:hover {
      background-color: #3a84ff !important;
    }
  }
  .bk-select-list {
    box-shadow: 0px 3px 6px rgba(51, 60, 72, 0.1);
    z-index: 10000;
    border: 1px solid #dde4eb;
    border-radius: 2px;
  }
}
</style>
