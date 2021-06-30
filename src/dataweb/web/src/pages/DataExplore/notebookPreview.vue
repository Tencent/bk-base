

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
  <div v-bkloading="{ isLoading: basicLoading, zIndex: 10 }"
    class="notebook-preview-wrapper">
    <div :class="['notebook-preview-left', { 'is-left-hide': !isShowLeftMenu }]">
      <div class="left-header">
        <bkdata-button title="大纲"
          :icon="isShowLeftMenu ? 'icon-data-shrink-line' : 'icon-data-expand-line'"
          class="mr10 iconMenu"
          @click.stop="handleToggleLeft">
          大纲
        </bkdata-button>
      </div>
      <div class="left-content bk-scroll-y">
        <div ref="list"
          class="tree-list">
          <div
            v-for="(item, index) in treeList"
            :key="index"
            :class="{ 'highlight-title': item.isActive }"
            @click="goAnchor(item)">
            <div v-if="item.el.localName === 'h1'"
              class="tree-h1 tree-text">
              {{ item.name }}
            </div>
            <div v-if="item.el.localName === 'h2'"
              class="tree-h2 tree-text">
              {{ item.name }}
            </div>
            <div v-if="item.el.localName === 'h3'"
              class="tree-h3 tree-text">
              {{ item.name }}
            </div>
          </div>
        </div>
      </div>
    </div>
    <div class="notebook-preview-center">
      <div class="notebook-preview-header">
        <ul class="header-left">
          <li>笔记报告：{{ notebookName }}</li>
          <li>发布者：{{ createdBy }}</li>
          <li>发布时间：{{ createdAt }}</li>
        </ul>
        <ul class="header-right">
          <li @click="copyUrl">
            <i class="icon-data-copy" /> 复制链接
          </li>
          <li @click="toViewNotebook">
            <i class="icon-jump-2" /> 查看笔记
          </li>
        </ul>
      </div>
      <div ref="a"
        class="notebook-preview-main bk-scroll-y">
        <bkdata-alert
          type="info"
          title="本报告的访问权限控制在项目成员内，你可以前往 我的项目 管理项目人员（项目观察员即可访问）"
          closable
          closeText="不再提醒" />
        <notebook-report @noteContent="noteContent"
          @noteList="noteList" />
      </div>
    </div>
  </div>
</template>

<script>
import notebookReport from './notebookReport.vue';

export default {
  components: { notebookReport },
  data() {
    return {
      isShowLeftMenu: false,
      createdBy: '',
      createdAt: '',
      notebookName: '',
      treeList: [],
      basicLoading: false,
    };
  },
  //挂载页面时，添加滚动监听
  mounted() {
    this.$refs.a.addEventListener('scroll', this.handleScroll);
  },
  // 退出页面时，应该取消监听
  destroyed() {
    this.$refs.a.removeEventListener('scroll', this.handleScroll);
  },
  methods: {
    handleToggleLeft() {
      this.isShowLeftMenu = !this.isShowLeftMenu;
    },
    noteContent(data) {
      this.createdAt = data.created_at;
      this.createdBy = data.created_by;
      this.notebookName = data.notebook_name;
    },
    noteList(list) {
      this.treeList = list;
    },
    copyUrl() {
      let url = window.location.href;
      clipboardCopy(url, showMsg('复制成功', 'success', { delay: 3000 }));
    },
    toViewNotebook() {
      let url = window.location.href.substring(0, window.location.href.indexOf('#'));
      let parameter = window.location.href.split('?')[1];
      let num = parameter.split('&');
      num = num.slice(0, num.length - 1);
      num = num.join('&');
      window.location.href = `${url}#/datalab?${num}`;
    },
    //点击目录，定位到页面
    goAnchor(item) {
      this.$refs.a.removeEventListener('scroll', this.handleScroll);
      this.treeList.forEach(element => {
        element.isActive = false;
      });
      item.isActive = true;

      item.el.scrollIntoView({ behavior: 'smooth', inline: 'nearest' });
      console.log('item.el.offsetTop', this.$refs.a.scrollTop);

      let temp = 0;
      let timer = setInterval(() => {
        let temp1 = this.$refs.a.scrollTop;
        if (temp != temp1) {
          //两次滚动高度不等，则认为还没有滚动完毕
          temp = temp1; //滚动高度赋值
        } else {
          clearInterval(timer);
          this.$refs.a.addEventListener('scroll', this.handleScroll);
          temp = null; //放弃引用
        }
      }, 100);
    },
    //页面滚动，定位到目录
    handleScroll(e) {
      let scrollTop = this.$refs.a.scrollTop; //当前滚动距离
      let flag = false;
      this.treeList.forEach((element, index) => {
        if (scrollTop + 90 >= element.el.offsetTop) {
          //当前滚动距离大于某一目录项时。
          for (let i = 0; i < index; i++) {
            this.treeList[i].isActive = false; //同一时刻，只能有一个目录项的状态位为Active，即此时其他目录项的isActive = false
          }
          element.isActive = true; //将对应的目录项状态位置为true
          flag = true;
        } else {
          element.isActive = false;
        }

        if (!flag) {
          this.treeList[0].isActive = true;
        }
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.notebook-preview-wrapper {
  width: 100%;
  height: 100%;
  overflow: hidden;
  display: flex;
  .notebook-preview-left {
    width: 240px;
    border-right: 1px solid #dcdee5;
    transition: width 0.1s linear;
    &.is-left-hide {
      width: 90px;
      white-space: nowrap;
      .left-content {
          background: #fff;
          .tree-text {
            opacity: 0;
          }
          .highlight-title {
              background-color: #fff;
          }
      }
      .left-content::-webkit-scrollbar {
        display: none;
      }
    }
    /deep/.left-header {
      line-height: 44px;
      max-height: 44px;
      border-bottom: 1px solid #dcdee5;
      padding: 0 16px;
      .bk-button {
        border: none;
        padding: 0;
        text-align: left !important;
        .bk-icon {
          font-size: 14px;
          padding-right: 12px;
          vertical-align: text-top;
        }
        :hover {
          color: #3a84ff;
        }
      }
    }
    .left-content {
      padding: 0 15px 80px;
      line-height: 30px;
      font-size: 12px;
      text-overflow: ellipsis;
      white-space: nowrap;
      transition: width 0.1s linear;
      height: calc(100% - 44px);
      .highlight-title {
        border-left: 3px solid rgb(15, 105, 223);
        background-color: rgb(243, 243, 243);
        z-index: -1;
        .tree-text {
          color: rgb(15, 105, 223);
        }
      }
      .tree-list {
        position: -webkit-sticky;
        position: sticky;
        top: 0;
        word-wrap: break-word;
        background-color: #fff;
        z-index: 999;
        .tree-text {
          transition: opacity, .1s;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          &:hover {
            color: #3a84ff;
            cursor: pointer;
          }
        }
      }
      .tree-h1 {
        text-indent: 2em;
      }
      .tree-h2 {
        text-indent: 3em;
      }
      .tree-h3 {
        text-indent: 4em;
      }
    }
  }
  .notebook-preview-center {
    flex: 1;
    overflow: hidden;
    height: 100%;
    transition: all 0.5s;
    .notebook-preview-header {
      line-height: 44px;
      max-height: 44px;
      border-bottom: 1px solid #dcdee5;
      display: flex;
      flex-wrap: nowrap;
      .header-left {
        flex: 7;
        display: flex;
        li {
          padding-left: 50px;
          max-width: 500px;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          font-size: 12px;
          color: #979ba5;
        }
        :nth-of-type(1) {
          font-size: 14px;
          color: #313238;
        }
      }
      .header-right {
        flex: 1;
        display: flex;
        color: #3a84ff;
        font-size: 12px;
        margin-right: 60px;
        li {
          flex: 1;
          text-align: right;
          cursor: pointer;
          i {
            font-size: 16px;
          }
        }
        :nth-of-type(1):hover {
          color: #699df4;
        }
        :nth-of-type(2):hover {
          color: #699df4;
        }
      }
    }
    /deep/.notebook-preview-main {
      margin: 20px 0;
      padding-right: 40px;
      height: calc(100% - 84px);
      .bk-alert-info {
        margin: 0px 0px 20px 50px;
        .bk-alert-close {
          color: #3a84ff;
          &:hover {
            color: #699df4;
          }
        }
      }
    }
  }
}
</style>
