

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
  <Layout :crumbName="$t('组件状态')">
    <div class="data-operationCenter">
      <!-- <div class="header">
                <p>{{ $t('组件状态') }}</p>
            </div> -->
      <ul v-bkloading="{ isLoading: isLoading }"
        class="content clearfix">
        <li v-for="(list, lid) in componentStatus"
          :key="lid"
          class="content-list">
          <div class="title">
            <p>
              {{ list.module_name }}
              <!-- <span>{{list.module_title}}</span> -->
            </p>
          </div>
          <ul class="component-status clearfix">
            <li v-for="(item, iindex) in list.components"
              :key="iindex"
              class="fl"
              :class="{ refuse: !item.status }">
              <div class="image fl">
                <span v-if="item.src === null"
                  class="noSrc">
                  {{ item.substr }}
                </span>
                <img v-else
                  :src="item.src"
                  alt="">
              </div>
              <p class="text fl"
                :title="item.name">
                {{ item.name }}
              </p>
              <div v-if="item.status"
                class="icon fr success">
                <i class="bk-icon icon-check-circle-shape" />
              </div>
              <div v-else
                class="icon fr refuse">
                <i class="bk-icon icon-info-circle-shape" />
              </div>
              <div v-if="!item.status"
                class="abnormal-tip">
                {{ item.message }}
              </div>
            </li>
          </ul>
          <div v-if="list.message"
            class="p20 text-danger">
            <span>{{ list.message }}</span>
          </div>
        </li>
      </ul>
    </div>
  </Layout>
</template>
<script type="text/javascript">
import Layout from '../../components/global/layout';
export default {
  components: { Layout },
  data() {
    return {
      isLoading: true,
      componentStatus: [],
    };
  },
  mounted() {
    this.getDperationList();
  },
  methods: {
    getDperationList() {
      this.isLoading = true;
      this.axios.get('ops/list_component/').then(res => {
        if (res.result) {
          for (let i = 0; i < res.data.length; i++) {
            for (let j = 0; j < res.data[i].components.length; j++) {
              res.data[i].components[j].substr = res.data[i].components[j].name
                .substr(0, 1)
                .toUpperCase();
            }
          }
          this.componentStatus = res.data;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.isLoading = false;
      });
    },
  },
};
</script>
<style lang="scss" type="text/css" media="screen">
$textColor: #212232;
$tipColor: #737987;
.data-operationCenter {
  background: #f2f4f9;
  min-height: calc(100% - 60px);
  .header {
    line-height: 74px;
    height: 74px;
    padding: 27px 75px;
    color: #1a1b2d;
    font-size: 16px;
    border: 1px solid #dfe2ef;
    p {
      line-height: 20px;
      border-left: 4px solid #3a84ff;
      padding-left: 18px;
    }
  }
  .content {
    // margin: 0 75px;
    background: #fff;
    min-height: 800px;
    .title {
      margin: 0 25px;
      line-height: 65px;
      font-size: 16px;
      color: $textColor;
      border-bottom: 1px solid #dfe2ef;
      span {
        font-size: 12px;
        color: $tipColor;
      }
    }
    .content-list {
      border-bottom: 1px solid #f2f4f9;
      &:nth-child(odd) {
        background: #fafafa;
        .component-status {
          li {
            background: #fff;
            &.refuse {
              cursor: pointer;
              background: #fff3ed;
              border-color: #ffcaa8;
            }
          }
        }
      }
    }
    .component-status {
      padding: 0 25px 24px 25px;
      li {
        width: 15.49%;
        height: 56px;
        background: #fafafa;
        border-radius: 2px;
        border: 1px solid #d6d6d6;
        margin: 25px 1.41% 0 0;
        line-height: 54px;
        .image {
          width: 75px;
          text-align: center;
          height: 54px;
          img {
            display: inline-block;
            vertical-align: middle;
            max-height: 32px;
            max-width: 95px;
          }
        }
        &:nth-child(6n) {
          margin-right: 0;
        }
        &.refuse {
          cursor: pointer;
          background: #fff3ed;
          border-color: #ffcaa8;
          position: relative;
          &:hover {
            .abnormal-tip {
              display: block;
              opacity: 1;
            }
          }
          .abnormal-tip {
            display: none;
            opacity: 0;
            position: absolute;
            right: -10px;
            bottom: 64px;
            font-size: 14px;
            color: #666;
            padding: 20px;
            line-height: 16px;
            width: 295px;
            background: #fff;
            border: 1px solid #d6d6d6;
            border-radius: 2px;
            box-shadow: 0px 2px 4px rgba(0, 0, 0, 0.06);
            word-break: break-all;
            &::after {
              position: absolute;
              right: 20px;
              bottom: -11px;
              content: '';
              border: 6px solid #000;
              border-color: #fff transparent transparent transparent;
            }
            &::before {
              position: absolute;
              right: 20px;
              bottom: -12px;
              content: '';
              border: 6px solid;
              border-color: #d6d6d6 transparent transparent transparent;
            }
          }
        }
        .noSrc {
          display: inline-block;
          vertical-align: -2px;
          width: 32px;
          height: 32px;
          line-height: 32px;
          border-radius: 2px;
          background: #3a84ff;
          font-size: 18px;
          text-align: center;
          color: #fff;
        }
        .text {
          width: calc(100% - 140px);
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .icon {
          color: #dddddd;
          margin-right: 15px;
          &.success {
            color: #85d14d;
          }
          &.refuse {
            color: #fe771d;
          }
        }
      }
    }
    @media (max-width: 1366px) {
      .component-status {
        li {
          width: 18.8%;
          margin: 25px 1.5% 0 0;
          &:nth-child(5n) {
            margin-right: 0;
          }
          &:nth-child(6n) {
            margin-right: 1.5%;
          }
        }
      }
    }
  }
}
</style>
