

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
  <div class="introduction">
    <bkdata-dialog
      :isShow.sync="basicCustomSetting.isShow"
      :width="basicCustomSetting.width"
      :title="basicCustomSetting.title"
      :closeIcon="false"
      :hasFooter="false"
      :padding="0">
      <div slot="content">
        <v-cutover :slides="slides" />
        <div class="footer">
          <div class="button">
            <button class="bk-button bk-primary check-video"
              @click="cancelFn">
              {{ $t('查看视频教程') }}
            </button>
            <button class="pass"
              @click="cancelFn">
              {{ $t('我是大神_跳过') }}
            </button>
          </div>
        </div>
      </div>
    </bkdata-dialog>
  </div>
</template>
<script>
import vCutover from '@/components/introduction/cutover.vue';
import { postMethodWarning } from '@/common/js/util.js';
export default {
  components: {
    vCutover,
  },
  props: {
    show: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      basicCustomSetting: {
        isShow: false,
        width: 980,
        title: '数据平台介绍',
      },
      slides: [
        {
          // src: require('../../common/images/1.png'),
          title: '数据接入',
          step: [
            { text: '定义数据基础信息' },
            { text: '下发采集器进行部署' },
            { text: '数据清洗' },
            { text: '数据开发' },
          ],
        },
        {
          // src: require('../../common/images/1.png'),
          title: '数据接入2',
          step: [
            { text: '1111111111111111' },
            { text: '2222222222222' },
            { text: '3333333333333' },
            { text: '4444444444444' },
          ],
        },
        {
          // src: require('../../common/images/1.png'),
          title: '数据接入3',
          step: [{ text: 'aaaaaaaa' }, { text: 'bbbbbb' }, { text: 'ccccc' }, { text: 'ddddddddd' }],
        },
        {
          // src: require('../../common/images/1.png'),
          title: '数据接入4',
          step: [{ text: '!!!!!!!!!!' }, { text: '@@@@@@@@' }, { text: '#######' }, { text: '$$$$$$$$$' }],
        },
      ],
    };
  },
  watch: {
    show(val) {
      this.basicCustomSetting.isShow = val;
    },
  },
  methods: {
    cancelFn() {
      this.basicCustomSetting.isShow = false;
      this.guideFinish();
    },
    guideFinish() {
      this.axios.post('tools/end_guide/', { guide: 'platform_intro' }).then(res => {
        if (res.result) {
          console.log(res);
        } else {
          postMethodWarning(res.message, 'error');
        }
      });
    },
  },
};
</script>
<style lang="scss" type="text/css">
.introduction {
  .bk-dialog-title {
    font-size: 30px;
    position: relative;
    &:after {
      content: '';
      width: 30px;
      height: 2px;
      background: #3a84ff;
      position: absolute;
      left: 50%;
      top: 50px;
      transform: translate(-50%, 0);
    }
  }
  .bk-dialog-position {
    margin-top: 60px;
    display: -webkit-box;
  }
  .footer {
    border-top: 1px solid #eeeeee;
    padding: 50px 0 70px 0;
    .button {
      margin-left: 370px;
    }
    .check-video {
      height: 56px;
      line-height: 56px;
      width: 240px;
      border-radius: 30px;
      font-size: 18px;
    }
    .pass {
      height: 56px;
      line-height: 56px;
      background: none;
      border: none;
      margin-left: 40px;
      font-size: 14px;
    }
  }
}
</style>
