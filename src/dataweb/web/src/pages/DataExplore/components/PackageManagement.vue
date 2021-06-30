

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
  <div class="package-management-layout">
    <bkdata-alert
      class="package-alert"
      type="info"
      :title="mangerTitle" />
    <div class="package-form">
      <div class="package-item">
        <span class="package-item-name">{{ $t('安装库') }}</span>
        <package-input v-model="installStr"
          class="package-item-content"
          :reg="packageReg" />
      </div>
      <div class="package-item">
        <span class="package-item-name">{{ $t('卸载库') }}</span>
        <package-input v-model="uninstallStr"
          class="package-item-content" />
      </div>
    </div>
    <div class="footer">
      <span v-if="isRunning"
        class="tips mr10">
        {{ $t('执行过程包含构建 Docker 镜像，预计耗时20秒，请稍后') }}
      </span>
      <bkdata-button class="mr10"
        theme="primary"
        :disabled="disabled"
        :loading="isRunning"
        @click="handleSubmit">
        {{ $t('执行') }}
      </bkdata-button>
      <bkdata-button theme="default"
        @click="handleClose">
        {{ $t('取消') }}
      </bkdata-button>
    </div>
  </div>
</template>

<script>
import PackageInput from './PackageInput.vue';
import Bus from '@/common/js/bus.js';

export default {
  components: {
    PackageInput,
  },
  data() {
    return {
      isRunning: false,
      installStr: '',
      uninstallStr: '',
      packageReg: {
        isError: false,
        tips: '',
      },
      existedList: [
        {
          name: 'node',
          version: '10.0.0',
        },
      ],
      timer: null,
      successInfo: null,
      message: this.$t('重启成功_库已生效'),
    };
  },
  computed: {
    disabled() {
      return (!this.installStr && !this.uninstallStr) || this.packageReg.isError;
    },
    managerTitle() {
      return this.$t(
        '笔记运行在 K8S 上，在 项目 或 个人 下安装库，会创建单独的镜像，下次启动会使用创建的镜像;'
          + '后台会在 Dockerfile 中逐行添加 pip 安装命令（pip 源：https://mirrors.tencent.com/pypi/simple);'
          + '内置的 Python Package 列表可以在笔记页面右上角的 内置库 中找到。'
      );
    }
  },
  watch: {
    installStr(val) {
      val && this.validatePackage(val);
    },
  },
  methods: {
    validatePackage(val) {
      this.timer && clearTimeout(this.timer);
      this.timer = setTimeout(() => {
        const packageList = val.split('\n');
        const regSingle = /=/g;
        const regSymbol = /(=){2}/;
        for (let i = 0; i < packageList.length; i++) {
          const curPackage = packageList[i];
          if (curPackage.indexOf('=') > -1) {
            const count = curPackage.match(regSingle).length;
            const isDoubleEqual = regSymbol.test(curPackage);
            if (count !== 2 || (count === 2 && !isDoubleEqual)) {
              this.packageReg.isError = true;
              this.packageReg.tips = this.$t('请输入正确格式');
              return false;
            }
            const packageInfo = curPackage.split('==');
            if (this.existedList.find(item => item.name === packageInfo[0] && item.version === packageInfo[1])) {
              this.packageReg.isError = true;
              this.packageReg.tips = this.$t('该库已安装，请勿重复安装');
              return false;
            }
          } else if (!curPackage) {
            this.packageReg.isError = true;
            this.packageReg.tips = this.$t('请勿输入空行');
            return false;
          }
          if (i === packageList.length - 1) {
            this.packageReg.isError = false;
            this.packageReg.tips = '';
          }
        }
      }, 400);
    },
    handleSubmit() {
      if (this.packageReg.isError) return;
      this.isRunning = true;
      this.handleClose();
      const h = this.$createElement;
      this.successInfo = this.$bkInfo({
        type: 'success',
        title: '添加用户成功',
        showFooter: false,
        subHeader: h(
          'i18n',
          {
            props: {
              path: '点击重启当前环境生效',
              tag: 'div',
            },
          },
          [
            h(
              'a',
              {
                attrs: {
                  place: 'key',
                  href: 'javascript:',
                },
                on: {
                  click: this.handleReset,
                },
              },
              '重启'
            ),
          ]
        ),
      });
    },
    handleClose() {
      this.$emit('close');
    },
    handleReset() {
      Bus.$emit('reload-notebook');
      this.successInfo && this.successInfo.close();
      const timer = setTimeout(() => {
        this.$bkMessage({
          theme: 'success',
          message: this.message,
        });
        clearTimeout(timer);
      }, 600);
    },
  },
};
</script>

<style lang="scss" scoped>
.package-management-layout {
  .package-alert {
    margin: 0 24px;
  }
  .package-form {
    padding: 0 24px 26px;
  }
  .package-item {
    margin-top: 20px;
  }
  .package-item-name {
    display: inline-block;
    padding-bottom: 10px;
  }
  .package-item-content {
    display: block;
    ::v-deep .bk-tooltip-ref {
      width: 100%;
    }
  }
  .footer {
    text-align: right;
    font-size: 0;
    padding: 12px 24px;
    background-color: #fafbfd;
    border-top: 1px solid #dcdee5;
    border-radius: 2px;
    .tips {
      display: inline-block;
      vertical-align: middle;
      font-size: 12px;
      color: #979ba5;
    }
    .bk-button {
      min-width: 76px;
    }
  }
}
</style>
