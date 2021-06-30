

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
  <DialogWrapper ref="dialog"
    :title="$t('申请资源')"
    :subtitle="$t('创建资源组说明')"
    :icon="'icon-apply'"
    :dialog="dialogConfig"
    :defaultFooter="true"
    extCls="applied-recource-dialog up100px"
    @cancle="dialogConfig.isShow = false"
    @confirm="submitForm">
    <template #content>
      <div class="header">
        <AppliedCard v-for="(card, index) in cards"
          :key="index"
          :extCls="card.disabled ? 'disabled' : ''"
          :card="card"
          :active="card.active"
          @cardClick="cardClickHandle(index)" />
        <AppliedCard v-bk-tooltips="$t('暂未开放')"
          :extCls="'disabled'"
          :card="developingCard"
          :active="developingCard.active" />
      </div>
      <div class="form-data">
        <AppliedForm ref="appliedForm"
          :userName="userName"
          :groupID="resourceCard.resource_group_id"
          :groupDisName="cardDisName"
          :resourceType="activeCard" />
        <bkdata-form />
      </div>
    </template>
  </DialogWrapper>
</template>

<script>
import DialogWrapper from '@/components/dialogWrapper';
import AppliedForm from './AppliedForm';
import AppliedCard from './AppliedCard';
import { postMethodWarning } from '@/common/js/util.js';
import Cookies from 'js-cookie';
export default {
  components: {
    DialogWrapper,
    AppliedForm,
    AppliedCard,
  },
  props: {
    resourceCard: {
      type: Object,
      default: () => ({}),
    },
    focusCard: {
      type: Number,
      default: null,
    },
    request: {
      type: String,
      default: 'increase',
    },
  },
  data() {
    return {
      activeCardIndex: 0,
      cards: [],
      dialogConfig: {
        isShow: false,
        width: 882,
        quickClose: false,
        loading: false,
      },
      userName: '',
      developingCard: {
        icon: 'icon-plugin',
        title: this.$t('总线资源'),
        content: this.$t('资源组管理员可以申请计算接入资源_资源单位再商讨'),
        active: this.focusCard === null ? false : this.focusCard === 2,
        disabled: true,
      },
    };
  },
  computed: {
    activeCard() {
      const carsName = ['processing', 'storage', 'databus'];
      return carsName[this.activeCardIndex];
    },
    cardDisName() {
      return `${this.resourceCard.resource_group_id}(${this.resourceCard.group_name})`;
    },
  },
  watch: {
    'dialogConfig.isShow'(val) {
      !val && this.$refs.appliedForm.clearForm();
    },
  },
  created() {
    this.init();
  },
  methods: {
    init() {
      this.initCard();
      this.initInfo();
    },
    initCard() {
      this.activeCardIndex = this.focusCard === null ? 0 : this.focusCard;
      this.$nextTick(() => {
        this.cards.push(
          {
            icon: 'icon-calculate',
            title: this.$t('计算资源'),
            content: this.$t('管理员可以申请计算资源_包括CPU内存等多种设备型号'),
            active: this.focusCard === null ? true : this.focusCard === 0,
            disabled: this.focusCard === null ? false : this.focusCard !== 0,
          },
          {
            icon: 'icon-disk-save',
            title: this.$t('存储资源'),
            content: this.$t('申请存储资源_会链接到外部的各个存储系统进行申请'),
            active: this.focusCard === null ? false : this.focusCard === 1,
            disabled: this.focusCard === null ? false : this.focusCard !== 1,
          }
        );
      });
    },
    initInfo() {
      const user = this.$store.getters.getUserName;
      this.userName = user;
    },
    cardClickHandle(index) {
      if (this.cards[index].disabled) return;
      this.cards.forEach((item, idx) => {
        if (index === idx) {
          this.activeCardIndex = index;
          item.active = true;
        } else {
          item.active = false;
        }
      });
    },
    submitForm() {
      this.$refs.appliedForm.formData.apply_type = this.request;
      this.$refs.appliedForm.validateForm().then(
        res => {
          this.$refs.dialog.footerConfig.loading = true;
          this.bkRequest
            .httpRequest('resourceManage/createGroupCapacityApply', {
              params: this.$refs.appliedForm.formData,
            })
            .then(res => {
              if (res.result) {
                postMethodWarning(this.$t('提交成功'), 'success');
                this.dialogConfig.isShow = false;
                this.$router.push({ name: 'Records' });
              } else {
                this.getMethodWarning(res.message, res.data);
              }
            })
            ['finally'](res => {
              this.$refs.dialog.footerConfig.loading = false;
              this.$refs.dialog.isShow = false;
            });
        },
        reject => {
          console.log('表单验证不通过');
        }
      );
    },
  },
};
</script>
<style lang="scss" scoped>
.applied-recource-dialog {
  .header {
    display: flex;
    padding: 28px 54px;
    .applied-card-wrapper {
      margin-right: 12px;
      outline: none;
    }
  }
  .form-data {
    margin-top: 2px;
    padding-bottom: 57px;
    ::v-deep .bk-form .form-item-input {
      width: 777px;
      .tips-icon {
        position: absolute;
        right: -25px;
        cursor: pointer;
      }
    }
  }
}
</style>
