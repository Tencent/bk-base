

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
  <div class="card-wrapper">
    <div class="header">
      <div class="resource-name">
        <span v-bk-tooltips="statusTip(card.status)"
          :class="['status', card.status]" />
        <div class="name">
          <p class="title">
            <span class="text">{{ card.resource_group_id }}</span>
            <span class="icon-edit"
              @click="editCard" />
          </p>
          <p class="subtitle">
            {{ card.group_name }}
          </p>
        </div>
      </div>
      <div class="title-icon">
        <span class="icon-resourses"
          style="font-size: 22px" />
      </div>
    </div>
    <div class="content">
      <div v-for="(item, index) in sourceArray"
        :key="index"
        class="resource-info">
        <div class="title">
          <span class="name">{{ getTitle(item) }}</span>
          <div class="resource-content">
            <div class="content-item">
              <span class="content-item-title">CPU</span>
              <span class="content-item-num">{{ getNum(card.metrics[item].cpu) }}</span>
              <span class="content-item-unit">{{ getUnit(card.metrics[item].cpu) }}</span>
              <template v-if="item === 'storage'">
                <span class="content-item-title">{{ $t('磁盘') }}</span>
                <span class="content-item-num">{{ getNum(card.metrics[item].disk) }}</span>
                <span class="content-item-unit">{{ getUnit(card.metrics[item].disk) }}</span>
              </template>
              <template v-else>
                <span class="content-item-title">{{ $t('内存') }}</span>
                <span class="content-item-num">{{ getNum(card.metrics[item].memory) }}</span>
                <span class="content-item-unit">{{ getUnit(card.metrics[item].memory) }}</span>
              </template>
            </div>
          </div>
        </div>
        <div class="load"
          style="display: none">
          <span class="text">
            {{ `${$t('负载')}:` }}
          </span>
          <span :class="card.metrics[item].status">
            {{ card.metrics[item].load }}
          </span>
          <span class="icon icon-solid-arrow"
            :class="card.metrics[item].trend" />
        </div>
        <div class="more">
          <span class="icon-ellipsis"
            @click="openDetail(item)" />
        </div>
      </div>
    </div>
    <div class="footer">
      <div class="new-group">
        <span v-bk-tooltips.top="$t('资源申请')"
          class="icon-plus-bold"
          :class="{ disabled: operateBtnDisabled }"
          @click="applyResource" />
      </div>
      <div class="control-panel">
        <span v-bk-tooltips.top="$t('成员管理')"
          class="icon-user icon"
          :class="{ disabled: operateBtnDisabled }"
          @click="memberClick" />
        <span v-bk-tooltips.top="authListTip(card.group_type)"
          class="icon-structure icon"
          :class="{ disabled: authDisabled }"
          @click="authListClick(card)" />
        <span v-bk-tooltips.top="$t('删除资源组')"
          class="icon-delete icon"
          @click="deleteIconClick" />
      </div>
    </div>
  </div>
</template>

<script>
import { confirmMsg, postMethodWarning } from '@/common/js/util';

export default {
  props: {
    card: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      // sourceArray: ['processing', 'storage', 'databus'], 暂时去除总线
      sourceArray: ['processing', 'storage'],
      statusMap: {
        approve: window.$t('审批中'),
        succeed: window.$t('生效中'),
        reject: window.$t('拒绝'),
      },
      iconMap: {
        approve: 'icon-approve-shape',
        succeed: 'icon-check-circle-fill',
        reject: 'icon-delete-fill',
      },
    };
  },
  computed: {
    authDisabled() {
      return this.card.group_type === 'public' || this.card.status === 'approve';
    },
    operateBtnDisabled() {
      return this.card.status === 'approve';
    },
  },
  methods: {
    getUnit(val) {
      return val.split(' ').length && val.split(' ')[1];
    },
    getNum(val) {
      return val.split(' ').length && val.split(' ')[0];
    },
    authListClick(card) {
      if (this.authDisabled) return;
      this.$emit('authListShow', card);
    },
    authListTip(type) {
      return type === 'public' ? this.$t('公开资源组无需授权') : this.$t('授权列表');
    },
    statusTip(status) {
      return this.statusMap[status];
    },
    openDetail(type) {
      let resourceGroup = {};
      for (const [key, value] of Object.entries(this.card)) {
        if (key === 'resource_group_id' || key === 'group_name') {
          resourceGroup[key] = value;
        }
      }
      this.$router.push({
        name: 'resource-detail',
        params: {
          resourceId: this.card.resource_group_id,
          resourceGroup: JSON.stringify(resourceGroup),
          type: type,
        },
      });
    },
    applyResource() {
      if (this.operateBtnDisabled) return;
      this.$emit('applyResource', this.card);
    },
    memberClick() {
      if (this.operateBtnDisabled) return;
      this.$emit('openMemberManage', this.card);
    },
    editCard() {
      this.$emit('editCard', this.card.resource_group_id);
    },
    deleteIconClick() {
      confirmMsg(
        this.$t('确认删除资源组'),
        '',
        () => {
          this.$emit('deleteStart');
          this.bkRequest
            .httpRequest('resourceManage/deleteGroup', {
              params: {
                resource_group_id: this.card.resource_group_id,
              },
            })
            .then(res => {
              if (res.result) {
                postMethodWarning(this.$t('删除成功'), 'success');
                this.$emit('deleteSuccess');
              } else {
                this.getMethodWarning(res.message, res.code);
              }
            })
            ['finally'](() => {
              this.$emit('deleteEnd');
            });
        },
        () => {},
        { theme: 'danger' }
      );
    },
    getTitle(key) {
      const name = {
        processing: this.$t('计算资源'),
        storage: this.$t('存储资源'),
        databus: this.$t('总线资源'),
      };
      return name[key];
    },
  },
};
</script>

<style lang="scss" scoped>
.card-wrapper {
  background: #ffffff;
  border: 1px solid #dcdee5;
  border-radius: 2px;
  box-shadow: 0px 2px 4px 0px rgba(0, 0, 0, 0.1);
  margin-bottom: 30px;
  &:hover {
    box-shadow: 0px 5px 9px rgba(0, 0, 0, 0.32);
    .header {
      border-color: #3a84ff;
      background: #3a84ff;
      .resource-name .name {
        .title,
        .subtitle {
          color: #fff;
        }
      }
      .title-icon {
        background: #e1ecff;
        color: #3a84ff;
      }
    }
  }
  .header {
    height: 60px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    padding: 9px 20px 12px 20px;
    border-bottom: 1px solid #dcdee5;
    .resource-name {
      height: 39px;
      width: 296px;
      display: flex;
      position: relative;
      .status {
        margin-top: 5px;
        margin-right: 10px;
        height: 14px;
        width: 14px;
        border: 2px solid #fff;
        border-radius: 50%;
        outline: none;
        cursor: pointer;
        &.approve {
          background: #ff9c01;
        }
        &.succeed {
          background: #2dcb56;
        }
        &.reject {
          background: #ea3636;
        }
      }
      .name {
        flex: 1;
        height: 100%;
      }
      .title {
        width: 100%;
        height: 22px;
        font-size: 16px;
        text-align: left;
        color: #313238;
        display: flex;
        align-items: center;
        .text {
          max-width: calc(100% - 30px);
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .icon-edit {
          font-size: 12px;
          margin-left: 5px;
          cursor: pointer;
          &:hover {
            color: #3a84ff;
          }
        }
      }
      .subtitle {
        height: 17px;
        font-size: 12px;
        font-weight: 400;
        text-align: left;
        color: #979ba5;
        line-height: 17px;
      }
    }
    .title-icon {
      width: 36px;
      height: 36px;
      background: #fafbfd;
      color: #c4c6cc;
      border-radius: 50%;
      display: flex;
      justify-content: center;
      align-items: center;
    }
  }
  .content {
    padding: 17px 20px 15px 20px;
    border-bottom: 1px solid #dcdee5;
    .resource-info {
      margin-bottom: 10px;
      position: relative;
      // display: flex; // 暂时隐藏后面内容，加宽数据显示区域
      // justify-content: space-between; //暂时隐藏后面内容，加宽数据显示区域
      .title {
        // width: 207px; //暂时隐藏后面内容，加宽数据显示区域
        display: flex;
        align-items: center;
        font-family: PingFangSC;
        .name {
          padding-right: 10px;
          border-right: 1px solid #eaeaea;
          height: 20px;
          font-weight: 400;
          font-size: 14px;
          text-align: left;
          color: #63656e;
          line-height: 20px;
        }
        .resource-content {
          margin-left: 5px;
          font-size: 12px;
          color: #979ba5;
          .content-item-title {
            margin-left: 10px;
          }
          .content-item-num {
            display: inline-block;
            width: 35px;
            text-align: right;
            margin-left: 3px;
            margin-right: 4px;
          }
          .content-item-unit:not(:last-child) {
            padding-right: 10px;
            border-right: 1px solid #eaeaea;
          }
        }
        .info {
          height: 17px;
          font-size: 12px;
          font-weight: 400;
          text-align: left;
          color: #979ba5;
          line-height: 17px;
        }
      }
      .load {
        position: relative;
        margin-left: 7px;
        .text {
          font-size: 12px;
        }
        .icon {
          display: inline-block;
          transform: scale(0.5) translate(-10px, -10px);
          &.down {
            transform: scale(0.5) translate(-10px, -10px) rotate(180deg);
          }
        }
        .red {
          color: #ea3636;
        }
        .yellow {
          color: #ff9c01;
        }
        .green {
          color: #2dcb56;
        }
        .up {
          color: #fd9c9c;
        }
        .down {
          color: #94f5a4;
        }
      }
      .more {
        position: absolute;
        right: 0;
        top: 0;
        margin-left: 5px;
        margin-right: 10px;
        font-size: 14px;
        cursor: pointer;
        &:hover {
          color: #3a84ff;
        }
      }
    }
  }
  .footer {
    display: flex;
    align-items: center;
    height: 43px;
    padding-right: 25px;
    .new-group {
      width: 79px;
      height: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
      border-right: 1px solid #dcdee5;
      .icon-plus-bold {
        outline: none;
        font-size: 22px;
        padding: 5px 6px;
        border-radius: 2px;
        cursor: pointer;
        &:hover {
          background: #3a84ff;
          color: #fff;
        }
      }
    }
    .control-panel {
      flex: 1;
      display: flex;
      justify-content: flex-end;
      .icon {
        font-size: 18px;
        padding: 5px 6px;
        border-radius: 2px;
        outline: none;
        cursor: pointer;
        &:not(:last-child) {
          margin-right: 12px;
        }
        &:hover {
          background: #3a84ff;
          color: #fff;
        }
      }
    }
  }
}
</style>
