

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
  <div class="data-detail-left">
    <div class="data-detail-left-content">
      <div class="type shadows mb10">
        <span>
          {{ $t('接入类型') }}
        </span>
        <span v-show="accessType"
          class="name fr">
          {{ accessType }}
        </span>
      </div>
      <div class="mt10 shadows data-info">
        <div class="info data">
          <div class="type">
            <span>
              {{ $t('数据信息') }}
            </span>
            <div v-if="dataChannel.includes(details.bk_app_code)"
              class="total-count fr">
              <i :title="$t('编辑')"
                class="bk-icon icon-edit"
                @click="changeDataId('define')" />
            </div>
          </div>

          <div class="info-detail">
            <form class="bk-form"
              style="width: 370px">
              <div class="bk-form-item">
                <label class="bk-label info-left"> {{ $t('数据ID') }}</label>
                <div class="bk-form-content info-right"
                  :title="basicInfo.id">
                  {{ basicInfo.id }}
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label info-left"> {{ $t('数据源名称') }}</label>
                <div class="bk-form-content info-right"
                  :title="basicInfo.raw_data_name">
                  {{ basicInfo.raw_data_name ? basicInfo.raw_data_name : basicInfo.raw_data }}
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label info-left"> {{ $t('中文名称') }}</label>
                <div class="bk-form-content info-right"
                  :title="basicInfo.raw_data_alias">
                  {{ basicInfo.raw_data_alias ? basicInfo.raw_data_alias : basicInfo.raw_data }}
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label info-left"> {{ $t('数据源描述') }}</label>
                <div class="bk-form-content info-right"
                  :title="basicInfo.description">
                  {{ basicInfo.description }}
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label info-left"> {{ $t('所属业务') }}</label>
                <div class="bk-form-content info-right"
                  :title="basicInfo.bk_biz_name">
                  [{{ basicInfo.bk_biz_id }}]{{ basicInfo.bk_biz_name }}
                </div>
              </div>
              <div class="bk-form-item tags-item">
                <label class="bk-label info-left"> {{ $t('数据来源') }}</label>
                <div class="bk-form-content info-right data-manager"
                  :title="basicInfo.data_source_alias">
                  <template v-for="item in basicInfo.data_source_tags">
                    <span :key="item.id"
                      class="single-tag">
                      {{ item.alias }}
                    </span>
                  </template>
                  <!-- {{basicInfo.data_source_alias ? basicInfo.data_source_alias : basicInfo.data_source}} -->
                </div>
              </div>
              <div class="bk-form-item tags-item">
                <label class="bk-label info-left"> {{ $t('数据标签') }}</label>
                <div class="bk-form-content info-right data-manager"
                  :title="basicInfo.data_category_alias">
                  <template v-for="item in dataTag">
                    <span :key="item.id"
                      :class="['multiple-tag']">
                      {{ item.alias }}
                    </span>
                  </template>
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label info-left"> {{ $t('字符集编码') }}</label>
                <div class="bk-form-content info-right"
                  :title="basicInfo.data_encoding">
                  {{ basicInfo.data_encoding ? basicInfo.data_encoding : '-' }}
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label info-left"> {{ $t('区域') }}</label>
                <div class="bk-form-content info-right"
                  :title="basicInfo.data_region">
                  {{ region }}
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label info-left"> {{ $t('创建时间') }}</label>
                <div class="bk-form-content info-right"
                  :title="basicInfo.created_at">
                  {{ basicInfo.created_at ? basicInfo.created_at : '-' }}
                </div>
              </div>
            </form>
          </div>
        </div>

        <div class="info auth">
          <div class="type">
            <span>
              {{ $t('数据权限') }}
            </span>
            <div v-if="dataChannel.includes(details.bk_app_code)"
              class="total-count fr">
              <i :title="$t('编辑')"
                class="bk-icon icon-edit"
                @click="changeDataId('auth')" />
            </div>
          </div>

          <div class="auto-detail info-detail">
            <form class="bk-form"
              style="width: 370px">
              <div class="bk-form-item">
                <label class="bk-label info-left"> {{ $t('数据敏感性') }}</label>
                <div class="bk-form-content info-right">
                  <span v-show="basicInfo.sensitivity === 'public'"
                    class="level open">
                    {{ $t('公开') }}
                  </span>
                  <span v-show="basicInfo.sensitivity === 'private'"
                    class="level private">
                    {{ $t('业务私有') }}
                  </span>
                  <span v-show="basicInfo.sensitivity === 'confidential'"
                    class="level sensitive">
                    {{ $t('机密') }}
                  </span>
                  <span v-show="basicInfo.sensitivity === 'sensitive'"
                    class="level sensitive">
                    {{ $t('敏感') }}
                  </span>
                </div>
              </div>
              <div
                v-for="(role, index) in members"
                :key="index"
                v-bkloading="{ isLoading: memberLoading }"
                class="bk-form-item">
                <label class="bk-label info-left"> {{ role.role_name }}</label>
                <div class="bk-form-content info-right data-manager">
                  <span v-for="(name, xIndex) of role.users"
                    :key="xIndex"
                    class="name">
                    {{ name }}
                  </span>
                </div>
              </div>
            </form>
          </div>
        </div>

        <div class="info msg">
          <div class="type">
            <span>
              {{ $t('接入备注') }}
            </span>
            <div v-if="dataChannel.includes(details.bk_app_code)"
              class="total-count fr">
              <i :title="$t('编辑')"
                class="bk-icon icon-edit"
                @click="changeDataId('marks')" />
            </div>
          </div>

          <div class="info-detail bk-form">
            <div class="bk-form-item"
              style="width: 100%">
              <div class="bk-form-content ml0">
                <textarea
                  class="bk-form-textarea des"
                  :placeholder="$t('数据备注信息')"
                  disabled
                  :value="details.description" />
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <MemberManagerWindow
      ref="member"
      :roleIds="roleIds"
      :isOpen.sync="isOpen"
      :objectClass="objectClass"
      :scopeId="basicInfo.id"
      @members="updateMembers" />
  </div>
</template>
<script>
import MemberManagerWindow from '@/pages/authCenter/parts/MemberManagerWindow';
import mixin from '@/components/dataTag/tagMixin.js';
import { mapGetters } from 'vuex';
import { getCurrentComponent } from '@/pages/DataAccess/Config/index.js';
export default {
  components: {
    MemberManagerWindow,
  },
  mixins: [mixin],
  props: {
    details: {
      type: Object,
      default() {
        return {
          bk_app_code: 'dataweb',
        };
      },
    },
  },
  data() {
    return {
      dataChannel: ['dataweb', 'data'], // 允许编辑的接入渠道
      isOpen: false,
      roleIds: ['raw_data.manager', 'raw_data.cleaner'],
      objectClass: 'raw_data',
      memberLoading: true,
      members: [],
      accessType: ''
    };
  },
  computed: {
    ...mapGetters({
      getDataTagList: 'dataTag/getTagList',
      getMultipleTagsGroup: 'dataTag/getMultipleTagsGroup',
      getSingleTagsGroup: 'dataTag/getSingleTagsGroup',
    }),

    basicInfo() {
      return this.details.access_raw_data || {};
    },
    // 数据信息添加区域
    region() {
      try {
        return this.details.access_raw_data.tags.manage.geog_area[0].alias;
      } catch (error) {
        return '-';
      }
    },
    dataTag() {
      if (this.details.access_raw_data) {
        const { business = [], desc = [] } = this.details.access_raw_data.tags;
        const tags = business.concat(desc);
        this.handleTagData(tags, this.getSingleTagsGroup, false);
        this.handleTagData(tags, this.getMultipleTagsGroup);
        return tags;
      }

      return [];
    },
  },
  watch: {
    'basicInfo.id'(val) {
      val && this.getMemberData();
    },
    details: {
      immediate: true,
      handler() {
        this.getAccessType();
      }
    }
  },
  mounted() {
    this.getDataTagListFromServer();
    this.getEditAbleAppCode();
  },
  methods: {
    updateMembers(updateMembers) {
      this.members = updateMembers.map(item => {
        item.users = item.user_ids;
        return item;
      });
    },
    getMemberData() {
      this.bkRequest
        .httpRequest('authV1/queryUserRoles', {
          query: {
            scope_id: this.basicInfo.id,
            object_class: this.objectClass,
            show_display: true,
          },
        })
        .then(res => {
          if (res.result) {
            this.members = res.data[0].roles.filter(item => {
              return this.roleIds.includes(item.role_id);
            });
          } else {
            this.getMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.memberLoading = false;
        });
    },
    changeDataId(hash) {
      if (hash === 'auth') {
        this.isOpen = true;
      } else {
        this.$router.push({
          name: 'updateDataid',
          params: { did: this.$route.params.did },
          hash: `#${hash}`,
          query: { from: this.$route.path },
        });
      }
    },
    /** 获取接入渠道可编辑白名单 */
    getEditAbleAppCode() {
      this.bkRequest.httpRequest('dataAccess/getEditAbleAppCode').then(res => {
        if (res.result) {
          this.dataChannel = res.data;
        }
      });
    },
    getAccessType() {
      getCurrentComponent(this.details.data_scenario, 'detail').then(type => {
        if (this.details.data_scenario === 'queue') {
          getCurrentComponent(this.details.access_conf_info.resource.type, 'detail').then(resp => {
            this.accessType = `${type.name} - ${resp.name}`;
          });
        } else {
          this.accessType = type.name;
        }
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.info-detail {
  form {
    border: 1px solid #d9dfe5;
    .bk-form-item {
      margin-top: 0;
      border-top: 1px solid #d9dfe5;

      &:first-child {
        border-top: none;
      }
    }
    .bk-form-item {
      background: #efefef;
    }
    label.info-left {
      width: 124px;
      background: #efefef;
    }

    .info-right {
      display: inherit;
      line-height: 34px;
      min-height: 34px;
      margin-left: 124px;
      background: #fff;
      padding-left: 15px;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    span.name {
      padding: 2px 5px;
      border-radius: 2px;
      background: #f7f7f7;
      border: 1px solid #dbe1e7;
      display: inline-block;
      margin-right: 3px;
      color: #737987;
      line-height: 25px;
    }
    .tags-item {
      font-size: 12px;
    }
    .single-tag,
    .multiple-tag {
      display: inline-block;
      height: 25px;
      line-height: 25px;
      background: #fdf6ec;
      color: #faad14;
      font-size: 12px;
      padding: 0 5px;
      border-radius: 3px;
      margin-right: 5px;
      margin-bottom: 3px;
      margin-top: 3px;
    }
    .multiple-tag {
      background: #e1ecff;
      color: #3a84ff;
    }
  }
}
.data-detail-left {
  display: flex;
  margin-bottom: 20px;
  .right-info {
    display: none;
    width: calc(100% - 415px);
    .info-detail-con {
      padding: 0 14px 14px;
    }
    .detail-box {
      width: 33.333%;
      float: left;
      border: 1px solid #dbe1e7;
    }
    .detail-box + .detail-box {
      border-left: none;
    }
    .info-title {
      width: 91px;
      background: #efefef;
      padding: 10px;
      border-right: 1px solid #dbe1e7;
    }
    .info-content {
      width: calc(100% - 91px);
      padding: 10px;
      color: #737987;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .info-detail-raw {
      div {
        border-top: none;
      }
    }
    .permission {
      padding: 0 14px 14px;
    }
  }
  .permission-table {
    width: 100%;
    color: #212232;
    td {
      border: 1px solid #dbe1e7;
      padding: 12px 20px;
      height: 40px;
      color: #737987;
    }
    .title-td {
      width: 124px;
      background: #efefef;
    }
    span.name {
      padding: 2px 10px;
      border-radius: 2px;
      background: #f7f7f7;
      border: 1px solid #dbe1e7;
      margin-top: 5px;
      display: inline-block;
      margin-right: 5px;
      color: #737987;
    }
  }
  .level {
    // width: 48px;
    line-height: 1;
    display: flex;
    justify-content: center;
    align-items: center;
    height: 24px;
    border-radius: 2px;
    color: white;
    padding: 3px 10px;
  }
  .open {
    background: #9dcb6b;
  }
  .sensitive {
    background: #ff5555;
  }
  .private {
    background: #f6ae00;
  }
  .data-detail-left-content {
    width: 400px;
    margin-right: 15px;
    .access-overview {
      height: 163px;
      .count {
        height: 15px;
        line-height: 15px;
        width: 36px;
        margin-left: 15px;
      }
      .process-section {
        display: flex;
        justify-content: space-between;
        padding: 0px 15px;
        .custom {
          width: 60px;
          height: 60px;
          border-radius: 50%;
          border: 5px solid #9dcb6b;
        }
      }
      .process-section-custom {
        text-align: center;
        i {
          display: inline-block;
          font-weight: bold;
          color: #9dcb6b;
          margin-top: 13px;
        }
        .custom {
          width: 60px;
          height: 60px;
          border-radius: 50%;
          border: 5px solid #9dcb6b;
          margin: 0 auto;
        }
      }
      .bk-circle {
        cursor: pointer;
      }
    }
    .data-info {
      width: 100%;
      .info {
        height: auto;
        &-detail {
          display: flex;
          flex-direction: row;
          margin: 0px 15px;
          .info-left div {
            border: 1px solid #d9dfe5;
            border-right: none;
            border-top: none;
            box-sizing: border-box;
            padding: 12px 20px;
            width: 124px;
            background: #efefef;
            &:first-of-type {
              border-top: 1px solid #d9dfe5;
            }
          }
          .info-right {
            display: inherit;
            width: calc(100% - 124px);
            color: #737987;
            line-height: 34px;
            white-space: nowrap;

            &.data-manager {
              white-space: normal;
              display: flex;
              flex-wrap: wrap;

              span.name {
                margin-top: 2px;
              }
            }

            > div {
              width: 100%;
              background: #fff;
              border: 1px solid #efefef;
              border-left: none;
              border-top: none;
              box-sizing: border-box;
              padding: 12px 20px;
              overflow: hidden;
              text-overflow: ellipsis;
              white-space: nowrap;
              &:first-of-type {
                border-top: 1px solid #d9dfe5;
              }
            }
          }
          .des {
            color: #232232;
          }
        }
      }
      .msg {
        margin-bottom: 10px;
      }
      .auth {
        .auto-detail {
          margin: 0px 15px;
        }
      }
    }
  }
}
</style>
