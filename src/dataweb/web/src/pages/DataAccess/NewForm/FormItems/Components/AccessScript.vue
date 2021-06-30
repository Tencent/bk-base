

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
  <div class="access-script">
    <span v-if="!isReadonly"
      class="bk-item-des">
      {{ $t('蓝鲸基础计算平台会将脚本下发到指定的服务器_并托管执行该脚本') }}
    </span>
    <Container>
      <template v-for="(item, index) in params.scope">
        <ScopeItem :key="index"
          :height="'auto'"
          :showWrapper="!isReadonly"
          :step="index + 1">
          <Item class="x2">
            <label class="bk-label">{{ $t('采集范围') }}：</label>
            <div class="bk-form-content">
              <div v-tooltip.notrigger.left="validateScopes[index]['bk_biz_id']"
                class="pick-type">
                <template v-if="!isReadonly">
                  <iModule
                    v-model="params.scope[index]"
                    v-tooltip.notrigger="validateScopes[index]['scope_object']"
                    :bizid="bizid"
                    :beforeOpen="() => handleSelectModule(index)" />
                </template>

                <!-- <span class="module-select mr5"
                                    v-tooltip.notrigger="validateScopes[index]['scope_object']"
                                    v-if="!isReadonly"
                                    @click="handleSelectModule(index)">
                                    <i class="bk-icon icon-cc-square mr5"></i>{{$t('选择模块和IP')}}
                                </span> -->
                <i18n path="已选择_个模块"
                  tag="span">
                  <span class="module-count"
                    place="num">
                    {{ item.module_scope.length }}
                  </span>
                </i18n>
                <span>、</span>
                <i18n path="_个IP"
                  tag="span">
                  <span class="module-count"
                    place="num">
                    {{ item.host_scope.length }}
                  </span>
                </i18n>
                <span class="script-server-warn">
                  <i class="bk-icon icon-info-tip" /> {{ $t('_使用之前请先确认服务器已安装jq命令') }}
                </span>
              </div>
            </div>
            <div
              v-if="!isReadonly"
              v-tooltip.notrigger="validateScopes[index].deleValidate"
              class="delete delete-obj"
              @click="handleDeleteScope(index)">
              <i :title="$t('删除')"
                class="bk-icon icon-delete" />
            </div>
          </Item>
          <Item class="x2 script-item">
            <label class="bk-label">{{ $t('脚本') }}：</label>
            <div
              v-tooltip.notrigger="validateScopes[index]['scope_config']"
              :class="['bk-form-content', !isReadonly && 'editor']">
              <a v-if="isReadonly"
                href="javascript:;"
                class="editor-preview-btn"
                @click="showScriptEditor(index)">
                {{ `${showScript ? $t('关闭脚本') : $t('点击查看脚本')}` }}
              </a>
              <Monaco
                v-if="!isReadonly"
                ref="editor"
                v-bkloading="{ isLoading: item.isLoading }"
                height="100%"
                language="shell"
                theme="vs-dark"
                :options="editor.options"
                :tools="editor.tools"
                :code="item.scope_config.content"
                @codeChange="code => editorChange(code, item)">
                <ul slot="header-tool">
                  <li class="bk-icon icon-debug fl"
                    @click="handleDebugScript(item, index)">
                    <button type="button"
                      class="bk-button bk-primary"
                      :title="$t('调试')">
                      <i class="arrow" />
                      <span>{{ $t('调试') }}</span>
                    </button>
                  </li>
                </ul>
              </Monaco>
            </div>
            <bkdata-dialog
              v-model="debugShow"
              extCls="bkdata-dialog"
              :content="'component'"
              :hasHeader="false"
              :closeIcon="true"
              :hasFooter="false"
              :maskClose="false"
              :width="800"
              @confirm="debugShow = !debugShow"
              @cancel="debugShow = !debugShow">
              <div class="debug-list result-list">
                <div class="debug-result debug-success">
                  <div v-for="(debug, xindex) in debugResult.error_hosts"
                    :key="xindex">
                    <span class="debug-info error"><i class="bk-icon icon-close-circle" /></span>
                    <span>{{ debug.msg }}</span>
                    <span>IP：{{ debug.ip }}</span>
                  </div>
                </div>
                <div class="debug-result debug-failed">
                  <div v-for="(debug, xindex) in debugResult.success_hosts"
                    :key="xindex">
                    <span class="debug-info success"><i class="bk-icon icon-check-circle" /></span>
                    <span>{{ debug.msg }}</span>
                    <span>IP：{{ debug.ip }}</span>
                  </div>
                </div>
              </div>
            </bkdata-dialog>
          </Item>
        </ScopeItem>
      </template>
      <div v-if="!isReadonly"
        class="add-button mb5">
        <span
          v-tooltip.notrigger="{
            content: addValidate.accessCountContent,
            visible: addValidate.visible,
            class: 'error-red',
          }"
          @click="handleAddScript">
          <i class="bk-icon icon-plus-circle" />{{ $t('添加接入对象') }}
        </span>
      </div>
      <template v-if="isReadonly">
        <!--eslint-disable-next-line-->
        <bkdata-dialog
          v-model="showScript"
          extCls="bkdata-dialog"
          :content="'component'"
          :hasHeader="false"
          :closeIcon="true"
          :hasFooter="false"
          :maskClose="false"
          :okText="$t('创建')"
          :cancelText="$t('取消')"
          :width="707"
          :height="355"
          @confirm="showScriptEditor(0)"
          @cancel="showScriptEditor(0)">
          <div class="debug-list">
            <Container>
              <Item class="x2 script-item">
                <div class="bk-form-content editor">
                  <Monaco
                    height="100%"
                    language="shell"
                    theme="vs-dark"
                    :options="{ readOnly: true, fontSize: 10 }"
                    :tools="editor.tools"
                    :code="params.scope[activeScopeItemIndex].scope_config.content" />
                </div>
              </Item>
            </Container>
          </div>
        </bkdata-dialog>
      </template>
    </Container>
  </div>
</template>
<script>
// import ModuleSelector from '@/pages/DataAccess/Components/ModuleSelector/dialog.vue'
import iModule from '@/components/imoduleDialog/imoduleIndex.vue';
import ScopeItem from '@/pages/DataAccess/Components/Scope/ScopeForm.vue';
import { postMethodWarning } from '@/common/js/util.js';
import Monaco from '@/components/monaco';
import Container from '../ItemContainer';
import Item from '../Item';
import Bus from '@/common/js/bus.js';
import { scriptContent } from '@/i18n/textContent.js';
import mixin from '@/pages/DataAccess/Config/mixins.js';
export default {
  components: {
    iModule,
    Monaco,
    Container,
    Item,
    ScopeItem,
  },
  mixins: [mixin],
  props: {
    showScript: {
      type: Boolean,
      default: false,
    },
    bizid: {
      type: Number,
      default: 0,
    },
    mode: {
      type: String,
      default: 'edit', // ['edit' || 'details'] 编辑模式 || 详情模式
    },
  },
  data() {
    return {
      isFirstValidate: true,
      isDebuging: false,
      editor: {
        language: 'shell',
        tools: {
          enabled: true,
          title: '',
          toolList: {
            format_sql: false,
            view_data: false,
            guid_help: false,
            full_screen: true, // 是否启用全屏设置
            event_fullscreen_default: true, // 是否启用默认的全屏功能，如果设置为false需要自己监听全屏事件组后续处理
          },
        },
        options: {
          readOnly: false,
          fontSize: 10,
        },
      },
      params: {
        scope: [
          {
            isLoading: false,
            module_scope: [],
            host_scope: [],
            scope_config: {
              content: scriptContent[this.$getLocale()],
            },
          },
        ],
      },
      ipList: [],
      modList: [],
      addValidate: {
        accessCount: 3,
        accessCountContent: this.$t('接入对象不能超过3个'),
        pathsCount: 5,
        pathsCountContent: this.$t('日志路径不能超过5个)'),
      },
      validate: {
        deleValidate: {
          content: window.$t('至少要有一个接入对象'),
          visible: false,
          class: 'error-red',
        },
        bk_biz_id: {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
        scope_object: {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: '',
          visible: false,
          placement: 'top',
          class: 'error-red',
        },
        scope_config: {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
      validateScopes: [],
      activeScopeItemIndex: 0,
      debugResult: {},
      debugShow: false,
      initModuleIpInfo: {
        module: [],
        ip: [],
      },
    };
  },
  computed: {
    isReadonly() {
      return this.mode === 'details';
    },
  },
  watch: {
    params: {
      handler(newVal) {
        !this.isFirstValidate && this.validateForm(null, false);
      },
      deep: true,
    },
  },
  created() {
    this.$set(this.validateScopes, 0, JSON.parse(JSON.stringify(this.validate)));
  },
  methods: {
    handleDeleteScope(index) {
      // 至少需要一个接入对象
      if (this.params.scope.length < 2) {
        this.$set(this.validateScopes[index].deleValidate, 'visible', true);
        this.$forceUpdate();
        setTimeout(() => {
          this.$set(this.validateScopes[index].deleValidate, 'visible', false);
          this.$forceUpdate();
        }, 1000);
        return;
      }
      this.params.scope.splice(index, 1);
      this.validateScopes.splice(index, 1);
    },
    handleAddScript() {
      const len = this.params.scope.length;
      if (len >= this.addValidate.accessCount) {
        this.$set(this.addValidate, 'visible', true);
        this.$nextTick(() => {
          setTimeout(() => {
            this.$set(this.addValidate, 'visible', false);
          }, 1000);
        });
        return;
      }
      this.$set(this.validateScopes, this.validateScopes.length, JSON.parse(JSON.stringify(this.validate)));
      this.params.scope.push({
        isLoading: false,
        module_scope: [],
        host_scope: [],
        scope_config: {
          content: scriptContent,
        },
      });
    },
    handleDebugScript(item, index) {
      if (!this.validateItem(item, index)) {
        this.$forceUpdate();
        return;
      }
      item.isLoading = true;
      const postParams = {
        bk_biz_id: this.bizid,
        content: item['scope_config'].content,
        module_scope: item['module_scope'],
        host_scope: item['host_scope'],
      };

      this.bkRequest
        .request('/v3/access/collector/script/check/', { method: 'POST', params: postParams, useSchema: false })
        .then(res => {
          if (res.result) {
            this.debugResult = res.data;
            item.isLoading = true;
          } else {
            postMethodWarning(this.$t('脚本调试失败:') + res.message, 'error');
          }
        })
        ['catch'](e => {
          postMethodWarning(this.$t('脚本调试失败:') + e, 'error');
        })
        ['finally'](res => {
          item.isLoading = false;
          this.debugShow = true;
        });
    },
    editorChange(code, item) {
      item.scope_config.content = code;
    },
    getCheckedNodes(data) {
      let res = [];
      data[0].forEach(d => {
        res.push({
          bk_inst_id: d.bk_inst_id,
          bk_obj_id: d.bk_obj_id,
          bk_inst_name: d.bk_inst_name,
        });
      });
      this.params.scope[this.activeScopeItemIndex].module_scope = res;
    },
    getCheckedIps(data) {
      let res = [];
      data[0].forEach(d => {
        res.push(d);
      });
      this.params.scope[this.activeScopeItemIndex].host_scope = res.filter(ip => {
        return ip.checked === true;
      });
    },
    validateItem(scope, index) {
      let isValidate = true;
      if (scope.module_scope.length !== 0 || scope.host_scope.length !== 0) {
        this.validateScopes[index]['scope_object'].visible = false;
      } else {
        isValidate = false;
        this.validateScopes[index]['scope_object'].visible = true;
        this.validateScopes[index]['scope_object'].content = window.$t('请先选择模块或者IP');
      }
      if (scope.scope_config.content) {
        this.validateScopes[index]['scope_config'].visible = false;
      } else {
        isValidate = false;
        this.validateScopes[index]['scope_config'].visible = true;
        this.validateScopes[index]['scope_config'].content = window.$t('不能为空');
      }

      return isValidate;
    },
    validateForm(validateFunc, isSubmit = true) {
      if (isSubmit) {
        this.isFirstValidate = false;
      }
      let isValidate = true;
      this.params.scope.forEach((scope, index) => {
        if (!this.validateItem(scope, index)) {
          isValidate = false;
        }
      });

      this.$forceUpdate();
      return isValidate;
    },
    validateBizid(index) {
      if (this.bizid) {
        this.$set(this.validateScopes[index]['bk_biz_id'], 'visible', false);
        return true;
      } else {
        this.$set(this.validateScopes[index]['bk_biz_id'], 'visible', true);
        this.$set(this.validateScopes[index]['bk_biz_id'], 'content', this.$t('请先选择所属业务'));
        return false;
      }
    },
    handleSelectModule(index) {
      this.activeScopeItemIndex = index;
      if (!this.validateBizid(index)) {
        this.$forceUpdate();
        return false;
      }

      return true;
      // // 更新模块IP初始化设置
      // this.initModuleIpInfo = {
      //     module: this.params.scope[index].module_scope,
      //     ip: this.params.scope[index].host_scope
      // }

      // let modlists = []
      // this.params.scope[index].module_scope.forEach(s => {
      //     modlists.push(s)
      // })
      // this.modList = modlists
      // this.$refs.selectModuleAndIP.open()
    },
    formatFormData() {
      return {
        group: 'access_conf_info',
        identifier: 'resource',
        data: { scope: this.params.scope },
      };
    },
    renderData(data) {
      const scope = data['access_conf_info'].resource.scope;
      scope.forEach(item => {
        item.isLoading = false;
      });
      scope && this.$set(this.params, 'scope', scope);
      this.$set(this, 'validateScopes', new Array(scope.length).fill(JSON.parse(JSON.stringify(this.validate))));
    },
    showScriptEditor(index) {
      this.activeScopeItemIndex = index;
      // Bus.$emit('resetCarouselHeight', !this.showScript)
    },
  },
};
</script>
<style lang="scss" scoped>
.script-server-warn {
  float: right;
  color: #ea3636;
  margin-left: 4px;
  i {
    color: #fff;
    background: #ea3636;
    border-radius: 50%;
    font-size: 12px;
    padding: 1px;
  }
}
.access-script {
  ::v-deep .bk-access-scope {
    margin-bottom: 15px;
  }
  .editor {
    height: 300px;
  }
  .module-select {
    padding: 5px;
    border: 1px solid #c3cdd7;
    background: #fff;
    cursor: pointer;
    .bk-icon {
      font-size: 14px;
    }
  }
  .module-count {
    padding: 0px 5px;
    font-size: 12px;
    border-radius: 2px;
    background: #3a84ff;
    color: #fff;
    margin: 0 5px;
  }
  .script-item {
    align-items: start;
    padding-bottom: 20px;
    position: relative;

    .debug-list {
      width: 480px;
      height: 200px;
      overflow: auto;
      margin-top: 15px;

      &.result-list {
        width: 780px;

        .debug-result {
          padding-bottom: 5px;

          div {
            margin-bottom: 10px;
          }
        }
      }

      .debug-info {
        &.error {
          color: red;
        }

        &.success {
          color: green;
        }
      }
    }
  }

  .add-button {
    width: 750px;
    text-align: center;
    margin: 10px 0;
    span {
      border: 1px dashed #c3cdd7;
      display: inline-block;
      padding: 0px 10px;
      text-align: center;
      min-width: 100px;
      cursor: pointer;
      padding: 6px 10px;
      font-size: 14px;
      border-radius: 2px;
      &:hover {
        background: #3a84ff;
        color: #fff;
        cursor: pointer;
        border: solid 1px #3a84ff;
      }
    }
    .bk-icon {
      margin-right: 8px;
      font-size: 16px;
      vertical-align: -2px;
    }
  }
  .delete-obj {
    position: absolute;
    right: -20px;
    top: -15px;

    i {
      font-size: 18px;
    }

    &:hover {
      color: #ff5656;
    }
  }
}
.fixed-editor {
  position: fixed;
  left: 0;
  top: 0;
  width: 100%;
  height: 100%;
  background: rgba(0, 0, 0, 0.5);
}
.editor-preview-btn {
  display: inline-block;
  margin-top: 4px;
  color: #3a84ff;
  font-weight: bold;
}

.show-editor {
  height: 260px;
}
</style>
