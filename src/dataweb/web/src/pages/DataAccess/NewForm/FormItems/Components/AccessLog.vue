

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
  <div>
    <span class="bk-item-des">{{ $t('多个接入对象的内容必须一致_否则会出错') }}</span>
    <Container class="bk-access-object">
      <template v-for="(scope, i) of params.scope">
        <Item :key="i">
          <ScopeItem :height="'auto'"
            :step="i + 1">
            <Item class="select-type mb10">
              <label class="bk-label">{{ $t('采集范围') }}：</label>
              <div v-tooltip.notrigger.left="validate['bk_biz_id']"
                class="bk-form-content pick-type">
                <!-- <span class="module-select mr15"
                                    v-tooltip.notrigger="scope.validate"
                                    @click="handleSelectModule(i)">
                                    <i class="bk-icon icon-cc-square mr5"></i>{{$t('选择模块和IP')}}</span> -->
                <iModule
                  v-model="params.scope[i]"
                  v-tooltip.notrigger="scope.validate"
                  :bizid="bizid"
                  :beforeOpen="() => handleSelectModule(i)" />
                <i18n path="已选择_个模块"
                  tag="span">
                  <span class="module-count"
                    place="num">
                    {{ scope.module_scope.length }}
                  </span>
                </i18n>
                <span>、</span>
                <i18n path="_个IP"
                  tag="span">
                  <span class="module-count"
                    place="num">
                    {{ scope.host_scope.length }}
                  </span>
                </i18n>
              </div>
              <div
                v-tooltip.notrigger="scope.deleValidate"
                class="delete delete-obj"
                @click="handleDeleteScope(i, 'mod', scope)">
                <i :title="$t('删除')"
                  class="bk-icon icon-delete" />
              </div>
            </Item>
            <Item class="select-type">
              <label class="bk-label">{{ $t('日志路径') }}：</label>
              <div v-focus="{ currFocus }"
                class="bk-form">
                <div
                  v-for="(path, index) of scope.scope_config.paths"
                  :key="index"
                  class="bk-form-content path-row mb10">
                  <SystemPicker :selectedItem="path.system"
                    @systemChecked="sys => handleSystemChecked(sys, path)" />
                  <bkdata-input
                    v-model="path['path']"
                    v-tooltip.notrigger="path.validate"
                    autoFocus="true"
                    :placeholder="path.placeHolder"
                    type="text"
                    class="log-path"
                    name="validation_name"
                    @input="handlePathInput(path)" />
                  <i
                    :title="$t('删除')"
                    class="bk-icon icon-minus-circle ml10"
                    @click="handleDeleteOrAppendLog('min', i, index)" />
                  <i
                    v-if="index === scope.scope_config.paths.length - 1"
                    v-tooltip.notrigger="{
                      content: addValidate.pathsCountContent,
                      visible: scope.visible,
                      class: 'error-red',
                    }"
                    :title="$t('添加')"
                    class="bk-icon icon-plus-circle ml5"
                    @click="handleDeleteOrAppendLog('plus', i, index)" />
                </div>
              </div>
            </Item>
          </ScopeItem>
        </Item>
      </template>
      <div class="add-button mb5">
        <span
          v-tooltip.notrigger="{
            content: addValidate.accessCountContent,
            visible: addValidate.visible,
            class: 'error-red',
          }"
          @click="handleAddAccessObject">
          <i class="bk-icon icon-plus-circle" />{{ $t('添加接入对象') }}
        </span>
      </div>
    </Container>
  </div>
</template>
<script>
import Container from '../ItemContainer';
import Item from '../Item';
import { postMethodWarning } from '@/common/js/util.js';
import SystemPicker from './SystemPicker';
import ScopeItem from '@/pages/DataAccess/Components/Scope/ScopeForm.vue';
// import ModuleSelector from '@/pages/DataAccess/Components/ModuleSelector/dialog.vue'
import iModule from '@/components/imoduleDialog/imoduleIndex.vue';
import mixin from '@/pages/DataAccess/Config/mixins.js';
export default {
  components: {
    Container,
    Item,
    SystemPicker,
    iModule,
    ScopeItem,
    // ModuleSelector
  },
  directives: {
    focus: {
      componentUpdated: function (el, binding) {
        const { appendIndex, focus } = binding.value.currFocus;
        if (focus && appendIndex) {
          const inputs = el.querySelectorAll('[auto-focus="true"]');
          const input = inputs[appendIndex];
          const inputLen = inputs.length - 1;
          const insertIndex = inputLen === appendIndex ? appendIndex : inputLen;
          const insetInput = inputs[insertIndex];
          if (input && insetInput && !insetInput.dataset.focused) {
            input.focus();
            insetInput.dataset.focused = true;
          }
        }
      },
    },
  },
  mixins: [mixin],
  props: {
    showDataTag: {
      type: Boolean,
      default: false,
    },
    bizid: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
      bkBizId: 0,
      iModuleVisiable: false,
      isFirstValidate: true,
      isSubmitting: false,
      currFocus: {
        appendIndex: 0,
        focus: false,
      },
      list: {
        logPick: '',
        pathList: [
          {
            name: 'win',
            value: 'windows',
            icon: 'win',
          },
          {
            name: 'linux',
            value: 'linux',
            icon: 'linux',
          },
          {
            value: 'apple',
            name: 'apple',
            icon: 'apple',
          },
        ],
      },
      isDeepTarget: true,
      params: {
        scope: [
          {
            module_scope: [],
            host_scope: [],
            deleValidate: {
              content: window.$t('至少要有一个接入对象'),
              visible: false,
              class: 'error-red',
            },
            validate: {
              content: '',
              visible: false,
              class: 'error-red',
            },
            scope_config: {
              paths: [
                {
                  system: 'linux',
                  path: '',
                  placeHolder: '',
                  validate: {
                    content: '',
                    visible: false,
                    class: 'error-red',
                  },
                },
              ],
            },
          },
        ],
      },
      pathFormatValidate: {
        windows: { exp: /^[a-zA-Z]:\\[\\\S|*\S]?.*$/gi },
        linux: { exp: /(^\/\w+|\.\w?)+/gi },
      },
      validate: {
        bk_biz_id: {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
      addValidate: {
        accessCount: 3,
        accessCountContent: window.$t('接入对象不能超过3个'),
        pathsCount: 5,
        pathsCountContent: window.$t('日志路径不能超过5个'),
      },
      ipList: [],
      modList: [],
      errMsg: [],
      activeAccessObjIndex: 0,
      initModuleIpInfo: {
        module: [],
        ip: [],
      },
    };
  },
  watch: {
    showDataTag(val) {
      if (val && this.validate['bk_biz_id'].visible) {
        const bk_biz_id = JSON.parse(JSON.stringify(this.validate['bk_biz_id']));
        this.validate['bk_biz_id'] = { ...bk_biz_id, visible: false };
      }
    },
    params: {
      handler(newVal) {
        !this.isFirstValidate
          && !this.isSubmitting
          && this.$nextTick(() => {
            this.validateForm(null, false);
          });
      },
      deep: true,
    },
    bizid: {
      handler(val) {
        this.bkBizId = val;
        this.$set(this.validate['bk_biz_id'], 'visible', false);
      },
      immediate: true,
    },
  },

  methods: {
    handlePathInput(path) {
      this.validatePath([path]);
    },
    handleSystemChecked(sys, item) {
      if (!sys.isMounted) {
        item.system = sys.value;
      }
      item.placeHolder = sys.placeHolder;

      !sys.isMounted && this.validatePath([item]);
    },
    formatFormData() {
      let _params = JSON.parse(JSON.stringify(this.params));
      for (let i = 0; i < _params.scope.length; i++) {
        let pathConf = {};
        _params.scope[i].scope_config.paths.forEach(p => {
          if (pathConf[p.system]) {
            if (Array.isArray(p.path)) {
              pathConf[p.system].push(...p.path);
            } else {
              pathConf[p.system].push(p.path);
            }
          } else {
            if (Array.isArray(p.path)) {
              Object.assign(pathConf, { [p.system]: p.path });
            } else {
              Object.assign(pathConf, { [p.system]: [p.path] });
            }
          }
        });
        _params.scope[i].scope_config.paths = Array.prototype.map.call(Object.keys(pathConf), key => {
          return {
            system: key,
            path: pathConf[key],
          };
        });
      }
      return {
        group: 'access_conf_info',
        identifier: 'resource',
        data: _params,
      };
    },
    validatePath(paths) {
      let isValidate = true;
      Array.prototype.forEach.call(paths, (path, index) => {
        const _path = Array.isArray(path.path) ? path.path[0] : path.path;
        if (_path) {
          if (
            (path.system === 'windows' && /^[a-zA-Z]:\\[\\\S|*\S]?.*$/gi.test(_path))
            || (path.system === 'linux' && /(\/\w+|\.\w?)+/gi.test(_path))
          ) {
            this.$set(path.validate, 'visible', false);
          } else {
            isValidate = false;
            this.$set(path.validate, 'visible', true);
            this.$set(path.validate, 'content', window.$t('文件路径格式错误'));
          }
        } else {
          isValidate = false;
          this.$set(path.validate, 'visible', true);
          this.$set(path.validate, 'content', window.$t('不能为空'));
        }
      });

      return isValidate;
    },
    validateModuleIps(scope) {
      let isValidate = true;
      if (scope.module_scope.length !== 0 || scope.host_scope.length !== 0) {
        this.$set(scope.validate, 'visible', false);
      } else {
        isValidate = false;
        this.$set(scope.validate, 'visible', true);
        this.$set(scope.validate, 'content', window.$t('请先选择模块或者IP'));
      }

      return isValidate;
    },
    validateForm(validateFunc, isSubmit = true) {
      let isValidate = true;
      this.isSubmitting = isSubmit;
      this.params.scope.forEach((scope, index) => {
        if (!this.validateModuleIps(scope, this.validate[index])) {
          isValidate = false;
        }
        if (!this.validatePath(scope['scope_config'].paths, this.validate[index])) {
          isValidate = false;
        }
      });

      if (isSubmit) {
        this.isFirstValidate = false;
      }
      this.isSubmitting = false;
      this.$forceUpdate();
      return isValidate;
    },
    validateBizid(index = 0) {
      if (this.bizid) {
        this.$set(this.validate['bk_biz_id'], 'visible', false);
        return true;
      } else {
        this.$set(this.validate['bk_biz_id'], 'visible', true);
        this.$set(this.validate['bk_biz_id'], 'content', this.$t('请先选择所属业务'));
        return false;
      }
    },
    renderData(data) {
      const key = 'scope';
      this.params.scope = data['access_conf_info'].resource[key].map(scope => {
        return this.initScopeItem(scope);
      });
    },
    initScopeItem(scope) {
      let paths = [];
      scope['scope_config'].paths.forEach(item => {
        const _paths = item.path.map(path => {
          return {
            system: item.system,
            path: path,
            placeHolder: '',
            validate: {
              content: '',
              visible: false,
            },
          };
        });
        paths.push(..._paths);
      });
      return {
        deploy_plan_id: scope['deploy_plan_id'],
        module_scope: scope['module_scope'],
        host_scope: scope['host_scope'],
        deleValidate: {
          content: window.$t('至少要有一个接入对象'),
          visible: false,
        },
        validate: {
          content: '',
          visible: false,
        },
        scope_config: {
          paths: paths,
        },
      };
    },
    getCheckedIps(data) {
      let res = [];
      data[0].forEach(d => {
        res.push(d);
      });

      this.params.scope[data[1]].host_scope = res.filter(ip => {
        return ip.checked === true;
      });
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
      this.params.scope[data[1]].module_scope = res;
    },
    handleSelectModule(index) {
      this.activeAccessObjIndex = index;
      if (!this.validateBizid(index)) {
        this.$forceUpdate();
        return false;
      }
      return true;
      // const currentAccessObj = this.params.scope[index]
      // 更新模块IP初始化设置
      // this.initModuleIpInfo = {
      //     module: currentAccessObj.module_scope,
      //     ip: currentAccessObj.host_scope
      // }

      // let modlists = []
      // currentAccessObj.module_scope.forEach(s => {
      //     modlists.push(s)
      // })
      // this.modList = modlists
      // this.$refs.selectModuleAndIP.open(index)
    },
    handleDeleteOrAppendLog(mode, i, index) {
      let paths = this.params.scope[i].scope_config.paths;
      if (mode === 'min') {
        paths.forEach((path, l) => {
          if (l === index && paths.length > 1) {
            paths.splice(l, 1);
          }
        });
      } else if (mode === 'plus') {
        if (paths.length >= this.addValidate.pathsCount) {
          this.$set(this.params.scope[i], 'visible', true);
          this.$nextTick(() => {
            setTimeout(() => {
              this.$set(this.params.scope[i], 'visible', false);
            }, 1000);
          });
          this.$forceUpdate();
          return;
        }
        this.currFocus.appendIndex = paths.length;
        this.currFocus.focus = true;
        // 复制最后一行作为新的一行
        this.$set(paths, paths.length, JSON.parse(JSON.stringify(paths[paths.length - 1])));
      }
    },
    /**
     * 删除接入对象
     */
    handleDeleteScope(index, mod, scope) {
      // 若刚好配置了当前接入对象的模块IP,为避免模块IP选择器声明时取值异常, 将activeAccessObjIndex置为0
      if (this.activeAccessObjIndex === index) {
        this.activeAccessObjIndex = 0;
      }

      // 至少需要一个接入对象
      if (this.params.scope.length < 2) {
        this.$set(scope.deleValidate, 'visible', true);
        this.$forceUpdate();
        setTimeout(() => {
          this.$set(scope.deleValidate, 'visible', false);
          this.$forceUpdate();
        }, 1000);
        return;
      }
      this.params.scope.splice(index, 1);
    },
    /**
     * 添加接入对象
     */
    handleAddAccessObject() {
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
      this.params.scope.splice(len + 1, 0, {
        module_scope: [],
        host_scope: [],
        validate: {
          content: '',
          visible: false,
        },
        scope_config: {
          paths: [
            {
              system: 'linux',
              path: '',
              placeHolder: '',
              validate: {
                content: '',
                visible: false,
              },
            },
          ],
        },
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.bk-access-object {
  flex-direction: column;
  .select-type {
    display: flex;
    // line-height: 2;
    .title {
      &.log-path {
        padding: 2px 0;
      }
    }
    .bk-form-content {
      width: 520px;
      display: flex;
      align-items: center;
      margin-left: 0;

      &.path-row {
        width: 550px;
      }

      .bk-form-input {
        &.log-path {
          border-left: 0;
          height: 30px;
          font-size: 12px;
          width: 445px;
        }
      }

      &:last-child {
        margin-bottom: 0 !important;
      }
    }
    i {
      //   padding: 8px 0 0 10px;
      font-size: 20px;
      cursor: pointer;
    }
  }
  .pick-type {
    flex: 1;
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
</style>
