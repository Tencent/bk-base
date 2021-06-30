

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
  <div class="access-object-container">
    <carousel v-if="details.access_conf_info"
      :pageSize="scopePageSize"
      :height="200"
      @change="handleCarouselChange">
      <carousel-item
        v-for="(item, index) in details.access_conf_info.resource.scope"
        :key="index"
        :itemIndex="index"
        :title="$t('点击查看接入对象详情')"
        @itemChecked="handleScopeItemClick(index)">
        <ScopeItem
          :ref="'scope_item_' + index"
          :step="index + 1"
          :width="'100%'"
          :height="190"
          :class="[activeScope === index && 'active', 'scope-item']">
          <AccessDBDetails :details="item"
            :filterCondition="filterCondition" />
        </ScopeItem>
      </carousel-item>
    </carousel>
  </div>
</template>
<script>
import ScopeItem from '../Components/Scope/ScopeForm';
import carousel from '@/components/carousel/carousel';
import carouselItem from '@/components/carousel/carouselItem';
import AccessDBDetails from '@/pages/DataAccess/NewForm/FormItems/Components/AccessDBDetails.vue';
export default {
  components: { ScopeItem, carousel, carouselItem, AccessDBDetails },
  props: {
    details: {
      type: Object,
      default: () => {},
    },
    ipList: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      activeScope: 0,
      scopePageSize: 2,
      activePageIndex: 0,
      aciveItemInPage: {},
      arrawTipsStyle: {},
    };
  },
  computed: {
    activeScopeDetails() {
      if (!this.details.access_conf_info) {
        return {};
      }
      return this.details.access_conf_info.resource.scope[this.activeScope];
    },
    filterCondition() {
      return (
        (Array.isArray(this.details.access_conf_info.filters)
          && this.details.access_conf_info.filters.reduce((filter, currernt) => {
            return `${filter}${(filter && currernt.logic_op) || ''}
              ${currernt.key} ${currernt.op} ${currernt.value} `;
          }, ''))
        || '-'
      );
    },
  },
  watch: {
    activeScope: {
      handler(val) {
        this.$nextTick(() => {
          this.setArrawTipsStyle();
        });
      },
      immediate: true,
    },
  },
  methods: {
    handleCarouselChange(index, position) {
      this.activePageIndex = index;
      this.$nextTick(() => {
        const preActiveIndex = this.aciveItemInPage[`${this.activePageIndex}`];
        this.activeScope = preActiveIndex !== undefined ? preActiveIndex : this.scopePageSize * (index - 1) + 1;
        this.$set(this.aciveItemInPage, this.activePageIndex, this.activeScope);
        this.setArrawTipsStyle(index);
      });
    },
    handleScopeItemClick(index) {
      this.$set(this.aciveItemInPage, this.activePageIndex, index);
      this.activeScope = index;
    },
    setArrawTipsStyle(index) {
      const activeComp = this.$refs[`scope_item_${this.activeScope}`];
      const activeItem = (activeComp && activeComp[0] && activeComp[0].$el) || null;

      if (!activeItem) {
        return;
      }

      const container = activeItem.closest('.bk-carousel-items');
      const parentOffsetMath = container && container.style.transform.match(/.*\((-?\d{0,10})px/);
      const parentOffset = (parentOffsetMath && Number(parentOffsetMath[1])) || 1;
      if (activeItem) {
        const width = activeItem.offsetWidth;
        let offsetleft = activeItem.offsetLeft + width / 2 + parentOffset;
        offsetleft = offsetleft > 240 ? offsetleft : 240;
        this.arrawTipsStyle = {
          left: `${offsetleft}px`,
        };
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.access-object-container {
  margin-top: -20px;
}

.file-path {
  display: flex;
  align-items: center;
}

.scope-item {
  cursor: pointer;
  margin: 0 5px;
  &.active {
    box-shadow: 1px 1px 6px rgba(83, 84, 165, 0.6);
  }
}

::v-deep .access-preview-file {
  align-items: flex-start;
}

.icon-edit {
  cursor: pointer;

  &:hover {
    color: #5354a5;
  }
}
</style>
