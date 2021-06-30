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

<!--eslint-disable -->


<template>
    <div id="data-map-wrap">
        <Layout :crumbName="[{ name: $t('数据地图'), to: '/data-mart/data-map' }]">
            <template slot="defineTitle">
                <a href="javascript:void(0);"
                    class="crumb-item with-router"
                    @click.stop="reflshPage('DataMap')">
                    {{ $t('数据地图') }}
                </a>
            </template>
            <span class="crumb-desc">{{ $t('对数据进行分类盘点') }}</span>
            <div class="page-main">
                <div class="metadata-table">
                    <div class="pull-left">
                        <div>
                            <div class="bkdata-button-group"
                                style="margin-left: 0px">
                                <bkdata-button :class="[
                                'button-group',
                                groupSetting.selected === 'allData'
                                ? 'is-selected' : '']"
                                    @click="groupSetting.selected = 'allData'">
                                    {{ $t('全量数据') }}
                                </bkdata-button>
                                <bkdata-button :disabled="true"
                                    :class="['button-group',
                                    groupSetting.selected === 'opsTheme' ? 'is-selected' : '']"
                                    @click="groupSetting.selected = 'opsTheme'">
                                    {{ $t('运维主题') }}
                                </bkdata-button>
                                <bkdata-button :disabled="true"
                                    :class="['button-group',
                                    groupSetting.selected === 'operationTheme' ? 'is-selected' : '']"
                                    @click="groupSetting.selected = 'operationTheme'">
                                    {{ $t('运营主题') }}
                                </bkdata-button>
                            </div>
                        </div>
                    </div>
                    <!-- <div class="pull-right" style="margin-left:15px;">
                    <div class="bk-form-content">
                        <bkdata-input type="text" style="width:250px"
                            v-model="search_text"
                            :placeholder="$t('数据集名_表名_描述_标签')"
                            :right-icon="'bk-icon icon-search'"/>
                    </div>
                </div> -->
                    <div class="pull-right"
                        style="margin-left: 15px">
                        <bkdata-selector class="map-select"
                            searchable
                            :allowClear="true"
                            :selected.sync="project_id"
                            :isLoading="projectLoading"
                            :displayKey="'displayName'"
                            :placeholder="$t('按项目筛选')"
                            :list="projectList"
                            :settingKey="'project_id'"
                            searchKey="displayName"
                            @change="choseProject" />
                    </div>
                    <div class="pull-right"
                        style="margin-left: 15px">
                        <bkdata-selector
                            class="map-select"
                            searchable
                            :allowClear="true"
                            :selected.sync="biz_id"
                            :isLoading="bizLoading"
                            :displayKey="'displayName'"
                            :placeholder="$t('按业务筛选')"
                            :list="bizList"
                            :settingKey="'bk_biz_id'"
                            searchKey="displayName"
                            @item-selected="changeBkBizName"
                            @clear="reSetBkBizName" />
                    </div>
                    <div style="clear: both" />
                </div>

                <!-- 标签选择器 start -->
                <div>
                    <div
                        class="border-up-empty"
                        :class="{
                            'all-data': groupSetting.selected === 'allData',
                            'ops-theme': groupSetting.selected === 'opsTheme',
                            'operation-theme': groupSetting.selected === 'operationTheme',
                        }">
                        <span />
                    </div>
                    <div class="function-area">
                        <div id="tag-div"
                            v-bkloading="{ isLoading: tag_loading }"
                            style="font-size: 13px; background-color: rgba(225, 236, 255, 0.5); border: 1px solid rgb(205, 209, 214); margin-bottom: 10px; color: #666bb4; min-height: 84.5px">
                            <div v-for="(row, row_index) in tag_list"
                                :key="row_index">
                                <div
                                    v-if="!row.is_not_show"
                                    :class="[{ 'tag-row-dashed': row_index != 0 && row_index != 4 && row_index != 5 },
                                        { 'tag-row-dashed-no-biz': row_index == 0 },
                                        { 'tag-row-dashed-no-biz-type': row_index == 1 },
                                        { 'tag-row-solid': row_index == 0 }]"
                                    style="overflow: hidden; white-space: nowrap; display: flex; position: relative; align-items: center">
                                    <div class="ver-center"
                                        style="font-size: 14px; display: inline-block; width: 100px">
                                        <div v-if="row.tag_code != 'datamap_source' && row.tag_code != 'datamap_other' && row.is_click"
                                            class="ver-center tag-width"
                                            style="cursor: pointer; height: 19px; display: flex; align-items: center"
                                            @click="clickTag(row, row.tag_alias, false)">
                                            <bkdata-popover placement="top"
                                                class="tag-height">
                                                <span class="tag-chosen str-omit tag-height"
                                                    style="font-weight: 900; max-width: 88px; position: relative">
                                                    <span class="str-omit font15"
                                                        style="max-width: 73px; padding-right: 14px; line-height: 17px; height: 100%">
                                                        {{ getEnName(row) }}
                                                    </span>
                                                    <span class="close tag-close" />
                                                </span>
                                                <div slot="content"
                                                    style="white-space: normal">
                                                    <div class="data-map-label-tips">
                                                        {{ row.tooltip }}
                                                    </div>
                                                </div>
                                            </bkdata-popover>
                                            <span
                                                v-if="row_index <= 2"
                                                class="icon-heat-fill"
                                                :style="{
                                                    color: row_index === 0 ? 'red' : row_index === 1 ? '#f15602' : '#f6700b',
                                                }" />
                                        </div>

                                        <div v-if="!['datamap_source', 'datamap_other', 'data_type'].includes(row.tag_code) && !row.is_click"
                                            class="ver-center tag-height tag-width"
                                            style="cursor: pointer"
                                            @click="clickTag(row, row.tag_alias, false)">
                                            <bkdata-popover placement="top"
                                                class="tag-height">
                                                <div class="tag-heat-wrap">
                                                    <span class="str-omit tag-height font15"
                                                        style="outline: none; font-weight: 900; padding-bottom: 0px; display: inline-block; max-width: 88px">
                                                        {{ getEnName(row) }}
                                                    </span>
                                                    <span
                                                        v-if="row_index <= 2"
                                                        class="icon-heat-fill"
                                                        :style="{
                                                            color: row_index === 0 ? 'red' : row_index === 1 ? '#f15602' : '#f6700b',
                                                        }" />
                                                </div>
                                                <!-- tooltip换行 -->
                                                <div slot="content"
                                                    style="white-space: normal">
                                                    <div class="data-map-label-tips">
                                                        {{ row.tooltip }}
                                                    </div>
                                                </div>
                                            </bkdata-popover>
                                        </div>
                                        <div v-if="['datamap_source', 'datamap_other', 'data_type'].includes(row.tag_code)"
                                            class="ver-center tag-width"
                                            style="height: 19px; display: inline-block">
                                            <span class="str-omit no-hover font15"
                                                :title="getEnName(row)"
                                                style="font-weight: 900; padding-bottom: 0px; display: inline-block; max-width: 88px; height: 19px; line-height: 19px">
                                                {{ getEnName(row) }}
                                            </span>
                                        </div>
                                    </div>

                                    <div
                                        id="topn-width"
                                        :style="{
                                            width: 88 * topn + 'px',
                                            minHeight: '1px',
                                            overflow: 'hidden',
                                            display: 'flex',
                                            alignItems: 'center',
                                        }">
                                        <div v-for="(item, index) in row.sub_top_list"
                                            :key="index"
                                            style="display: inline-block"
                                            class="hori-ver-center">
                                            <div v-if="item.is_click"
                                                class="hori-ver-center tag-height"
                                                style="width: 88px; cursor: pointer; display: flex; align-items: center; justify-content: center"
                                                @click="clickTag(item, row.tag_alias, true)">
                                                <span :title="getEnName(item)"
                                                    class="tag-chosen str-omit"
                                                    style="font-weight: 900; max-width: 88px; position: relative; height: 19px">
                                                    <span class="str-omit"
                                                        :class="{ font12: index > 0 }"
                                                        style="max-width: 73px; padding-right: 14px; height: 100%; line-height: 17px">
                                                        {{ getEnName(item) }}
                                                    </span>
                                                    <span class="close tag-close" />
                                                </span>
                                            </div>
                                            <div v-if="!item.is_click"
                                                class="hori-ver-center tag-height"
                                                style="width: 88px; cursor: pointer"
                                                @click="clickTag(item, row.tag_alias, true)">
                                                <span :title="getEnName(item)"
                                                    class="str-omit"
                                                    :class="{ font12: index > 0 }"
                                                    style="outline: none; padding-bottom: 0px; display: inline-block; max-width: 88px">
                                                    {{ getEnName(item) }}
                                                </span>
                                            </div>
                                        </div>
                                    </div>

                                    <div v-if="row.tag_code != 'data_type'"
                                        style="width: 88px; display: inline-block; text-align: center; cursor: pointer; position: absolute; right: 0"
                                        @click="clickRowMore(row)">
                                        <div v-if="!row.is_more_show"
                                            style="height: 19px">
                                            <span style="padding-top: 0px; margin-bottom: 0px; display: inline-block; color: #504848">{{ $t('更多') }}</span>
                                            <i class="bk-icon icon-down-shape"
                                                style="color: #504848; margin-top: -3px" />
                                        </div>
                                        <div v-if="row.is_more_show"
                                            style="height: 19px">
                                            <span style="padding-top: 0px; margin-bottom: 0px; display: inline-block; color: #504848">{{ $t('收起') }}</span>
                                            <i class="bk-icon icon-up-shape"
                                                style="color: #666bb4; margin-top: -3px" />
                                        </div>
                                    </div>
                                </div>
                                <!-- 点击更多以后显示的内容 -->
                                <div v-if="row.is_more_show"
                                    style="background-color: #ffffff">
                                    <div v-for="(each_sub_row, row_more_index) in row.sub_list"
                                        :key="row_more_index"
                                        style="font-size: 12px; vertical-align: middle; line-height: 20px; outline: none">
                                        <div
                                            :class="[{ 'tag-row-dashed': row_more_index != 0 || row_index != 0 }, { 'tag-row-no-dashed': row_more_index == 0 && row_index == 0 }, { 'tag-row-dashed-no-biz': row_index == 0 }]"
                                            style="overflow: hidden; white-space: nowrap; display: flex; position: relative; outline: none; background-color: #ffffff">
                                            <!-- 最前面空了一列用于保证对齐,只有第一行是有title的 -->
                                            <div style="display: flex; align-item: flex-start; vertical-align: middle; width: 100px; min-height: 1px; line-height: 19px">
                                                <div
                                                    v-if="each_sub_row.title && row.tag_code != 'datamap_source' && row.tag_code != 'datamap_other' && row.is_click"
                                                    class="outline-none ver-center"
                                                    style="padding-left: 3px; height: 100%; width: 88px; display: flex; align-items: flex-start; padding-top: 2px; font-size: 13px; cursor: pointer"
                                                    @click="clickTag(row, row.tag_alias, false)">
                                                    <bkdata-popover placement="top">
                                                        <span class="tag-chosen str-omit"
                                                            style="font-weight: 900; max-width: 88px; position: relative; height: 19px">
                                                            <span class="str-omit font15"
                                                                style="max-width: 73px; padding-right: 14px; height: 18px; line-height: 18px">
                                                                {{ each_sub_row.title }}
                                                            </span>
                                                            <span class="close tag-close" />
                                                        </span>
                                                        <div slot="content"
                                                            style="white-space: normal">
                                                            <div class="data-map-label-tips">
                                                                {{ row.tooltip }}
                                                            </div>
                                                        </div>
                                                    </bkdata-popover>
                                                    <span
                                                        v-if="row_index <= 2"
                                                        class="icon-heat-fill"
                                                        :style="{
                                                            color: row_index === 0 ? 'red' : row_index === 1 ? '#f15602' : '#f6700b',
                                                        }" />
                                                </div>
                                                <div
                                                    v-if="each_sub_row.title && row.tag_code != 'datamap_source' && row.tag_code != 'datamap_other' && !row.is_click"
                                                    class="ver-center outline-none"
                                                    style="padding-left: 3px; height: 19px; width: 88px; display: inline-block; font-size: 14px; cursor: pointer"
                                                    @click="clickTag(row, row.tag_alias, false)">
                                                    <bkdata-popover placement="top">
                                                        <div class="tag-heat-wrap">
                                                            <span class="str-omit font15"
                                                                style="font-weight: 900; padding-bottom: 0px; display: inline-block; max-width: 88px; height: 18px; line-height: 18px">
                                                                {{ each_sub_row.title }}
                                                            </span>
                                                            <span
                                                                v-if="row_index <= 2"
                                                                class="icon-heat-fill"
                                                                :style="{
                                                                    color: row_index === 0 ? 'red' : row_index === 1 ? '#f15602' : '#f6700b',
                                                                }" />
                                                        </div>
                                                        <div slot="content"
                                                            style="white-space: normal">
                                                            <div class="data-map-label-tips">
                                                                {{ row.tooltip }}
                                                            </div>
                                                        </div>
                                                    </bkdata-popover>
                                                </div>
                                                <div v-if="!each_sub_row.title"
                                                    class="ver-center outline-none tag-width"
                                                    style="pointer-events: none; height: 19px; display: inline-block; font-size: 13px; cursor: pointer"
                                                    @click="clickTag(row, row.tag_alias, false)">
                                                    <span class="str-omit font15"
                                                        :class="{ fw600: row_more_index === 0 }"
                                                        style="font-weight: 900; padding-bottom: 0px; display: inline-block; min-width: 88px; height: 18px; line-height: 18px">
                                                        {{ $t(each_sub_row.title) }}
                                                    </span>
                                                </div>
                                                <div v-if="(each_sub_row.title && row.tag_code == 'datamap_source') || row.tag_code == 'datamap_other'"
                                                    class="tag-width"
                                                    style="height: 19px; display: inline-block; font-size: 13px; vertical-align: middle">
                                                    <span class="str-omit no-hover font15"
                                                        :class="{ fw600: row_more_index === 0 }"
                                                        style="font-weight: 900; padding-bottom: 0px; display: inline-block; max-width: 88px; height: 18px; line-height: 18px">
                                                        {{ $t(each_sub_row.title) }}
                                                    </span>
                                                </div>
                                            </div>
                                            <div
                                                id="topn-width-plus3"
                                                :style="{
                                                    width: 88 * topn + 3 + 'px',
                                                    minHeight: '1px',
                                                    overflow: 'hidden',
                                                    display: 'flex',
                                                }">
                                                <div class="hori-ver-center"
                                                    style="display: inline-block; width: 88px">
                                                    <div
                                                        v-if="each_sub_row.is_click && each_sub_row.tag_alias != $t('其它')"
                                                        class="hori-ver-center"
                                                        style="width: 88px; cursor: pointer; height: 19px; line-height: 19px; display: flex; align-item: center; justify-content: center"
                                                        @click="clickTag(each_sub_row, row.tag_alias, true)">
                                                        <bkdata-popover placement="top">
                                                            <span class="str-omit tag-chosen hori-ver-center"
                                                                style="font-size: 13px; font-weight: 450; max-width: 88px; height: 18px; line-height: 18px; position: relative">
                                                                <span class="str-omit fw600"
                                                                    style="max-width: 73px; padding-right: 14px; height: 100%; line-height: 16px">
                                                                    {{ getEnName(each_sub_row) }}
                                                                </span>
                                                                <span class="close tag-close" />
                                                            </span>
                                                            <div slot="content"
                                                                style="white-space: normal">
                                                                <div class="data-map-label-tips">
                                                                    {{ each_sub_row.tooltip }}
                                                                </div>
                                                            </div>
                                                        </bkdata-popover>
                                                    </div>
                                                    <div
                                                        v-if="!each_sub_row.is_click && each_sub_row.tag_alias != $t('其它')"
                                                        class="hori-ver-center"
                                                        style="height: 19px; width: 88px; cursor: pointer; line-height: 19px; display: flex; align-item: center; justify-content: center"
                                                        @click="clickTag(each_sub_row, row.tag_alias, true)">
                                                        <bkdata-popover placement="top">
                                                            <span class="str-omit hori-ver-center fw600"
                                                                style="font-size: 13px; font-weight: 450; max-width: 88px; height: 18px; line-height: 18px">
                                                                {{ getEnName(each_sub_row) }}
                                                            </span>
                                                            <div slot="content"
                                                                style="white-space: normal">
                                                                <div class="data-map-label-tips">
                                                                    {{ each_sub_row.tooltip }}
                                                                </div>
                                                            </div>
                                                        </bkdata-popover>
                                                    </div>

                                                    <div v-if="each_sub_row.tag_alias == $t('其它')"
                                                        class="hori-ver-center"
                                                        style="height: 19px; width: 88px; line-height: 19px">
                                                        <span class="str-omit hori-ver-center fw600"
                                                            style="font-size: 13px; font-weight: 450; max-width: 88px; height: 18px; line-height: 18px">
                                                            {{ getEnName(each_sub_row) }}
                                                        </span>
                                                    </div>
                                                </div>

                                                <div
                                                    id="topn-minus1-width-plus3"
                                                    :style="{
                                                        width: 88 * (topn - 1) + 3 + 'px',
                                                        minHeight: '1px',
                                                        whiteSpace: 'normal',
                                                    }">
                                                    <div v-for="(item, index) in each_sub_row.sub_list"
                                                        :key="index"
                                                        class="hori-ver-center"
                                                        style="display: inline-block">
                                                        <div v-if="item.is_click"
                                                            class="hori-ver-center"
                                                            style="width: 88px; cursor: pointer; display: flex; justify-content: center; align-item: flex-start"
                                                            @click="clickTag(item, row.tag_alias, false)">
                                                            <span :title="getEnName(item)"
                                                                class="str-omit hori-ver-center tag-chosen"
                                                                style="height: 18px; line-height: 18px; max-width: 88px; position: relative">
                                                                <span class="str-omit fw600"
                                                                    style="max-width: 73px; padding-right: 14px; height: 100%; line-height: 16px">
                                                                    {{ getEnName(item) }}
                                                                </span>
                                                                <span class="close tag-close" />
                                                            </span>
                                                        </div>
                                                        <div v-if="!item.is_click"
                                                            class="hori-ver-center"
                                                            style="width: 88px; cursor: pointer; line-height: 19px; height: calc(100% - 2px); display: flex; justify-content: center; align-item: flex-start"
                                                            @click="clickTag(item, row.tag_alias, false)">
                                                            <span :title="getEnName(item)"
                                                                class="str-omit hori-ver-center"
                                                                style="height: 18px; line-height: 18px; padding-bottom: 0px; max-width: 88px">
                                                                {{ getEnName(item) }}
                                                            </span>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                            <div style="width: 88px; float: left">
                                                <div v-if="each_sub_row.title"
                                                    style="width: 88px; display: inline-block; text-align: center; cursor: pointer; position: absolute; right: 0"
                                                    @click="clickRowMore(row)">
                                                    <span style="color: #504848; padding-top: 0px; margin-bottom: 0px; display: inline-block; line-height: 20px; font-size: 13px">{{ $t('收起') }}</span>
                                                    <i class="bk-icon icon-up-shape"
                                                        style="color: #504848; margin-top: -3px" />
                                                </div>
                                            </div>
                                            <div style="clear: both" />
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <!-- 标签选择器 end -->
                <div id="flowcontainer"
                    ref="flowcontainer"
                    style="margin-top: 10px; position: relative"
                    class="clearfix">
                    <div
                        v-bkloading="{ isLoading: sta_loading }"
                        style="width: 320px; height: 669px; margin-left: 18px; padding: 10px 15px; z-index: 999"
                        :class="['connected-info', 'no-shadows', 'mb10', { 'switch-remove': !showStatisticData, right70: isExpend, right0: !isExpend }, { 'switch-no-remove': showStatisticData }]">
                        <div class="switch"
                            :class="[isZhCn ? '' : 'en-switch']"
                            @click="clickStatisticData">
                            {{ showStatisticData ? $t('收起') : $t('展开') }}
                        </div>
                        <div id="indicator-sta"
                            :class="['inner-info', 'Custom-scroll', 'box-card']"
                            style="height: 100%">
                            <p style="margin-bottom: 4px; font-size: 14px; font-weight: bolder">
                                {{ $t('共检索到') }}
                            </p>
                            <div style="width: 100%; height: 65px; display: flex; color: #1a1b2d">
                                <div id="data-source-card"
                                    class="card-sta hori-ver-center"
                                    style="width: calc(50% - 5px); height: 65px">
                                    <div class="header clearfix"
                                        style="background-color: #e1ecff">
                                        <span class="icon-datasource" />
                                        <span>{{ $t('数据源') }}</span>
                                    </div>
                                    <div class="text item"
                                        style="margin-top: 5px">
                                        <span style="cursor: pointer; font-weight: bolder; font-size: 18px; height: 20px; vertical-align: middle"
                                            @click="linkToDictionary('raw_data')">
                                            {{ sta_data.data_source_count }}
                                        </span>
                                    </div>
                                </div>

                                <div id="dataset-card"
                                    class="card-sta hori-ver-center"
                                    style="margin-left: 10px; width: calc(50% - 5px); height: 65px">
                                    <div class="header clearfix"
                                        style="background-color: #e1ecff">
                                        <span class="icon-resulttable" />
                                        <span>{{ $t('结果表') }}</span>
                                    </div>
                                    <div class="text item"
                                        style="margin-top: 5px">
                                        <span style="font-weight: bolder; font-size: 18px; height: 20px; vertical-align: middle">
                                            <span
                                                v-if="is_standard && sta_data.standard_dataset_count"
                                                v-bk-tooltips.bottom="$t('结果表_标准化')"
                                                :style="{
                                                    color: '#83cf51',
                                                    cursor: isShowStandard ? 'pointer' : 'inherit',
                                                }"
                                                @click="linkToDictionary('standard_table')">
                                                {{ isShowStandard ? sta_data.standard_dataset_count : 0 }}
                                            </span>
                                            <span v-if="is_standard && sta_data.standard_dataset_count">/ </span>
                                            <span v-bk-tooltips.bottom="$t('结果表_总')"
                                                style="cursor: pointer"
                                                @click="linkToDictionary('result_table')">
                                                {{ sta_data.dataset_count }}
                                            </span>
                                        </span>
                                    </div>
                                </div>
                            </div>
                            <p style="margin-bottom: 4px; margin-top: 10px; z-index: 100; font-size: 14px; font-weight: bolder">
                                {{ $t('分布于') }}
                            </p>
                            <div style="width: 100%; height: 65px; display: flex; color: #1a1b2d">
                                <div id="biz-card"
                                    class="card-sta hori-ver-center"
                                    style="width: calc(50% - 5px); height: 65px">
                                    <div class="header clearfix">
                                        <span class="icon-biz" />
                                        <span>{{ $t('业务') }}</span>
                                    </div>
                                    <div class="text item"
                                        style="margin-top: 5px">
                                        <span style="font-weight: bolder; font-size: 18px; height: 20px; vertical-align: middle">{{ sta_data.bk_biz_count }}</span>
                                    </div>
                                </div>

                                <div id="project-card"
                                    class="card-sta hori-ver-center"
                                    style="width: calc(50% - 5px); height: 65px; margin-left: 10px">
                                    <div class="header clearfix">
                                        <span class="icon-biz" />
                                        <span>{{ $t('项目') }}</span>
                                    </div>
                                    <div class="text item"
                                        style="margin-top: 5px">
                                        <span style="font-weight: bolder; font-size: 18px; height: 20px; vertical-align: middle">{{ sta_data.project_count }}</span>
                                    </div>
                                </div>
                            </div>
                            <p style="margin-bottom: 4px; margin-top: 10px; z-index: 100; font-size: 14px; font-weight: bolder">
                                {{ $t('增长趋势') }}
                            </p>
                            <div id="project-card"
                                class="box-card card-line hori-ver-center"
                                style="width: 100%; height: 202px">
                                <div class="header clearfix">
                                    <span class="icon-datasource" />
                                    <span style="color: #1a1b2d">{{ $t('数据源增长') }}</span>
                                </div>
                                <div class="text item"
                                    style="margin-top: 0px; padding: 10px 10px">
                                    <chartEditor :chartData="sta_data.recent_data_source_format"
                                        :chartConfig="{ operationButtons: [] }"
                                        :chartLayout="chartLayout_datasource" />
                                </div>
                            </div>

                            <div class="box-card card-line hori-ver-center"
                                style="margin-top: 10px; width: 100%; height: 213px">
                                <div class="header clearfix">
                                    <span class="icon-resulttable" />
                                    <span style="color: #1a1b2d">{{ $t('结果表增长') }}</span>
                                </div>
                                <div class="text item"
                                    style="margin-top: 0px; padding: 10px 10px">
                                    <chartEditor :chartData="sta_data.recent_dataset_format"
                                        :chartConfig="{ operationButtons: [] }"
                                        :chartLayout="chartLayout_dataset" />
                                </div>
                            </div>
                        </div>
                    </div>
                    <div id="tree-struc"
                        style=""
                        :class="[{ 'whole-width': !showStatisticData }, { 'limit-width': showStatisticData }]">
                        <div class="box-card type shadows">
                            <div class="legend-div">
                                <div class="fl"
                                    style="display: flex; height: 40px">
                                    <div v-bk-tooltips.top="{ content: $t('节点说明'), delay: 0, duration: 0 }"
                                        style="margin-top: 11px; cursor: pointer; outline: none; z-index: 999"
                                        @click="clickInfo()">
                                        <span style="font-size: 18px"
                                            class="icon-info-circle-shape" />
                                    </div>
                                    <div id="legend-desc"
                                        class="nowrap"
                                        style="margin-top: 10px; margin-left: 2px; display: none">
                                        {{ $t('节点说明') }}
                                    </div>
                                    <div style="overflow: hidden">
                                        <div style="margin-left: 3px; z-index: 999"
                                            :class="[{ 'legend-remove': !is_info_show }, { 'legend-no-remove': is_info_show }]">
                                            <div style="height: 40px; right: 5px; z-index: 999">
                                                <div style="height: 40px">
                                                    <div style="display: flex">
                                                        <div style="display: flex; height: 39px">
                                                            <div id="<%= id %>"
                                                                style="margin-left: 3px; -webkit-box-shadow: 0px 0px 0px #aaa !important; cursor: default"
                                                                class="legend-node-background jtk-window jtk-node current"
                                                                data-placement="bottom"
                                                                data-node="<%= id %>"
                                                                data-type="current">
                                                                <div class="node-background-other"
                                                                    :title="$t('数据分类')"
                                                                    style="height: 26px; display: flex; align-items: center; justify-content: space-between; padding: 0 5px">
                                                                    <span class="node-desc-map">
                                                                        {{ $t('数据分类') }}
                                                                    </span>
                                                                </div>
                                                            </div>
                                                            <div
                                                                id="<%= id %>"
                                                                style="margin-left: 5px; -webkit-box-shadow: 0px 0px 0px #aaa !important; cursor: default"
                                                                class="legend-node-background jtk-window jtk-node hasstandard bk-mark-triangle-map-legend bk-success-map"
                                                                data-toggle="popover"
                                                                data-placement="bottom"
                                                                data-node="<%= id %>"
                                                                data-type="hasstandard">
                                                                <div class="node-background-other"
                                                                    style="height: 26px; display: flex; align-items: center; justify-content: space-between; padding: 0 5px">
                                                                    <span class="node-desc-map"
                                                                        :title="$t('含标准数据')"
                                                                        style="display: inline-block; vertical-align: middle; text-align: left">
                                                                        {{ $t('含标准数据') }}
                                                                    </span>
                                                                </div>
                                                            </div>
                                                            <div
                                                                id="<%= id %>"
                                                                style="margin-left: 5px; -webkit-box-shadow: 0px 0px 0px #aaa !important; cursor: default"
                                                                class="legend-node-background jtk-window jtk-node standard bk-mark-square-map-legend"
                                                                :class="[isZhCn ? '' : 'before-hidden']"
                                                                data-placement="bottom"
                                                                data-node="<%= id %>"
                                                                data-type="standard">
                                                                <p :class="[isZhCn ? '' : 'hidden']"
                                                                    class="stan-p-legend">
                                                                    {{ $t('标准') }}
                                                                </p>
                                                                <div class="node-background-standard"
                                                                    style="height: 26px; display: flex; align-items: center; justify-content: space-between; padding: 0 14px 0 5px">
                                                                    <span class="node-desc-map"
                                                                        :title="$t('标准')">
                                                                        {{ $t('标准分类') }}
                                                                    </span>
                                                                </div>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </div>
                                                <!-- <div style="border: 1px solid #ddd;border-top:0;height:52px;width:325px;display:flex">
                                                <div style="border-right: 1px solid #ddd;width:30px">
                                                    <span class="icon-layer" style="margin-left: 7px;margin-top: 19px;"></span>
                                                </div>
                                                <div style="padding:10px 7px">
                                                    <bkdata-button @click="layerClick('quality')" :class="['button-group', layerSetting.selected === 'quality' ? 'is-selected-legend' : '']" style="margin-right:3px;width:88px">数据质量</bkdata-button>
                                                    <bkdata-button @click="layerClick('heat')" :class="['button-group', layerSetting.selected === 'heat' ? 'is-selected-legend' : '']" style="margin-right:3px;width:88px">数据热度</bkdata-button>
                                                    <bkdata-button @click="layerClick('value')" :class="['button-group', layerSetting.selected === 'value' ? 'is-selected-legend' : '']" style="width:88px">数据价值</bkdata-button>
                                                </div>
                                            </div> -->
                                            </div>
                                        </div>
                                    </div>
                                </div>

                                <div class="fr"
                                    style="margin-right: 5px; right: 0; z-index: 999">
                                    <div style="height: 40px; right: 5px; z-index: 99">
                                        <div style="height: 40px">
                                            <div style="display: flex; justify-content: flex-end">
                                                <bkdata-switcher v-model="show_count_zero"
                                                    size="small"
                                                    theme="primary"
                                                    style="margin-top: 11px"
                                                    @change="val => change_show_count_zero(val)" />
                                                <div style="margin-top: 9px; margin-left: 5px">
                                                    <span class="nowrap"
                                                        style="font-size: 12px">
                                                        {{ $t('显示空分类') }}
                                                    </span>
                                                </div>
                                                <bkdata-switcher v-model="just_show_standard"
                                                    size="small"
                                                    theme="primary"
                                                    style="margin-top: 11px; margin-left: 13px"
                                                    @change="val => change_show_non_standard(val)" />
                                                <div style="margin-top: 9px; margin-left: 5px">
                                                    <span class="nowrap"
                                                        style="font-size: 12px">
                                                        {{ $t('仅统计标准数据') }}
                                                    </span>
                                                </div>
                                                <i v-if="!is_full_screen"
                                                    id="full-screen"
                                                    v-bk-tooltips="$t('全屏')"
                                                    class="bk-icon icon-full-screen icon-screen"
                                                    @click="full_screen" />
                                                <i v-if="is_full_screen"
                                                    id="exit-full-screen"
                                                    style="position: relative"
                                                    class="bk-icon icon-un-full-screen icon-screen"
                                                    @mouseleave="isQuitScreen = false"
                                                    @mouseover="isQuitScreen = true"
                                                    @click="un_full_screen">
                                                    <div v-if="isQuitScreen"
                                                        class="h-tooltip vue-tooltip vue-tooltip-visible"
                                                        x-placement="bottom-start"
                                                        style="position: absolute; left: -380%; top: 20px">
                                                        <div class="tooltip-arrow"
                                                            x-arrow=""
                                                            style="right: 0px" />
                                                        <div class="tooltip-content text-overflow">
                                                            {{ $t('退出全屏') }}
                                                        </div>
                                                    </div>
                                                </i>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <!-- 图例 end-->
                            <div id="dfv2-padding"
                                v-bkloading="{ isLoading: dataflow_loading }">
                                <!-- <dataflow id="dfv2" style="width:100%;overflow-x:hidden;overflow-y:hidden" ref="dataflow" :options="dataflow.options" :canvas-draggable="canvasDraggable">
                                <template slot="tools">
                                </template>
                            </dataflow> -->
                                <div id="dfv2" />
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </Layout>
    </div>
</template>

<script>
import { mapGetters } from 'vuex';
import { findParent } from '../../common/js/util.js';
import { confirmMsg, postMethodWarning } from '@/common/js/util';
import chartEditor from '@/components/plotly';
import FlowNodeToolKit from './flowNodeToolkitBothDirection.js';
import Layout from '../../components/global/layout';
import tippy from 'tippy.js';
import loadingImg from '../../common/images/loading.gif';
import Bus from '@/common/js/bus.js';
import D3Graph from '@blueking/bkflow.js';
import NodeTemplate from './dataflow/nodeTemplate.js';
let dataMapD3Graph = null;
// 根节点宽度
const rootWidth = 166;
export default {
  name: 'DataMap',
  components: {
    chartEditor,
    Layout,
  },
  data() {
    return {
      is_full_screen: false,
      showStatisticData: true,
      is_info_show: true,
      show_count_zero: true,
      show_non_standard: true,
      just_show_standard: false,
      layerSetting: {
        selected: '',
      },
      dataflow_loading: false,
      sta_loading: false,
      placements: 'top',
      delay: 0,
      duration: 0,
      // top10 业务和项目列表
      top_bk_biz_list: [],
      top_project_list: [],

      groupSetting: {
        selected: 'allData',
      },
      // 项目列表&项目loading
      projectList: [],
      projectLoading: false,
      bizLoading: false,
      chartLayout_datasource: {
        height: 137,
        width: 255,
        xaxis: {
          title: {
            // text: '时间',
            font: {
              size: 12,
            },
          },
          zeroline: true,

          showgrid: false,
          showline: true,
          // 'tickwidth': 1,
          autorange: true,
          range: [0, 5],
          tickfont: { size: 10 },
          tickangle: 30,
        },
        margin: {
          l: 38,
          r: 38,
          b: 27,
          t: 10,
        },
        yaxis: {
          automargin: true,
          title: {
            text: this.$t('个数'),
            font: {
              size: 12,
            },
          },
          rangemode: 'tozero',
          autorange: true,
        },
        titlefont: { size: 12 },
      },
      chartLayout_dataset: {
        height: 162,
        width: 255,
        barmode: 'group',
        // 图例
        legend: {
          orientation: 'h',
          x: 0.05,
          y: -0.28,
          // height:10,
          // width:240,
          // bgcolor: 'rgba(255, 255, 255, 0)',
          // bordercolor: 'rgba(255, 255, 255, 0)'
          font: {
            size: 8,
          },
        },
        hoverinfo: 'y',
        hoverlabel: {
          align: 'left',
        },
        xaxis: {
          title: {
            // text: '时间',
            font: {
              size: 8,
            },
          },
          zeroline: true,
          showgrid: false,
          showline: true,
          autorange: true,
          tickfont: { size: 10 },
          tickangle: 30,
        },
        margin: {
          l: 38,
          r: 38,
          b: 20,
          t: 10,
        },

        yaxis: {
          automargin: true,
          title: {
            text: this.$t('个数'),
            font: {
              size: 12,
            },
          },
          rangemode: 'tozero',
          autorange: true,
        },
        titlefont: { size: 12 },
      },
      chartLayout_standard_dataset: {
        height: 200,
        width: 340,
        hoverinfo: 'y',
        hoverlabel: {
          align: 'left',
        },
        xaxis: {
          title: {
            // text: '时间',
            font: {
              size: 12,
            },
          },
          zeroline: true,
          showgrid: false,
          showline: true,
          autorange: true,
          tickfont: { size: 10 },
          tickangle: 30,
        },
        margin: {
          l: 38,
          r: 38,
          b: 27,
          t: 10,
        },
        yaxis: {
          automargin: true,
          title: {
            text: this.$t('个数'),
            font: {
              size: 12,
            },
          },
          rangemode: 'nonnegative',
          autorange: true,
        },
        titlefont: { size: 12 },
      },
      bar_data: [
        {
          type: 'bar',
          mode: 'markers',
          name: 'Primary',
          marker: {
            color: '#3279ff',
          },
          marker2: {
            color: '#83cf51',
          },
        },
      ],
      line_data: [
        {
          type: 'lines',
          line: {
            color: '#3279ff',
          },
        },
        {
          type: 'lines',
          line: {
            color: '#83cf51',
          },
        },
      ],

      tip_content: '点击取消',
      // switch_options:  ['基础统计', '数据标准'],
      switch_dict: [
        { name: '基础统计', is_clicked: true, code: 'statistic' },
        { name: '数据标准', is_clicked: false, code: 'standard' },
      ],
      checkboxGroup: ['基础统计'],
      customStyle: {
        backgroundColor: 'red',
        width: '150px',
        position: 'relative',
        left: 0,
        top: 0,
        zIndex: 99999999,
      },

      // chartSettings: {
      //     shape: 'circle'
      // },

      cloudbuttoncss: {},
      filterText: null,
      // defaultProps: {
      //     children: 'children',
      //     label: 'label'
      // },
      tree: [],
      parent_tag_list: [],
      value_parent_id: [],
      // value_parent_id列表的最后一个，也就是父标签
      parent_id: '',
      tag_disabled: false,

      layer: 'allData',
      loading: false,
      options_category: [],
      value_tag_type: '',
      search_text: '',
      label_list: [],
      filtered_data: [],
      fileList: [],
      paging: {
        page: 1,
        page_size: 10,
        total_page: 1,
        page_data: [],
      },
      tree_structure: [],
      dataflow: {
        options: {
          isEdit: false,
          defaultColor: '#a9adb5',
          id: 'ch',
          onRemoveNodeBefore: () => {},
          onRemoveNodeAfter: () => {},
          onDblClick: () => {},
          isAllowCreateLine: () => {},
          onCreateNodeAfter: () => {},
          onCreateLineBefore: () => {},
          onToolDrag: () => {},
          onToolDragBefore: () => {},
          onRemoveLineAfter: () => {},
          onRemoveLineBefore: () => {},
          onCreateLineAfter: () => {},
          onNodeMoveAfter: () => {},
          locationConfig: {
            // 节点的类型以及端点的位置
            current: ['Top', 'Left', 'Right', 'Bottom'],
            standard: ['Top', 'Left', 'Right', 'Bottom'],
            other: ['Top', 'Left', 'Right', 'Bottom'],
            zero: ['Top', 'Left', 'Right', 'Bottom'],
            root: ['Top', 'Left', 'Right', 'Bottom'],
            rootstandard: ['Top', 'Left', 'Right', 'Bottom'],
            hasstandard: ['Top', 'Left', 'Right', 'Bottom'],
            curhasstandard: ['Top', 'Left', 'Right', 'Bottom'],
            curstandard: ['Top', 'Left', 'Right', 'Bottom'],
            zerohasstandard: ['Top', 'Left', 'Right', 'Bottom'],
            zerostandard: ['Top', 'Left', 'Right', 'Bottom'],
          },
          data_tmp: {},
          data: {
            version: null,
            lines: [],
            locations: [],
          },
          defaultLocation: {
            // 必须具备的字段
            id: '',
            type: '',
            auto: false,
            has_kafka: false,
            has_data_trend: false,
            has_data_delay: false,
            monitorStartTime: '',
            x: 0,
            y: 0,
            input_data_count: '-',
            output_data_count: '-',
            pointDebugStatus: 'success',
            alert_status: 'unconfig',
            // 额外字段
            start_time: '', // 离线打点调试的时间
            interval: 'null', // 离线打点调试的时间间隔
            name: this.$t('双击进行配置'),
            status: 'unconfig',
            title: '',
            has_modify: false,
            debugStatus: 'no',
            status_display: '调试三分钟',
          },
          showMiniWindow: false, // 是否显示小地图
        },
      },
      entity_data: {
        // 'version': null,
        lines: [],
        locations: [],
      },

      // 用于记录初始entity_data，并在点击+和-时变更entity_data的内容
      entity_data_record: {
        // 'version': null,
        lines: [],
        locations: [],
      },

      canvasDraggable: {
        // dataflow画布可拖动及配置
        isDraggable: true,
        width: 5000,
        height: 5000,
      },
      // 当前节点的位置
      current_entity_x: 150,
      current_entity_y: 300,

      entity_attribute_value: [],

      // 树形结构有关end
      biz_id: '',
      project_id: '',
      // 热门标签展示的内容
      tag_list: [],
      // is_more_show: true,
      is_row_more_show: false,
      chosen_tag: {},
      chosen_tag_list: [],
      tag_loading: false,
      popover_dict: {
        data_source_count: this.$t('数据源'),
        dataset_count: this.$t('结果表'),
        bk_biz_count: this.$t('业务'),
        project_count: this.$t('项目'),
      },
      popover_stan_dict: {
        dataset_count: this.$t('标准结果表'),
        bk_biz_count: this.$t('覆盖业务'),
        project_count: this.$t('应用项目'),
      },
      status_dict: {},
      stInsSearchText: null,
      // 获取当时是dev/stag/prod
      run_mode: null,
      topn: 10,
      total_height: 3000,
      total_width: 1410,
      tooltip_dict: {},
      dataflow_width: 1000,
      dataflow_height: 797,
      sta_data: {},
      // 用于记录选中的图例
      cal_type: [],
      // 用于记录数据标准是否选中
      is_standard: true,
      indicator_width: 400,
      instance: null,
      // 用于记录当前鼠标悬浮的实体
      entity: null,
      // 用于记录tooltip拿到的内容
      popover_info: null,
      instance_list: [],
      tooltip_test: '标准化结果表',
      entity_data_record_count_zero: {},
      is_info_click: false,
      topn_total_width: null,
      isShowMore: false,
      isExpend: false,
      canvas_height_before: null,
      canvas_width_before: null,
      canvas_top_before: null,
      canvas_left_before: null,
      isQuitScreen: false,
      clientWidthTimer: null,
      not_biz_tga_list: ['datamap_source', 'datamap_other', 'data_type'],
      bkBizName: null,
      locale: 'zh-cn',
      isShowStandard: true,
    };
  },
  computed: {
    ...mapGetters({
      allBizList: 'global/getAllBizList',
    }),
    bizList() {
      return this.allBizList.map(item => {
        return Object.assign({}, item, {
          displayName: item.bk_biz_name,
        });
      });
    },
    isZhCn() {
      return this.$i18n.locale === 'zh-cn';
    },
  },
  watch: {
    // 前端过滤
    // 'search_text': function() {
    //     this.stInsSearchText && clearTimeout(this.stInsSearchText)
    //     this.stInsSearchText = setTimeout(() => {
    //         this.$nextTick(() => {
    //             this.setData()
    //             this.getStatisticData()
    //             this.$forceUpdate()
    //         })
    //     }, 1000)
    // },

    topn_total_width: function () {
      // 计算可以展示topn标签
      this.topn = (document.getElementById('tag-div').clientWidth - 176) / 88;
      if (this.topn - parseInt(this.topn) < 0.3) {
        this.topn = parseInt(this.topn) - 1;
      } else {
        this.topn = parseInt(this.topn);
      }
    },
  },
  activated() {
    document.getElementById('tree-struc').style.pointerEvents = 'all'; // 组件缓存时，重新进入需要重置画布的tooltips
  },
  created() {
    let self = this;
    // window.onkeydown = function(e) {
    //     let key = window.event.keyCode
    //     if (key == 27) {
    //         self.un_full_screen();
    //     }
    // }
    document.addEventListener('fullscreenchange', function () {
      if (document.fullscreenElement != null) {
      } else {
        self.exit_full_screen();
      }
    });
  },
  async mounted() {
    if (!this.$modules.isActive('standard')) {
      this.isShowStandard = false;
    }
    // window.onresize = () => {
    //     if (this.clientWidthTimer) return
    //     this.getClientWidth()
    // }
    const that = this;
    dataMapD3Graph = new D3Graph('#dfv2', {
      // mode: 'edit',
      background: 'white',
      autoBestTarget: false,
      nodeConfig: [
        { type: 'default', width: 131, height: 34, radius: '0.5em' },
        { type: 'root', width: 166, height: 52, radius: '0.5em' },
      ],
      lineConfig: {
        canvasLine: false,
        color: 'rgb(169, 173, 181)',
        activeColor: '#3a84ff',
      },
      onNodeRender: node => {
        return NodeTemplate.getTemplate(node.type, node);
      },
    })
      .on('nodeMouseEnter', function (param) {
        // 移到节点改变节点周围线的颜色
        that.show_tooltip(that.entity_attribute_value);
        Array.from(document.getElementsByClassName('bk-graph-connectors')[0].querySelectorAll('path'))
          .filter(item => {
            return item.getAttribute('data-target') === param.id
                        || item.getAttribute('data-source') === param.id;
          })
          .forEach(item => {
            item.style.stroke = '#3a84ff';
          });
      })
      .on('nodeMouseLeave', function (param) {
        Array.from(document.getElementsByClassName('bk-graph-connectors')[0].querySelectorAll('path'))
          .filter(item => {
            return item.getAttribute('data-target') === param.id
                        || item.getAttribute('data-source') === param.id;
          })
          .forEach(item => {
            item.style.stroke = 'rgb(169, 173, 181)';
          });
      })
      .on('canvasDragStart', () => {
        this.showNodeIcon();
      });

    this.getMineProjectList();

    // 计算可以展示topn标签
    this.topn = (document.getElementById('tag-div').clientWidth - 176) / 88;
    if (this.topn - parseInt(this.topn) < 0.3) {
      this.topn = parseInt(this.topn) - 1;
    } else {
      this.topn = parseInt(this.topn);
    }

    /** 标签数据 */
    this.get_tag_sort_count(this.topn);
    // this.dataflow_width = document.getElementById(`canvas`).clientWidth

    // 按照父组件设置折线图的宽度
    this.indicator_width = document.getElementById('indicator-sta').clientWidth;
    this.chartLayout_datasource.width = this.indicator_width - 15 > this.chartLayout_datasource.width
      ? this.indicator_width - 15 : this.chartLayout_datasource.width - 10;
    this.chartLayout_dataset.width = this.indicator_width - 15 > this.chartLayout_dataset.width
      ? this.indicator_width - 15 : this.chartLayout_dataset.width - 10;
    this.chartLayout_standard_dataset.width = this.indicator_width - 15 > this.chartLayout_standard_dataset.width
      ? this.indicator_width - 15
      : this.chartLayout_standard_dataset.width - 10;

    // 首先判断“数据标准”选项卡有无选中，如果没有，则两个卡片占满右侧
    let card_width = null;
    if (!this.is_standard) {
      card_width = parseInt((this.indicator_width - 10) / 2);
    } else {
      card_width = parseInt((this.indicator_width - 20) / 3);
    }

    /** 画布数据 */
    this.setData();

    /** 折线图数据 */
    this.getStatisticData();

    this.show_exit_full_screen_tooltip();

    // 当鼠标悬浮在业务和项目上，显示top10业务和项目
    (window.show_top = item => {
      this.item = item;
      let topId = `top${this.entity.id}`;
      let strTip = '';
      let keys = [];
      let list = [];
      let show_item_class = `show_top${item}`;
      // 后面通过class为show_item_class将background-color设置为浅蓝色
      if (document.getElementsByClassName(show_item_class)[0]) {
        document.getElementsByClassName(show_item_class)[0].style.backgroundColor = '#a3c5fd';
      }
      // 判断item是业务还是项目
      // 判断出是业务还是项目以后去全业务和项目列表去拿到对应的名称展示在tooltip里面
      if (item == 'bk_biz_count') {
        // 如果是业务
        // 右侧的top10从全业务列表中去拿到
        this.top_bk_biz_list = [];
        for (let each_biz of this.bk_biz_list) {
          for (let each_biz_info of this.bizList) {
            if (each_biz == each_biz_info.bk_biz_id) {
              this.top_bk_biz_list.push({
                bk_biz_id: each_biz_info.bk_biz_id,
                bk_biz_name: each_biz_info.bk_biz_name,
              });
              break;
            }
          }
        }
        list = this.top_bk_biz_list;

        strTip +=
          '<div id=\''
          + topId
          + '\' onmouseenter=\'show_item_background("'
          + item
          + '")\' onmouseleave=\'hide_item_background("'
          + item
          + `\")' style='padding-left:10px;border-left:1px solid rgb(205, 209, 214);
          display:block;height:115px;overflow:auto;margin-left:10px;'>
          <table class='table' style='width:140px;'><thead><tr>
          <th class='pop-str-omit text-overflow'>${this.$t('业务名称')}</th></tr></thead>`;
        strTip += '<tbody>';
        keys = ['bk_biz_id', 'bk_biz_name'];
      } else {
        // 如果是项目
        // 右侧的top10从全项目列表中去拿到
        this.top_project_list = [];
        for (let each_project of this.project_list) {
          for (let each_project_info of this.projectList) {
            if (each_project == each_project_info.project_id) {
              this.top_project_list.push({
                project_id: each_project_info.project_id,
                project_name: each_project_info.project_name,
              });
              break;
            }
          }
        }
        list = this.top_project_list;
        strTip +=
          '<div id=\''
          + topId
          + '\' onmouseenter=\'show_item_background("'
          + item
          + '")\' onmouseleave=\'hide_item_background("'
          + item
          + `\")'  style='padding-left:10px;border-left:1px solid rgb(205, 209, 214);
          display:block;height:115px;overflow:auto;margin-left:10px;'>
          <table class='table' style='width:140px;'><thead><tr>
          <th class='pop-str-omit text-overflow'>${this.$t('项目名称')}</th></tr></thead>`;
        strTip += '<tbody>';
        keys = ['project_id', 'project_name'];
      }
      if (list.length > 0) {
        for (let each of list) {
          strTip += '<tr>';
          for (let item of keys) {
            if (item == 'project_id') {
              strTip += `<td class='pop-str-omit text-overflow' title='${each.project_name}'>`;
              strTip += `[${each[item]}]${each.project_name}</td>`;
            } else if (item == 'bk_biz_id') {
              strTip += `<td class='pop-str-omit text-overflow' title='${each.bk_biz_name}'>`;
              strTip += `${each.bk_biz_name}</td>`;
            }
          }
          strTip += '</tr>';
        }
      } else {
        strTip += `<tr><td class='pop-str-omit text-overflow'>${this.$t('暂无')}</td></tr>`;
      }
      strTip += '</tbody>';
      strTip += '</table>';
      // 通过选择器，把display:none的属性选择出来，设置为block

      $('#' + topId).html(strTip);
      document.getElementById(topId).style = 'display:block;';
    }),
    (window.show_item_background = item => {
      this.item = item;
      this.$nextTick(() => {
        let show_item_class = `show_top${item}`;
        // 后面通过class为show_item_class将background-color设置为浅蓝色
        if (document.getElementsByClassName(show_item_class)[0]) {
          document.getElementsByClassName(show_item_class)[0].style.backgroundColor = '#a3c5fd';
        }
      });
    }),
    (window.hide_item_background = item => {
      this.item = item;
      this.$nextTick(() => {
        let show_item_class = `show_top${item}`;
        // 后面通过class为show_item_class将background-color设置为浅蓝色
        if (document.getElementsByClassName(show_item_class)[0]) {
          document.getElementsByClassName(show_item_class)[0].style.removeProperty('background-color');
        }
      });
    }),
    (window.hide_top = item => {
      this.item = item;
      let show_item_class = `show_top${item}`;
      // 后面通过class为show_item_class将background-color的浅蓝色去掉
      document.getElementsByClassName(show_item_class)[0].style.removeProperty('background-color');
      let topId = `top${this.entity.id}`;
      $('#' + topId).hide();
    });

    window.not_hide_top = () => {
      let show_item_class = `show_top${this.item}`;
      // 后面通过class为show_item_class将background-color的浅蓝色去掉
      document.getElementsByClassName(show_item_class)[0].style.backgroundColor = '#a3c5fd';
      let topId = `top${this.entity.id}`;
      $('#' + topId).show();
    };

    window.node_click = code => {
      // 点击节点
      for (let each_node of this.entity_data.locations) {
        if (each_node.id != code) {
          const node = document.getElementById(each_node.id);
          if (node && node.style.outline) {
            node.style.outline = 'none';
          }
        }
      }
      // document.getElementById(code).style.outline  = '#78b2f6 solid'
      document.getElementById(code).style.outline = '#699df4 solid 4px';
    };

    window.dataMapEvent = {
      linkToStandard(id) {
        if (!window.gVue.$modules.isActive('standard')) return;
        window.gVue.$router.push({
          name: 'DataStandardDetail',
          query: {
            id,
          },
        });
        tippy.hideAll();
      },
    };

    window.node_hover = code => {
      // 点击节点
      for (let each_node of this.entity_data.locations) {
        if (each_node.id == code) {
          each_node.is_mouseover = true;
        }
      }
    };

    // 点击-，将节点收起
    window.minus_click = code => {
      let entity_x = null;
      let entity_y = null;
      let direction = null;
      this.dataflow_loading = true;
      this.dataflow_width = document.getElementById('tree-struc').clientWidth - 22;
      let drag_left = 0;
      for (let entity of this.entity_data.locations) {
        if (entity.code == code) {
          if (entity.direction == 'left') {
            direction = 'left';
          } else {
            direction = 'right';
          }
        }
      }
      let tooltipId = `tips${code}`;
      $('#' + tooltipId).hide();
      this['delete'](this.tree_structure, code);
      this.dataflow_width = document.getElementById('tree-struc').clientWidth - 22;
      let toolkit = this.getTookit(rootWidth, drag_left, drag_top, direction, code);

      this.entity_data = toolkit.updateFlowGraph();

      this.updateGraph();

      // 显示加减号
      this.showPlusMinus(this.entity_data.locations);
      document.getElementById(code).style.outline = '#699df4 solid 4px';

      this.$forceUpdate();
      // tooltip显示节点指标信息
      this.show_tooltip(this.entity_attribute_value);
      this.dataflow_loading = false;
    };

    // 点击+，将节点展开
    window.plus_click = code => {
      // 先将点击的节点的tooltip隐藏掉
      let entity_x = null;
      let entity_y = null;
      let direction = null;
      this.dataflow_loading = true;

      let tooltipId = `tips${code}`;
      $('#' + tooltipId).hide();
      // 点击+号后递归展开所有节点,treeNodes树形结构list,code表示当前展开的节点
      this.expand(this.tree_structure, code);
      this.dataflow_width = document.getElementById('tree-struc').clientWidth - 22;
      let canvas_top = document.getElementById('dfv2').getBoundingClientRect().top;
      let canvas_left = document.getElementById('dfv2').getBoundingClientRect().left;
      let drag_left = 0;
      if (canvas_left) {
        // let canvas_left_list = canvas_left.split('px')
        // drag_left = canvas_left_list.length > 0 ? parseInt(canvas_left_list[0]) : 0
        drag_left = parseInt(canvas_left);
      }
      let drag_top = 0;
      if (canvas_top) {
        // let canvas_top_list = canvas_top.split('px')
        // drag_top = canvas_top_list.length > 0 ? parseInt(canvas_top_list[0]) : 0
        drag_top = parseInt(canvas_top);
      }
      for (let entity of this.entity_data.locations) {
        if (entity.code == code) {
          if (entity.direction == 'left') {
            direction = 'left';
          } else {
            direction = 'right';
          }
        }
      }

      let toolkit = this.getTookit(rootWidth, drag_left, drag_top, direction, code);
      let toolkit_dict = toolkit.getOffset() ? toolkit.getOffset() : {};
      let offset = toolkit_dict.offset;
      let offsetY = toolkit_dict.offsetY;
      let offset_direction = toolkit_dict.direction;
      this.entity_data = toolkit.updateFlowGraph();

      this.updateGraph();

      let current_node = this.findChild(this.tree_structure, code);
      let child_list = [];
      if (current_node) {
        for (let child of current_node.sub_list) {
          child_list.push(child.category_name);
        }
        this.showPlusMinus(this.entity_data.locations);
      }

      // tooltip显示节点指标信息
      this.dataflow_loading = false;
      this.show_tooltip(this.entity_attribute_value);
      // document.getElementById(code).style.outline  = '#78b2f6 solid'
      document.getElementById(code).style.outline = '#699df4 solid 4px';
      this.$forceUpdate();
    };

    // 树形节点中页面跳转
    window.route_click = (entity, nodeType = 'all') => {
      if (entity.count == 0) {
        postMethodWarning('当前分类共0条数据，无需跳转数据字典页面', 'warning');
        return;
      }

      // if (id != 'virtual_data_mart' && !this.chosen_tag_list.includes(id)) {
      //     this.chosen_tag_list.push(id)
      // }
      let tmp = $('#' + entity.id);
      let instance = tmp[0]._tippy;
      tippy.hideAll({ duration: 0 });
      instance.hide();
      document.getElementById('tree-struc').style.pointerEvents = 'none'; // 点击跳转，禁用所有节点的tooltips
      let parentNodes = this.getNodeParents(entity.id);
      if (parentNodes[0].category_name !== 'virtual_data_mart') {
        parentNodes.unshift({
          category_name: this.tree_structure[0].category_name,
          category_alias: this.getEnName(this.tree_structure[0], 'category_name', 'category_alias'),
        });
      }
      tippy.hideAll({ duration: 0 });
      this.$router.push({
        name: 'DataSearchResult',
        params: {
          nodeId: entity.id,
          nodeType,
          parentNodes,
          dataMapParams: {
            biz_id: this.biz_id,
            project_id: this.project_id,
            search_text: this.search_text,
            chosen_tag_list: this.chosen_tag_list,
            isShowStandard: this.just_show_standard,
            meType: entity.type === 'standard' ? 'standard' : 'tag',
          },
        },
      });
    };

    // 点击tooltip里面的内容，跳转到数据接入页面
    window.click_dataaccess = () => {
      let res = confirm('跳转数据集成页面功能正在开发中，敬请期待!');
      // if (res) {
      //     postMethodWarning('跳转成功', 'success')
      // } else {
      //     postMethodWarning('已取消跳转', 'info')
      // }
    };

    // 点击tooltip里面的内容，跳转到数据接入页面
    window.click_datadev = () => {
      let res = confirm('跳转数据开发页面功能正在开发中，敬请期待!');
      // if (res) {
      //     postMethodWarning('跳转成功', 'success')
      // } else {
      //     postMethodWarning('已取消跳转', 'info')
      // }
    };
  },
  beforeDestroy() {
    delete window.dataMapEvent;
    dataMapD3Graph.destroy();
    dataMapD3Graph = null;
  },
  methods: {
    getTookit(rootWidth, left, top, direction, code) {
      const costumeWidth = this.dataflow_width / 2 - rootWidth / 2;
      const width =  this.dataflow_width;
      const data = this.entity_data;
      return new FlowNodeToolKit(data, costumeWidth, width, left, top, direction, code);
    },
    getEnName(row, enField = 'tag_code', zhField = 'tag_alias') {
      if (this.$i18n.locale === 'en') {
        return row[enField];
      }
      return row[zhField];
    },
    reSetBkBizName() {
      this.bkBizName = null;
      this.choseBiz();
    },
    changeBkBizName(id, item) {
      this.bkBizName = item.bk_biz_name;
      this.choseBiz();
    },
    // 显示节点右侧的加减号
    showNodeIcon() {
      // this.dataflow_width/2 - 65是画布一半宽度 - 节点一半宽度
      this.dataflow_width = document.getElementById('tree-struc').clientWidth - 22;
      let drag_left = 0;
      let drag_top = 0;
      let toolkit = this.getTookit(rootWidth, drag_left, drag_top);
      this.entity_data = toolkit.updateFlowGraph();
      this.showPlusMinus(this.entity_data.locations);
    },
    reflshPage(routeName) {
      Bus.$emit('dataMarketReflshPage');
    },
    // 获取某个节点所有的祖先节点
    getNodeParents(nodeName) {
      if (!this.tree_structure.length) return;
      const that = this;
      let currentNode = null;
      let parentNode = [];
      function lookForNode(arr, nodeName) {
        arr.forEach(node => {
          if (node.category_name === nodeName) {
            currentNode = node;
            return null;
          } else {
            if (!node.sub_list.length) {
              return null;
            } else {
              lookForNode(node.sub_list, nodeName);
            }
          }
        });
        return currentNode;
      }
      function lookParentNode(arr, nodeName) {
        currentNode = null;
        let node = lookForNode(arr, nodeName);
        if (node) {
          if (node.category_name === 'virtual_data_mart') return parentNode;
          parentNode.unshift({
            category_name: node.category_name,
            category_alias: that.getEnName(node, 'category_name', 'category_alias'),
          });
          lookParentNode(arr, node.parent_code);
        } else {
          return parentNode;
        }
      }
      const nodeInfo = lookForNode(this.tree_structure, nodeName);
      parentNode.push({
        category_name: nodeInfo.category_name,
        category_alias: that.getEnName(nodeInfo, 'category_name', 'category_alias'),
      });
      lookParentNode(this.tree_structure, nodeInfo.parent_code);
      return parentNode;
    },
    linkToDictionary(nodeType) {
      if (nodeType === 'standard_table' && !this.isShowStandard) return;
      this.$router.push({
        name: 'DataSearchResult',
        params: {
          nodeType,
          dataMapParams: {
            biz_id: this.biz_id,
            project_id: this.project_id,
            search_text: this.search_text,
            chosen_tag_list: this.chosen_tag_list,
          },
        },
      });
    },
    full_screen() {
      this.$nextTick(() => {
        // 用于将树形结构div全屏
        this.is_full_screen = true;
        document.getElementById('tree-struc').requestFullscreen();
        // 记录画布之前的高
        this.canvas_height_before = document.getElementById('dfv2').style.height;
        this.canvas_width_before = document.getElementById('dfv2').style.width;
        this.canvas_top_before = 0;
        this.canvas_left_before = 0;
        let height_full_screeen = window.screen.height + 'px';
        document.getElementById('dfv2').style.height = height_full_screeen;
        document.getElementById('dfv2-padding').style.padding = '0';
        this.show_tooltip(this.entity_attribute_value);
        this.show_exit_full_screen_tooltip();
      });
    },

    un_full_screen() {
      this.$nextTick(() => {
        // 用于将树形结构取消全屏
        this.is_full_screen = false;
        document.getElementById('dfv2').style.height = '607px';
        document.getElementById('dfv2-padding').style.padding = '10px';
        document.exitFullscreen();
      });
    },

    clickStatisticData() {
      this.isExpend = !this.isExpend;
      this.showStatisticData = !this.showStatisticData;
      // 改变画布宽度
      // 调整每个节点的连线端点和节点的位置
      if (!this.showStatisticData) {
        document.getElementById('tree-struc').style.width = 'calc(100%)';
      } else {
        document.getElementById('tree-struc').style.width = 'calc(100% - 330px)';
      }
      // 用于显示节点右侧的加减号
      this.showNodeIcon();
      this.updateGraph();

      // tooltip显示节点指标信息
      this.show_tooltip(this.entity_attribute_value);
      this.dataflow_loading = false;
      this.$forceUpdate();
      // 改变根节点位置
      this.$refs.flowcontainer.height = this.dataflow_width + 'px';
      dataMapD3Graph.reSet();
    },

    clickInfo() {
      // 显示节点
      // if (!this.is_info_click) {
      this.timer && clearTimeout(this.timer);
      if (this.timer == null) {
        this.is_info_show = !this.is_info_show;
      }
      // this.is_info_show = !this.is_info_show
      if (!this.is_info_show) {
        // this.is_info_click = true
        this.timer = setTimeout(() => {
          document.getElementById('legend-desc').style.display = 'block';
          // this.is_info_click = false
          this.timer = null;
        }, 800);
      } else {
        document.getElementById('legend-desc').style.display = 'none';
      }
      // }
    },
    // 显示空分类
    change_show_count_zero(val) {
      this.show_count_zero = val;
      this.setData(true);
      this.getStatisticData();
    },

    change_show_non_standard(val) {
      // 显示非标准节点
      // this.show_non_standard = val
      this.just_show_standard = val;
      this.setData(true);
      this.getStatisticData();
    },

    // 点击图层
    layerClick(choice) {
      if (this.layerSetting.selected != choice) {
        this.layerSetting.selected = choice;
      } else {
        this.layerSetting.selected = '';
      }
    },

    // 获取项目列表
    getMineProjectList() {
      this.projectLoading = true;
      this.axios.get(`${window.BKBASE_Global.siteUrl}projects/all_projects/`).then(response => {
        if (response.result) {
          this.projectList = response.data;
          this.projectList.forEach(item => {
            item.displayName = `[${item.project_id}]${item.project_name}`;
          });
        } else {
          postMethodWarning(response.message, 'error');
        }
        this.projectLoading = false;
      });
    },

    // 点击图例
    // click_switch(name) {
    //     let card_width = null
    //     this.$nextTick(() => {
    //         for (let each_option of this.switch_dict) {
    //             if (name == each_option.name) {
    //                 each_option.is_clicked = !each_option.is_clicked
    //                 // 如果数据标准是选中的状态，则记录is_standard为true
    //                 if (each_option.name == '数据标准') {
    //                     this.is_standard = !this.is_standard
    //                 }
    //             }
    //         }
    //         this.setData()
    //         this.getStatisticData()
    //         this.$forceUpdate()
    //     })
    // },

    // 点击tag弹框的确认按钮
    click_confirm(row) {
      row.is_more_show = false;
    },

    // remove 列表中指定元素
    remove(arr, val) {
      let index = arr.indexOf(val);
      if (index > -1) {
        arr.splice(index, 1);
      }
    },
    // 点击选中tag的x按钮
    closeTag(parent_tag_alias, tag, item, dict) {
      // 1.把该标签的选中状态设为空
      this.$set(item, 'is_click', !item.is_click);
      // 2.从this.chosen_tag里面删除
      delete this.chosen_tag[item.tag_code];
      // 3.更新this.chosen_tag_list
      this.chosen_tag_list = Object.keys(this.chosen_tag);

      // 为了保持和上面的删除逻辑统一

      // current_row用于记录当前行
      let current_row = null;
      for (let each_row of this.tag_list) {
        if (each_row.tag_alias == parent_tag_alias) {
          current_row = each_row;
          break;
        }
      }
      if (item.is_top10) {
        // 取消选中的是top10，就把取消选中的标签移到top10的最后面
        // 首先把取消选中的标签从top10中删除掉
        // 1.因为不知道当前是哪一行，所以要先遍历tag_list,找到当前行

        if (current_row) {
          this.remove(current_row.sub_top_list, item);
          // 然后把标签添加到top10的最后面
          current_row.sub_top_list.push(item);
        }
      } else {
        // 取消选中的非top10
        // 1.先把取消选中的tag从top10中删掉
        if (current_row) {
          let index = current_row.sub_top_list.indexOf(item);
          this.remove(current_row.sub_top_list, item);

          if (index != -1) {
            // 2.然后把取消选中的tag放回到更多里面
            let flag_found = false;
            for (let each_sub_list of current_row.sub_list) {
              if (item.parent_id == each_sub_list.tag_id) {
                each_sub_list.sub_list.push(item);
                flag_found = true;
                break;
              }
            }
            if (!flag_found) {
              current_row.sub_list.push({
                tag_alias: item.be_category_alias,
                sub_list: [item],
              });
            }

            // 3.把更多里面属于top10的tag挪到top10位置
            let pop_top10_tag = null;
            for (let each_sub_list of current_row.sub_list) {
              for (let each_flat_tag of each_sub_list.sub_list) {
                if (each_flat_tag.is_top10) {
                  pop_top10_tag = each_flat_tag;
                  this.remove(each_sub_list.sub_list, each_flat_tag);
                  break;
                }
              }
              if (pop_top10_tag) {
                break;
              }
            }
            if (pop_top10_tag) {
              current_row.sub_top_list.push(pop_top10_tag);
            }
          }
        }
      }
      this.setData();
      this.$forceUpdate();
    },

    tipContat(tag_alias, description) {
      // tooltip拼接
      let tooltip_content = null;
      if (description) {
        tooltip_content = tag_alias + '(' + description + ')';
      } else {
        tooltip_content = tag_alias;
      }
      return tooltip_content;
    },

    // 选择tag和取消选择tag，不管是否为topn标签选中的时候只去改变is_click的状态，且改变选中的标签列表
    clickTag(item, parent_tag_alias, is_top10) {
      // is_top10如果为true则表示，点击选中和取消选中是top10，如果为false则表示，点击选中和取消选中是非top10
      this.$nextTick(() => {
        this.$set(item, 'is_click', !item.is_click);
        if (Object.keys(this.chosen_tag).indexOf(item.tag_code) == -1) {
          // 如果标签点击选中
          if (item.is_click) {
            // 记录选中标签的字典
            this.chosen_tag[item.tag_code] = {
              tag_alias: item.tag_alias,
              parent_tag_alias: parent_tag_alias,
              item: item,
              is_top10: is_top10,
            };
          }
        } else {
          // 标签点击取消选中
          if (!item.is_click) {
            delete this.chosen_tag[item.tag_code];
          }
        }
        // 记录选中标签的列表
        this.chosen_tag_list = Object.keys(this.chosen_tag);
        this.setData();
        this.getStatisticData();
        this.$forceUpdate();
      });
    },

    // 点击tag行内的更多
    clickRowMore(row) {
      this.$nextTick(() => {
        if (!row.is_more_show) {
          this.isShowMore = true;
          for (let each_row of this.tag_list) {
            // each_row是五大分类
            // row是当前行的分类
            if (each_row.tag_code != row.tag_code && each_row.is_more_show) {
              each_row.is_more_show = false;
              // 点击一行的更多，其他行都要收起（相当于点击行内“收起”，把更多里面的结果放回topn)
              this.putTopnBack(each_row);
            }
          }
          row.is_more_show = true;
          // 点击当前行内“更多”，把topn放在更多里面
          this.putTopnCategory(row);
        } else {
          row.is_more_show = false;
          // 点击行内“收起”，把更多里面的结果放回topn
          this.putTopnBack(row);
        }
      });
    },
    // 点击行内“更多”以后，把topn塞回到更多里面
    putTopnCategory(row) {
      var _this = this;
      // 五大分类所在的当前行不显示
      row.is_not_show = true;
      // 判断当前点击的行topn真正有几个
      let len = row.sub_top_list.length - 1;
      let multi_appear_list = [];
      for (let i = len; i >= 0; i--) {
        let each_topn = row.sub_top_list[i];
        // 1.从sub_top_list删除each_topn
        this.remove(row.sub_top_list, each_topn);
        // 2.找到分类，加进去
        let flag_found = false;
        for (let each_sub_list of row.sub_list) {
          if (flag_found) {
            break;
          }
          if ((each_topn.be_category_id == each_sub_list.tag_id
                    || each_topn.parent_id == each_sub_list.tag_id)
                    && each_topn.be_category_id != row.tag_id) {
            // each_topn添加到数组的头部
            each_sub_list.sub_list.unshift(each_topn);
            // 只要当前点击的标签在更多中找到分类，则不需要进行下面的操作（3.4.）
            flag_found = true;
            break;
          }
          // 第四层标签和第三层平行放一起，例如硬件配置
          for (let each_plat of each_sub_list.sub_list) {
            if ((each_topn.be_category_id == each_plat.tag_id
                        || each_topn.parent_id == each_plat.tag_id)
                        && each_topn.be_category_id != row.tag_id) {
              // each_topn添加到数组的头部
              each_sub_list.sub_list.unshift(each_topn);
              // 只要当前点击的标签在更多中找到分类，则不需要进行下面的操作（3.4.）
              flag_found = true;
              break;
            }
          }
        }
        // 3.没有找到分类
        if (!flag_found && each_topn.be_category_id != row.tag_id) {
          multi_appear_list.push(each_topn);
        }
        // 4.如果top10就是更多里面的父分类，将父分类加上is_click=true的状态
        if (!flag_found && each_topn.be_category_id == row.tag_id) {
          let not_found = true;
          // 点击的top10就是更多里面的父分类
          for (let each_sub_list of row.sub_list) {
            if (each_topn.tag_id == each_sub_list.tag_id) {
              // 那么现在更多里面的父分类就是被选中的状态，且该分类就是top10
              not_found = false;
              each_sub_list.is_top10 = true;
              each_sub_list.is_click = each_topn.is_click;
              break;
            }
          }
          if (not_found) {
            row.sub_list.push({
              tag_alias: each_topn.tag_alias,
              tag_code: each_topn.tag_code,
              sub_list: [],
              tag_id: each_topn.tag_id,
              be_category_id: row.tag_id,
              tooltip: this.tipContat(each_topn.tag_alias, each_topn.description),
              is_click: each_topn.is_click,
              is_top10: true,
            });
          }
        }
      }
      // 硬件配置
      for (let each_tag of multi_appear_list) {
        let flag_found = false;
        for (let each_sub_list of row.sub_list) {
          if (flag_found) {
            break;
          }
          if ((each_tag.be_category_id == each_sub_list.tag_id
                    || each_tag.parent_id == each_sub_list.tag_id)
                    && each_tag.be_category_id != row.tag_id) {
            // each_tag添加到数组的头部
            each_sub_list.sub_list.unshift(each_tag);
            // 只要当前点击的标签在更多中找到分类，则不需要进行下面的操作（3.4.）
            flag_found = true;
            break;
          }
          for (let each_plat of each_sub_list.sub_list) {
            if ((each_tag.be_category_id == each_plat.tag_id
                        || each_tag.parent_id == each_plat.tag_id)
                        && each_tag.be_category_id != row.tag_id) {
              // each_tag添加到数组的头部
              each_sub_list.sub_list.unshift(each_tag);
              // 只要当前点击的标签在更多中找到分类，则不需要进行下面的操作（3.4.）
              flag_found = true;
              break;
            }
          }
        }
      }
      // 如果发现更多中的父分类的sub_list为空，则将这些父分类平铺在一行
      // 1.先将父分类平铺进去
      // 2.再将这些父分类删除
      let len_flat = row.sub_list.length - 1;
      for (let i = len_flat; i >= 0; i--) {
        let each_sub_list = row.sub_list[i];
        // 1.先把当前行从row的sub_list中删除，然后再把当前行平铺进sub_list中
        if (each_sub_list.sub_list.length == 0) {
          // 现在要把each_sub_list放进当前更多的子分类中
          // 如果找到对应分类
          let flat_found = false;
          for (let each_sub_list_tmp of row.sub_list) {
            if (each_sub_list.be_category_id == each_sub_list_tmp.tag_id
                        || each_sub_list.parent_id == each_sub_list_tmp.tag_id) {
              each_sub_list_tmp.sub_list.push(each_sub_list);
              // 只要当前点击的标签在更多中找到分类，则不需要进行下面的操作
              flat_found = true;
              break;
            }
          }
          // 如果没有找到对应分类
          if (!flat_found) {
            row.sub_list.push({
              tag_alias: '其它',
              tag_code: row.tag_id + '_other',
              sub_list: [each_sub_list],
              tag_id: each_sub_list.be_category_id
                ? each_sub_list.be_category_id : each_sub_list.parent_id,
              be_category_id: row.tag_id,
            });
          }

          // 首先判断sub_list里面有无alias是flat的父分类
          this.remove(row.sub_list, each_sub_list);
        }
      }
      // 对row.sub_list进行排序，保证 其他 出现的更多里面的最下面
      row.sub_list.sort(this.cmp_sub_list);
      // 点击更多以后只有第一行是有title的
      let sub_list_len = row.sub_list.length;
      if (sub_list_len > 0) {
        row.sub_list[0].title = row.tag_alias;
        for (let i = 1; i < sub_list_len; i++) {
          row.sub_list[i].title = '';
        }
      }
    },

    // 对row.sub_list进行排序，保证 其他 出现的更多里面的最下面
    cmp_sub_list(left, right) {
      left = left['tag_alias'];
      right = right['tag_alias'];
      if (left == '其它') {
        return 1;
      }
      if (right == '其它') {
        return -1;
      }
      return 0;
    },

    // 点击行内“收起”以后，把topn从更多里面拿出来
    // 目前问题，点击行内的游戏用户-用户状态之后，topn中就会多出现一个
    putTopnBack(row) {
      row.is_not_show = false;
      var _this = this;
      // 1.从更多里面取选中的标签：a.选中的标签在父分类中 b.选中的标签正常平铺
      for (let each_sub_list of row.sub_list) {
        // a.选中的标签在更多里的父分类中
        if (row.sub_top_list.length < this.topn) {
          if (Object.keys(each_sub_list).indexOf('is_click') && each_sub_list.is_click) {
            row.sub_top_list.push(each_sub_list);
          }
        }
        if (row.sub_top_list.length >= this.topn) {
          break;
        }
        // b.选中的标签不在更多里的父分类中
        let len = each_sub_list.sub_list.length - 1;
        for (let i = len; i >= 0; i--) {
          let each_sub_tag = each_sub_list.sub_list[i];
          if (Object.keys(each_sub_tag).indexOf('is_click') && each_sub_tag.is_click) {
            row.sub_top_list.push(each_sub_tag);
            this.remove(each_sub_list.sub_list, each_sub_tag);
          }
          if (row.sub_top_list.length >= this.topn) {
            break;
          }
        }
      }
      // 2.从更多里面取没有选中但为topn的标签:a.没有选中的topn标签在父分类中 b.没有选中的标签在平铺的标签中
      if (row.sub_top_list.length < this.topn) {
        for (let each_sub_list of row.sub_list) {
          // a.没有选中的topn标签在父分类中
          if (row.sub_top_list.length < this.topn) {
            if (Object.keys(each_sub_list).indexOf('is_top10')
                        && each_sub_list.is_top10 && !each_sub_list.is_click) {
              row.sub_top_list.push(each_sub_list);
            }
          }
          if (row.sub_top_list.length >= this.topn) {
            break;
          }
          // b.没有选中的标签在平铺的标签中
          let len = each_sub_list.sub_list.length - 1;
          for (let i = len; i >= 0; i--) {
            let each_sub_tag = each_sub_list.sub_list[i];
            if (each_sub_tag.is_top10 && !each_sub_tag.is_click) {
              row.sub_top_list.push(each_sub_tag);
              this.remove(each_sub_list.sub_list, each_sub_tag);
            }
            if (row.sub_top_list.length >= this.topn) {
              break;
            }
          }
        }
      }
    },

    choseBiz() {
      this.$nextTick(() => {
        this.setData();
        this.getStatisticData();
        this.$forceUpdate();
      });
    },

    choseProject() {
      this.$nextTick(() => {
        this.setData();
        this.getStatisticData();
        this.$forceUpdate();
      });
    },

    // 点击+号后递归展开所有节点,treeNodes树形结构list,code表示当前展开的节点
    expand(treeNodes, code) {
      // 是否可以从树中找到当前节点
      let node_found = this.findChild(treeNodes, code);
      if (node_found) {
        // 找到当前节点，将当前节点的直接子级加进去
        // node_found为当前点击的节点
        this.addParseTreeJson(node_found);
      } else {
        postMethodWarning('当前节点没子节点', 'warning');
      }
    },

    // 点击-号后递归收起所有节点,treeNodes树形结构list,code表示当前展开的节点
    delete(treeNodes, code) {
      // 是否可以从树中找到当前节点
      let node_found = this.findChild(treeNodes, code);
      if (node_found) {
        this.deleteParseTreeJson(node_found);
      } else {
        postMethodWarning('当前节点没子节点可收起!', 'warning');
      }
    },

    findChild(treeNodes, code) {
      // code是需要寻找的节点
      if (!treeNodes || treeNodes.length == 0) return null;
      for (var i = 0, len = treeNodes.length; i < len; i++) {
        if (treeNodes[i].category_name == code) {
          return treeNodes[i];
        } else {
          // var childs = treeNodes[i].children;
          var childs = treeNodes[i].sub_list;
          let node = this.findChild(childs, code);
          if (node) {
            return node;
          }
        }
      }
      return null;
    },

    addParseTreeJsonCountZero(node) {},

    addParseTreeJson(node) {
      // 添加node节点的直接子级
      if (!node || !node.sub_list || node.sub_list.length == 0) return;
      var childs = node.sub_list;
      // 先看孩子有多少个，
      // 如果孩子<=4,执行原来逻辑
      // if (childs.length<=4) {
      for (var i = 0, len = childs.length; i < len; i++) {
        // 添加树形结构的节点和边
        this.addNode(node, childs[i]);
      }
      // } else {
      //     // 如果孩子>4,带...的逻辑
      //     // 1.构建...的数据结构
      //     let node_dot = {
      //         id: 'dot'+ '_' + node.code,
      //         type: 'other',
      //         x: 0,
      //         y: 0,
      //         name: 'dot'+ '_' + node.code,
      //         code: 'dot'+ '_' + node.code,
      //         category_name: 'dot'+ '_' + node.code,
      //         count: 0,
      //         is_selected: false,
      //         is_leaf: false,
      //         is_plus: false,
      //         sub_list: [],
      //     }
      //     // 2.把隐藏的节点塞进...的sublist里面
      //     for (var i = 2, len = childs.length-1; i < len; i++) {
      //         node_dot.sub_list.push(childs[i])
      //     }
      //     // 3.构建一个list,把1，2节点，...,和最后一个节点放进去
      //     let childs_list = [childs[0],childs[1],node_dot,childs[childs.length-1]]
      //     console.log('node_dot', node_dot)
      //     // 4.调用addNode函数
      //     for (var i = 0, len = childs_list.length; i < len; i++) {
      //         // 添加树形结构的节点和边
      //         this.addNode(node, childs_list[i])
      //     }
      // }
    },

    deleteParseTreeJson(node) {
      if (!node || !node.sub_list || node.sub_list.length == 0) return;
      var childs = node.sub_list;
      for (var i = 0, len = childs.length; i < len; i++) {
        // 删除node下面的节点childs[i]
        this.deleteNode(node, childs[i]);
        this.deleteParseTreeJson(childs[i]);
      }
    },

    deleteParseTreeJsonCountZero(node) {
      if (!node || !node.sub_list || node.sub_list.length == 0) return;
      var childs = node.sub_list;
      for (var i = 0, len = childs.length; i < len; i++) {
        // 删除node下面的节点childs[i]
        if (childs[i].dataset_count == 0) {
          this.deleteNode(node, childs[i]);
        }
        this.deleteParseTreeJsonCountZero(childs[i]);
      }
    },

    addNode(node1, node2) {
      // 添加树形结构的节点和边信息
      // node1是node2的父节点
      // 莫非当时是想写...的逻辑？
      let line_right = {
        source: {
          arrow: 'Right',
          id: node1.category_name,
        },
        target: {
          arrow: 'Left',
          id: node2.category_name,
        },
      };
      let line_left = {
        source: {
          arrow: 'Left',
          id: node1.category_name,
        },
        target: {
          arrow: 'Right',
          id: node2.category_name,
        },
      };
      if (node2.category_name.includes('dot_')) {
        this.entity_data.locations.push(node2);
        this.entity_data.lines.push({
          source: {
            arrow: 'Right',
            id: node1.category_name,
          },
          target: {
            arrow: 'Left',
            id: node2.category_name,
          },
        });
      } else {
        for (let each_node of this.entity_data_record.locations) {
          if (each_node.code === node2.category_name) {
            // 无法确定添加节点的状态为什么会改变，所以将node2赋值给node1？
            // 这段代码一点儿都没看懂自己当时为什么要写for循环
            each_node.is_selected = node2.is_selected;
            each_node.type = node2.type;
            each_node.standard_id = node2.standard_id;
            this.entity_data.locations.push(each_node);
            if (node2.direction == 'right') {
              this.entity_data.lines.push(line_right);
            } else {
              this.entity_data.lines.push(line_left);
            }
            break;
          }
        }
      }
    },

    deleteNode(node1, node2) {
      // node1是node2的父节点
      let flag = false; // false表示未找到
      for (let num in this.entity_data.locations) {
        if (this.entity_data.locations[num].code == node2.category_name) {
          this.entity_data.locations.splice(num, 1);
          flag = true;
          break;
        }
      }
      if (flag) {
        for (let num in this.entity_data.lines) {
          if (this.entity_data.lines[num].source.id == node1.category_name
                    && this.entity_data.lines[num].target.id == node2.category_name) {
            this.entity_data.lines.splice(num, 1);
            break;
          }
        }
      }
    },

    getStatisticData() {
      // 获得数据地图右侧统计数据
      this.sta_loading = true;
      let params = {
        bk_biz_id: this.biz_id ? this.biz_id : null,
        project_id: this.project_id ? this.project_id : null,
        keyword: this.search_text,
        tag_ids: this.chosen_tag_list,
      };
      params.cal_type = [];
      if (this.is_standard && !params.cal_type.includes('standard')) {
        params.cal_type.push('standard');
      }
      if (this.just_show_standard && !params.cal_type.includes('only_standard')) {
        params.cal_type.push('only_standard');
      }
      const postUrl = `${window.BKBASE_Global.siteUrl}datamart/datamap/statistic/statistic_data/`;
      this.axios.post(postUrl, params).then(response => {
        if (response.result) {
          this.sta_data = response.data;
          // 数据源
          this.sta_data.recent_data_source_format.type = this.line_data[0].type;
          this.sta_data.recent_data_source_format.line = this.line_data[0].line;
          // this.sta_data.recent_data_source_format.mode = this.bar_data[0].mode
          // this.sta_data.recent_data_source_format.marker = this.bar_data[0].marker
          this.sta_data.recent_data_source_format.hoverinfo = 'y';
          this.sta_data.recent_data_source_format.hoverlabel = {
            align: 'left',
          };
          let flag_datasource = false;
          for (let each_y of this.sta_data.recent_data_source_format.y) {
            if (each_y > 0) {
              flag_datasource = true;
              delete this.chartLayout_datasource.yaxis.range;
              this.chartLayout_datasource.yaxis.autorange = true;
              break;
            }
          }
          if (!flag_datasource) {
            this.chartLayout_datasource.yaxis.range = [0, 5];
            this.chartLayout_datasource.yaxis.autorange = false;
          }
          this.sta_data.recent_data_source_format = [this.sta_data.recent_data_source_format];

          // 结果表
          this.sta_data.recent_dataset_format.type = this.line_data[0].type;
          this.sta_data.recent_dataset_format.line = this.line_data[0].line;
          // this.sta_data.recent_dataset_format.mode = this.bar_data[0].mode
          // this.sta_data.recent_dataset_format.marker = this.bar_data[0].marker
          this.sta_data.recent_dataset_format.name = this.$t('结果表_总');
          this.sta_data.recent_dataset_format.hoverinfo = 'y';
          this.sta_data.recent_dataset_format.hoverlabel = {
            align: 'left',
          };
          let flag_dataset = false;
          for (let each_y of this.sta_data.recent_dataset_format.y) {
            if (each_y > 0) {
              flag_dataset = true;
              delete this.chartLayout_dataset.yaxis.range;
              this.chartLayout_dataset.yaxis.autorange = true;
              break;
            }
          }
          if (!flag_dataset) {
            this.chartLayout_dataset.yaxis.range = [0, 5];
            this.chartLayout_dataset.yaxis.autorange = false;
          }
          if (!this.is_standard) {
            this.sta_data.recent_dataset_format = [this.sta_data.recent_dataset_format];
          } else {
            // 标准结果表
            this.sta_data.recent_standard_dataset_format.type = this.line_data[1].type;
            this.sta_data.recent_standard_dataset_format.line = this.line_data[1].line;
            this.sta_data.recent_standard_dataset_format.name = this.$t('结果表_标准化');
            this.sta_data.recent_standard_dataset_format.hoverinfo = 'y';
            this.sta_data.recent_standard_dataset_format.hoverlabel = {
              align: 'left',
            };
            let flag_standard_datasource = false;
            for (let each_y of this.sta_data.recent_standard_dataset_format.y) {
              if (each_y > 0) {
                flag_standard_datasource = true;
                delete this.chartLayout_standard_dataset.yaxis.range;
                this.chartLayout_standard_dataset.yaxis.autorange = true;
                break;
              }
            }
            if (!flag_standard_datasource) {
              this.chartLayout_standard_dataset.yaxis.range = [0, 5];
              this.chartLayout_standard_dataset.yaxis.autorange = false;
            }
            this.sta_data.recent_dataset_format = [
              this.sta_data.recent_dataset_format,
              this.sta_data.recent_standard_dataset_format
            ];
            this.sta_data.recent_standard_dataset_format = [this.sta_data.recent_standard_dataset_format];
          }
        } else {
          postMethodWarning(response.message, 'error');
        }
        this.sta_loading = false;
      });
    },

    setData(isReset = false) {
      this.dataflow_loading = true;
      let params = {
        bk_biz_id: this.biz_id ? this.biz_id : null,
        project_id: this.project_id ? this.project_id : null,
        keyword: this.search_text,
        tag_ids: this.chosen_tag_list,
        is_show_count_zero: this.show_count_zero,
        bk_biz_name: this.bkBizName,
      };
      params.cal_type = [];
      if (this.is_standard && !params.cal_type.includes('standard')) {
        params.cal_type.push('standard');
      }
      if (this.just_show_standard && !params.cal_type.includes('only_standard')) {
        params.cal_type.push('only_standard');
      }

      const selfaxios = this.axios;
      const _this = this;
      this.axios.post(`${window.BKBASE_Global.siteUrl}datamart/datamap/tree/`, params).then(response => {
        if (response.result) {
          this.tree_structure = response.data.tree_structure;
          this.entity_data.lines = [];
          this.entity_data.locations = [];
          this.entity_data_record.lines = [];
          this.entity_data_record.locations = [];
          if (response.data.relations.length === 0) {
            // postMethodWarning('树形结构暂无节点', 'info')
          }
          for (let each_entity of response.data.entity_attribute_value) {
            this.status_dict[each_entity.id] = {
              column: each_entity.column,
              is_selected: each_entity.is_selected,
              parent_zero: each_entity.parent_zero,
              direction: each_entity.direction,
            };
          }
          for (let each_entity of response.data.relations) {
            // 前两层不管是什么节点都加到树形结构中
            // 第三层只加父节点count不为0的
            // 其他层只加搜索到的，或者在搜索路径上的
            // 在数据集市右侧的节点
            let line_right = {
              source: {
                id: each_entity.fromEntityId,
                arrow: 'Right',
              },
              target: {
                id: each_entity.toEntityId,
                arrow: 'Left',
              },
            };
            let line_left = {
              source: {
                id: each_entity.fromEntityId,
                arrow: 'Left',
              },
              target: {
                id: each_entity.toEntityId,
                arrow: 'Right',
              },
            };
            if (this.status_dict[each_entity.toEntityId].column < 2) {
              // 如果入节点的direction=left，则push line_left
              if (this.status_dict[each_entity.toEntityId].direction == 'right') {
                let index = this.entity_data.lines.push(line_right);
                this.entity_data_record.lines.push(line_right);
              } else {
                let index = this.entity_data.lines.push(line_left);
                this.entity_data_record.lines.push(line_left);
              }
            } else if (this.status_dict[each_entity.toEntityId].column == 2) {
              if (this.status_dict[each_entity.toEntityId].parent_zero != 1 || [1, 2, 3]
                .indexOf(this.status_dict[each_entity.toEntityId].is_selected) != -1) {
                if (this.status_dict[each_entity.toEntityId].direction == 'right') {
                  this.entity_data.lines.push(line_right);
                } else {
                  this.entity_data.lines.push(line_left);
                }
              }
              if (this.status_dict[each_entity.toEntityId].direction == 'right') {
                this.entity_data_record.lines.push(line_right);
              } else {
                this.entity_data_record.lines.push(line_left);
              }
            } else {
              if (this.status_dict[each_entity.toEntityId].is_selected == 1
                            || this.status_dict[each_entity.toEntityId].is_selected == 2
                            || this.status_dict[each_entity.toEntityId].is_selected == 3) {
                if (this.status_dict[each_entity.toEntityId].direction == 'right') {
                  this.entity_data.lines.push(line_right);
                } else {
                  this.entity_data.lines.push(line_left);
                }
              }
              if (this.status_dict[each_entity.toEntityId].direction == 'right') {
                this.entity_data_record.lines.push(line_right);
              } else {
                this.entity_data_record.lines.push(line_left);
              }
            }
          }
          this.entity_attribute_value = response.data.entity_attribute_value;
          for (let entity of response.data.entity_attribute_value) {
            entity.x = entity.column * 300 + this.current_entity_x;
            entity.y = entity.row * 130 + this.current_entity_y;
            // 默认只展示前3级
            let node = this.findChild(this.tree_structure, entity.code);
            // 判断是不是叶子节点
            let is_leaf = false;
            if (!node || !node.sub_list || node.sub_list.length == 0) {
              is_leaf = true;
            }
            entity.is_leaf = is_leaf;
            // 前两层不管是什么节点都加到树形结构中
            // 第三层加父节点count不为0的,搜索到的，或者在搜索路径上的
            // 其他层只加搜索到的，或者在搜索路径上的
            if (entity.column < 2) {
              this.entity_data.locations.push({
                id: entity.id,
                direction: entity.direction,
                type: entity.type,
                x: entity.x,
                y: entity.y,
                name: entity.name,
                code: entity.code,
                tptGroup: entity.tptGroup,
                count: entity.count,
                has_standard: entity.has_standard,
                is_selected: entity.is_selected,
                parent_code: entity.parent_code,
                is_leaf: is_leaf,
                is_plus: false,
                child_show: entity.child_show ? entity.child_show : -1,
                is_mouseover: false,
              });
              this.entity_data_record.locations.push({
                id: entity.id,
                direction: entity.direction,
                type: entity.type,
                x: entity.x,
                y: entity.y,
                name: entity.name,
                code: entity.code,
                tptGroup: entity.tptGroup,
                count: entity.count,
                has_standard: entity.has_standard,
                is_selected: entity.is_selected,
                parent_code: entity.parent_code,
                is_leaf: is_leaf,
                is_plus: false,
                child_show: entity.child_show ? entity.child_show : -1,
                is_mouseover: false,
              });
            } else if (entity.column == 2) {
              if (entity.parent_zero != 1 || [1, 2, 3].indexOf(entity.is_selected) != -1) {
                this.entity_data.locations.push({
                  id: entity.id,
                  direction: entity.direction,
                  type: entity.type,
                  x: entity.x,
                  y: entity.y,
                  name: entity.name,
                  code: entity.code,
                  tptGroup: entity.tptGroup,
                  count: entity.count,
                  has_standard: entity.has_standard,
                  is_selected: entity.is_selected,
                  parent_code: entity.parent_code,
                  is_leaf: is_leaf,
                  is_plus: false,
                  child_show: entity.child_show ? entity.child_show : -1,
                  is_mouseover: false,
                });
              }
              this.entity_data_record.locations.push({
                id: entity.id,
                direction: entity.direction,
                type: entity.type,
                x: entity.x,
                y: entity.y,
                name: entity.name,
                code: entity.code,
                tptGroup: entity.tptGroup,
                count: entity.count,
                has_standard: entity.has_standard,
                is_selected: entity.is_selected,
                parent_code: entity.parent_code,
                is_leaf: is_leaf,
                is_plus: false,
                child_show: entity.child_show ? entity.child_show : -1,
                is_mouseover: false,
              });
            } else {
              if (entity.is_selected == 1 || entity.is_selected == 2 || entity.is_selected == 3) {
                this.entity_data.locations.push({
                  id: entity.id,
                  direction: entity.direction,
                  type: entity.type,
                  x: entity.x,
                  y: entity.y,
                  name: entity.name,
                  code: entity.code,
                  tptGroup: entity.tptGroup,
                  count: entity.count,
                  has_standard: entity.has_standard,
                  is_selected: entity.is_selected,
                  parent_code: entity.parent_code,
                  is_leaf: is_leaf,
                  is_plus: false,
                  child_show: entity.child_show ? entity.child_show : -1,
                  is_mouseover: false,
                });
              }
              this.entity_data_record.locations.push({
                id: entity.id,
                direction: entity.direction,
                type: entity.type,
                x: entity.x,
                y: entity.y,
                name: entity.name,
                code: entity.code,
                tptGroup: entity.tptGroup,
                count: entity.count,
                has_standard: entity.has_standard,
                is_selected: entity.is_selected,
                parent_code: entity.parent_code,
                is_leaf: is_leaf,
                is_plus: false,
                child_show: entity.child_show ? entity.child_show : -1,
                is_mouseover: false,
              });
            }
          }
          this.showNodeIcon();
          this.updateGraph(isReset);
          if (response.data.entity_attribute_value.length !== 0) {
            this.show_tooltip(response.data.entity_attribute_value);
          }
          this.dataflow_loading = false;

          this.timer = setTimeout(() => {
            if (document.getElementById('legend-desc')) {
              document.getElementById('legend-desc').style.display = 'block';
              // this.is_info_click = false
              this.is_info_show = false;
              this.timer = null;
            }
          }, 800);

          // this.canvasMoveStatus.isMoving = false
        } else {
          postMethodWarning(response.message, 'error');
        }
      });
    },

    show_tooltip(entity_attribute_value) {
      // 用于显示tooltip节点指标信息，未展示top10
      // for (let entity of entity_attribute_value) {
      for (let entity of this.entity_data.locations) {
        let tooltipId = `tips${entity.id}`;
        let div = document.getElementById(tooltipId);

        let strTip = '<div id=\'' + tooltipId
                + '\' style=\'text-align: center;height: 100%;width:100%\'><img src=' + loadingImg + '></div>';
        let id_tmp = '#' + entity.id;
        let timeoutKey = 0;
        const _this = this;
        const selfaxios = this.axios;

        let is_leaf_offset = '0,15';
        if (entity.is_leaf || entity.direction != 'right' || entity.id == 'virtual_data_mart') {
          is_leaf_offset = '0,0';
        }
        let placement = 'right';

        tippy.hideAll({ duration: 0 });
        // 设置instance数组来存放tippy instance
        this.dataflow_width = document.getElementById('tree-struc').clientWidth - 22;
        let tooltip = tippy($(id_tmp)[0], {
          content: strTip,
          interactive: true,
          hideOnClick: false,
          arrow: true,
          placement: placement,
          offset: is_leaf_offset,
          delay: [0, 0],
          duration: [0, 0],
          trigger: 'manual',
          // appendTo: () => document.getElementById('dfv2'),
          async onShow(instance) {
            // 如果没有发生拖拽，此时节点的位置应该是entity.x
            // 如果画布发生拖拽，此时节点的位置应该是entity.x + canvas.left
            // 整个画布可以看到的宽度为 this.dataflow_width
            // 因此节点最右侧距离画布最右侧的宽度是
            // this.dataflow_width - position_x - document.getElementById('canvas').clientWidth
            // 将canvas的left转化为数值
            let canvas_left_with_px = 0;
            let left_len = canvas_left_with_px.length;
            let canvas_left_without_px = 0;
            if (left_len > 2) {
              canvas_left_without_px = parseInt(canvas_left_with_px.substring(0, left_len - 2));
            }
            let position_x = entity.x + canvas_left_without_px;
            let cur_width = parseInt(_this.dataflow_width) -
                        parseInt(position_x) -
                        parseInt(document.getElementById(`${entity.id}`).clientWidth);
            if (cur_width < 214) {
              placement = 'left';
              // is_leaf_offset = "0,0"
              if (entity.direction == 'right' || entity.id == 'virtual_data_mart') {
                is_leaf_offset = '0,0';
              } else if (entity.direction == 'left' && entity.id != 'virtual_data_mart') {
                is_leaf_offset = '0,15';
              }
            } else {
              placement = 'right';
              is_leaf_offset = '0,15';
              if (entity.is_leaf || entity.direction != 'right' || entity.id == 'virtual_data_mart') {
                is_leaf_offset = '0,0';
              }
            }
            instance.set({
              placement: placement,
              offset: is_leaf_offset,
            });

            _this.instance_list.forEach(instance => {
              instance.hide();
              entity.is_mouseover = false;
            });
            _this.instance_list = [];
            _this.instance_list.push(instance);
            var _self = this;
            _this.instance = instance;
            _this.entity = entity;
            let params = {
              bk_biz_id: _this.biz_id ? _this.biz_id : null,
              project_id: _this.project_id ? _this.project_id : null,
              tag_ids: _this.chosen_tag_list,
              keyword: _this.search_text,
              tag_code: entity.id,
            };
            if (entity.type == 'standard') {
              params.me_type = 'standard';
              params.parent_code = entity.parent_code;
            }
            if (entity.has_standard == 1) {
              params.has_standard = 1;
            }

            // 如果选择了数据标准的图例，多传一个参数，没有选的时候不用传
            params.cal_type = [];
            if (_this.is_standard && !params.cal_type.includes('standard')) {
              params.cal_type.push('standard');
            }
            if (_this.just_show_standard && !params.cal_type.includes('only_standard')) {
              params.cal_type.push('only_standard');
            }
            var popover_info = {};
            const postUrl = `${window.BKBASE_Global.siteUrl}v3/datamanage/datamap/retrieve/get_basic_info/`;
            // 调接口拿到tooltip里面放的指标数据
            await selfaxios.post(postUrl, params)
              .then(response => {
                if (response.result) {
                  popover_info = response.data;
                  _this.project_list = response.data.project_list;
                  _this.bk_biz_list = response.data.bk_biz_list;
                  _this.popover_info = popover_info;
                }
              });
            let tooltipId = `tips${entity.id}`;
            let strTip = '<div id=\'' + tooltipId
                        + '\' style=\'font-size:12px;width:100%;text-align:left;margin-bottom:0px;\'>';
            strTip += '<div style=\'border-bottom: 1px solid #ebebeb;margin-bottom:1px\'>'
                        +'<div class=\'popover-header\' '
                        +'style=\'width:170px;display:flex;justify-content: space-between;align-items: center;\'>';
            strTip += `<div style='display:flex'><span onclick='route_click(${JSON.stringify(entity)})'
                        style='${ entity.count === 0 ? 'cursor:inherit;pointer-events:none;' : 'cursor:pointer;'}
                        overflow:hidden;text-overflow:ellipsis;white-space:nowrap;
                        display: inline-block;max-width:165px;color:#699df4;'
                        title='${entity.name}'>`;
            strTip += entity.name;
            strTip += '</span>';
            strTip += '</div>';
            strTip += '</div></div>';
            strTip += '<div style=\'width:100%;text-align:left;'
                        +'margin-bottom:0px;display:flex;font-size:12px\'>';
            // sta-metric用于在右侧top10显示的时候给左侧加上右border
            strTip += '<div class=\'sta-metric\' style=\'height:115px\'>';
            let topId = `top${entity.id}`;
            for (let item of Object.keys(_this.popover_dict)) {
              if (item == 'data_source_count') {
                let num = entity.type == 'standard' ? 0 : popover_info[item];
                // 如果是数据源
                strTip += '<div style=\'display:flex;width:170px;margin-bottom:5px;margin-top:5px;\'>'
                                +'<span style=\'width:47px;display:flex;justify-content:flex-end;'
                                +'margin-bottom:0px;margin-right:0px\'>';
                strTip += _this.popover_dict[item];
                strTip += '</span>';
                strTip += '<div style=\'width:90px\'><span style=\'text-align:left;'
                                +'margin-bottom:0px\'>：</span>'
                                +'<span style=\'margin-bottom:0px;font-weight:bolder\'>';
                strTip += num;
                strTip += `</span><span>${_this.$t('个')}</span>`;
                strTip += `</span></div><div style='width:33px;${num === 0
                  ? 'display:none;pointer-events: none;' : 'display:block;'}'
                                onclick='route_click(${JSON.stringify(entity)},  "raw_data")'>
                                <i class='bk-icon icon-list' title=${_this.$t('查看详情')}
                                style='cursor:pointer;margin:1px 1px 0 0;color:#699df4;
                                display:flex;justify-content:flex-end;'></i></div>`;
                strTip += '</div>';
              } else if (item == 'dataset_count') {
                // 如果是结果表
                strTip += '<div style=\'display:flex;width:170px;margin-bottom:5px;\'>'
                                +'<span style=\'width:47px;display:flex;'
                                +'justify-content:flex-end;margin-bottom:0px;margin-right:0px\'>';
                strTip += _this.popover_dict[item];
                strTip += '</span>';
                strTip += '<div style=\'width:90px;display:flex\'>'
                                +'<span style=\'text-align:left;margin-bottom:0px\'>';
                strTip += '：';
                strTip += '</span>';
                if (entity.has_standard == 1) {
                  strTip += '<span style=\'margin-right:3px;color:#83cf51;font-weight:bolder\'>';
                  strTip += _this.isShowStandard ? popover_info['standard_dataset_count'] : 0;
                  strTip += '</span><span style=\'margin-right:3px\'>/</span>';
                }
                strTip += '<span style=\'font-weight:bolder\'>';
                strTip += ' ' + popover_info[item];
                strTip += `</span><span>${_this.$t('个')}</span>`;
                strTip += '</div>';
                strTip += `<div style='width:33px;${popover_info[item] === 0
                  ? 'display:none;pointer-events: none;' : 'display:block;'}'
                                onclick='route_click(${JSON.stringify(entity)}, "result_table")'>
                                <i class='bk-icon icon-list' title=${_this.$t('查看详情')}
                                style='cursor:pointer;margin:1px 1px 0 0;color:#699df4;display:flex;
                                justify-content:flex-end;'></i></div>`;
                strTip += '</div>';
              } else {
                // 在业务前加上分布于：
                if (item == 'bk_biz_count') {
                  strTip += `<div style='width:170px;margin-bottom:5px'>${_this.$t('分布于')}：</div>`;
                }
                let show_item_class = `show_top${item}`;
                // 后面通过class为show_item_class将background-color设置为浅紫色
                strTip +=
                  '<div class=\''
                  + show_item_class
                  + '\' id=\''
                  + show_item_class
                  + '\' onmouseenter=\'show_top("'
                  + item
                  + '")\' onmouseleave=\'hide_top("'
                  + item
                  + '")\' style=\'cursor:pointer;display:flex;width:170px;margin-bottom:5px;\'>'
                  +'<span style=\'width:60px;display:flex;justify-content:flex-end;margin-bottom:0px;'
                  +'margin-right:0px;font-weight:bolder;text-decoration:underline\'>';
                strTip += popover_info[item];
                strTip += '</span>';
                strTip += '<span style=\'text-align:left;margin-bottom:0px\'>';
                strTip += ` ${_this.$t('个')}
                                ${_this.popover_dict[item]} ${_this.isZhCn ? _this.$t('中') : ''}`;
                strTip += '</span></div>';
              }
            }
            strTip += '</div>';
            // }
            strTip += '<div id=\'' + topId
                        + '\' style=\'display:none;height:115px;\' onmouseenter=\'not_hide_top()\'></div>';
            strTip += '</div></div></div>';
            instance.setContent(strTip);
          },
        });

        $(id_tmp).mouseover(event => {
          let tmp = $(id_tmp);
          let instance = tmp[0]._tippy;
          timeoutKey && clearTimeout(timeoutKey);
          if (entity.is_mouseover) {
            instance.show();
          }
          $('#' + tooltipId)
            .parent()
            .on('mouseenter', e => {
              timeoutKey && clearTimeout(timeoutKey);
            })
            .on('mouseleave', e => {
              timeoutKey = setTimeout(() => {
                instance.hide();
                entity.is_mouseover = false;
              }, 500);
            });
        });

        $(id_tmp).mouseleave(event => {
          let tmp = $(id_tmp);
          let instance = tmp[0]._tippy;
          timeoutKey = setTimeout(() => {
            instance.hide();
            entity.is_mouseover = false;
          }, 500);
        });
      }
    },

    show_exit_full_screen_tooltip() {
      $('#exit-full-screen').mouseover(event => {
        let tmp = $('#exit-full-screen');
        let instance = tmp._tippy;
        timeoutKey && clearTimeout(timeoutKey);
        instance.show();
        $('#exit-full-screen')
          .parent()
          .on('mouseenter', e => {
            timeoutKey && clearTimeout(timeoutKey);
          })
          .on('mouseleave', e => {
            timeoutKey = setTimeout(() => {
              instance.hide();
            }, 500);
          });
      });

      $('#exit-full-screen').mouseleave(event => {
        let tmp = $('#exit-full-screen');
        let instance = tmp._tippy;
        timeoutKey = setTimeout(() => {
          instance.hide();
        }, 500);
      });
    },

    showPlusMinus(nodes) {
      // 用于展示加减号
      for (let each_node of nodes) {
        // 如果是「数据集市」节点，则不显示+/-
        // 如果from不为0，则显示-
        if (each_node.from > 0 && each_node.code != 'virtual_data_mart') {
          this.setNodeStyle(each_node.id, each_node.direction == 'right' ? 0 : 2);
        }
        // 如果from=0，is_leaf=true,不显示
        // 如果from=0，is_leaf=false,则显示+
        if (each_node.from == 0 && !each_node.is_leaf && each_node.code != 'virtual_data_mart') {
          each_node.is_plus = true;
          this.setNodeStyle(each_node.id, (each_node.direction == 'right' && 1) || 3);
        }
      }
    },

    setNodeStyle(id, childIndex) {
      const childNodes = (document.getElementById(id) && document.getElementById(id).children) || null;
      if (childNodes && childNodes.length > childIndex) {
        childNodes[childIndex].style.display = 'block';
      }
    },

    get_tag_sort_count(topn) {
      // 获取展示的标签
      // topn为默认展示topn的标签
      this.tag_loading = true;
      this.axios
        .get(`${window.BKBASE_Global.siteUrl}datamart/datamap/tag/sort_count/`, {
          params: {
            top: topn,
            delete_biz_tag: 1,
          },
        })
        .then(response => {
          if (response.result) {
            this.tag_list = response.data;
          } else {
            postMethodWarning(response.message, 'error');
          }
          this.tag_loading = false;
        });
    },

    get_run_mode() {
      this.axios.get(`${window.BKBASE_Global.siteUrl}datamart/datamap/get_run_mode/`).then(response => {
        if (response.result) {
          this.run_mode = response.data;
        } else {
          postMethodWarning(response.message, 'error');
        }
      });
    },

    exit_full_screen() {
      this.$nextTick(function () {
        // 用于将树形结构取消全屏
        this.is_full_screen = false;
        document.getElementById('dfv2').style.height = '607px';
        document.getElementById('dfv2-padding').style.padding = '10px';
      });
    },
    getClientWidth() {
      this.clientWidthTimer = setTimeout(() => {
        let dom = document.getElementById('tag-div');
        if (!dom) return;
        this.topn_total_width = document.getElementById('tag-div').clientWidth;
        clearTimeout(this.clientWidthTimer);
        this.clientWidthTimer = null;
      }, 800);
    },

    updateGraph(isReset = false) {
      if (this.$i18n.locale === 'en') {
        this.entity_data.locations.forEach(item => {
          item.name = item.code;
        });
      }
      dataMapD3Graph.renderGraph(this.entity_data);
      if (isReset) {
        dataMapD3Graph.reSet();
      }
      this.showNodeIcon();
    },
  },
};
</script>

<style lang="scss">
// popover项目和字符串超过一定长度
.pop-str-omit {
  width: 130px;
  display: inline-block !important;
}
#data-map-wrap {
  height: calc(100% - 1px);
  .global-layout-container {
    height: 100% !important;
    margin-bottom: 0px !important;
  }
  #dfv2-padding {
    cursor: move;
    position: relative;
    height: 100%;
    padding: 10px;
    overflow: hidden;
  }
  .crumb-item {
    font-weight: 700;
    height: 40px;
    line-height: 40px;
    background: #efefef;
    font-size: 14px;
  }
  // 面包屑描述样式
  .crumb-desc {
    position: absolute;
    top: 71px;
    left: 138px;
    z-index: 999;
    margin-left: 20px;
    padding-top: 1px;
    display: inline-block;
    font-size: 12px;
    color: rgb(160, 157, 157);
  }

  .data-map-label-tips {
    min-width: 20px;
    max-width: 250px;
    max-height: 300px;
    font-size: 14px !important;
  }
  // 页面最下边padding过大
  .layout-content {
    padding: 15px 15px 0 15px !important;
    min-width: 1300px;
  }

  .popover-header {
    width: 170px;
    font-size: 14px;
    width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    display: inline-block;
    padding: 4px 4px;
    margin-bottom: 0;
    color: #fff;
    font-weight: bolder;
    border-top-left-radius: 2px;
    border-top-right-radius: 2px;
    padding-right: 0;
  }

  .font15 {
    font-size: 15px;
  }
  .font12 {
    font-size: 12px;
  }

  .fw600 {
    font-weight: 600 !important;
  }

  .data-map-label-tips {
    min-width: 20px;
    max-width: 250px;
    max-height: 300px;
    font-size: 14px !important;
  }

  // 字符串超过一定长度
  .str-omit {
    overflow: hidden !important;
    text-overflow: ellipsis !important;
    white-space: nowrap !important;
    display: inline-block !important;
    &:hover {
      border: 1px solid rgba(102, 107, 180, 0.5);
      height: calc(100%);
    }
  }
  .outline-none {
    outline: none;
  }

  .tag-outer {
    border: 1px solid rgb(205, 209, 214);
    margin-bottom: 10px;
    color: #666bb4;
    min-height: 209.83px;
  }

  .page-main {
    .icon-screen {
      cursor: pointer;
      outline: none;
      margin-top: 12px;
      margin-left: 13px;
      color: #3a84ff;
    }
    .plot-container {
      overflow: hidden;
    }
    outline: none;
    .legend-css {
      margin-left: 5px;
      box-shadow: 0px 0px 0px #aaa !important;
      cursor: default;
    }
    .legend-remove {
      transform: translateX(-470px) !important;
      transition: all 1s ease-in-out !important;
    }
    .legend-no-remove {
      transform: translateX(0px);
      transition: all 0.7s ease-in-out !important;
    }
    .switch-remove {
      transform: translateX(450px);
    }
    .right70 {
      right: 132px !important;
    }
    .right0 {
      right: 0px;
    }

    .switch-no-remove {
      transform: translateX(0px);
    }

    .whole-width {
      width: calc(100%);
    }
    .limit-width {
      width: calc(100% - 330px);
    }

    .connected-info {
      position: absolute !important;
      right: 0px;
      width: calc(68% - 695px);
      height: calc(100% + 10px);
      background: #fff;

      .inner-info {
        height: 100%;
        position: relative;
      }

      .switch {
        position: absolute;
        top: 41%;
        left: -25px;
        width: 24px;
        font-size: 12px;
        font-weight: bold;
        color: #666bb4;
        padding: 6px;
        background: #fff;
        border: 1px solid #c3cdd7;
        border-right: none;
        box-shadow: -2px 0px 10px rgba(33, 34, 50, 0.35);
        cursor: pointer;
        transform: translate(0, -50%);
      }
    }
    .workflow-tools {
      border-right: 0 !important;
    }
    .legend-div {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 5px;
      background: #f7f7f7;
    }

    .legend-node-background {
      margin-top: 5px;
      height: 28px;
      border-color: #a9adb5;
      position: relative;
      box-shadow: 0px 0px 0px #aaa !important;
    }

    .jtk-window {
      min-width: 73px;
      border: 1px solid #a9adb5;
      text-align: center;
      z-index: 24;
      cursor: pointer;
      box-shadow: 2px 2px 19px #aaa !important;
      border-radius: 0.5em;
      min-width: 50px;
      color: #000;
      transition: box-shadow 0.15s ease-in !important;
    }

    // 三角徽章样式（图例）start
    .bk-mark-triangle-map-legend.bk-success-map:before {
      border-color: #83cf51 #83cf51 transparent transparent;
    }

    .bk-mark-triangle-map-legend:before {
      content: '';
      width: 0px;
      height: 0;
      border: 6px solid;
      position: absolute;
      border-radius: 0 6px;
      right: 0;
      top: -1px;
    }
    // 三角徽章样式（图例）end

    // 三角徽章样式
    .bk-mark-triangle-map.bk-success-map:before {
      border-color: #83cf51 #83cf51 transparent transparent;
    }
    .bk-mark-triangle-map-root.bk-success-map:before {
      border-color: #83cf51 #83cf51 transparent transparent;
    }

    .bk-mark-triangle-map:before {
      content: '';
      width: 0px;
      height: 0;
      border: 8px solid;
      position: absolute;
      border-radius: 0 6px;
      right: -1px;
      top: 0px;
    }
    .bk-mark-triangle-map-root:before {
      content: '';
      width: 0px;
      height: 0;
      border: 10px solid;
      position: absolute;
      border-radius: 0 6px;
      left: 130px;
      top: -1px;
    }
    // 正方形样式
    .bk-mark-square:before {
      content: '';
      color: #83cf51;
      width: 0px;
      height: 0;
      border: 5px solid;
      position: absolute;
      // border-radius: 0px 6px 0px 0px;
      left: 120px;
      top: -1px;
    }
    // 图例标准徽章样式
    .bk-mark-square-map-legend:before {
      content: '';
      height: 14px !important;
      color: #83cf51;
      width: 0px;
      height: 0;
      border: 7px solid;
      position: absolute;
      left: 58px;
      top: -1px;
    }
    // 标准徽章样式
    .bk-mark-square-map:before {
      content: '';
      width: 0px;
      position: absolute;
      right: -1px;
      top: 0px;
      border-top-right-radius: 0.5em;
      border-bottom-right-radius: 0.5em;
      bottom: 0;
      width: 15px;
      background: #83cf51;
    }
    .stan-p {
      writing-mode: vertical-rl;
      font-size: 12px;
      position: absolute;
      color: #fff;
      right: -3px;
      top: 4px;
      -webkit-transform-origin-x: 0;
      transform: scale(0.8);
    }

    .stan-p-legend {
      writing-mode: vertical-rl;
      font-size: 12px;
      position: absolute;
      color: #fff;
      right: -5px;
      top: 1px;
      -webkit-transform-origin-x: 0;
      transform: scale(0.8);
    }

    /* 标签close icon的样式*/
    .close {
      /* still bad on picking color */
      color: #666bb4;
      /* make a round button */
      border-radius: 12px;
      /* center text */
      line-height: 18px;
      text-align: center;
      font-size: 22px;
      margin-top: -2px;
      font-weight: 100;
    }
    /* use cross as close button */
    .close::before {
      content: '\00D7';
    }
    .tag-row-no-dashed {
      padding: 10px 0 10px 10px;
      .ver-center {
        .bk-tooltip {
          height: 19px;
        }
      }
      .bk-tooltip-ref {
        height: 19px;
      }
    }

    .tag-row-no-dashed {
      padding: 10px 0 10px 10px;
    }

    // 用于选择标签选择器的上边框显示实线/虚线
    .tag-row-dashed {
      padding: 10px 0 10px 10px;
      border-top: 1px dashed rgb(205, 209, 214);
      .ver-center {
        .bk-tooltip {
          height: 19px;
        }
      }
    }
    // 用于选择标签选择器的上边框显示实线/虚线
    .tag-row-dashed-no-biz {
      padding: 10px 0 10px 10px;
      // border-top:1px dashed rgb(205, 209, 214);
      .ver-center {
        .bk-tooltip {
          height: 19px;
        }
      }
      background-color: rgba(255, 221, 221, 0.35) !important;
    }
    .tag-row-dashed-no-biz-type {
      padding: 10px 0 10px 10px;
      border-top: 1px dashed rgb(205, 209, 214);
      .ver-center {
        .bk-tooltip {
          height: 19px;
        }
      }
      background-color: rgba(255, 232, 195, 0.35);
    }
    .tag-row-solid {
      padding: 10px 0 10px 10px;
      // border-top:1px solid rgb(205, 209, 214);
    }
    // 设置数据地图页面最上面的tab字体
    .border-up-empty {
      width: 0;
      height: 0;
      border-left: 9px solid transparent;
      border-right: 9px solid transparent;
      border-bottom: 11px solid rgb(205, 209, 214);
      position: relative;
      margin-left: 30px;
      z-index: 999;
    }
    .border-up-empty span {
      display: block;
      width: 0;
      height: 0;
      border-left: 8px solid transparent;
      border-right: 8px solid transparent;
      border-bottom: 10px solid rgb(244, 245, 253);
      position: absolute;
      left: -8px;
      top: 2px;
    }
    .all-data {
      margin-left: 30px;
    }
    .ops-theme {
      margin-left: 111px;
    }
    .operation-theme {
      margin-left: 192px;
    }
    .button-group.bk-button {
      font-size: 12px;
    }

    // tooltip宽度
    .tippy-tooltip {
      max-width: 4440px;
    }
    .tippy-tooltip.light-theme {
      max-width: 440px !important;
    }
    // tag选择样式
    .tag-chosen {
      border: 1px solid #666bb4;
      padding: 0 3px 0 3px;
      border-radius: 2px;
      outline: none;
      // 选中的标签悬浮不加样式
      &:hover .str-omit {
        border: none;
      }
    }

    .no-hover {
      &:hover {
        border: none;
      }
    }

    .tag-height {
      line-height: 19px;
      height: 19px;
    }

    .tag-width {
      padding-left: 3px;
      width: 88px;
    }

    .tag-close {
      // 标签点击取消选中的样式
      position: absolute;
      right: -1px;
      top: 56%;
      transform: translateY(-50%);
    }

    .bk-tooltip-ref {
      outline: none;
      // height: 19px;
      // line-height: 19px;
    }

    // 去掉浅蓝色outline
    .outline-none {
      outline: none;
    }

    // 水平垂直居中
    .hori-ver-center {
      text-align: center;
      vertical-align: middle;
      outline: none;
    }
    .tag-heat-wrap {
      display: flex;
      align-content: center;
    }
    // 垂直居中
    .ver-center {
      vertical-align: middle;
      outline: none;
      .tag-heat-wrap {
        display: flex;
        align-content: center;
      }
      .icon-heat-fill {
        color: #ea3636;
        line-height: 19px;
        margin-left: 3px;
      }
    }

    .nsewdrag.drag {
      width: 265px;
    }

    //按钮组样式
    .button-group {
      margin-right: -5px;
    }

    // 单选框css
    .bk-select.map-select {
      width: 220px !important;
      .bk-tooltip-ref {
        outline: none;
      }
    }
    // 水平居右
    .pull-right {
      float: right !important;
    }
    // 水平居左
    .pull-left {
      float: left !important;
    }

    //box css
    .shadows {
      -webkit-box-shadow: 2px 3px 5px 0px rgba(33, 34, 50, 0.15);
      box-shadow: 2px 3px 5px 0px rgba(33, 34, 50, 0.15);
      border-radius: 2px;
      border: solid 1px rgba(195, 205, 215, 0.6);
    }

    .no-shadows {
      border-radius: 2px;
      border: solid 1px rgba(195, 205, 215, 0.6);
    }

    // 按钮组选中效果
    .bk-button {
      &.is-selected {
        background-color: #e1ecff;
        border-color: #3a84ff;
        color: #3a84ff;
        position: relative;
        z-index: 1;
      }
      &.is-selected-legend {
        background-color: #e1ecff;
        border-color: #3a84ff;
        color: #3a84ff;
        position: relative;
        z-index: 1;
      }
    }

    // 右侧统计结果卡片
    .card-sta {
      background: #fff;
      border: 1px solid #ddd;
      -webkit-box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
      box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
      float: left;
      position: relative;
      .header {
        border-bottom: 1px solid #ddd;
        padding: 4px;
        height: 30px;
        -webkit-box-align: center;
        align-items: center;
        background-color: #fafafa;
      }
    }

    // 右侧line chart卡片
    .card-line {
      background: #fff;
      border: 1px solid #ddd;
      -webkit-box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
      box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
      float: left;
      position: relative;
      .header {
        border-bottom: 1px solid #ddd;
        padding: 8px;
        height: 38px;
        -webkit-box-align: center;
        align-items: center;
        background-color: #fafafa;
      }
    }

    // chosen_tag css start
    .data-map-tag-border {
      cursor: default;
      border: solid 1px #666bb4;
      background-color: #666bb4;
      color: #fff;
      border-radius: 1px;
      padding: 4px;
      font-size: 10px;
    }
    .data-map-parent-tag-border {
      cursor: default;
      border: solid 1px #666bb4;
      border-radius: 1px;
      padding: 4px;
      font-size: 10px;
    }

    // 列表视图、标签视图样式
    $bgColor: #666bb4;
    .primary-link {
      color: $bgColor;
      cursor: pointer;
    }
    .primary-link:hover {
      color: #777cc5;
    }

    // page-hearder相关样式
    .title {
      .bk-button {
        margin: auto 0;
      }
      display: flex;
      align-items: 'center';
      height: 40px !important;
      color: #212232;
    }

    .page-header {
      padding: 0 90px;
      background-color: #f2f4f9;
      height: 40px !important;
      > .title {
        .bk-button {
          margin: auto 0;
        }
        display: flex;
        align-items: 'center';
        height: 60px;
        color: #212232;
      }
    }

    .bg-purple {
      background-color: #666bb4 !important;
      color: #fff !important;
    }

    // dataflow css
    $bgColor: #666bb4;
    .primary-link {
      color: $bgColor;
      cursor: pointer;
    }

    .primary-link:hover {
      color: #777cc5;
    }

    .page-header {
      padding: 0 90px;
      background-color: #f2f4f9;
      > .title {
        .bk-button {
          margin: auto 0;
        }
        display: flex;
        align-items: 'center';
        height: 60px;
        color: #212232;
      }
    }
    .plus_extend_entity_right {
      display: none;
      cursor: pointer;
      position: absolute;
      top: 9px;
      color: #bbbbbb;
      right: -17px;
      outline: none;
    }
    .plus_extend_entity_left {
      display: none;
      cursor: pointer;
      position: absolute;
      top: 9px;
      color: #bbbbbb;
      left: -17px;
      outline: none;
    }
    .standard-detail-box {
      width: 100%;
      .access-button {
        position: absolute;
        right: 20px;
        top: 8px;
        z-index: 100;
      }
      .detail-tab {
        box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
        height: 687px;
      }
    }
    .bg-violet {
      background-color: #669 !important;
      color: #fff !important;
    }
    .bg-success {
      background-color: #9dcb6b;
      color: #fff !important;
    }
    .bg-orange {
      background-color: #ef9549 !important;
      color: #fff !important;
    }
    table.grid-table {
      width: 100%;
      font-family: verdana, arial, sans-serif;
      font-size: 15px;
      border-width: 1px;
      border-color: #ebeef5;
      border-collapse: collapse;
    }
    table.grid-table td {
      border-width: 1px;
      padding: 12px;
      border-color: #ebeef5;
    }
    .title {
      font-size: 4px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    #flowcontainer {
      overflow: hidden;
      .node-desc-map {
        color: #737987;
        font-size: 12px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        display: inline-block;
        text-align: left;
        vertical-align: middle;
        flex: 1;
      }
      .node-desc-map-root {
        color: #666bb4;
        font-size: 14px;
        width: 153px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        display: inline-block;
        text-align: center;
        vertical-align: middle;
      }
      .node-background-other {
        background: #fff !important;
        height: 32px;
        padding: 0 5px;
        border-radius: 6px;
        display: table-cell;
        vertical-align: middle;
        border-color: #a9adb5;
        color: #737987;
        overflow: hidden !important;
      }
      .node-background-root {
        background: #a3c5fd !important;
        font-size: 14px;
        font-weight: 900;
        height: 50px;
        padding: 0 5px;
        border-radius: 6px;
        display: table-cell;
        vertical-align: middle;
        border-color: #a9adb5;
        color: #737987;
        overflow: hidden !important;
      }
      .node-background-standard {
        height: 32px;
        padding: 0 5px;
        border-radius: 6px;
        display: table-cell;
        vertical-align: middle;
        text-align: center;
        border-color: #a9adb5;
        overflow: hidden !important;
      }
      .node-desc-map-zero {
        color: #c1bfbf;
        font-size: 12px;
        width: 70px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        display: inline-block;
        text-align: left;
        vertical-align: middle;
      }
      .node-background-zero {
        background: #fafafa !important;
        height: 32px;
        padding: 0 5px;
        border-radius: 6px;
        display: table-cell;
        vertical-align: middle;
        text-align: center;
        border-color: #d6d6d6;
        color: #ccc;
        overflow: hidden !important;
      }
      .node-desc-count-map {
        color: #a09d9d;
        font-size: 12px;
        max-width: 51px;
        line-height: 12px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        display: inline-block;
        vertical-align: middle;
        margin-right: 0px;
      }
      .node-desc-count-map-root {
        color: #666bb4;
        font-size: 12px;
        max-width: 59px;
        line-height: 12px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        display: inline-block;
        vertical-align: middle;
        margin-right: 0px;
      }
      .node-desc-count-map-zero {
        // background-color: #666bb4;
        color: #ccc;
        font-size: 12px;
        max-width: 59px;
        line-height: 12px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        display: inline-block;
        vertical-align: middle;
      }
      // 当前搜索到的节点的样式
      .node-background-current {
        background: rgba(225, 236, 255, 0.5);
        height: 32px;
        padding: 0 5px;
        border-radius: 6px;
        display: table-cell;
        vertical-align: middle;
        text-align: center;
        border-color: #a9adb5;
        overflow: hidden !important;
      }

      .plus_extend_entity {
        display: none;
        cursor: pointer;
        position: absolute;
        top: 9px;
        color: #bbbbbb;
        z-index: 99999;
      }
      .dataflow .jtk-content {
        height: 100%;
      }
      #dfv2 {
        height: 607px;
        .jtk-window {
          min-width: 100px;
          background-color: white;
          border: 1px solid #346789;
          text-align: center;
          z-index: 24;
          cursor: pointer;
          box-shadow: 2px 2px 19px #aaa;
          border-radius: 0.5em;
          min-width: 50px;
          color: black;
          transition: box-shadow 0.15s ease-in;
        }
      }
      .dataflow .jtk-overlay {
        width: 16px;
        height: 16px;
        text-align: center;
        display: none;
        background: #737987;
        border-radius: 50%;
        padding-top: 2px;
        z-index: 2600;
        &:hover {
          background: #666bb4;
        }
      }
      .jtk-endpoint {
        display: none;
        position: relative;
        z-index: 120;
        padding: 13px;
        transform: translate(-30%, -32%);
        background: transparent;
        cursor: pointer;
        circle {
          stroke: transparent;
          fill: transparent;
        }
        svg {
          top: 50%;
          left: 50%;
          transform: translate(50%, 50%);
        }

        &:hover circle {
          fill: #ff7234;
          stroke: #ff7234;
        }
        &:hover svg {
          top: 50% !important;
          left: 50% !important;
          transform: translate(-27%, -26%) scale(1.2);
          transform-origin: 50% 50%;
        }
      }

      .dataflow .workflow-canvas {
        padding-left: 0px;
        margin-top: -48px;
      }

      .dataflow .workflow-tools {
        width: 0px;
      }
      .item {
        margin: 4px;
      }
      .top {
        text-align: center;
      }
      .left {
        float: left;
        width: 60px;
      }
    }
  }
}
</style>
<style lang="scss" scoped>
.hidden {
  display: none;
}
.nowrap {
  white-space: nowrap;
}
.before-hidden {
  &::before {
    display: none;
  }
}
.en-switch {
  left: -58px !important;
  width: auto !important;
}
</style>
