/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

import { fromUnixTime, getUnixTime, parse, add, isEqual, format, isBefore } from 'date-fns';
const formatString = 'yy/MM/dd HH:mm:ss';
function reply() {
  if (arguments.length < 1) {
    throw new TypeError('reply - takes at least one argument');
  }
  postMessage({
    queryMethodListener: arguments[0],
    queryMethodArguments: Array.prototype.slice.call(arguments, 1),
  });
}

/* This method is called when main page calls QueryWorker's postMessage method directly*/
function defaultReply(message) {
  throw new TypeError('reply - cannot find available queryMethod');
}

function convertTimeStringToUnixStamp(timeString, format = null) {
  const date = parse(timeString, format || formatString, new Date());
  return getUnixTime(date);
}

/**
 * 格式化不连续的timestamp列表为不同的分组数据
 * @param items 不连续的列表
 * @param output 输出
 * @param tsFreq 时间间隔
 */
function formatTimeListToGroupList(items: Array<string>, output = [], tsFreq = 0, activePage = 0) {
  const items = items || [];
  let item = { start: 0, end: 0, startValue: '', endValue: '' };
  let preTime = null;
  const length = items.length - 1;
  items.forEach((timestamp, index) => {
    if (!preTime) {
      item.start = timestamp;
      item.startValue = timestamp;
      preTime = timestamp;
    } else {
      /** 根据上一行数据和数据频率计算当前数据时间  */
      const current = preTime + tsFreq; //add(parse(preTime, formatString, new Date()), { seconds: tsFreq })

      /** 当前时间 */
      const currentMoment = timestamp;

      /** 如果当前数据和和根据频率计算出来的数据不一致，则认为是新的一个区间段 */
      if (!isEqual(current, currentMoment)) {
        item.end = preTime;
        item.endValue = preTime;

        const { start, end, startValue, endValue } = item;
        output.push({ start, end, startValue, endValue, page: activePage });
        item = {
          start: currentMoment,
          end: 0,
          startValue: timestamp,
          endValue: '',
        };
      }

      /** 如果是最后一个数据 */
      if (index === length) {
        item.end = timestamp;
        item.endValue = timestamp;
        const { start, end, startValue, endValue } = item;
        output.push({ start, end, startValue, endValue, page: activePage });
      }

      preTime = timestamp;
    }
  });

  return output;
}

/**
 * 更新样本曲线状态
 * 新的状态
 * @param statu
 * @param originData 数据预览曲线数据
 * @param previewData 数据预览Table数据
 * @param start 数据开始区间
 * @param end 数据结束区间
 */
function updateLinesStatus(
  statu: string,
  originData: Array<any> = [],
  previewData: Array<any> = [],
  start = '',
  end = ''
) {
  // let startIndex = originData.findIndex(data => data.timestamp === start)
  // let endIndex = originData.findIndex(data => data.timestamp === end)

  // const labels = originData.map(data => (data.timestamp))
  // if (startIndex === -1) {
  //     const closetItems = getClosetTick(labels, start)
  //     startIndex = labels.findIndex(item => item === closetItems[0])
  // }

  // let endIndex = originData.findIndex(data => data.timestamp === end)
  // if (endIndex === -1) {
  //     const closetItems = getClosetTick(labels, end)
  //     endIndex = labels.findIndex(item => item === closetItems[0])
  // }

  originData
    .filter(item => item.timestamp >= start && item.timestamp <= end)
    .forEach(item => {
      item['__#status'] = statu;
      item['#status'] = getStatusAlias(statu);

      const tableItem = (previewData.list || []).find(field => field.timestamp === item.timestamp);
      if (tableItem) {
        tableItem['__#status'] = item['__#status'];
        tableItem['#status'] = item['#status'];
      }
    });

  return { originData, previewData };
}

/**
 * 映射状态显示名称
 * @param status
 */
function getStatusAlias(status: string | null) {
  if (status === '' || status === null) {
    return '未添加';
  } else {
    return { init: '未添加', added: '已添加', deleting: '待删除', adding: '待添加' }[status] || '未添加';
  }
}

// /** 查找邻近的时间 */
// function getClosetTick (labels: Array<any>, timestamp: number) {
//     const length = labels.length
//     if (length === 0) return labels
//     const middleIndex = Math.floor(length / 2)
//     if (labels[middleIndex] > timestamp) {
//         if (labels[middleIndex - 1] < timestamp) {
//             return [labels[middleIndex - 1], labels[middleIndex]]
//         } else {
//             return getClosetTick(labels.slice(0, middleIndex + 1), timestamp)
//         }
//     } else {
//         if (labels[middleIndex + 1] > timestamp) {
//             return [labels[middleIndex], labels[middleIndex + 1]]
//         } else {
//             return getClosetTick(labels.slice(middleIndex + 1, length), timestamp)
//         }
//     }
// }

/** 查找邻近的时间 */
function getClosetTick(ticks: Array<any>, matchValue = 0) {
  const length = ticks.length;
  if (length <= 2) {
    return ticks;
  }
  const middleIndex = Math.floor(length / 2);
  if (ticks[middleIndex] > matchValue) {
    if (ticks[middleIndex - 1] < matchValue) {
      return [ticks[middleIndex - 1], ticks[middleIndex]];
    } else {
      return getClosetTick(ticks.slice(0, middleIndex + 1), matchValue);
    }
  } else {
    if (ticks[middleIndex + 1] > matchValue) {
      return [ticks[middleIndex], ticks[middleIndex + 1]];
    } else {
      return getClosetTick(ticks.slice(middleIndex + 1, length), matchValue);
    }
  }
}

const manager = {
  /**
   * 样本曲线框选选中区域值
   * @param data timestamp 数组
   * @param originData 数据源列表 用于筛选框选数据
   * @param nextIsEmpty 用于标识当前是否为最后一组计算，默认 true
   */
  getPreviewPanedRows: function (panedData: any, originData = [], nextIsEmpty = true) {
    // const start = panedData.startValue
    // const end = panedData.endValue
    let previewPanedRows = [];
    if (panedData) {
      const { startValue, endValue } = panedData;
      previewPanedRows = originData
        .filter(item => item.timestamp >= startValue && item.timestamp <= endValue)
        .map(item => item.timestamp);
    }
    reply('getPreviewPanedRows', panedData, previewPanedRows, nextIsEmpty);
  },

  /**
   * 点击添加标注事件
   * 计算新的区间值
   * 用于渲染和提交
   * */
  handleActionClick: function (info: any) {
    const {
      previewLineData = {},
      preCreateSamples = [],
      preDeleteSamples = [],
      tsFreq = 0,
      activePage = 0,
      statu,
      previewData = {},
      start,
      end,
      batch,
    } = info;

    const updateValue = updateLinesStatus(statu, previewLineData.list, previewData, start, end);
    previewLineData.list = updateValue.originData.filter(item => !item['#blank'] || item['#blank'] !== 1);
    previewData = updateValue.previewData;

    /** 计算待提交表单数据 */
    const submitSamples = {
      create: (preCreateSamples || []).filter(item => item.page !== activePage),
      delete: (preDeleteSamples || []).filter(item => item.page !== activePage),
    };
    reply('handleActionClick', submitSamples, previewLineData, previewData, batch, statu);
  },
  /**
   * 用于切换分页保持数据状态
   */
  updateCacheDate(info) {
    const { previewData, createSamples, deleteSamples } = info;
    const previewPanedRows = [];
    const max = Math.max.apply(
      null,
      createSamples.concat(deleteSamples).map(item => item.end)
    );
    const min = Math.min.apply(
      null,
      createSamples.concat(deleteSamples).map(item => item.start)
    );
    const list = previewData.list;
    for (let i = 0, item; (item = list[i++]);) {
      let timestamp = list[i].timestamp;
      if (timestamp < min) continue;
      if (timestamp > max) break;
      if (createSamples.length) {
        const isExit = createSamples.some(item => timestamp > item.start && timestamp < item.end);
        if (isExit) {
          list[i]['__#status'] = 'adding';
          list[i]['#status'] = getStatusAlias('adding');
          previewPanedRows.push(format(fromUnixTime(timestamp), formatString));
          continue;
        }
      }
      if (deleteSamples.length) {
        const isExit = deleteSamples.some(item => timestamp > item.start && timestamp < item.end);
        if (isExit) {
          list[i]['__#status'] = 'deleting';
          list[i]['#status'] = getStatusAlias('deleting');
          previewPanedRows.push(format(fromUnixTime(timestamp), formatString));
          continue;
        }
      }
    }
    reply('updateCacheDate', previewData, previewPanedRows);
  },
};

onmessage = function (event) {
  if (
    event.data instanceof Object
        && event.data.hasOwnProperty('queryMethod')
        && event.data.hasOwnProperty('queryArguments')
  ) {
    manager[event.data.queryMethod].apply(self, event.data.queryArguments);
  } else {
    defaultReply(event.data);
  }
};
