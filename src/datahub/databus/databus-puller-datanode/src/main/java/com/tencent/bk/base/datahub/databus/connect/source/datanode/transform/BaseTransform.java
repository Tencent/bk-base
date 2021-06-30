/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.bk.base.datahub.databus.connect.source.datanode.transform;

import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;

import com.tencent.bk.base.datahub.databus.commons.TaskContext;
import com.tencent.bk.base.datahub.databus.commons.convert.Converter;
import com.tencent.bk.base.datahub.databus.commons.convert.ConverterFactory;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.TimeZone;


/**
 * 算子基础类，封装一些公共参数还有初始化
 */
public abstract class BaseTransform implements INodeTransform {

    private static final Logger log = LoggerFactory.getLogger(BaseTransform.class);
    protected String connector;
    protected String config;
    protected Converter converter;
    protected TaskContext ctx;
    protected String[] colsInOrder;
    protected SimpleDateFormat dateFormat;
    protected SimpleDateFormat dteventtimeDateFormat;


    public BaseTransform(String connector, String config, Converter converter, TaskContext ctx) {
        this.connector = connector;
        this.config = config;
        this.converter = converter;
        this.ctx = ctx;
    }


    @Override
    public final void configure() {
        try {
            //日期格式初始化
            dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            dteventtimeDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            // 如果命令行参数传入了时区信息，则使用，否则使用机器默认的时区信息
            String tz = System.getProperty(EtlConsts.DISPLAY_TIMEZONE);
            if (tz != null) {
                dateFormat.setTimeZone(TimeZone.getTimeZone(tz));
                dteventtimeDateFormat.setTimeZone(TimeZone.getTimeZone(tz));
            }
            if (this.ctx != null) {
                //解析器初始化
                ctx.setSourceMsgType(Consts.AVRO);
                converter = ConverterFactory.getInstance().createConverter(ctx);
                doConfigure();
            }
        } catch (Exception e) {
            LogUtils.warn(log, "BaseTransform configure() error :{}", e.getMessage());
        }
    }

    @Override
    public final void refreshConfig(String config) {
        this.config = config;
        this.doConfigure();
    }

    /**
     * 初始化配置
     */
    protected abstract void doConfigure();


}
