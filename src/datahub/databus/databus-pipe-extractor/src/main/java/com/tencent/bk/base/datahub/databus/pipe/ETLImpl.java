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

package com.tencent.bk.base.datahub.databus.pipe;

import com.tencent.bk.base.datahub.databus.pipe.TimeTransformer.Transformer;
import com.tencent.bk.base.datahub.databus.pipe.cal.Cal;
import com.tencent.bk.base.datahub.databus.pipe.convert.Convert;
import com.tencent.bk.base.datahub.databus.pipe.dispatch.Expr;
import com.tencent.bk.base.datahub.databus.pipe.exception.EmptyEtlResultError;
import com.tencent.bk.base.datahub.databus.pipe.exception.PipeExtractorException;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.pipe.utils.ZipUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ETLImpl implements ETL {

    private Map<String, Map<String, String>> props = null;

    private TimeTransformer.Transformer timeTransformer;

    // 当前有效parser
    private Node parser;
    private Context ctx;

    // 扁平化的schema
    private Fields schema;

    // ETL配置ID（一个dataid可以配置多个ETL）
    private int dataId = 0;

    // ETL配置
    private final String conf;

    public Map<String, Object> confObj;
    public int timeFieldIdx = -1;
    public String encoding = "UTF8";
    private boolean shouldUZip = false;
    String delimiter;
    String parseType = EtlConsts.PARSE;

    // 数据切分逻辑
    Map<Integer, Expr> dispatch;

    // 字段派生
    private Derive derive = null;
    private List<Cal> cals = null;
    // 清洗阶段简单的数据梳理函数
    List<Convert> converts = null;

    /**
     * 实现类.
     */
    public ETLImpl(String conf) throws Exception {
        this.conf = conf;
        confObj = JsonUtils.readMap(conf);
        newParser();
    }

    public ETLImpl(String conf, Map<String, Map<String, String>> props) throws Exception {
        this.conf = conf;
        this.props = props;
        confObj = JsonUtils.readMap(conf);
        newParser();
    }

    /**
     * 根据新配置生成新的parser.
     */
    private void newParser() throws Exception {
        synchronized (this) {
            Context ctx = new Context(this);
            Node parser = Config.parse(ctx, this.conf);

            this.parser = parser;
            if (this.props != null && props.containsKey(EtlConsts.CC_CACHE)) {
                ctx.ccCacheConfig = props.get(EtlConsts.CC_CACHE);
            }
            this.ctx = ctx;

            //调整了原始的schema，应该在this.schema使用之前处理
            derive = Config.parseDerive(ctx, this.conf);
            cals = Config.parseCalculate(ctx, this.conf);
            this.schema = new Fields();
            for (Field field : this.ctx.flattenSchema(ctx.getSchema())) {
                this.schema.append(field);
            }

            //依赖this.schema
            initSimpleConf();

            this.dispatch = Config.parseDispatch(this, this.conf);
            if (this.dispatch != null) {
                this.parseType = EtlConsts.SPLIT;
            }

            this.converts = Config.parseConvert(this, this.conf);
        }

    }

    /**
     * 时间字段.
     */
    private void initSimpleConf() {
        if (!confObj.containsKey(EtlConsts.CONF)) {
            return;
        }

        Map<String, Object> conf = (Map<String, Object>) confObj.get(EtlConsts.CONF);

        String timeFieldName = (String) conf.get(EtlConsts.TIME_FIELD_NAME);
        String outputTimeFieldName;
        if (timeFieldName.equals(EtlConsts.DEFAULT_TIME_FIELD_NAME)) {
            outputTimeFieldName = EtlConsts.DEFAULT_OUTPUT_FIELD_NAME;
            timeFieldIdx = EtlConsts.DEFAULT_TIME_FIELD_INDEX;
        } else {
            outputTimeFieldName = (String) conf.get(EtlConsts.OUTPUT_FIELD_NAME);
            timeFieldIdx = schema.fieldIndex(timeFieldName);
        }
        final String timeFormat = (String) conf.get(EtlConsts.TIME_FORMAT);
        final int timestampLen = (int) conf.get(EtlConsts.TIMESTAMP_LEN);
        final double timezone =
                conf.containsKey(EtlConsts.TIMEZONE) ? (Double.parseDouble(conf.get(EtlConsts.TIMEZONE).toString()))
                        : 8;
        final int timeRound = conf.containsKey(EtlConsts.TIME_ROUND) ? ((int) conf.get(EtlConsts.TIME_ROUND)) : 1;

        delimiter = (String) conf.get(EtlConsts.DELIMITER);

        // 这里设置一个timestamp内部字段，用于保存用户指定的时间转换字段转换为timestamp的值，添加在用户配置的字段尾部
        Field timeField = new Field(outputTimeFieldName, EtlConsts.INT);
        timeField.setIsInternal(true);
        schema.append(timeField);

        timeTransformer = TimeTransformer.transformerFactory(timeFormat, timestampLen, timezone, timeRound);

        encoding = conf.containsKey(EtlConsts.ENCODING) ? ((String) conf.get(EtlConsts.ENCODING)) : EtlConsts.UTF8;
        shouldUZip = (conf.containsKey(EtlConsts.COMPRESSED) ? ((int) conf.get(EtlConsts.COMPRESSED)) : 0) == 1;

        // 设置平坦化后schema每个字段”是否为输出字段""
        ctx.setIgnoreFields((List<String>) conf.get(EtlConsts.IGNORE_FIELDS));
        for (Object fieldItem : schema) {
            Field field = (Field) fieldItem;
            if (this.ctx.ignoreFields.containsKey(field.getName())) {
                field.setIsOutputField(false);
                ctx.getSchema().index();
            }
        }
    }


    /**
     * 同步数据处理函数.
     */
    @Override
    public ETLResult handle(String data) {

        /**
         * 这里要localize parser和上下文环境，防止因为配置修改造成的临界条件.
         */
        Node parser = this.parser;
        Context ctx = this.ctx;
        data = data.trim();

        ctx.clearValues();
        ctx.origData = data;

        try {
            parser.execute(ctx, data);

        } catch (EmptyEtlResultError e) {
            //一般的，打包的数据会有部分成功，部分失败的，这里的异常表示所有数据都失败了
            return ctx.toETLResult(this);
        } catch (PipeExtractorException e) {
            //走到这里的，一般是顶层的数据有问题
            ctx.appendBadValues(data, e, true);
        }

        return ctx.toETLResult(this);
    }

    @Override
    public ETLResult handle(byte[] data) throws IOException {

        if (shouldUZip) {
            data = ZipUtils.unGzip(data);
        }
        return handle(new String(data, encoding));
    }

    /**
     * 获取当前输出数据格式信息.
     */
    @Override
    public Fields getSchema() {
        return this.schema;
    }

    public String getConfString() {
        return this.conf;
    }

    public Integer getDataId() {
        return this.dataId;
    }

    public String getParseType() {
        return parseType;
    }

    public String getEncoding() {
        return encoding;
    }

    /**
     * return the derive.
     *
     * @return the derive
     */
    public Derive getDerive() {
        return derive;
    }

    public Transformer getTimeTransformer() {
        return timeTransformer;
    }

    /**
     * 分隔符.
     */
    public String getDelimiter() throws IllegalArgumentException {
        if (delimiter == null) {
            throw new IllegalArgumentException("no delimiter given");
        }
        return delimiter;
    }

    public List<Cal> getCals() {
        return this.cals;
    }

    /**
     * 根据清洗的配置对msg进行验证,返回解析msg的结果
     *
     * @param msg 待验证的msg内容
     * @return 解析msg的结果(json格式的字符串)
     */
    public String verifyConf(String msg) throws IOException {
        ctx.setContextToVerifyConf();
        List<List<Object>> parsedValues = new ArrayList<>();
        try {
            ETLResult etlResult = handle(msg);
            parsedValues = etlResult.getValues().get(dataId);
            // 这里如果有assign json节点的话，将其值由map转换为json字符串，便于前端调试页面显示
            for (List<Object> record : parsedValues) {
                for (int i = 0; i < schema.size(); i++) {
                    Object obj = schema.get(i);
                    if (obj instanceof Field) {
                        Field field = (Field) obj;
                        if (field.isJson()) {
                            int idx = schema.fieldIndex(field);
                            Object val = record.get(idx);
                            record.set(idx, JsonUtils.writeValueAsString(val));
                        }
                    }
                }
            }
        } catch (NullPointerException e) {
            // ignore
        }

        Map<String, Object> result = ctx.getVerifyResult();
        result.put(EtlConsts.RESULT, parsedValues);
        result.put(EtlConsts.SCHEMA, schema.getSimpleDesc());

        // 增加字段的显示设置
        List<String> displayConf = new ArrayList<>(schema.size());
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i) instanceof Field) {
                Field field = (Field) schema.get(i);
                if (field.isInternal()) {
                    String displayType =
                            field.getName().equals(EtlConsts.TIMESTAMP) ? EtlConsts.INTER_FIELDS : EtlConsts.HIDDEN;
                    displayConf.add(displayType);
                } else {
                    displayConf.add(EtlConsts.USER_FIELDS);
                }
            }
        }
        result.put(EtlConsts.DISPLAY, displayConf);

        return JsonUtils.writeValueAsString(result);
    }

}
