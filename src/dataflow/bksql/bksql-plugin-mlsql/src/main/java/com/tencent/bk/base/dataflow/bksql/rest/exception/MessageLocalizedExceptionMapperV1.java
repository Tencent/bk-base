package com.tencent.bk.base.dataflow.bksql.rest.exception;

import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;

public class MessageLocalizedExceptionMapperV1 extends AbstractExceptionMapperV1<MessageLocalizedExceptionV1> {

    @Override
    protected String toMessage(MessageLocalizedExceptionV1 t) {
        return t.getMessage(LocaleHolder.instance().get());
    }

    @Override
    protected String errorCode(MessageLocalizedExceptionV1 t) {
        return t.getCode();
    }
}
