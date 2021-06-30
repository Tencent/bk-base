package com.tencent.bk.base.dataflow.bksql.rest.exception;

import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import com.tencent.bk.base.datalab.bksql.util.LocalizedMessage;
import java.util.Locale;

public class MessageLocalizedExceptionV1 extends RuntimeException {
    private final String messageKey;
    private final Object[] args;
    private final Class<?> sourceClass;
    private final String code;

    public MessageLocalizedExceptionV1(String messageKey, Object[] args, Class<?> sourceClass, String code) {
        this.messageKey = messageKey;
        this.args = args;
        this.sourceClass = sourceClass;
        this.code = code;
    }

    @Override
    public String getMessage() {
        return getMessage(LocaleHolder.instance().get());
    }

    public String getMessage(Locale locale) {
        return new LocalizedMessage(messageKey,
                args,
                sourceClass,
                locale).getMessage();
    }

    public String getCode() {
        return this.code;
    }
}
