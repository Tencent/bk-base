package com.tencent.bk.base.dataflow.bksql.rest.exception;

import com.tencent.bk.base.datalab.bksql.rest.wrapper.ResponseEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractExceptionMapperV1<E extends Throwable> implements ExceptionMapper<E> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractExceptionMapperV1.class);
    
    @Override
    public Response toResponse(E t) {
        logger.error("Error: ", t);
        String errorCode = errorCode(t);
        return Response.ok()
                .entity(ResponseEntity.builder()
                        .result(false)
                        .code(errorCode)
                        .message(toMessage(t))
                        .data(structured(t))
                        .create())
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    protected abstract String toMessage(E t);

    protected abstract String errorCode(E t);

    protected Object structured(E t) {
        return null;
    }
}
