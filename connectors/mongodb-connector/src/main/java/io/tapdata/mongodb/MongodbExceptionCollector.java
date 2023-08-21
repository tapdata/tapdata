package io.tapdata.mongodb;

import com.mongodb.MongoException;
import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.exception.TapPdkRetryableEx;
import io.tapdata.kit.ErrorKit;

public class MongodbExceptionCollector extends AbstractExceptionCollector {

    protected String getPdkId() {
        return "mongodb";
    }

    @Override
    public void revealException(Throwable cause) {
        if (cause instanceof MongoException) {
            throw new TapPdkRetryableEx(getPdkId(), ErrorKit.getLastCause(cause))
//                    .withServerErrorCode(String.valueOf(((MongoException) cause).getCode()))
                    ;
        }
    }
}
