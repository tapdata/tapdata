package io.tapdata.supervisor.convert.entity;

import java.util.Map;

public class WCodeCatch extends WCode {

    @Override
    public WCodeCatch parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        return this;
    }

    public WException getException() {
        return exception;
    }

    public void setException(WException exception) {
        this.exception = exception;
    }
}
