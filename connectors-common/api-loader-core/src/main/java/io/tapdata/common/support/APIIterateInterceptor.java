package io.tapdata.common.support;

import io.tapdata.common.support.comom.APIIterateError;

import java.util.Map;

public interface APIIterateInterceptor {
    public boolean iterate(Map<String,Object> data, Object offset, APIIterateError error);
}
