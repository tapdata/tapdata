package io.tapdata.quickapi.core.annotation;

import io.tapdata.quickapi.core.emun.SupportApi;

public @interface ApiType {
    SupportApi type() default SupportApi.POST_MAN;
}
