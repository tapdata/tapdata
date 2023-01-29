package io.tapdata.common.support.core.annotation;

import io.tapdata.common.support.core.emun.SupportApi;

public @interface ApiType {
    SupportApi type() default SupportApi.POST_MAN;
}
