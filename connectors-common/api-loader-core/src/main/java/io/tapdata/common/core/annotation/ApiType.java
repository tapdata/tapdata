package io.tapdata.common.core.annotation;

import io.tapdata.common.core.emun.SupportApi;

public @interface ApiType {
    SupportApi type() default SupportApi.POST_MAN;
}
