package io.tapdata.pdk.apis.javascript.core.annotation;

import io.tapdata.pdk.apis.javascript.core.emun.SupportApi;

public @interface ApiType {
    SupportApi type() default SupportApi.POST_MAN;
}
