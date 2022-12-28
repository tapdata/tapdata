package com.tapdata.tm.init.patches;

import com.tapdata.tm.init.PatchTypeEnums;
import com.tapdata.tm.sdk.util.AppType;

import java.lang.annotation.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 16:59 Create
 */
@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface PatchAnnotation {
    String version();

    AppType[] appTypes() default AppType.DAAS;

    PatchTypeEnums patchType() default PatchTypeEnums.Script;
}
