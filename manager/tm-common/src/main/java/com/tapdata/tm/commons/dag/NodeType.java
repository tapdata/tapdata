package com.tapdata.tm.commons.dag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/5 上午10:11
 * @description
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface NodeType {

    String value();

}
